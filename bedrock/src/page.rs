use crate::{PAGE_SIZE, PageId, transaction::Snapshot};

pub type TransactionId = u32;

pub type CommandId = u32;

pub const INVALID_PAGE_ID: PageId = 0;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct PageHeaderData {
    pub lsn: u64,

    pub checksum: u16,

    pub flags: u16,

    pub lower_offset: u16,

    pub upper_offset: u16,

    pub next_page_id: PageId,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ItemIdData {
    pub offset: u16,

    pub length: u16,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct HeapTupleHeaderData {
    pub xmin: TransactionId,

    pub xmax: TransactionId,

    pub cmin_cmax: CommandId,

    pub infomask: u16,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct Page {
    pub id: PageId,

    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: PageId) -> Self {
        let mut page = Page {
            id,
            data: [0; PAGE_SIZE],
        };
        page.initialize();
        page
    }

    pub fn initialize(&mut self) {
        let mut header = self.read_header();
        header.lsn = 0;
        header.checksum = 0;
        header.flags = 0;
        header.lower_offset = std::mem::size_of::<PageHeaderData>() as u16;
        header.upper_offset = PAGE_SIZE as u16;
        header.next_page_id = INVALID_PAGE_ID;
        self.write_header(&header);
    }

    pub fn read_header(&self) -> PageHeaderData {
        unsafe { std::ptr::read_unaligned(self.data.as_ptr() as *const PageHeaderData) }
    }

    pub fn write_header(&mut self, header: &PageHeaderData) {
        unsafe {
            std::ptr::write_unaligned(self.data.as_mut_ptr() as *mut PageHeaderData, *header);
        }
    }

    pub fn add_tuple(
        &mut self,
        tuple: &[u8],
        xmin: TransactionId,
        xmax: TransactionId,
    ) -> Option<u16> {
        let tuple_header_len = std::mem::size_of::<HeapTupleHeaderData>();
        let tuple_len = tuple.len() + tuple_header_len;
        let item_id_len = std::mem::size_of::<ItemIdData>();
        let needed_space = tuple_len + item_id_len;

        let header = self.read_header();
        crate::bedrock_debug_log!(
            "[Page::add_tuple] PageId: {}, Needed space: {}, Free space: {}",
            self.id,
            needed_space,
            header.upper_offset.saturating_sub(header.lower_offset)
        );
        if header.upper_offset.saturating_sub(header.lower_offset) < needed_space as u16 {
            crate::bedrock_debug_log!("[Page::add_tuple] Not enough space on page {}", self.id);
            return None;
        }

        let item_id_offset = header.lower_offset;
        let tuple_offset = header.upper_offset - tuple_len as u16;

        let item_id_index =
            (item_id_offset - std::mem::size_of::<PageHeaderData>() as u16) / item_id_len as u16;

        let mut item_id = self.read_item_id(item_id_offset);
        item_id.offset = tuple_offset;
        item_id.length = tuple_len as u16;
        self.write_item_id(item_id_offset, &item_id);

        let mut tuple_header = self.read_tuple_header(tuple_offset);
        tuple_header.xmin = xmin;
        tuple_header.xmax = xmax;
        tuple_header.cmin_cmax = 0;
        tuple_header.infomask = 0;
        self.write_tuple_header(tuple_offset, &tuple_header);

        let tuple_data_offset = tuple_offset + tuple_header_len as u16;
        let tuple_data = self.tuple_mut(tuple_data_offset, tuple.len());
        tuple_data.copy_from_slice(tuple);

        let mut header = self.read_header();
        header.lower_offset += item_id_len as u16;
        header.upper_offset = tuple_offset;
        self.write_header(&header);

        crate::bedrock_debug_log!(
            "[Page::add_tuple] Added tuple to page {}, item_id_index: {}, xmin: {}, xmax: {}",
            self.id,
            item_id_index,
            xmin,
            xmax
        );

        Some(item_id_index)
    }

    pub fn get_tuple(&self, item_id: u16) -> Option<&[u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        let tuple_header_len = std::mem::size_of::<HeapTupleHeaderData>() as u16;
        let data_len = item_id_data.length.checked_sub(tuple_header_len)?;
        Some(self.tuple(item_id_data.offset + tuple_header_len, data_len as usize))
    }

    pub fn get_raw_tuple(&self, item_id: u16) -> Option<&[u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.tuple(item_id_data.offset, item_id_data.length as usize))
    }

    pub fn get_tuple_mut(&mut self, item_id: u16) -> Option<&mut [u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        let tuple_header_len = std::mem::size_of::<HeapTupleHeaderData>() as u16;
        let data_len = item_id_data.length.checked_sub(tuple_header_len)?;
        Some(self.tuple_mut(item_id_data.offset + tuple_header_len, data_len as usize))
    }

    pub fn get_raw_tuple_mut(&mut self, item_id: u16) -> Option<&mut [u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.tuple_mut(item_id_data.offset, item_id_data.length as usize))
    }

    pub fn get_tuple_header_mut(&mut self, item_id: u16) -> Option<HeapTupleHeaderData> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.read_tuple_header(item_id_data.offset))
    }

    pub fn get_item_id_data(&self, item_id: u16) -> Option<ItemIdData> {
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let item_id_offset = header_size + item_id * item_id_size;

        if item_id_offset + item_id_size > self.read_header().lower_offset {
            return None;
        }
        let item_id_data = self.read_item_id(item_id_offset);

        if item_id_data.offset < self.read_header().lower_offset
            || item_id_data.offset + item_id_data.length > PAGE_SIZE as u16
        {
            return None;
        }
        if item_id_data.length == 0 {
            return None;
        }
        Some(item_id_data)
    }

    pub fn is_visible(
        &self,
        snapshot: &Snapshot,
        current_tx_id: TransactionId,
        item_id: u16,
    ) -> bool {
        let Some(item_id_data) = self.get_item_id_data(item_id) else {
            return false;
        };
        let header = self.read_tuple_header(item_id_data.offset);

        if header.xmin == current_tx_id {
            return header.xmax == 0;
        }

        if snapshot.is_visible(header.xmin) {
            if header.xmax == 0 {
                return true;
            }
            if header.xmax == current_tx_id {
                return true;
            }
            return !snapshot.is_visible(header.xmax);
        }

        false
    }

    pub fn get_tuple_count(&self) -> u16 {
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let lower = self.read_header().lower_offset;

        if lower < header_size {
            return 0;
        }
        (lower - header_size) / item_id_size
    }

    fn read_item_id(&self, offset: u16) -> ItemIdData {
        unsafe {
            std::ptr::read_unaligned(self.data.as_ptr().offset(offset as isize) as *const ItemIdData)
        }
    }

    fn write_item_id(&mut self, offset: u16, item_id: &ItemIdData) {
        unsafe {
            std::ptr::write_unaligned(
                self.data.as_mut_ptr().offset(offset as isize) as *mut ItemIdData,
                *item_id,
            );
        }
    }

    pub fn read_tuple_header(&self, offset: u16) -> HeapTupleHeaderData {
        unsafe {
            std::ptr::read_unaligned(
                self.data.as_ptr().offset(offset as isize) as *const HeapTupleHeaderData
            )
        }
    }

    pub fn write_tuple_header(&mut self, offset: u16, header: &HeapTupleHeaderData) {
        unsafe {
            std::ptr::write_unaligned(
                self.data.as_mut_ptr().offset(offset as isize) as *mut HeapTupleHeaderData,
                *header,
            );
        }
    }

    pub fn tuple(&self, offset: u16, len: usize) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().offset(offset as isize), len) }
    }

    fn tuple_mut(&mut self, offset: u16, len: usize) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.data.as_mut_ptr().offset(offset as isize), len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn snapshot(xmin: TransactionId, xmax: TransactionId, active: &[TransactionId]) -> Snapshot {
        Snapshot {
            xmin,
            xmax,
            active_transactions: Arc::new(active.iter().copied().collect::<HashSet<_>>()),
        }
    }

    fn set_xmax(page: &mut Page, item_id: u16, xmax: TransactionId) {
        let item = page.get_item_id_data(item_id).expect("item exists");
        let mut header = page.read_tuple_header(item.offset);
        header.xmax = xmax;
        page.write_tuple_header(item.offset, &header);
    }

    #[test]
    fn own_insert_is_visible_and_own_delete_is_invisible() {
        let mut page = Page::new(0);
        let tx = 10;
        let item_id = page
            .add_tuple(b"hello", tx, 0)
            .expect("tuple should be added");
        let snap = snapshot(10, 11, &[tx]);

        assert!(page.is_visible(&snap, tx, item_id));
        set_xmax(&mut page, item_id, tx);
        assert!(!page.is_visible(&snap, tx, item_id));
    }

    #[test]
    fn concurrent_uncommitted_insert_is_not_visible() {
        let mut page = Page::new(0);
        let writer_tx = 20;
        let reader_tx = 30;
        let item_id = page
            .add_tuple(b"v", writer_tx, 0)
            .expect("tuple should be added");
        let snap = snapshot(20, 31, &[writer_tx, reader_tx]);

        assert!(!page.is_visible(&snap, reader_tx, item_id));
    }

    #[test]
    fn concurrent_committed_insert_is_visible() {
        let mut page = Page::new(0);
        let writer_tx = 20;
        let reader_tx = 30;
        let item_id = page
            .add_tuple(b"v", writer_tx, 0)
            .expect("tuple should be added");
        let snap = snapshot(30, 31, &[reader_tx]);

        assert!(page.is_visible(&snap, reader_tx, item_id));
    }

    #[test]
    fn committed_delete_is_not_visible_but_in_progress_delete_is_visible() {
        let mut page = Page::new(0);
        let creator_tx = 20;
        let deleter_tx = 25;
        let reader_tx = 30;
        let item_id = page
            .add_tuple(b"v", creator_tx, 0)
            .expect("tuple should be added");
        set_xmax(&mut page, item_id, deleter_tx);

        let committed_delete_snapshot = snapshot(30, 31, &[reader_tx]);
        assert!(!page.is_visible(&committed_delete_snapshot, reader_tx, item_id));

        let in_progress_delete_snapshot = snapshot(25, 31, &[deleter_tx, reader_tx]);
        assert!(page.is_visible(&in_progress_delete_snapshot, reader_tx, item_id));
    }
}
