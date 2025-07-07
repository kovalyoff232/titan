//! The layout of a page on disk.
use crate::{transaction::Snapshot, PageId, PAGE_SIZE};

pub type TransactionId = u32;
pub type CommandId = u32;

pub const INVALID_PAGE_ID: PageId = 0;

/// The header of a page on disk.
/// This structure is laid out in memory exactly as it is on disk.
#[repr(C)]
pub struct PageHeaderData {
    /// Log Sequence Number of the latest log record modifying this page.
    pub lsn: u64,
    /// Checksum of the page content.
    pub checksum: u16,
    /// Flags for the page.
    pub flags: u16,
    /// Offset to the end of the free space.
    pub lower_offset: u16,
    /// Offset to the start of the free space.
    pub upper_offset: u16,
    /// Page ID of the next page in the table's page chain.
    pub next_page_id: PageId,
}

/// An item identifier (or tuple pointer) on a page.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ItemIdData {
    /// Offset to the start of the tuple.
    pub offset: u16,
    /// Length of the tuple.
    pub length: u16,
}

/// The header of a heap tuple.
/// This structure is laid out in memory exactly as it is on disk.
#[repr(C)]
pub struct HeapTupleHeaderData {
    /// ID of the transaction that created this tuple.
    pub xmin: TransactionId,
    /// ID of the transaction that deleted this tuple.
    pub xmax: TransactionId,
    /// Command ID of the creator/deleter.
    pub cmin_cmax: CommandId,
    /// Bitmask of flags.
    pub infomask: u16,
}

/// A page is a fixed-size block of data that is read from and written to disk.
/// The contents of the page are managed by the `Page` struct.
#[derive(Debug, Clone)]
#[repr(C)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    /// Initializes a new page with a valid header.
    pub fn new(id: PageId) -> Self {
        let mut page = Page {
            id,
            data: [0; PAGE_SIZE],
        };
        page.initialize();
        page
    }

    /// Initializes the page header.
    pub fn initialize(&mut self) {
        let header = self.header_mut();
        header.lsn = 0;
        header.checksum = 0;
        header.flags = 0;
        header.lower_offset = std::mem::size_of::<PageHeaderData>() as u16;
        header.upper_offset = PAGE_SIZE as u16;
        header.next_page_id = INVALID_PAGE_ID;
    }

    /// Returns a mutable reference to the page header.
    pub fn header_mut(&mut self) -> &mut PageHeaderData {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut PageHeaderData) }
    }

    /// Returns a reference to the page header.
    pub fn header(&self) -> &PageHeaderData {
        unsafe { &*(self.data.as_ptr() as *const PageHeaderData) }
    }

    /// Adds a new tuple to the page.
    /// Returns the item id of the new tuple, or None if there is not enough space.
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

        let header = self.header();
        println!(
            "[Page::add_tuple] PageId: {}, Needed space: {}, Free space: {}",
            self.id,
            needed_space,
            header.upper_offset.saturating_sub(header.lower_offset)
        );
        if header.upper_offset.saturating_sub(header.lower_offset) < needed_space as u16 {
            println!("[Page::add_tuple] Not enough space on page {}", self.id);
            return None;
        }

        let item_id_offset = header.lower_offset;
        let tuple_offset = header.upper_offset - tuple_len as u16;

        let item_id_index =
            (item_id_offset - std::mem::size_of::<PageHeaderData>() as u16) / item_id_len as u16;

        let item_id = self.item_id_mut(item_id_offset);
        item_id.offset = tuple_offset;
        item_id.length = tuple_len as u16;

        let tuple_header = self.tuple_header_mut(tuple_offset);
        tuple_header.xmin = xmin;
        tuple_header.xmax = xmax;
        tuple_header.cmin_cmax = 0; // Make sure all fields are initialized
        tuple_header.infomask = 0;

        let tuple_data_offset = tuple_offset + tuple_header_len as u16;
        let tuple_data = self.tuple_mut(tuple_data_offset, tuple.len());
        tuple_data.copy_from_slice(tuple);

        let header = self.header_mut();
        header.lower_offset += item_id_len as u16;
        header.upper_offset = tuple_offset;

        println!(
            "[Page::add_tuple] Added tuple to page {}, item_id_index: {}, xmin: {}, xmax: {}",
            self.id, item_id_index, xmin, xmax
        );

        Some(item_id_index)
    }

    /// Returns the tuple data for the given item id.
    pub fn get_tuple(&self, item_id: u16) -> Option<&[u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        let tuple_header_len = std::mem::size_of::<HeapTupleHeaderData>() as u16;
        let data_len = item_id_data.length.checked_sub(tuple_header_len)?;
        Some(self.tuple(item_id_data.offset + tuple_header_len, data_len as usize))
    }

    /// Returns the raw tuple data (header + data) for the given item id.
    pub fn get_raw_tuple(&self, item_id: u16) -> Option<&[u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.tuple(item_id_data.offset, item_id_data.length as usize))
    }

    /// Returns the mutable tuple data for the given item id.
    pub fn get_tuple_mut(&mut self, item_id: u16) -> Option<&mut [u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        let tuple_header_len = std::mem::size_of::<HeapTupleHeaderData>() as u16;
        let data_len = item_id_data.length.checked_sub(tuple_header_len)?;
        Some(self.tuple_mut(item_id_data.offset + tuple_header_len, data_len as usize))
    }

    /// Returns the mutable raw tuple data (header + data) for the given item id.
    pub fn get_raw_tuple_mut(&mut self, item_id: u16) -> Option<&mut [u8]> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.tuple_mut(item_id_data.offset, item_id_data.length as usize))
    }

    /// Returns a mutable reference to the tuple header for the given item id.
    pub fn get_tuple_header_mut(&mut self, item_id: u16) -> Option<&mut HeapTupleHeaderData> {
        let item_id_data = self.get_item_id_data(item_id)?;
        Some(self.tuple_header_mut(item_id_data.offset))
    }

    /// Helper to safely get ItemIdData
    pub fn get_item_id_data(&self, item_id: u16) -> Option<ItemIdData> {
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let item_id_offset = header_size + item_id * item_id_size;

        if item_id_offset + item_id_size > self.header().lower_offset {
            return None;
        }
        let item_id_data = *self.item_id(item_id_offset);

        if item_id_data.offset < self.header().lower_offset
            || item_id_data.offset + item_id_data.length > PAGE_SIZE as u16
        {
            return None;
        }
        if item_id_data.length == 0 {
            return None;
        }
        Some(item_id_data)
    }

    /// Returns true if the tuple is visible to the given snapshot.
    pub fn is_visible(
        &self,
        snapshot: &Snapshot,
        current_tx_id: TransactionId,
        item_id: u16,
    ) -> bool {
        let header = self.tuple_header(self.get_item_id_data(item_id).unwrap().offset);

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

    /// Returns the number of tuples on the page.
    pub fn get_tuple_count(&self) -> u16 {
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let lower = self.header().lower_offset;

        if lower < header_size {
            return 0;
        }
        (lower - header_size) / item_id_size
    }

    fn item_id(&self, offset: u16) -> &ItemIdData {
        unsafe { &*(self.data.as_ptr().offset(offset as isize) as *const ItemIdData) }
    }

    fn item_id_mut(&mut self, offset: u16) -> &mut ItemIdData {
        unsafe { &mut *(self.data.as_mut_ptr().offset(offset as isize) as *mut ItemIdData) }
    }

    pub fn tuple_header(&self, offset: u16) -> &HeapTupleHeaderData {
        unsafe { &*(self.data.as_ptr().offset(offset as isize) as *const HeapTupleHeaderData) }
    }

    fn tuple_header_mut(&mut self, offset: u16) -> &mut HeapTupleHeaderData {
        unsafe {
            &mut *(self.data.as_mut_ptr().offset(offset as isize) as *mut HeapTupleHeaderData)
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
