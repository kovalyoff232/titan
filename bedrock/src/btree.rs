//! B-Tree index implementation.
use crate::buffer_pool::BufferPoolManager;
use crate::transaction::TransactionManager;
use crate::wal::{WalManager, WalRecord};
use crate::{Page, PageId, TupleId};
use std::io;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

pub type Key = i32;

const B_TREE_PAGE_HEADER_SIZE: usize = size_of::<BTreePageHeader>();
const LEAF_CELL_SIZE: usize = size_of::<LeafCell>();
const INTERNAL_CELL_SIZE: usize = size_of::<InternalCell>();
const LEAF_MAX_CELLS: usize = (crate::PAGE_SIZE - B_TREE_PAGE_HEADER_SIZE) / LEAF_CELL_SIZE;

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
enum BTreePageType {
    Leaf = 1,
    Internal = 2,
}

#[repr(C)]
struct BTreePageHeader {
    page_type: BTreePageType,
    num_cells: u16,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct LeafCell {
    key: Key,
    tuple_id: TupleId,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct InternalCell {
    key: Key,
    page_id: PageId,
}

impl Page {
    pub fn as_btree_leaf_page(&mut self) {
        let header = self.btree_header_mut();
        header.page_type = BTreePageType::Leaf;
        header.num_cells = 0;
    }

    pub fn as_btree_internal_page(&mut self) {
        let header = self.btree_header_mut();
        header.page_type = BTreePageType::Internal;
        header.num_cells = 0;
    }

    fn btree_header_mut(&mut self) -> &mut BTreePageHeader {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut BTreePageHeader) }
    }

    fn btree_header(&self) -> &BTreePageHeader {
        unsafe { &*(self.data.as_ptr() as *const BTreePageHeader) }
    }

    fn leaf_cell_mut(&mut self, index: usize) -> &mut LeafCell {
        let offset = B_TREE_PAGE_HEADER_SIZE + index * LEAF_CELL_SIZE;
        unsafe { &mut *(self.data.as_mut_ptr().add(offset) as *mut LeafCell) }
    }

    fn leaf_cell(&self, index: usize) -> &LeafCell {
        let offset = B_TREE_PAGE_HEADER_SIZE + index * LEAF_CELL_SIZE;
        unsafe { &*(self.data.as_ptr().add(offset) as *const LeafCell) }
    }

    fn internal_cell(&self, index: usize) -> &InternalCell {
        let offset = B_TREE_PAGE_HEADER_SIZE + size_of::<PageId>() + index * INTERNAL_CELL_SIZE;
        unsafe { &*(self.data.as_ptr().add(offset) as *const InternalCell) }
    }

    fn internal_cell_mut(&mut self, index: usize) -> &mut InternalCell {
        let offset = B_TREE_PAGE_HEADER_SIZE + size_of::<PageId>() + index * INTERNAL_CELL_SIZE;
        unsafe { &mut *(self.data.as_mut_ptr().add(offset) as *mut InternalCell) }
    }

    fn right_child_mut(&mut self) -> &mut PageId {
        let offset = B_TREE_PAGE_HEADER_SIZE;
        unsafe { &mut *(self.data.as_mut_ptr().add(offset) as *mut PageId) }
    }

    fn right_child(&self) -> &PageId {
        let offset = B_TREE_PAGE_HEADER_SIZE;
        unsafe { &*(self.data.as_ptr().add(offset) as *const PageId) }
    }
}

/// Searches for a key in the B-Tree.
pub fn btree_search(bpm: &Arc<BufferPoolManager>, root_page_id: PageId, key: Key) -> io::Result<Option<TupleId>> {
    let mut current_page_id = root_page_id;
    loop {
        let current_page_guard = bpm.acquire_page(current_page_id)?;
        let current_page = current_page_guard.read();
        match current_page.btree_header().page_type {
            BTreePageType::Leaf => {
                let num_cells = current_page.btree_header().num_cells as usize;
                let mut low = 0;
                let mut high = num_cells;
                while low < high {
                    let mid = low + (high - low) / 2;
                    let cell_key = current_page.leaf_cell(mid).key;
                    match cell_key.cmp(&key) {
                        std::cmp::Ordering::Less => low = mid + 1,
                        std::cmp::Ordering::Greater => high = mid,
                        std::cmp::Ordering::Equal => return Ok(Some(current_page.leaf_cell(mid).tuple_id)),
                    }
                }
                return Ok(None);
            }
            BTreePageType::Internal => {
                let num_cells = current_page.btree_header().num_cells as usize;
                let mut low = 0;
                let mut high = num_cells;
                let mut next_page_id = *current_page.right_child();
                while low < high {
                    let mid = low + (high - low) / 2;
                    let cell_key = current_page.internal_cell(mid).key;
                    if key < cell_key {
                        high = mid;
                    } else {
                        low = mid + 1;
                    }
                }
                if high > 0 {
                    next_page_id = current_page.internal_cell(high - 1).page_id;
                }
                current_page_id = next_page_id;
            }
        }
    }
}

pub fn btree_insert(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    mut root_page_id: PageId,
    key: Key,
    tuple_id: TupleId,
) -> io::Result<PageId> {
    let root_page_guard = bpm.acquire_page(root_page_id)?;
    if root_page_guard.read().btree_header().num_cells as usize == LEAF_MAX_CELLS {
        let new_page_guard = bpm.new_page()?;
        let new_page_id = new_page_guard.read().id;

        let median_key = {
            let mut root_page = root_page_guard.write();
            let mut new_page = new_page_guard.write();
            leaf_split_and_move(&mut root_page, &mut new_page)
        };

        let new_root_page_guard = bpm.new_page()?;
        let new_root_page_id = new_root_page_guard.read().id;
        {
            let mut new_root_page = new_root_page_guard.write();
            new_root_page.as_btree_internal_page();
            *new_root_page.right_child_mut() = new_page_id;
            *new_root_page.internal_cell_mut(0) = InternalCell {
                key: median_key,
                page_id: root_page_id,
            };
            new_root_page.btree_header_mut().num_cells = 1;
        }

        if key < median_key {
            let mut root_page = root_page_guard.write();
            leaf_insert(&mut root_page, key, tuple_id);
        } else {
            let mut new_page = new_page_guard.write();
            leaf_insert(&mut new_page, key, tuple_id);
        }

        // Log changes to WAL
        log_btree_page(tm, wm, tx_id, &root_page_guard.read())?;
        log_btree_page(tm, wm, tx_id, &new_page_guard.read())?;
        log_btree_page(tm, wm, tx_id, &new_root_page_guard.read())?;

        root_page_id = new_root_page_id;
    } else {
        let mut root_page = root_page_guard.write();
        leaf_insert(&mut root_page, key, tuple_id);
        log_btree_page(tm, wm, tx_id, &root_page)?;
    }
    Ok(root_page_id)
}

fn leaf_split_and_move(old_page: &mut Page, new_page: &mut Page) -> Key {
    new_page.as_btree_leaf_page();
    let mid_point = LEAF_MAX_CELLS / 2;
    let mut new_cell_idx = 0;

    for i in mid_point..LEAF_MAX_CELLS {
        *new_page.leaf_cell_mut(new_cell_idx) = *old_page.leaf_cell(i);
        new_cell_idx += 1;
    }

    old_page.btree_header_mut().num_cells = mid_point as u16;
    new_page.btree_header_mut().num_cells = (LEAF_MAX_CELLS - mid_point) as u16;

    new_page.leaf_cell(0).key
}

fn leaf_insert(page: &mut Page, key: Key, tuple_id: TupleId) {
    let num_cells = page.btree_header().num_cells as usize;
    
    let mut low = 0;
    let mut high = num_cells;
    while low < high {
        let mid = low + (high - low) / 2;
        if page.leaf_cell(mid).key < key {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let insert_idx = low;

    for i in (insert_idx..num_cells).rev() {
        let cell = *page.leaf_cell(i);
        *page.leaf_cell_mut(i + 1) = cell;
    }

    *page.leaf_cell_mut(insert_idx) = LeafCell { key, tuple_id };
    page.btree_header_mut().num_cells += 1;
}

fn log_btree_page(
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    page: &Page,
) -> io::Result<()> {
    let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
    let lsn = wm.lock().unwrap().log(
        tx_id,
        prev_lsn,
        &WalRecord::BTreePage {
            tx_id,
            page_id: page.id,
            data: page.data.to_vec(),
        },
    )?;
    tm.set_last_lsn(tx_id, lsn);
    Ok(())
}
