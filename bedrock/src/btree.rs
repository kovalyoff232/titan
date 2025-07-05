//! B-Tree index implementation.
use crate::{Page, PageId, TupleId};
use crate::buffer_pool::BufferPoolManager;
use crate::transaction::TransactionManager;
use crate::wal::{WalManager, WalRecord};
use std::io;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

pub type Key = i32;

const B_TREE_PAGE_HEADER_SIZE: usize = size_of::<BTreePageHeader>();
const LEAF_CELL_SIZE: usize = size_of::<LeafCell>();
const INTERNAL_CELL_SIZE: usize = size_of::<InternalCell>();
const LEAF_MAX_CELLS: usize = (crate::PAGE_SIZE - B_TREE_PAGE_HEADER_SIZE) / LEAF_CELL_SIZE;
const INTERNAL_MAX_CELLS: usize = (crate::PAGE_SIZE - B_TREE_PAGE_HEADER_SIZE - size_of::<PageId>()) / INTERNAL_CELL_SIZE;

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
                
                while low < high {
                    let mid = low + (high - low) / 2;
                    let cell_key = current_page.internal_cell(mid).key;
                    if key < cell_key {
                        high = mid;
                    } else {
                        low = mid + 1;
                    }
                }
                
                let next_page_id = if high > 0 {
                    current_page.internal_cell(high - 1).page_id
                } else {
                    *current_page.right_child()
                };
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
    let (median_key, new_page_id) =
        btree_insert_recursive(bpm, tm, wm, tx_id, root_page_id, key, tuple_id)?;

    if let Some(median_key) = median_key {
        let new_root_page_guard = bpm.new_page()?;
        let new_root_page_id = new_root_page_guard.read().id;
        {
            let mut new_root_page = new_root_page_guard.write();
            new_root_page.as_btree_internal_page();
            *new_root_page.right_child_mut() = new_page_id.unwrap();
            *new_root_page.internal_cell_mut(0) = InternalCell {
                key: median_key,
                page_id: root_page_id,
            };
            new_root_page.btree_header_mut().num_cells = 1;
        }
        log_btree_page(tm, wm, tx_id, &new_root_page_guard.read())?;
        root_page_id = new_root_page_id;
    }

    Ok(root_page_id)
}

fn btree_insert_recursive(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    current_page_id: PageId,
    key: Key,
    tuple_id: TupleId,
) -> io::Result<(Option<Key>, Option<PageId>)> {
    let page_guard = bpm.acquire_page(current_page_id)?;
    let page_type = page_guard.read().btree_header().page_type;

    match page_type {
        BTreePageType::Leaf => {
            let mut page = page_guard.write();
            leaf_insert(&mut page, key, tuple_id);

            if page.btree_header().num_cells as usize > LEAF_MAX_CELLS {
                let new_page_guard = bpm.new_page()?;
                let new_page_id = new_page_guard.read().id;
                let median_key = {
                    let mut new_page = new_page_guard.write();
                    leaf_split_and_move(&mut page, &mut new_page)
                };
                log_btree_page(tm, wm, tx_id, &page)?;
                log_btree_page(tm, wm, tx_id, &new_page_guard.read())?;
                Ok((Some(median_key), Some(new_page_id)))
            } else {
                log_btree_page(tm, wm, tx_id, &page)?;
                Ok((None, None))
            }
        }
        BTreePageType::Internal => {
            let (child_median_key, child_new_page_id) = {
                let page = page_guard.read();
                let num_cells = page.btree_header().num_cells as usize;
                let mut low = 0;
                let mut high = num_cells;
                while low < high {
                    let mid = low + (high - low) / 2;
                    if key < page.internal_cell(mid).key {
                        high = mid;
                    } else {
                        low = mid + 1;
                    }
                }
                let next_page_id = if high > 0 {
                    page.internal_cell(high - 1).page_id
                } else {
                    *page.right_child()
                };
                drop(page);
                btree_insert_recursive(bpm, tm, wm, tx_id, next_page_id, key, tuple_id)?
            };

            if let Some(median_key) = child_median_key {
                let mut page = page_guard.write();
                internal_insert(&mut page, median_key, child_new_page_id.unwrap());

                if page.btree_header().num_cells as usize > INTERNAL_MAX_CELLS {
                    let new_page_guard = bpm.new_page()?;
                    let new_page_id = new_page_guard.read().id;
                    let promoted_key = {
                        let mut new_page = new_page_guard.write();
                        internal_split_and_move(&mut page, &mut new_page)
                    };
                    log_btree_page(tm, wm, tx_id, &page)?;
                    log_btree_page(tm, wm, tx_id, &new_page_guard.read())?;
                    Ok((Some(promoted_key), Some(new_page_id)))
                } else {
                    log_btree_page(tm, wm, tx_id, &page)?;
                    Ok((None, None))
                }
            } else {
                Ok((None, None))
            }
        }
    }
}

fn internal_split_and_move(old_page: &mut Page, new_page: &mut Page) -> Key {
    new_page.as_btree_internal_page();
    let mid_point = INTERNAL_MAX_CELLS / 2;
    let promoted_key = old_page.internal_cell(mid_point).key;

    let mut new_cell_idx = 0;
    // Move cells after midpoint to the new page
    for i in (mid_point + 1)..=INTERNAL_MAX_CELLS {
        *new_page.internal_cell_mut(new_cell_idx) = *old_page.internal_cell(i);
        new_cell_idx += 1;
    }
    *new_page.right_child_mut() = *old_page.right_child();
    *old_page.right_child_mut() = old_page.internal_cell(mid_point).page_id;

    old_page.btree_header_mut().num_cells = mid_point as u16;
    new_page.btree_header_mut().num_cells = (INTERNAL_MAX_CELLS - mid_point) as u16;

    promoted_key
}

fn internal_insert(page: &mut Page, key: Key, new_page_id: PageId) {
    let num_cells = page.btree_header().num_cells as usize;
    let mut insert_idx = 0;
    while insert_idx < num_cells && page.internal_cell(insert_idx).key < key {
        insert_idx += 1;
    }

    for i in (insert_idx..num_cells).rev() {
        *page.internal_cell_mut(i + 1) = *page.internal_cell(i);
    }

    *page.internal_cell_mut(insert_idx) = InternalCell { key, page_id: new_page_id };
    page.btree_header_mut().num_cells += 1;
}

fn leaf_split_and_move(old_page: &mut Page, new_page: &mut Page) -> Key {
    new_page.as_btree_leaf_page();
    let mid_point = LEAF_MAX_CELLS / 2;
    let mut new_cell_idx = 0;

    for i in mid_point..=LEAF_MAX_CELLS {
        *new_page.leaf_cell_mut(new_cell_idx) = *old_page.leaf_cell(i);
        new_cell_idx += 1;
    }

    old_page.btree_header_mut().num_cells = mid_point as u16;
    new_page.btree_header_mut().num_cells = (LEAF_MAX_CELLS - mid_point + 1) as u16;

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

pub fn btree_delete(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    mut root_page_id: PageId,
    key: Key,
) -> io::Result<PageId> {
    btree_delete_recursive(bpm, tm, wm, tx_id, 0, root_page_id, key)?;

    // If the root node is an internal node with one child, the child becomes the new root.
    let root_page_guard = bpm.acquire_page(root_page_id)?;
    let root_page = root_page_guard.read();
    if root_page.btree_header().page_type == BTreePageType::Internal && root_page.btree_header().num_cells == 0 {
        let new_root_id = *root_page.right_child();
        // TODO: Deallocate the old root page
        root_page_id = new_root_id;
    }

    Ok(root_page_id)
}

fn btree_delete_recursive(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    parent_page_id: PageId,
    current_page_id: PageId,
    key: Key,
) -> io::Result<()> {
    let page_guard = bpm.acquire_page(current_page_id)?;
    let page_type = page_guard.read().btree_header().page_type;

    match page_type {
        BTreePageType::Leaf => {
            let mut page = page_guard.write();
            leaf_delete(&mut page, key);
            log_btree_page(tm, wm, tx_id, &page)?;
        }
        BTreePageType::Internal => {
            let mut page = page_guard.write();
            let num_cells = page.btree_header().num_cells as usize;
            let mut low = 0;
            let mut high = num_cells;
            while low < high {
                let mid = low + (high - low) / 2;
                if key < page.internal_cell(mid).key {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            let child_page_id = if high > 0 {
                page.internal_cell(high - 1).page_id
            } else {
                *page.right_child()
            };
            
            btree_delete_recursive(bpm, tm, wm, tx_id, current_page_id, child_page_id, key)?;

            // Re-fetch child page to check for underflow
            let child_page_guard = bpm.acquire_page(child_page_id)?;
            let child_page = child_page_guard.read();
            let min_cells = if child_page.btree_header().page_type == BTreePageType::Leaf {
                LEAF_MAX_CELLS / 2
            } else {
                INTERNAL_MAX_CELLS / 2
            };

            if (child_page.btree_header().num_cells as usize) < min_cells {
                handle_underflow(bpm, tm, wm, tx_id, &mut page, child_page_id, high)?;
            }
        }
    }
    Ok(())
}

fn handle_underflow(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    parent_page: &mut Page,
    child_page_id: PageId,
    child_idx_in_parent: usize,
) -> io::Result<()> {
    // Try to borrow from the left sibling first
    if child_idx_in_parent > 0 {
        let left_sibling_id = parent_page.internal_cell(child_idx_in_parent - 1).page_id;
        let left_sibling_guard = bpm.acquire_page(left_sibling_id)?;
        let mut left_sibling = left_sibling_guard.write();
        
        let min_cells = if left_sibling.btree_header().page_type == BTreePageType::Leaf { LEAF_MAX_CELLS / 2 } else { INTERNAL_MAX_CELLS / 2 };

        if (left_sibling.btree_header().num_cells as usize) > min_cells {
            let child_guard = bpm.acquire_page(child_page_id)?;
            let mut child_page = child_guard.write();
            
            let separator_key_idx = child_idx_in_parent - 1;
            let separator_key = parent_page.internal_cell(separator_key_idx).key;
            
            if child_page.btree_header().page_type == BTreePageType::Leaf {
                let borrowed_cell = *left_sibling.leaf_cell(left_sibling.btree_header().num_cells as usize - 1);
                left_sibling.btree_header_mut().num_cells -= 1;
                leaf_insert(&mut child_page, borrowed_cell.key, borrowed_cell.tuple_id);
                parent_page.internal_cell_mut(separator_key_idx).key = borrowed_cell.key;
            } else {
                let borrowed_cell = *left_sibling.internal_cell(left_sibling.btree_header().num_cells as usize - 1);
                let borrowed_right_child = *left_sibling.right_child();
                left_sibling.btree_header_mut().num_cells -= 1;
                *left_sibling.right_child_mut() = borrowed_cell.page_id;

                internal_insert(&mut child_page, separator_key, borrowed_right_child);
                parent_page.internal_cell_mut(separator_key_idx).key = borrowed_cell.key;
            }
            
            log_btree_page(tm, wm, tx_id, &*left_sibling)?;
            log_btree_page(tm, wm, tx_id, &*child_page)?;
            log_btree_page(tm, wm, tx_id, parent_page)?;
            return Ok(());
        }
    }

    // Try to borrow from the right sibling
    if child_idx_in_parent < parent_page.btree_header().num_cells as usize {
        let right_sibling_id = parent_page.internal_cell(child_idx_in_parent).page_id;
        let right_sibling_guard = bpm.acquire_page(right_sibling_id)?;
        let mut right_sibling = right_sibling_guard.write();

        let min_cells = if right_sibling.btree_header().page_type == BTreePageType::Leaf { LEAF_MAX_CELLS / 2 } else { INTERNAL_MAX_CELLS / 2 };

        if (right_sibling.btree_header().num_cells as usize) > min_cells {
            let child_guard = bpm.acquire_page(child_page_id)?;
            let mut child_page = child_guard.write();
            
            let separator_key_idx = child_idx_in_parent;
            let separator_key = parent_page.internal_cell(separator_key_idx).key;

            if child_page.btree_header().page_type == BTreePageType::Leaf {
                let borrowed_cell = *right_sibling.leaf_cell(0);
                leaf_delete(&mut right_sibling, borrowed_cell.key); // Simple delete, as it's the first element
                leaf_insert(&mut child_page, borrowed_cell.key, borrowed_cell.tuple_id);
                parent_page.internal_cell_mut(separator_key_idx).key = right_sibling.leaf_cell(0).key;
            } else {
                let borrowed_cell = *right_sibling.internal_cell(0);
                let borrowed_right_child = *right_sibling.right_child();
                
                // Shift cells left in right_sibling
                for i in 0..right_sibling.btree_header().num_cells as usize - 1 {
                    *right_sibling.internal_cell_mut(i) = *right_sibling.internal_cell(i + 1);
                }
                right_sibling.btree_header_mut().num_cells -= 1;
                *right_sibling.right_child_mut() = borrowed_cell.page_id;

                internal_insert(&mut child_page, separator_key, borrowed_right_child);
                parent_page.internal_cell_mut(separator_key_idx).key = borrowed_cell.key;
            }

            log_btree_page(tm, wm, tx_id, &*right_sibling)?;
            log_btree_page(tm, wm, tx_id, &*child_page)?;
            log_btree_page(tm, wm, tx_id, parent_page)?;
            return Ok(());
        }
    }

    // If redistribution is not possible, merge pages
    if child_idx_in_parent > 0 {
        // Merge with left sibling
        let left_sibling_id = parent_page.internal_cell(child_idx_in_parent - 1).page_id;
        merge_pages(bpm, tm, wm, tx_id, parent_page, left_sibling_id, child_page_id, child_idx_in_parent - 1)?;
    } else {
        // Merge with right sibling
        let right_sibling_id = parent_page.internal_cell(child_idx_in_parent).page_id;
        merge_pages(bpm, tm, wm, tx_id, parent_page, child_page_id, right_sibling_id, child_idx_in_parent)?;
    }

    Ok(())
}

fn merge_pages(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    parent_page: &mut Page,
    left_page_id: PageId,
    right_page_id: PageId,
    separator_idx: usize,
) -> io::Result<()> {
    let left_guard = bpm.acquire_page(left_page_id)?;
    let mut left_page = left_guard.write();
    let right_guard = bpm.acquire_page(right_page_id)?;
    let right_page = right_guard.read();

    let separator_key = parent_page.internal_cell(separator_idx).key;

    if left_page.btree_header().page_type == BTreePageType::Leaf {
        // Merge leaf pages
        let start_idx = left_page.btree_header().num_cells as usize;
        for i in 0..right_page.btree_header().num_cells as usize {
            *left_page.leaf_cell_mut(start_idx + i) = *right_page.leaf_cell(i);
        }
        left_page.btree_header_mut().num_cells += right_page.btree_header().num_cells;
    } else {
        // Merge internal pages
        let start_idx = left_page.btree_header().num_cells as usize;
        *left_page.internal_cell_mut(start_idx) = InternalCell { key: separator_key, page_id: *right_page.right_child() };
        for i in 0..right_page.btree_header().num_cells as usize {
            *left_page.internal_cell_mut(start_idx + 1 + i) = *right_page.internal_cell(i);
        }
        left_page.btree_header_mut().num_cells += right_page.btree_header().num_cells + 1;
    }

    // Remove separator from parent and shift remaining keys
    for i in separator_idx..parent_page.btree_header().num_cells as usize - 1 {
        *parent_page.internal_cell_mut(i) = *parent_page.internal_cell(i + 1);
    }
    parent_page.btree_header_mut().num_cells -= 1;

    log_btree_page(tm, wm, tx_id, &*left_page)?;
    log_btree_page(tm, wm, tx_id, parent_page)?;
    // TODO: Deallocate the right page
    Ok(())
}

fn leaf_delete(page: &mut Page, key: Key) {
    let num_cells = page.btree_header().num_cells as usize;
    let mut found_idx = None;

    for i in 0..num_cells {
        if page.leaf_cell(i).key == key {
            found_idx = Some(i);
            break;
        }
    }

    if let Some(idx) = found_idx {
        for i in idx..num_cells - 1 {
            let cell = *page.leaf_cell(i + 1);
            *page.leaf_cell_mut(i) = cell;
        }
        page.btree_header_mut().num_cells -= 1;
    }
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
