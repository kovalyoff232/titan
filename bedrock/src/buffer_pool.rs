//! Manages the buffer pool, a collection of in-memory frames that cache disk pages.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock};

use crate::pager::Pager;
use crate::{Page, PageId};

const BUFFER_POOL_SIZE: usize = 100;

/// A single frame in the buffer pool.
#[derive(Debug)]
struct Frame {
    page: RwLock<Page>,
    is_dirty: Mutex<bool>,
    pin_count: Mutex<u32>,
    recently_used: Mutex<bool>,
}

/// The buffer pool manager.
pub struct BufferPoolManager {
    pub pager: Mutex<Pager>,
    frames: Vec<Arc<Frame>>,
    page_table: RwLock<HashMap<PageId, usize>>,
    free_list: Mutex<Vec<usize>>,
    clock_hand: Mutex<usize>,
}

/// An RAII guard for a page.
pub struct PageGuard<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    page_id: PageId,
    frame: Arc<Frame>,
}

impl<'a> PageGuard<'a> {
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, Page> {
        self.frame.page.read().unwrap()
    }

    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, Page> {
        *self.frame.is_dirty.lock().unwrap() = true;
        self.frame.page.write().unwrap()
    }
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        self.bpm.unpin_page(self.page_id);
    }
}

impl BufferPoolManager {
    pub fn new(pager: Pager) -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        let mut free_list = Vec::with_capacity(BUFFER_POOL_SIZE);
        for i in 0..BUFFER_POOL_SIZE {
            frames.push(Arc::new(Frame {
                page: RwLock::new(Page::new(0)),
                is_dirty: Mutex::new(false),
                pin_count: Mutex::new(0),
                recently_used: Mutex::new(false),
            }));
            free_list.push(i);
        }
        Self {
            pager: Mutex::new(pager),
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: Mutex::new(free_list),
            clock_hand: Mutex::new(0),
        }
    }

    pub fn acquire_page(self: &Arc<Self>, page_id: PageId) -> io::Result<PageGuard<'_>> {
        // 1. Check if page is already in buffer pool.
        if let Some(&frame_index) = self.page_table.read().unwrap().get(&page_id) {
            let frame = self.frames[frame_index].clone();
            self.pin_frame(&frame);
            return Ok(PageGuard {
                bpm: self,
                page_id,
                frame,
            });
        }

        // 2. If not, find a free frame or evict one.
        let frame_index = self
            .find_victim_frame()
            .ok_or_else(|| io::Error::other("all pages are pinned"))?;
        let frame = self.frames[frame_index].clone();

        // 3. Evict the old page if the frame is dirty.
        self.evict_if_dirty(frame_index)?;

        // 4. Read the new page from disk.
        let new_page = self.pager.lock().unwrap().read_page(page_id)?;

        // 5. Update frame content and metadata.
        {
            let mut page = frame.page.write().unwrap();
            *page = new_page;
            *frame.is_dirty.lock().unwrap() = false;
            self.pin_frame(&frame);
        }

        // 6. Update the page table.
        self.page_table
            .write()
            .unwrap()
            .insert(page_id, frame_index);
        Ok(PageGuard {
            bpm: self,
            page_id,
            frame,
        })
    }

    pub fn new_page(self: &Arc<Self>) -> io::Result<PageGuard<'_>> {
        // 1. Find a free frame or evict one.
        let frame_index = self
            .find_victim_frame()
            .ok_or_else(|| io::Error::other("all pages are pinned"))?;
        let frame = self.frames[frame_index].clone();

        // 2. Evict the old page if the frame is dirty.
        self.evict_if_dirty(frame_index)?;

        // 3. Allocate a new page on disk.
        let new_page_id = self.pager.lock().unwrap().allocate_page()?;

        // 4. Update frame content and metadata.
        {
            let mut page = frame.page.write().unwrap();
            *page = Page::new(new_page_id);
            *frame.is_dirty.lock().unwrap() = true;
            self.pin_frame(&frame);
        }

        // 5. Update the page table.
        self.page_table
            .write()
            .unwrap()
            .insert(new_page_id, frame_index);
        Ok(PageGuard {
            bpm: self,
            page_id: new_page_id,
            frame,
        })
    }

    fn pin_frame(&self, frame: &Arc<Frame>) {
        let mut pin_count = frame.pin_count.lock().unwrap();
        *pin_count += 1;
        *frame.recently_used.lock().unwrap() = true;
    }

    fn evict_if_dirty(&self, frame_index: usize) -> io::Result<()> {
        let frame = &self.frames[frame_index];
        let mut page_table = self.page_table.write().unwrap();
        if let Some((&old_page_id, _)) = page_table.iter().find(|&(_, &idx)| idx == frame_index) {
            let mut is_dirty = frame.is_dirty.lock().unwrap();
            if *is_dirty {
                let page_to_write = frame.page.read().unwrap().clone();
                drop(page_table); // Drop lock before I/O
                self.pager.lock().unwrap().write_page(&page_to_write)?;
                *is_dirty = false;
                self.page_table.write().unwrap().remove(&old_page_id);
            } else {
                page_table.remove(&old_page_id);
            }
        }
        Ok(())
    }

    fn unpin_page(&self, page_id: PageId) {
        if let Some(&frame_index) = self.page_table.read().unwrap().get(&page_id) {
            let frame = &self.frames[frame_index];
            let mut pin_count = frame.pin_count.lock().unwrap();
            if *pin_count > 0 {
                *pin_count -= 1;
            }
        }
    }

    pub fn flush_page(&self, page_id: PageId) -> io::Result<()> {
        if let Some(&frame_index) = self.page_table.read().unwrap().get(&page_id) {
            let frame = &self.frames[frame_index];
            let mut is_dirty = frame.is_dirty.lock().unwrap();
            if *is_dirty {
                let page = frame.page.read().unwrap();
                self.pager.lock().unwrap().write_page(&page)?;
                *is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn flush_all_pages(&self) -> io::Result<()> {
        for (&page_id, _) in self.page_table.read().unwrap().iter() {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> io::Result<()> {
        if let Some(frame_index) = self.page_table.write().unwrap().remove(&page_id) {
            let frame = &self.frames[frame_index];
            *frame.is_dirty.lock().unwrap() = false;
            *frame.pin_count.lock().unwrap() = 0;
            *frame.recently_used.lock().unwrap() = false;
            self.free_list.lock().unwrap().push(frame_index);
        }
        self.pager.lock().unwrap().deallocate_page(page_id);
        Ok(())
    }

    fn find_victim_frame(&self) -> Option<usize> {
        if let Some(frame_index) = self.free_list.lock().unwrap().pop() {
            return Some(frame_index);
        }

        let frame_count = self.frames.len();
        if frame_count == 0 {
            return None;
        }

        let mut clock_hand = self.clock_hand.lock().unwrap();
        // Two full passes: first pass can clear second-chance bits, second can pick a victim.
        for _ in 0..(frame_count * 2) {
            let frame_index = *clock_hand;
            *clock_hand = (*clock_hand + 1) % frame_count;

            let frame = &self.frames[frame_index];
            let pin_count = frame.pin_count.lock().unwrap();

            if *pin_count == 0 {
                let mut recently_used = frame.recently_used.lock().unwrap();
                if *recently_used {
                    *recently_used = false;
                } else {
                    return Some(frame_index);
                }
            }
        }

        // All frames are pinned.
        None
    }
}
