//! Manages the buffer pool, a collection of in-memory frames that cache disk pages.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock};

use crate::pager::Pager;
use crate::{Page, PageId};

const BUFFER_POOL_SIZE: usize = 100; // A more realistic size

/// A single frame in the buffer pool.
/// The page itself is wrapped in a RwLock to allow concurrent reads.
#[derive(Debug)]
struct Frame {
    page: RwLock<Page>,
    is_dirty: Mutex<bool>,
    pin_count: Mutex<u32>,
    recently_used: Mutex<bool>,
}

/// The buffer pool manager. It's the single entry point for accessing pages.
/// It is designed to be thread-safe.
pub struct BufferPoolManager {
    pager: Mutex<Pager>,
    frames: Vec<Arc<Frame>>,
    page_table: RwLock<HashMap<PageId, usize>>,
    free_list: Mutex<Vec<usize>>,
    clock_hand: Mutex<usize>,
}

/// An RAII guard for a page. When this guard is dropped, the page is automatically
/// unpinned from the buffer pool. It provides methods to read and write page data.
pub struct PageGuard {
    bpm: Arc<BufferPoolManager>,
    page_id: PageId,
    frame: Arc<Frame>,
}

impl PageGuard {
    /// Provides read-only access to the page data.
    pub fn read(&self) -> std::sync::RwLockReadGuard<Page> {
        self.frame.page.read().unwrap()
    }

    /// Provides write access to the page data.
    /// Automatically marks the page as dirty.
    pub fn write(&self) -> std::sync::RwLockWriteGuard<Page> {
        *self.frame.is_dirty.lock().unwrap() = true;
        self.frame.page.write().unwrap()
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        self.bpm.unpin_page(self.page_id);
    }
}

impl BufferPoolManager {
    /// Creates a new BufferPoolManager.
    pub fn new(pager: Pager) -> Self {
        let mut frames = Vec::with_capacity(BUFFER_POOL_SIZE);
        let mut free_list = Vec::with_capacity(BUFFER_POOL_SIZE);

        for i in 0..BUFFER_POOL_SIZE {
            frames.push(Arc::new(Frame {
                page: RwLock::new(Page::new(0)), // Dummy page
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

    /// Acquires a page from the buffer pool, fetching it from disk if necessary.
    pub fn acquire_page(self: &Arc<Self>, page_id: PageId) -> io::Result<PageGuard> {
        // 1. Check if page is already in buffer pool
        if let Some(&frame_index) = self.page_table.read().unwrap().get(&page_id) {
            let frame = self.frames[frame_index].clone();
            {
                let mut pin_count = frame.pin_count.lock().unwrap();
                *pin_count += 1;
                *frame.recently_used.lock().unwrap() = true;
            } // pin_count guard is dropped here
            return Ok(PageGuard { bpm: self.clone(), page_id, frame });
        }

        // 2. Page not in pool, find a victim frame
        let frame_index = self.find_victim_frame()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "all pages are pinned"))?;
        
        let frame = self.frames[frame_index].clone();

        // 3. Evict old page if necessary
        {
            // Lock the page table for the whole eviction process to ensure atomicity.
            let mut page_table = self.page_table.write().unwrap();
            let victim_entry = page_table
                .iter()
                .find(|(_, &idx)| idx == frame_index)
                .map(|(pid, &idx)| (*pid, idx));

            if let Some((old_page_id, _)) = victim_entry {
                let frame = &self.frames[frame_index];
                let is_dirty = frame.is_dirty.lock().unwrap();

                if *is_dirty {
                    // To avoid holding the page_table lock during slow I/O, we read the page
                    // data, drop the locks, do the I/O, and then re-acquire locks to finish.
                    let page_to_write = (*frame.page.read().unwrap()).clone();

                    // Drop all locks before I/O
                    drop(is_dirty);
                    drop(page_table);

                    self.pager.lock().unwrap().write_page(&page_to_write)?;

                    // Re-acquire locks to update metadata
                    let mut is_dirty = frame.is_dirty.lock().unwrap();
                    *is_dirty = false;
                    self.page_table.write().unwrap().remove(&old_page_id);
                } else {
                    // If the page is not dirty, we just need to remove it from the page table.
                    page_table.remove(&old_page_id);
                }
            }
        }

        // 4. Read new page from disk
        let new_page = self.pager.lock().unwrap().read_page(page_id)?;
        
        // 5. Update frame content and metadata
        {
            let mut page = frame.page.write().unwrap();
            *page = new_page;
            *frame.is_dirty.lock().unwrap() = false;
            let mut pin_count = frame.pin_count.lock().unwrap();
            *pin_count = 1;
            *frame.recently_used.lock().unwrap() = true;
        }

        // 6. Update page table and return guard
        self.page_table.write().unwrap().insert(page_id, frame_index);
        
        Ok(PageGuard { bpm: self.clone(), page_id, frame })
    }

    /// Creates a new page in the buffer pool.
    pub fn new_page(self: &Arc<Self>) -> io::Result<PageGuard> {
        println!("[BPM] new_page called");
        // 1. Find a victim frame
        let frame_index = self.find_victim_frame()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "all pages are pinned"))?;
        println!("[BPM] Found victim frame: {}", frame_index);
        
        let frame = self.frames[frame_index].clone();

        // 2. Evict old page if necessary
        {
            // Lock the page table for the whole eviction process to ensure atomicity.
            let mut page_table = self.page_table.write().unwrap();
            let victim_entry = page_table
                .iter()
                .find(|(_, &idx)| idx == frame_index)
                .map(|(pid, &idx)| (*pid, idx));

            if let Some((old_page_id, _)) = victim_entry {
                println!("[BPM] Evicting page {}", old_page_id);
                let frame = &self.frames[frame_index];
                let is_dirty = frame.is_dirty.lock().unwrap();

                if *is_dirty {
                    let page_to_write = (*frame.page.read().unwrap()).clone();
                    drop(is_dirty);
                    drop(page_table);

                    self.pager.lock().unwrap().write_page(&page_to_write)?;

                    let mut is_dirty = frame.is_dirty.lock().unwrap();
                    *is_dirty = false;
                    self.page_table.write().unwrap().remove(&old_page_id);
                } else {
                    page_table.remove(&old_page_id);
                }
            }
        }

        // 3. Allocate a new page on disk
        let new_page_id = self.pager.lock().unwrap().allocate_page()?;
        println!("[BPM] Allocated new page_id from pager: {}", new_page_id);
        
        // 4. Update frame content and metadata
        {
            let mut page = frame.page.write().unwrap();
            *page = Page::new(new_page_id);
            *frame.is_dirty.lock().unwrap() = true; // New page is always dirty
            let mut pin_count = frame.pin_count.lock().unwrap();
            *pin_count = 1;
            *frame.recently_used.lock().unwrap() = true;
        }

        // 5. Update page table and return guard
        self.page_table.write().unwrap().insert(new_page_id, frame_index);
        println!("[BPM] Inserted page {} into page_table at frame {}", new_page_id, frame_index);

        Ok(PageGuard { bpm: self.clone(), page_id: new_page_id, frame })
    }

    /// Unpins a page, making it eligible for eviction.
    fn unpin_page(&self, page_id: PageId) {
        if let Some(&frame_index) = self.page_table.read().unwrap().get(&page_id) {
            let frame = &self.frames[frame_index];
            let mut pin_count = frame.pin_count.lock().unwrap();
            if *pin_count > 0 {
                *pin_count -= 1;
            }
        }
    }

    /// Flushes a specific page to disk if it's dirty.
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

    /// Flushes all dirty pages to disk.
    pub fn flush_all_pages(&self) -> io::Result<()> {
        for (&page_id, _) in self.page_table.read().unwrap().iter() {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    /// Finds a frame to evict using the Clock-Sweep algorithm.
    fn find_victim_frame(&self) -> Option<usize> {
        // 1. Try to get a frame from the free list first
        if let Some(frame_index) = self.free_list.lock().unwrap().pop() {
            return Some(frame_index);
        }

        // 2. If free list is empty, run the clock algorithm
        let mut clock_hand = self.clock_hand.lock().unwrap();
        let mut attempts = 0;
        loop {
            if attempts >= self.frames.len() * 2 {
                return None; // All frames are pinned
            }
            let frame_index = *clock_hand;
            *clock_hand = (*clock_hand + 1) % self.frames.len();
            
            let frame = &self.frames[frame_index];
            let pin_count = frame.pin_count.lock().unwrap();

            if *pin_count == 0 {
                let mut recently_used = frame.recently_used.lock().unwrap();
                if *recently_used {
                    *recently_used = false;
                } else {
                    // Found a victim
                    return Some(frame_index);
                }
            }
            attempts += 1;
        }
    }
}