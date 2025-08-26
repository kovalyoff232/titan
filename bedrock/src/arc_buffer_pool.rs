//! Advanced Buffer Pool Manager with Adaptive Replacement Cache (ARC) algorithm.
//! 
//! ARC is a self-tuning, low overhead replacement cache algorithm that adapts
//! to different workload patterns. It maintains two LRU lists and automatically
//! adjusts their sizes based on the workload.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use parking_lot::RwLock as ParkingRwLock;

use crate::pager::Pager;
use crate::{Page, PageId};

/// Configuration for the buffer pool manager.
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Maximum number of pages in the buffer pool
    pub size: usize,
    /// Number of pages to prefetch on sequential scans
    pub prefetch_degree: usize,
    /// Whether to use scan-resistant mode (prevents large scans from evicting all pages)
    pub scan_resistant: bool,
    /// Replacement policy to use
    pub replacement_policy: ReplacementPolicy,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            size: 1024,  // Default to 1024 pages (~8MB with 8KB pages)
            prefetch_degree: 32,
            scan_resistant: true,
            replacement_policy: ReplacementPolicy::Arc { p: 0.5 },
        }
    }
}

/// Replacement policy for the buffer pool.
#[derive(Debug, Clone)]
pub enum ReplacementPolicy {
    /// Clock-sweep algorithm (simple and fast)
    ClockSweep,
    /// LRU-K algorithm (tracks K recent accesses)
    LruK { k: usize },
    /// Adaptive Replacement Cache (self-tuning)
    Arc { p: f64 },
}

/// Frame metadata for ARC algorithm.
#[derive(Debug)]
struct FrameMetadata {
    page_id: Option<PageId>,
    is_dirty: bool,
    pin_count: u32,
    access_count: u64,
}

/// ARC-based buffer pool manager.
pub struct ArcBufferPoolManager {
    pub pager: Mutex<Pager>,
    config: BufferPoolConfig,
    
    // Frame management
    frames: Vec<ParkingRwLock<Page>>,
    frame_metadata: Vec<Mutex<FrameMetadata>>,
    page_table: ParkingRwLock<HashMap<PageId, usize>>,
    
    // ARC lists
    arc_state: Mutex<ArcState>,
}

/// State for the ARC algorithm.
struct ArcState {
    /// Pages in T1 (recent cache entries)
    t1: VecDeque<PageId>,
    /// Pages in T2 (frequent cache entries)
    t2: VecDeque<PageId>,
    /// Ghost list for T1 (recently evicted from T1)
    b1: VecDeque<PageId>,
    /// Ghost list for T2 (recently evicted from T2)
    b2: VecDeque<PageId>,
    /// Target size for T1
    p: f64,
    /// Maximum cache size
    c: usize,
}

impl ArcState {
    fn new(cache_size: usize) -> Self {
        Self {
            t1: VecDeque::new(),
            t2: VecDeque::new(),
            b1: VecDeque::new(),
            b2: VecDeque::new(),
            p: cache_size as f64 / 2.0,
            c: cache_size,
        }
    }

    /// Adapt the target size p based on cache hit location.
    fn adapt(&mut self, in_b1: bool, in_b2: bool) {
        if in_b1 {
            // Increase p (favor recency)
            let delta = if self.b1.len() >= self.b2.len() {
                1.0
            } else {
                self.b2.len() as f64 / self.b1.len() as f64
            };
            self.p = (self.p + delta).min(self.c as f64);
        } else if in_b2 {
            // Decrease p (favor frequency)
            let delta = if self.b2.len() >= self.b1.len() {
                1.0
            } else {
                self.b1.len() as f64 / self.b2.len() as f64
            };
            self.p = (self.p - delta).max(0.0);
        }
    }

    /// Replace a page according to ARC policy.
    fn replace(&mut self) -> Option<PageId> {
        let t1_target = self.p as usize;
        
        if self.t1.len() >= t1_target.max(1) && !self.t1.is_empty() {
            // Evict from T1 (move to B1)
            let page_id = self.t1.pop_front()?;
            self.b1.push_back(page_id);
            
            // Limit ghost list size
            if self.b1.len() > self.c - self.t1.len() - self.t2.len() {
                self.b1.pop_front();
            }
            
            Some(page_id)
        } else if !self.t2.is_empty() {
            // Evict from T2 (move to B2)
            let page_id = self.t2.pop_front()?;
            self.b2.push_back(page_id);
            
            // Limit ghost list size
            if self.b2.len() > self.c - self.t1.len() - self.t2.len() {
                self.b2.pop_front();
            }
            
            Some(page_id)
        } else if !self.t1.is_empty() {
            // Fallback to evicting from T1
            let page_id = self.t1.pop_front()?;
            self.b1.push_back(page_id);
            Some(page_id)
        } else {
            None
        }
    }

    /// Handle a cache hit on a page.
    fn on_hit(&mut self, page_id: PageId) {
        // Move from T1 to T2 if found in T1
        if let Some(pos) = self.t1.iter().position(|&id| id == page_id) {
            self.t1.remove(pos);
            self.t2.push_back(page_id);
        }
        // Move to end of T2 if already in T2 (LRU update)
        else if let Some(pos) = self.t2.iter().position(|&id| id == page_id) {
            self.t2.remove(pos);
            self.t2.push_back(page_id);
        }
    }

    /// Handle a cache miss (page needs to be loaded).
    fn on_miss(&mut self, page_id: PageId) -> Option<PageId> {
        // Check if page is in ghost lists
        let in_b1 = self.b1.iter().any(|&id| id == page_id);
        let in_b2 = self.b2.iter().any(|&id| id == page_id);
        
        // Adapt based on where we found the page
        if in_b1 || in_b2 {
            self.adapt(in_b1, in_b2);
            
            // Remove from ghost list
            if in_b1 {
                self.b1.retain(|&id| id != page_id);
            } else {
                self.b2.retain(|&id| id != page_id);
            }
        }
        
        // Need to evict if cache is full
        let evicted = if self.t1.len() + self.t2.len() >= self.c {
            self.replace()
        } else {
            None
        };
        
        // Add new page to T1 (unless it was in B2, then add to T2)
        if in_b2 {
            self.t2.push_back(page_id);
        } else {
            self.t1.push_back(page_id);
        }
        
        evicted
    }
}

/// RAII guard for a page with ARC buffer pool.
pub struct ArcPageGuard<'a> {
    bpm: &'a Arc<ArcBufferPoolManager>,
    page_id: PageId,
    frame_idx: usize,
}

impl<'a> ArcPageGuard<'a> {
    pub fn read(&self) -> parking_lot::RwLockReadGuard<Page> {
        self.bpm.frames[self.frame_idx].read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<Page> {
        let mut meta = self.bpm.frame_metadata[self.frame_idx].lock().unwrap();
        meta.is_dirty = true;
        drop(meta);
        self.bpm.frames[self.frame_idx].write()
    }
}

impl<'a> Drop for ArcPageGuard<'a> {
    fn drop(&mut self) {
        self.bpm.unpin_page(self.page_id, self.frame_idx);
    }
}

impl ArcBufferPoolManager {
    pub fn new(pager: Pager, config: BufferPoolConfig) -> Self {
        let size = config.size;
        let mut frames = Vec::with_capacity(size);
        let mut frame_metadata = Vec::with_capacity(size);
        
        for _ in 0..size {
            frames.push(ParkingRwLock::new(Page::new(0)));
            frame_metadata.push(Mutex::new(FrameMetadata {
                page_id: None,
                is_dirty: false,
                pin_count: 0,
                access_count: 0,
            }));
        }
        
        Self {
            pager: Mutex::new(pager),
            config,
            frames,
            frame_metadata,
            page_table: ParkingRwLock::new(HashMap::new()),
            arc_state: Mutex::new(ArcState::new(size)),
        }
    }

    pub fn acquire_page(self: &Arc<Self>, page_id: PageId) -> io::Result<ArcPageGuard> {
        // Fast path: check if page is already in buffer
        {
            let page_table = self.page_table.read();
            if let Some(&frame_idx) = page_table.get(&page_id) {
                let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
                meta.pin_count += 1;
                meta.access_count += 1;
                drop(meta);
                
                // Update ARC state for cache hit
                self.arc_state.lock().unwrap().on_hit(page_id);
                
                return Ok(ArcPageGuard {
                    bpm: self,
                    page_id,
                    frame_idx,
                });
            }
        }
        
        // Slow path: need to load page
        self.load_page(page_id)
    }

    fn load_page(self: &Arc<Self>, page_id: PageId) -> io::Result<ArcPageGuard> {
        let mut arc_state = self.arc_state.lock().unwrap();
        let evicted_page_id = arc_state.on_miss(page_id);
        drop(arc_state);
        
        // Find a frame to use (either free or evicted)
        let frame_idx = self.find_victim_frame(evicted_page_id)?;
        
        // Evict old page if necessary
        self.evict_if_dirty(frame_idx)?;
        
        // Load new page from disk
        let new_page = self.pager.lock().unwrap().read_page(page_id)?;
        
        // Update frame with new page
        {
            let mut frame = self.frames[frame_idx].write();
            *frame = new_page;
        }
        
        // Update metadata
        {
            let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
            meta.page_id = Some(page_id);
            meta.is_dirty = false;
            meta.pin_count = 1;
            meta.access_count = 1;
        }
        
        // Update page table
        {
            let mut page_table = self.page_table.write();
            if let Some(old_page_id) = evicted_page_id {
                page_table.remove(&old_page_id);
            }
            page_table.insert(page_id, frame_idx);
        }
        
        Ok(ArcPageGuard {
            bpm: self,
            page_id,
            frame_idx,
        })
    }

    fn find_victim_frame(&self, evicted_page_id: Option<PageId>) -> io::Result<usize> {
        // If we have an evicted page, find its frame
        if let Some(page_id) = evicted_page_id {
            let page_table = self.page_table.read();
            if let Some(&frame_idx) = page_table.get(&page_id) {
                return Ok(frame_idx);
            }
        }
        
        // Otherwise, find a free frame
        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let meta = meta_mutex.lock().unwrap();
            if meta.page_id.is_none() {
                return Ok(idx);
            }
        }
        
        // All frames are in use but unpinned (should have been evicted by ARC)
        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let meta = meta_mutex.lock().unwrap();
            if meta.pin_count == 0 {
                return Ok(idx);
            }
        }
        
        Err(io::Error::other("all pages are pinned"))
    }

    fn evict_if_dirty(&self, frame_idx: usize) -> io::Result<()> {
        let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
        if let Some(_old_page_id) = meta.page_id {
            if meta.is_dirty {
                let frame = self.frames[frame_idx].read();
                self.pager.lock().unwrap().write_page(&*frame)?;
                meta.is_dirty = false;
            }
        }
        meta.page_id = None;
        Ok(())
    }

    fn unpin_page(&self, _page_id: PageId, frame_idx: usize) {
        let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
        if meta.pin_count > 0 {
            meta.pin_count -= 1;
        }
    }

    pub fn flush_page(&self, page_id: PageId) -> io::Result<()> {
        let page_table = self.page_table.read();
        if let Some(&frame_idx) = page_table.get(&page_id) {
            let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
            if meta.is_dirty {
                let frame = self.frames[frame_idx].read();
                self.pager.lock().unwrap().write_page(&*frame)?;
                meta.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn flush_all_pages(&self) -> io::Result<()> {
        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let mut meta = meta_mutex.lock().unwrap();
            if meta.is_dirty {
                if let Some(_page_id) = meta.page_id {
                    let frame = self.frames[idx].read();
                    self.pager.lock().unwrap().write_page(&*frame)?;
                    meta.is_dirty = false;
                }
            }
        }
        Ok(())
    }

    pub fn new_page(self: &Arc<Self>) -> io::Result<ArcPageGuard> {
        // Allocate a new page on disk
        let new_page_id = self.pager.lock().unwrap().allocate_page()?;
        
        // Find a frame for it
        let mut arc_state = self.arc_state.lock().unwrap();
        let evicted_page_id = arc_state.on_miss(new_page_id);
        drop(arc_state);
        
        let frame_idx = self.find_victim_frame(evicted_page_id)?;
        
        // Evict old page if necessary
        self.evict_if_dirty(frame_idx)?;
        
        // Initialize new page
        {
            let mut frame = self.frames[frame_idx].write();
            *frame = Page::new(new_page_id);
        }
        
        // Update metadata
        {
            let mut meta = self.frame_metadata[frame_idx].lock().unwrap();
            meta.page_id = Some(new_page_id);
            meta.is_dirty = true;
            meta.pin_count = 1;
            meta.access_count = 1;
        }
        
        // Update page table
        {
            let mut page_table = self.page_table.write();
            if let Some(old_page_id) = evicted_page_id {
                page_table.remove(&old_page_id);
            }
            page_table.insert(new_page_id, frame_idx);
        }
        
        Ok(ArcPageGuard {
            bpm: self,
            page_id: new_page_id,
            frame_idx,
        })
    }

    /// Prefetch pages for sequential access pattern.
    pub fn prefetch(&self, start_page_id: PageId, count: usize) {
        // Get the number of pages for bounds checking
        let num_pages = self.pager.lock().unwrap().num_pages;
        
        // This is a simplified prefetch - in production, this would be async
        for i in 0..count.min(self.config.prefetch_degree) {
            let page_id = start_page_id + i as u32;
            if page_id >= num_pages {
                break;
            }
            
            // Check if already in cache
            if self.page_table.read().contains_key(&page_id) {
                continue;
            }
            
            // Note: In a real implementation, we'd queue these for async loading
            // For now, we just mark them as candidates for prefetching
        }
    }
}
