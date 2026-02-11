use parking_lot::RwLock as ParkingRwLock;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::pager::Pager;
use crate::{Page, PageId};

fn lock_mutex_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    pub size: usize,

    pub prefetch_degree: usize,

    pub scan_resistant: bool,

    pub replacement_policy: ReplacementPolicy,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            size: 1024,
            prefetch_degree: 32,
            scan_resistant: true,
            replacement_policy: ReplacementPolicy::Arc { p: 0.5 },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplacementPolicy {
    ClockSweep,

    LruK { k: usize },

    Arc { p: f64 },
}

#[derive(Debug)]
struct FrameMetadata {
    page_id: Option<PageId>,
    is_dirty: bool,
    pin_count: u32,
    access_count: u64,
}

pub struct ArcBufferPoolManager {
    pub pager: Mutex<Pager>,
    config: BufferPoolConfig,

    frames: Vec<ParkingRwLock<Page>>,
    frame_metadata: Vec<Mutex<FrameMetadata>>,
    page_table: ParkingRwLock<HashMap<PageId, usize>>,

    arc_state: Mutex<ArcState>,
}

struct ArcState {
    t1: VecDeque<PageId>,

    t2: VecDeque<PageId>,

    b1: VecDeque<PageId>,

    b2: VecDeque<PageId>,

    p: f64,

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

    fn adapt(&mut self, in_b1: bool, in_b2: bool) {
        if in_b1 {
            let delta = if self.b1.len() >= self.b2.len() {
                1.0
            } else {
                self.b2.len() as f64 / self.b1.len() as f64
            };
            self.p = (self.p + delta).min(self.c as f64);
        } else if in_b2 {
            let delta = if self.b2.len() >= self.b1.len() {
                1.0
            } else {
                self.b1.len() as f64 / self.b2.len() as f64
            };
            self.p = (self.p - delta).max(0.0);
        }
    }

    fn replace(&mut self) -> Option<PageId> {
        let t1_target = self.p as usize;

        if self.t1.len() >= t1_target.max(1) && !self.t1.is_empty() {
            let page_id = self.t1.pop_front()?;
            self.b1.push_back(page_id);

            if self.b1.len() > self.c - self.t1.len() - self.t2.len() {
                self.b1.pop_front();
            }

            Some(page_id)
        } else if !self.t2.is_empty() {
            let page_id = self.t2.pop_front()?;
            self.b2.push_back(page_id);

            if self.b2.len() > self.c - self.t1.len() - self.t2.len() {
                self.b2.pop_front();
            }

            Some(page_id)
        } else if !self.t1.is_empty() {
            let page_id = self.t1.pop_front()?;
            self.b1.push_back(page_id);
            Some(page_id)
        } else {
            None
        }
    }

    fn on_hit(&mut self, page_id: PageId) {
        if let Some(pos) = self.t1.iter().position(|&id| id == page_id) {
            self.t1.remove(pos);
            self.t2.push_back(page_id);
        } else if let Some(pos) = self.t2.iter().position(|&id| id == page_id) {
            self.t2.remove(pos);
            self.t2.push_back(page_id);
        }
    }

    fn on_miss(&mut self, page_id: PageId) -> Option<PageId> {
        let in_b1 = self.b1.iter().any(|&id| id == page_id);
        let in_b2 = self.b2.iter().any(|&id| id == page_id);

        if in_b1 || in_b2 {
            self.adapt(in_b1, in_b2);

            if in_b1 {
                self.b1.retain(|&id| id != page_id);
            } else {
                self.b2.retain(|&id| id != page_id);
            }
        }

        let evicted = if self.t1.len() + self.t2.len() >= self.c {
            self.replace()
        } else {
            None
        };

        if in_b2 {
            self.t2.push_back(page_id);
        } else {
            self.t1.push_back(page_id);
        }

        evicted
    }
}

pub struct ArcPageGuard<'a> {
    bpm: &'a Arc<ArcBufferPoolManager>,
    page_id: PageId,
    frame_idx: usize,
}

impl<'a> ArcPageGuard<'a> {
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Page> {
        self.bpm.frames[self.frame_idx].read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Page> {
        let mut meta = lock_mutex_recover(&self.bpm.frame_metadata[self.frame_idx]);
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

    pub fn acquire_page(self: &Arc<Self>, page_id: PageId) -> io::Result<ArcPageGuard<'_>> {
        {
            let page_table = self.page_table.read();
            if let Some(&frame_idx) = page_table.get(&page_id) {
                let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
                meta.pin_count += 1;
                meta.access_count += 1;
                drop(meta);

                lock_mutex_recover(&self.arc_state).on_hit(page_id);

                return Ok(ArcPageGuard {
                    bpm: self,
                    page_id,
                    frame_idx,
                });
            }
        }

        self.load_page(page_id)
    }

    fn load_page(self: &Arc<Self>, page_id: PageId) -> io::Result<ArcPageGuard<'_>> {
        let mut arc_state = lock_mutex_recover(&self.arc_state);
        let evicted_page_id = arc_state.on_miss(page_id);
        drop(arc_state);

        let frame_idx = self.find_victim_frame(evicted_page_id)?;

        self.evict_if_dirty(frame_idx)?;

        let new_page = lock_mutex_recover(&self.pager).read_page(page_id)?;

        {
            let mut frame = self.frames[frame_idx].write();
            *frame = new_page;
        }

        {
            let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
            meta.page_id = Some(page_id);
            meta.is_dirty = false;
            meta.pin_count = 1;
            meta.access_count = 1;
        }

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
        if let Some(page_id) = evicted_page_id {
            let page_table = self.page_table.read();
            if let Some(&frame_idx) = page_table.get(&page_id) {
                return Ok(frame_idx);
            }
        }

        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let meta = lock_mutex_recover(meta_mutex);
            if meta.page_id.is_none() {
                return Ok(idx);
            }
        }

        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let meta = lock_mutex_recover(meta_mutex);
            if meta.pin_count == 0 {
                return Ok(idx);
            }
        }

        Err(io::Error::other("all pages are pinned"))
    }

    fn evict_if_dirty(&self, frame_idx: usize) -> io::Result<()> {
        let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
        if let Some(_old_page_id) = meta.page_id {
            if meta.is_dirty {
                let frame = self.frames[frame_idx].read();
                lock_mutex_recover(&self.pager).write_page(&*frame)?;
                meta.is_dirty = false;
            }
        }
        meta.page_id = None;
        Ok(())
    }

    fn unpin_page(&self, _page_id: PageId, frame_idx: usize) {
        let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
        if meta.pin_count > 0 {
            meta.pin_count -= 1;
        }
    }

    pub fn flush_page(&self, page_id: PageId) -> io::Result<()> {
        let page_table = self.page_table.read();
        if let Some(&frame_idx) = page_table.get(&page_id) {
            let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
            if meta.is_dirty {
                let frame = self.frames[frame_idx].read();
                lock_mutex_recover(&self.pager).write_page(&*frame)?;
                meta.is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn flush_all_pages(&self) -> io::Result<()> {
        for (idx, meta_mutex) in self.frame_metadata.iter().enumerate() {
            let mut meta = lock_mutex_recover(meta_mutex);
            if meta.is_dirty {
                if let Some(_page_id) = meta.page_id {
                    let frame = self.frames[idx].read();
                    lock_mutex_recover(&self.pager).write_page(&*frame)?;
                    meta.is_dirty = false;
                }
            }
        }
        Ok(())
    }

    pub fn new_page(self: &Arc<Self>) -> io::Result<ArcPageGuard<'_>> {
        let new_page_id = lock_mutex_recover(&self.pager).allocate_page()?;

        let mut arc_state = lock_mutex_recover(&self.arc_state);
        let evicted_page_id = arc_state.on_miss(new_page_id);
        drop(arc_state);

        let frame_idx = self.find_victim_frame(evicted_page_id)?;

        self.evict_if_dirty(frame_idx)?;

        {
            let mut frame = self.frames[frame_idx].write();
            *frame = Page::new(new_page_id);
        }

        {
            let mut meta = lock_mutex_recover(&self.frame_metadata[frame_idx]);
            meta.page_id = Some(new_page_id);
            meta.is_dirty = true;
            meta.pin_count = 1;
            meta.access_count = 1;
        }

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

    pub fn prefetch(&self, start_page_id: PageId, count: usize) {
        let num_pages = lock_mutex_recover(&self.pager).num_pages;

        for i in 0..count.min(self.config.prefetch_degree) {
            let page_id = start_page_id + i as u32;
            if page_id >= num_pages {
                break;
            }

            if self.page_table.read().contains_key(&page_id) {
                continue;
            }
        }
    }
}
