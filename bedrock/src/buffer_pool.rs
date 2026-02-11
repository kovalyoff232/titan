use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::failpoint;
use crate::pager::Pager;
use crate::{Page, PageId};

const BUFFER_POOL_SIZE: usize = 100;

#[derive(Debug)]
struct Frame {
    page: RwLock<Page>,
    is_dirty: Mutex<bool>,
    pin_count: Mutex<u32>,
    recently_used: Mutex<bool>,
}

pub struct BufferPoolManager {
    pub pager: Mutex<Pager>,
    frames: Vec<Arc<Frame>>,
    page_table: RwLock<HashMap<PageId, usize>>,
    free_list: Mutex<Vec<usize>>,
    clock_hand: Mutex<usize>,
}

pub struct PageGuard<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    page_id: PageId,
    frame: Arc<Frame>,
}

fn lock_mutex_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_lock_recover<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock_recover<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl<'a> PageGuard<'a> {
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, Page> {
        read_lock_recover(&self.frame.page)
    }

    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, Page> {
        *lock_mutex_recover(&self.frame.is_dirty) = true;
        write_lock_recover(&self.frame.page)
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
        if let Some(&frame_index) = read_lock_recover(&self.page_table).get(&page_id) {
            let frame = self.frames[frame_index].clone();
            self.pin_frame(&frame);
            return Ok(PageGuard {
                bpm: self,
                page_id,
                frame,
            });
        }

        let frame_index = self
            .find_victim_frame()
            .ok_or_else(|| io::Error::other("all pages are pinned"))?;
        let frame = self.frames[frame_index].clone();

        self.evict_if_dirty(frame_index)?;

        let new_page = lock_mutex_recover(&self.pager).read_page(page_id)?;

        {
            let mut page = write_lock_recover(&frame.page);
            *page = new_page;
            *lock_mutex_recover(&frame.is_dirty) = false;
            self.pin_frame(&frame);
        }

        write_lock_recover(&self.page_table).insert(page_id, frame_index);
        Ok(PageGuard {
            bpm: self,
            page_id,
            frame,
        })
    }

    pub fn new_page(self: &Arc<Self>) -> io::Result<PageGuard<'_>> {
        let frame_index = self
            .find_victim_frame()
            .ok_or_else(|| io::Error::other("all pages are pinned"))?;
        let frame = self.frames[frame_index].clone();

        self.evict_if_dirty(frame_index)?;

        let new_page_id = lock_mutex_recover(&self.pager).allocate_page()?;

        {
            let mut page = write_lock_recover(&frame.page);
            *page = Page::new(new_page_id);
            *lock_mutex_recover(&frame.is_dirty) = true;
            self.pin_frame(&frame);
        }

        write_lock_recover(&self.page_table).insert(new_page_id, frame_index);
        Ok(PageGuard {
            bpm: self,
            page_id: new_page_id,
            frame,
        })
    }

    fn pin_frame(&self, frame: &Arc<Frame>) {
        let mut pin_count = lock_mutex_recover(&frame.pin_count);
        *pin_count += 1;
        *lock_mutex_recover(&frame.recently_used) = true;
    }

    fn evict_if_dirty(&self, frame_index: usize) -> io::Result<()> {
        let frame = &self.frames[frame_index];
        let mut page_table = write_lock_recover(&self.page_table);
        if let Some((&old_page_id, _)) = page_table.iter().find(|&(_, &idx)| idx == frame_index) {
            let mut is_dirty = lock_mutex_recover(&frame.is_dirty);
            if *is_dirty {
                let page_to_write = read_lock_recover(&frame.page).clone();
                drop(page_table);
                lock_mutex_recover(&self.pager).write_page(&page_to_write)?;
                *is_dirty = false;
                write_lock_recover(&self.page_table).remove(&old_page_id);
            } else {
                page_table.remove(&old_page_id);
            }
        }
        Ok(())
    }

    fn unpin_page(&self, page_id: PageId) {
        if let Some(&frame_index) = read_lock_recover(&self.page_table).get(&page_id) {
            let frame = &self.frames[frame_index];
            let mut pin_count = lock_mutex_recover(&frame.pin_count);
            if *pin_count > 0 {
                *pin_count -= 1;
            }
        }
    }

    pub fn flush_page(&self, page_id: PageId) -> io::Result<()> {
        if let Some(&frame_index) = read_lock_recover(&self.page_table).get(&page_id) {
            let frame = &self.frames[frame_index];
            let mut is_dirty = lock_mutex_recover(&frame.is_dirty);
            if *is_dirty {
                let page = read_lock_recover(&frame.page);
                lock_mutex_recover(&self.pager).write_page(&page)?;
                *is_dirty = false;
            }
        }
        Ok(())
    }

    pub fn flush_all_pages(&self) -> io::Result<()> {
        for (&page_id, _) in read_lock_recover(&self.page_table).iter() {
            failpoint::maybe_fail("bpm.flush.before_page")?;
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    pub fn delete_page(&self, page_id: PageId) -> io::Result<()> {
        if let Some(frame_index) = write_lock_recover(&self.page_table).remove(&page_id) {
            let frame = &self.frames[frame_index];
            *lock_mutex_recover(&frame.is_dirty) = false;
            *lock_mutex_recover(&frame.pin_count) = 0;
            *lock_mutex_recover(&frame.recently_used) = false;
            lock_mutex_recover(&self.free_list).push(frame_index);
        }
        lock_mutex_recover(&self.pager).deallocate_page(page_id);
        Ok(())
    }

    fn find_victim_frame(&self) -> Option<usize> {
        if let Some(frame_index) = lock_mutex_recover(&self.free_list).pop() {
            return Some(frame_index);
        }

        let frame_count = self.frames.len();
        if frame_count == 0 {
            return None;
        }

        let mut clock_hand = lock_mutex_recover(&self.clock_hand);

        for _ in 0..(frame_count * 2) {
            let frame_index = *clock_hand;
            *clock_hand = (*clock_hand + 1) % frame_count;

            let frame = &self.frames[frame_index];
            let pin_count = lock_mutex_recover(&frame.pin_count);

            if *pin_count == 0 {
                let mut recently_used = lock_mutex_recover(&frame.recently_used);
                if *recently_used {
                    *recently_used = false;
                } else {
                    return Some(frame_index);
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::BufferPoolManager;
    use crate::failpoint;
    use crate::pager::Pager;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn flush_all_pages_persists_dirty_page_without_failpoint() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("flush_ok.db");

        let pager = Pager::open(&db_path).unwrap();
        let bpm = Arc::new(BufferPoolManager::new(pager));
        let page_guard = bpm.new_page().unwrap();
        let page_id = page_guard.read().id;
        {
            let mut page = page_guard.write();
            page.data[256] = 0xAB;
        }
        drop(page_guard);

        failpoint::clear();
        bpm.flush_all_pages().unwrap();

        let mut pager_reopen = Pager::open(&db_path).unwrap();
        let page = pager_reopen.read_page(page_id).unwrap();
        assert_eq!(page.data[256], 0xAB);
    }

    #[test]
    fn flush_all_pages_failpoint_before_page_keeps_dirty_data_unflushed() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("flush_failpoint.db");

        let pager = Pager::open(&db_path).unwrap();
        let bpm = Arc::new(BufferPoolManager::new(pager));
        let page_guard = bpm.new_page().unwrap();
        let page_id = page_guard.read().id;
        {
            let mut page = page_guard.write();
            page.data[256] = 0xCD;
        }
        drop(page_guard);

        failpoint::clear();
        failpoint::enable("bpm.flush.before_page");
        let err = bpm.flush_all_pages().unwrap_err();
        failpoint::clear();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);

        let mut pager_reopen = Pager::open(&db_path).unwrap();
        let page = pager_reopen.read_page(page_id).unwrap();
        assert_ne!(page.data[256], 0xCD);
    }
}
