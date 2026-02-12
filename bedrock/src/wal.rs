use crate::{PageId, failpoint, pager::Pager};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

pub type Lsn = u64;
const WAL_HEADER_LEN: usize = std::mem::size_of::<u32>()
    + std::mem::size_of::<u32>()
    + std::mem::size_of::<u64>()
    + std::mem::size_of::<u32>();

fn lock_mutex_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalRecordHeader {
    pub total_len: u32,

    pub tx_id: u32,

    pub prev_lsn: Lsn,

    pub crc: u32,
}

impl WalRecordHeader {
    fn to_bytes(self) -> [u8; WAL_HEADER_LEN] {
        let mut bytes = [0u8; WAL_HEADER_LEN];
        bytes[0..4].copy_from_slice(&self.total_len.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.tx_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.prev_lsn.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.crc.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8; WAL_HEADER_LEN]) -> Self {
        let mut total_len_buf = [0u8; 4];
        total_len_buf.copy_from_slice(&bytes[0..4]);
        let mut tx_id_buf = [0u8; 4];
        tx_id_buf.copy_from_slice(&bytes[4..8]);
        let mut prev_lsn_buf = [0u8; 8];
        prev_lsn_buf.copy_from_slice(&bytes[8..16]);
        let mut crc_buf = [0u8; 4];
        crc_buf.copy_from_slice(&bytes[16..20]);

        let total_len = u32::from_le_bytes(total_len_buf);
        let tx_id = u32::from_le_bytes(tx_id_buf);
        let prev_lsn = u64::from_le_bytes(prev_lsn_buf);
        let crc = u32::from_le_bytes(crc_buf);
        Self {
            total_len,
            tx_id,
            prev_lsn,
            crc,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum WalRecord {
    Commit {
        tx_id: u32,
    },

    Abort {
        tx_id: u32,
    },

    InsertTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
        before_page: Vec<u8>,
        after_page: Vec<u8>,
    },

    DeleteTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
        before_page: Vec<u8>,
        after_page: Vec<u8>,
    },

    UpdateTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
        before_page: Vec<u8>,
        after_page: Vec<u8>,
    },

    BTreePage {
        tx_id: u32,
        page_id: PageId,
        data: Vec<u8>,
    },

    Checkpoint {
        active_tx_table: HashMap<u32, Lsn>,
        dirty_page_table: HashMap<PageId, Lsn>,
    },

    SetNextPageId {
        tx_id: u32,
        page_id: PageId,
        next_page_id: PageId,
    },

    CompensationLogRecord {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,

        undo_next_lsn: Lsn,
    },
}

impl WalRecord {
    pub fn tx_id(&self) -> u32 {
        match self {
            WalRecord::Commit { tx_id } => *tx_id,
            WalRecord::Abort { tx_id } => *tx_id,
            WalRecord::InsertTuple { tx_id, .. } => *tx_id,
            WalRecord::DeleteTuple { tx_id, .. } => *tx_id,
            WalRecord::UpdateTuple { tx_id, .. } => *tx_id,
            WalRecord::BTreePage { tx_id, .. } => *tx_id,
            WalRecord::SetNextPageId { tx_id, .. } => *tx_id,
            WalRecord::CompensationLogRecord { tx_id, .. } => *tx_id,
            WalRecord::Checkpoint { .. } => 0,
        }
    }
}

pub struct WalManager {
    file: Arc<Mutex<File>>,
    path: PathBuf,
    next_lsn: AtomicU64,
    last_checkpoint_lsn: AtomicU64,
    sync_thread_handle: Option<thread::JoinHandle<()>>,
    stop_sync_thread: Arc<AtomicBool>,
}

impl WalManager {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path_buf)?;

        let file_len = file.metadata()?.len();
        let next_lsn = AtomicU64::new(file_len);
        let last_checkpoint_lsn = AtomicU64::new(0);
        let file_arc = Arc::new(Mutex::new(file));
        let stop_sync_thread = Arc::new(AtomicBool::new(false));

        let file_clone = file_arc.clone();
        let stop_clone = stop_sync_thread.clone();
        let handle = thread::spawn(move || {
            while !stop_clone.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(10));
                let file = lock_mutex_recover(&file_clone);
                let _ = file.sync_all();
            }
        });

        Ok(Self {
            file: file_arc,
            path: path_buf,
            next_lsn,
            last_checkpoint_lsn,
            sync_thread_handle: Some(handle),
            stop_sync_thread,
        })
    }

    pub fn log(&mut self, tx_id: u32, prev_lsn: Lsn, record: &WalRecord) -> io::Result<Lsn> {
        let record_bytes = bincode::serialize(record).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL record serialize failed: {e}"),
            )
        })?;

        let header_len = WAL_HEADER_LEN as u32;
        let total_len = header_len + record_bytes.len() as u32;

        let lsn = self.next_lsn.fetch_add(total_len as u64, Ordering::SeqCst);

        let mut hasher = Hasher::new();
        hasher.update(&record_bytes);
        let crc = hasher.finalize();

        let header = WalRecordHeader {
            total_len,
            tx_id,
            prev_lsn,
            crc,
        };

        let mut file = lock_mutex_recover(&self.file);
        file.seek(SeekFrom::Start(lsn))?;
        file.write_all(&header.to_bytes())?;
        file.write_all(&record_bytes)?;

        Ok(lsn)
    }

    pub fn deserialize_record(buf: &[u8]) -> Option<WalRecord> {
        bincode::deserialize(buf).ok()
    }

    pub fn read_record(&mut self, lsn: Lsn) -> io::Result<(Option<WalRecord>, Lsn)> {
        let mut file = lock_mutex_recover(&self.file);
        let file_len = file.metadata()?.len();
        if lsn >= file_len {
            return Ok((None, 0));
        }

        file.seek(SeekFrom::Start(lsn))?;
        let mut header_buf = [0u8; WAL_HEADER_LEN];
        if file.read_exact(&mut header_buf).is_err() {
            return Ok((None, 0));
        }
        let header = WalRecordHeader::from_bytes(&header_buf);

        if header.total_len < WAL_HEADER_LEN as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record has invalid length",
            ));
        }

        let total_len = header.total_len as u64;
        if lsn.saturating_add(total_len) > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record is truncated",
            ));
        }

        let record_len = header.total_len as usize - WAL_HEADER_LEN;
        let mut record_buf = vec![0; record_len];
        file.read_exact(&mut record_buf)?;

        let mut hasher = Hasher::new();
        hasher.update(&record_buf);
        let crc = hasher.finalize();

        if crc != header.crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL record CRC mismatch",
            ));
        }

        let record = Self::deserialize_record(&record_buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "WAL record decode failed")
        })?;

        Ok((Some(record), header.prev_lsn))
    }

    pub fn checkpoint(
        &mut self,
        active_tx_table: HashMap<u32, Lsn>,
        dirty_page_table: HashMap<PageId, Lsn>,
    ) -> io::Result<()> {
        failpoint::maybe_fail("wal.checkpoint.before_record")?;
        let checkpoint_record = WalRecord::Checkpoint {
            active_tx_table,
            dirty_page_table,
        };
        let lsn = self.log(0, 0, &checkpoint_record)?;
        self.last_checkpoint_lsn.store(lsn, Ordering::SeqCst);
        failpoint::maybe_fail("wal.checkpoint.before_truncate")?;
        self.truncate_wal()
    }

    fn truncate_wal(&mut self) -> io::Result<()> {
        let last_checkpoint_lsn = self.last_checkpoint_lsn.load(Ordering::SeqCst);
        if last_checkpoint_lsn == 0 {
            return Ok(());
        }

        let mut file = lock_mutex_recover(&self.file);
        let file_len = file.metadata()?.len();

        if last_checkpoint_lsn < file_len {
            let mut temp_path = self.path.clone();
            temp_path.set_extension("tmp");
            let mut temp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;

            file.seek(SeekFrom::Start(last_checkpoint_lsn))?;
            io::copy(&mut *file, &mut temp_file)?;

            std::fs::rename(&temp_path, &self.path)?;
            self.next_lsn
                .store(file_len - last_checkpoint_lsn, Ordering::SeqCst);
            self.last_checkpoint_lsn.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn recover<P: AsRef<Path>>(wal_path: P, pager: &mut Pager) -> io::Result<u32> {
        let mut wal_file = OpenOptions::new()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(wal_path)?;
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf)?;

        if buf.is_empty() {
            return Ok(0);
        }

        #[derive(Clone)]
        struct ParsedWalEntry {
            lsn: Lsn,
            header: WalRecordHeader,
            record: WalRecord,
        }

        let header_len = WAL_HEADER_LEN;
        let mut entries = Vec::new();
        let mut pos = 0usize;

        while pos + header_len <= buf.len() {
            let header_slice = buf.get(pos..pos + header_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL header bounds out of range")
            })?;
            let header_arr: [u8; WAL_HEADER_LEN] = header_slice.try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL header size mismatch")
            })?;
            let header = WalRecordHeader::from_bytes(&header_arr);

            if header.total_len < header_len as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL record has invalid length",
                ));
            }

            let total_len = header.total_len as usize;
            if pos + total_len > buf.len() {
                break;
            }

            let record_start = pos + header_len;
            let record_end = pos + total_len;
            let record_buf = buf.get(record_start..record_end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL record bounds out of range")
            })?;

            let mut hasher = Hasher::new();
            hasher.update(record_buf);
            if hasher.finalize() != header.crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL record CRC mismatch",
                ));
            }

            let record = Self::deserialize_record(record_buf).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL record decode failed")
            })?;

            entries.push(ParsedWalEntry {
                lsn: pos as Lsn,
                header,
                record,
            });
            pos += total_len;
        }

        if entries.is_empty() {
            return Ok(0);
        }

        let mut highest_tx_id = entries.iter().map(|e| e.header.tx_id).max().unwrap_or(0);
        let mut active_tx_table: HashMap<u32, Lsn> = HashMap::new();
        let mut dirty_page_table: HashMap<PageId, Lsn> = HashMap::new();

        let mut start_idx = 0usize;
        for (idx, entry) in entries.iter().enumerate().rev() {
            if let WalRecord::Checkpoint {
                active_tx_table: att,
                dirty_page_table: dpt,
            } = &entry.record
            {
                active_tx_table = att.clone();
                dirty_page_table = dpt.clone();
                start_idx = idx + 1;
                break;
            }
        }

        for entry in entries.iter().skip(start_idx) {
            if entry.header.tx_id > highest_tx_id {
                highest_tx_id = entry.header.tx_id;
            }

            let tx_id = entry.record.tx_id();
            if tx_id != 0 {
                active_tx_table.insert(tx_id, entry.lsn);
            }

            match &entry.record {
                WalRecord::Commit { tx_id } | WalRecord::Abort { tx_id } => {
                    active_tx_table.remove(tx_id);
                }
                WalRecord::InsertTuple { page_id, .. }
                | WalRecord::DeleteTuple { page_id, .. }
                | WalRecord::UpdateTuple { page_id, .. }
                | WalRecord::BTreePage { page_id, .. }
                | WalRecord::SetNextPageId { page_id, .. } => {
                    dirty_page_table.entry(*page_id).or_insert(entry.lsn);
                }
                _ => {}
            }
        }

        let redo_start_lsn = dirty_page_table.values().min().copied().unwrap_or(0);
        for entry in entries.iter().filter(|e| e.lsn >= redo_start_lsn) {
            match &entry.record {
                WalRecord::BTreePage { page_id, data, .. } => {
                    let mut page = pager.read_page(*page_id)?;
                    let header = page.read_header();
                    let should_apply = header.lsn < entry.lsn
                        || (header.lsn == entry.lsn && page.data.as_slice() != data.as_slice());
                    if should_apply {
                        page.data.copy_from_slice(data);
                        let mut header = header;
                        header.lsn = entry.lsn;
                        page.write_header(&header);
                        pager.write_page(&page)?;
                    }
                }
                WalRecord::SetNextPageId {
                    page_id,
                    next_page_id,
                    ..
                } => {
                    let mut page = pager.read_page(*page_id)?;
                    let header = page.read_header();
                    let should_apply = header.lsn < entry.lsn
                        || (header.lsn == entry.lsn && header.next_page_id != *next_page_id);
                    if should_apply {
                        let mut header = header;
                        header.next_page_id = *next_page_id;
                        header.lsn = entry.lsn;
                        page.write_header(&header);
                        pager.write_page(&page)?;
                    }
                }
                WalRecord::InsertTuple {
                    page_id,
                    after_page,
                    ..
                }
                | WalRecord::DeleteTuple {
                    page_id,
                    after_page,
                    ..
                }
                | WalRecord::UpdateTuple {
                    page_id,
                    after_page,
                    ..
                } => {
                    let mut page = pager.read_page(*page_id)?;
                    let header = page.read_header();
                    let should_apply = header.lsn < entry.lsn
                        || (header.lsn == entry.lsn
                            && after_page.len() == page.data.len()
                            && page.data.as_slice() != after_page.as_slice());
                    if should_apply {
                        if after_page.len() == page.data.len() {
                            page.data.copy_from_slice(after_page);
                        }
                        let mut header = header;
                        header.lsn = entry.lsn;
                        page.write_header(&header);
                        pager.write_page(&page)?;
                    }
                }
                _ => {}
            }
        }

        let mut lsn_to_index: HashMap<Lsn, usize> = HashMap::new();
        for (idx, entry) in entries.iter().enumerate() {
            lsn_to_index.insert(entry.lsn, idx);
        }

        let loser_txs: Vec<u32> = active_tx_table.keys().copied().collect();
        for tx_id in loser_txs {
            let mut current_lsn = active_tx_table.get(&tx_id).copied().unwrap_or(0);

            while current_lsn > 0 {
                let Some(&idx) = lsn_to_index.get(&current_lsn) else {
                    break;
                };
                let Some(entry) = entries.get(idx) else {
                    break;
                };

                if entry.record.tx_id() != tx_id {
                    current_lsn = entry.header.prev_lsn;
                    continue;
                }

                match &entry.record {
                    WalRecord::CompensationLogRecord { undo_next_lsn, .. } => {
                        current_lsn = *undo_next_lsn;
                        continue;
                    }
                    WalRecord::InsertTuple {
                        page_id,
                        before_page,
                        ..
                    }
                    | WalRecord::DeleteTuple {
                        page_id,
                        before_page,
                        ..
                    }
                    | WalRecord::UpdateTuple {
                        page_id,
                        before_page,
                        ..
                    } => {
                        let mut page = pager.read_page(*page_id)?;
                        if before_page.len() == page.data.len() {
                            page.data.copy_from_slice(before_page);
                        }
                        let mut header = page.read_header();
                        header.lsn = header.lsn.max(current_lsn);
                        page.write_header(&header);
                        pager.write_page(&page)?;
                    }
                    _ => {}
                }

                current_lsn = entry.header.prev_lsn;
            }
        }

        Ok(highest_tx_id)
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        self.stop_sync_thread.store(true, Ordering::SeqCst);
        if let Some(handle) = self.sync_thread_handle.take() {
            let _ = handle.join();
        }

        if let Ok(file) = self.file.lock() {
            let _ = file.sync_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WAL_HEADER_LEN, WalManager, WalRecord, WalRecordHeader};
    use crate::{Page, PageId, failpoint, pager::Pager};
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir;

    fn sample_record(page_id: PageId) -> WalRecord {
        WalRecord::InsertTuple {
            tx_id: 7,
            page_id,
            item_id: 1,
            before_page: vec![1, 2, 3, 4],
            after_page: vec![5, 6, 7, 8],
        }
    }

    fn page_image(page_id: PageId, fill: u8) -> Vec<u8> {
        let mut page = Page::new(page_id);
        let payload_start = std::mem::size_of::<crate::page::PageHeaderData>();
        for byte in page.data[payload_start..].iter_mut() {
            *byte = fill;
        }
        page.data.to_vec()
    }

    fn page_image_with_lsn(page_id: PageId, image: Vec<u8>, lsn: u64) -> Vec<u8> {
        let mut page = Page::new(page_id);
        page.data.copy_from_slice(&image);
        let mut header = page.read_header();
        header.lsn = lsn;
        page.write_header(&header);
        page.data.to_vec()
    }

    #[test]
    fn test_log_and_read_record_roundtrip() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("roundtrip.wal");

        let mut wal = WalManager::open(&wal_path).unwrap();
        let record = sample_record(42);
        let lsn = wal.log(7, 0, &record).unwrap();

        let (decoded, prev_lsn) = wal.read_record(lsn).unwrap();
        assert_eq!(prev_lsn, 0);
        assert_eq!(decoded, Some(record));
    }

    #[test]
    fn test_crc_mismatch_is_detected() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("crc_mismatch.wal");

        let lsn = {
            let mut wal = WalManager::open(&wal_path).unwrap();
            wal.log(7, 0, &sample_record(11)).unwrap()
        };

        let header_len = WAL_HEADER_LEN as u64;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.seek(SeekFrom::Start(lsn + header_len)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(lsn + header_len)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();

        let mut wal = WalManager::open(&wal_path).unwrap();
        let err = wal.read_record(lsn).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_record_rejects_invalid_total_len() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("invalid_total_len.wal");

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .unwrap();
        let header = WalRecordHeader {
            total_len: (WAL_HEADER_LEN as u32) - 1,
            tx_id: 1,
            prev_lsn: 0,
            crc: 0,
        };
        file.write_all(&header.to_bytes()).unwrap();
        file.flush().unwrap();

        let mut wal = WalManager::open(&wal_path).unwrap();
        let err = wal.read_record(0).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_record_rejects_truncated_record() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("truncated_record.wal");

        let lsn = {
            let mut wal = WalManager::open(&wal_path).unwrap();
            wal.log(7, 0, &sample_record(5)).unwrap()
        };

        let meta = std::fs::metadata(&wal_path).unwrap();
        let truncated_len = meta.len().saturating_sub(2);
        let file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.set_len(truncated_len).unwrap();

        let mut wal = WalManager::open(&wal_path).unwrap();
        let err = wal.read_record(lsn).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_recover_applies_lsn_zero_update_on_empty_page() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("recover_lsn_zero.wal");
        let db_path = dir.path().join("recover_lsn_zero.db");

        let before_page = page_image(0, 0x01);
        let after_page = page_image(0, 0xAA);

        {
            let mut wal = WalManager::open(&wal_path).unwrap();
            let update = WalRecord::UpdateTuple {
                tx_id: 3,
                page_id: 0,
                item_id: 0,
                before_page: before_page.clone(),
                after_page: after_page.clone(),
            };
            let lsn = wal.log(3, 0, &update).unwrap();
            assert_eq!(lsn, 0);
            wal.log(3, lsn, &WalRecord::Commit { tx_id: 3 }).unwrap();
        }

        let mut pager = Pager::open(&db_path).unwrap();
        let highest_tx = WalManager::recover(&wal_path, &mut pager).unwrap();
        let page = pager.read_page(0).unwrap();

        assert_eq!(highest_tx, 3);
        assert_eq!(page.data.to_vec(), after_page);
    }

    #[test]
    fn test_recover_is_idempotent_for_committed_update() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("recover_idempotent.wal");
        let db_path = dir.path().join("recover_idempotent.db");

        let before_page = page_image(0, 0x11);
        let after_page = page_image(0, 0x22);
        let update_lsn = {
            let mut wal = WalManager::open(&wal_path).unwrap();
            let _ = wal.log(1, 0, &WalRecord::Commit { tx_id: 1 }).unwrap();
            let record = WalRecord::UpdateTuple {
                tx_id: 7,
                page_id: 0,
                item_id: 0,
                before_page: before_page.clone(),
                after_page: after_page.clone(),
            };
            let lsn = wal.log(7, 0, &record).unwrap();
            wal.log(7, lsn, &WalRecord::Commit { tx_id: 7 }).unwrap();
            lsn
        };

        let expected_after = page_image_with_lsn(0, after_page.clone(), update_lsn);

        let (first_highest_tx, first_page_data) = {
            let mut pager = Pager::open(&db_path).unwrap();
            let highest_tx = WalManager::recover(&wal_path, &mut pager).unwrap();
            let page = pager.read_page(0).unwrap();
            (highest_tx, page.data.to_vec())
        };

        let (second_highest_tx, second_page_data) = {
            let mut pager = Pager::open(&db_path).unwrap();
            let highest_tx = WalManager::recover(&wal_path, &mut pager).unwrap();
            let page = pager.read_page(0).unwrap();
            (highest_tx, page.data.to_vec())
        };

        assert_eq!(first_highest_tx, 7);
        assert_eq!(second_highest_tx, 7);
        assert_eq!(first_page_data, expected_after);
        assert_eq!(second_page_data, expected_after);
    }

    #[test]
    fn test_recover_ignores_truncated_tail_after_valid_records() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("recover_truncated_tail.wal");
        let db_path = dir.path().join("recover_truncated_tail.db");

        let before_page = page_image(2, 0x31);
        let after_page = page_image(2, 0x42);
        let update_lsn = {
            let mut wal = WalManager::open(&wal_path).unwrap();
            let _ = wal.log(1, 0, &WalRecord::Commit { tx_id: 1 }).unwrap();
            let record = WalRecord::UpdateTuple {
                tx_id: 9,
                page_id: 2,
                item_id: 0,
                before_page: before_page.clone(),
                after_page: after_page.clone(),
            };
            let lsn = wal.log(9, 0, &record).unwrap();
            wal.log(9, lsn, &WalRecord::Commit { tx_id: 9 }).unwrap();
            lsn
        };

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&wal_path)
            .unwrap();
        let dangling_header = WalRecordHeader {
            total_len: WAL_HEADER_LEN as u32 + 64,
            tx_id: 1234,
            prev_lsn: 0,
            crc: 0,
        };
        file.write_all(&dangling_header.to_bytes()).unwrap();
        file.flush().unwrap();

        let expected_after = page_image_with_lsn(2, after_page.clone(), update_lsn);
        let mut pager = Pager::open(&db_path).unwrap();
        let highest_tx = WalManager::recover(&wal_path, &mut pager).unwrap();
        let page = pager.read_page(2).unwrap();

        assert_eq!(highest_tx, 9);
        assert_eq!(page.data.to_vec(), expected_after);
    }

    #[test]
    fn test_checkpoint_failpoint_before_record_preserves_existing_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint_before_record.wal");
        let mut wal = WalManager::open(&wal_path).unwrap();
        let original_lsn = wal.log(7, 0, &WalRecord::Commit { tx_id: 7 }).unwrap();

        failpoint::clear();
        failpoint::enable("wal.checkpoint.before_record");
        let err = wal.checkpoint(HashMap::new(), HashMap::new()).unwrap_err();
        failpoint::clear();

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        let (record, _) = wal.read_record(original_lsn).unwrap();
        assert_eq!(record, Some(WalRecord::Commit { tx_id: 7 }));
    }

    #[test]
    fn test_checkpoint_failpoint_before_truncate_keeps_wal_untruncated() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint_before_truncate.wal");
        let mut wal = WalManager::open(&wal_path).unwrap();
        let _ = wal.log(8, 0, &WalRecord::Commit { tx_id: 8 }).unwrap();
        let len_before = std::fs::metadata(&wal_path).unwrap().len();

        failpoint::clear();
        failpoint::enable("wal.checkpoint.before_truncate");
        let err = wal.checkpoint(HashMap::new(), HashMap::new()).unwrap_err();
        failpoint::clear();

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        let len_after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(
            len_after > len_before,
            "checkpoint record should be appended when truncate is skipped"
        );
    }
}
