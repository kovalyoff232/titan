//! The Write-Ahead Log manager.

use crate::{pager::Pager, PageId};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// A Log Sequence Number.
pub type Lsn = u64;

/// Header for every WAL record.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalRecordHeader {
    /// The total length of the record, including the header.
    pub total_len: u32,
    /// The ID of the transaction that this record belongs to.
    pub tx_id: u32,
    /// The LSN of the previous record for this transaction.
    pub prev_lsn: Lsn,
    /// The CRC checksum of the record.
    pub crc: u32,
}

/// A single record in the WAL.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalRecord {
    /// Indicates the commit of a transaction.
    Commit { tx_id: u32 },
    /// Indicates the abort of a transaction.
    Abort { tx_id: u32 },
    /// A new tuple was inserted.
    InsertTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
    },
    /// A tuple was marked as deleted.
    DeleteTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
    },
    /// A tuple was updated (old data is logged for UNDO).
    UpdateTuple {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,
        old_data: Vec<u8>,
    },
    /// A B-Tree page was updated.
    BTreePage {
        tx_id: u32,
        page_id: PageId,
        data: Vec<u8>,
    },
    /// A checkpoint record.
    Checkpoint {
        active_tx_table: HashMap<u32, Lsn>,
        dirty_page_table: HashMap<PageId, Lsn>,
    },
    /// Sets the next_page_id pointer of a page.
    SetNextPageId {
        tx_id: u32,
        page_id: PageId,
        next_page_id: PageId,
    },
    /// A Compensation Log Record (CLR) for undoing a change.
    CompensationLogRecord {
        tx_id: u32,
        page_id: PageId,
        item_id: u16,

        undo_next_lsn: Lsn,
    },
}

impl WalRecord {
    /// Returns the transaction ID of the record.
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
            WalRecord::Checkpoint { .. } => 0, // Checkpoints don't belong to a tx
        }
    }
}

/// The WAL manager.
pub struct WalManager {
    file: Arc<Mutex<File>>,
    path: PathBuf,
    next_lsn: AtomicU64,
    last_checkpoint_lsn: AtomicU64,
    sync_thread_handle: Option<thread::JoinHandle<()>>,
    stop_sync_thread: Arc<AtomicBool>,
}

impl WalManager {
    /// Opens the WAL file and initializes the manager.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
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
                if let Ok(file) = file_clone.lock() {
                    let _ = file.sync_all();
                }
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

    /// Logs a record to the WAL and returns the LSN of the record.
    pub fn log(&mut self, tx_id: u32, prev_lsn: Lsn, record: &WalRecord) -> io::Result<Lsn> {
        let record_bytes = bincode::serialize(record).unwrap();

        let header_len = std::mem::size_of::<WalRecordHeader>() as u32;
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

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(lsn))?;
        file.write_all(unsafe {
            std::slice::from_raw_parts(&header as *const _ as *const u8, header_len as usize)
        })?;
        file.write_all(&record_bytes)?;

        Ok(lsn)
    }

    /// Deserializes a WAL record from a byte buffer.
    pub fn deserialize_record(buf: &[u8]) -> Option<WalRecord> {
        bincode::deserialize(buf).ok()
    }

    /// Reads a WAL record from the given LSN.
    pub fn read_record(&mut self, lsn: Lsn) -> io::Result<(Option<WalRecord>, Lsn)> {
        let mut file = self.file.lock().unwrap();
        if lsn >= file.metadata()?.len() {
            return Ok((None, 0));
        }

        file.seek(SeekFrom::Start(lsn))?;
        let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
        if file.read_exact(&mut header_buf).is_err() {
            return Ok((None, 0));
        }
        let header: WalRecordHeader = unsafe { std::mem::transmute(header_buf) };

        let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
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

        Ok((Self::deserialize_record(&record_buf), header.prev_lsn))
    }
 
    pub fn checkpoint(
        &mut self,
        active_tx_table: HashMap<u32, Lsn>,
        dirty_page_table: HashMap<PageId, Lsn>,
    ) -> io::Result<()> {
        let checkpoint_record = WalRecord::Checkpoint {
            active_tx_table,
            dirty_page_table,
        };
        let lsn = self.log(0, 0, &checkpoint_record)?;
        self.last_checkpoint_lsn.store(lsn, Ordering::SeqCst);
        self.truncate_wal()
    }

    fn truncate_wal(&mut self) -> io::Result<()> {
        let last_checkpoint_lsn = self.last_checkpoint_lsn.load(Ordering::SeqCst);
        if last_checkpoint_lsn == 0 {
            return Ok(());
        }

        let mut file = self.file.lock().unwrap();
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

    /// Recovers the database from the WAL using the ARIES algorithm.
    /// Returns the highest transaction ID found in the WAL.
    pub fn recover<P: AsRef<Path>>(wal_path: P, pager: &mut Pager) -> io::Result<u32> {
        let mut wal_file = OpenOptions::new()
            .read(true)
            .create(true)
            .truncate(true)
            .write(true)
            .open(wal_path)?;
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf)?;

        if buf.is_empty() {
            return Ok(0);
        }

        let mut highest_tx_id = 0;
        let mut active_tx_table: HashMap<u32, Lsn> = HashMap::new();
        let mut dirty_page_table: HashMap<PageId, Lsn> = HashMap::new();
        let mut last_checkpoint_lsn = 0;

        // Analysis pass: find the last checkpoint and build initial tables
        let mut current_pos = 0;
        while current_pos < buf.len() {
            let header_slice =
                &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader =
                unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };

            let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
            let record_start = current_pos + std::mem::size_of::<WalRecordHeader>();
            let record_end = record_start + record_len;
            let record_buf = &buf[record_start..record_end];

            if let Some(WalRecord::Checkpoint {
                active_tx_table: att,
                dirty_page_table: dpt,
            }) = Self::deserialize_record(record_buf)
            {
                active_tx_table = att;
                dirty_page_table = dpt;
                last_checkpoint_lsn = current_pos as Lsn;
            }
            current_pos += header.total_len as usize;
        }

        // Start analysis from the last checkpoint
        current_pos = last_checkpoint_lsn as usize;
        // We need to skip the checkpoint record itself
        if current_pos > 0 {
            let header_slice =
                &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader =
                unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };
            current_pos += header.total_len as usize;
        }

        while current_pos < buf.len() {
            let header_slice =
                &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader =
                unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };

            if header.tx_id > highest_tx_id {
                highest_tx_id = header.tx_id;
            }

            let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
            let record_start = current_pos + std::mem::size_of::<WalRecordHeader>();
            let record_end = record_start + record_len;
            let record_buf = &buf[record_start..record_end];

            let mut hasher = Hasher::new();
            hasher.update(record_buf);
            if hasher.finalize() != header.crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL record CRC mismatch",
                ));
            }

            if let Some(record) = Self::deserialize_record(record_buf) {
                if record.tx_id() != 0 && !active_tx_table.contains_key(&record.tx_id()) {
                    active_tx_table.insert(record.tx_id(), current_pos as Lsn);
                }

                match record {
                    WalRecord::Commit { tx_id } | WalRecord::Abort { tx_id } => {
                        active_tx_table.remove(&tx_id);
                    }
                    WalRecord::InsertTuple { page_id, .. }
                    | WalRecord::DeleteTuple { page_id, .. }
                    | WalRecord::UpdateTuple { page_id, .. }
                    | WalRecord::BTreePage { page_id, .. }
                    | WalRecord::SetNextPageId { page_id, .. } => {
                        dirty_page_table
                            .entry(page_id)
                            .or_insert(current_pos as Lsn);
                    }
                    _ => {}
                }
            }
            current_pos += header.total_len as usize;
        }

       
        let redo_start_lsn = dirty_page_table.values().min().cloned().unwrap_or(0);
        current_pos = redo_start_lsn as usize;

        while current_pos < buf.len() {
            let header_slice =
                &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader =
                unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };

            let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
            let record_start = current_pos + std::mem::size_of::<WalRecordHeader>();
            let record_end = record_start + record_len;
            let record_buf = &buf[record_start..record_end];
 
             let mut hasher = Hasher::new();
             hasher.update(record_buf);
             if hasher.finalize() != header.crc {
                 return Err(io::Error::new(
                     io::ErrorKind::InvalidData,
                     "WAL record CRC mismatch",
                 ));
             }
 
             if let Some(WalRecord::BTreePage { page_id, data, .. }) =
                 Self::deserialize_record(record_buf)
            {
                let mut page = pager.read_page(page_id)?;
                if page.read_header().lsn < current_pos as u64 {
                    page.data.copy_from_slice(&data);
                    let mut header = page.read_header();
                    header.lsn = current_pos as u64;
                    page.write_header(&header);
                    pager.write_page(&page)?;
                }
            } else {
               
            }
            current_pos += header.total_len as usize;
        }

      
        let to_undo: HashSet<u32> = active_tx_table.keys().cloned().collect();

     
        let mut max_lsn = 0;
        for tx_id in &to_undo {
            if let Some(lsn) = active_tx_table.get(tx_id) {
                if *lsn > max_lsn {
                    max_lsn = *lsn;
                }
            }
        }

        let mut current_pos = max_lsn as usize;
        while current_pos > 0 {
            let header_slice =
                &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader =
                unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };

            if to_undo.contains(&header.tx_id) {
                let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
                let record_start = current_pos + std::mem::size_of::<WalRecordHeader>();
                let record_end = record_start + record_len;
                let record_buf = &buf[record_start..record_end];
 
                 let mut hasher = Hasher::new();
                 hasher.update(record_buf);
                 if hasher.finalize() != header.crc {
                     return Err(io::Error::new(
                         io::ErrorKind::InvalidData,
                         "WAL record CRC mismatch",
                     ));
                 }
 
                 if let Some(WalRecord::UpdateTuple { page_id, .. }) =
                     Self::deserialize_record(record_buf)
                {
                   
                    let page = pager.read_page(page_id)?;
                   
                    if page.read_header().lsn >= current_pos as u64 {
                       
                        println!(
                            "[RECOVERY-UNDO] Undoing update for tx {} on page {}",
                            header.tx_id, page_id
                        );
                    }
                }
            }
            current_pos = header.prev_lsn as usize;
        }

        Ok(highest_tx_id)
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        self.stop_sync_thread.store(true, Ordering::SeqCst);
        if let Some(handle) = self.sync_thread_handle.take() {
            handle.join().unwrap();
        }
        // Final sync on drop
        if let Ok(file) = self.file.lock() {
            let _ = file.sync_all();
        }
    }
}
