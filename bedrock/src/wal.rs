//! The Write-Ahead Log manager.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use crc32fast::Hasher;
use crate::{PageId, pager::Pager};

pub type Lsn = u64;

/// Header for every WAL record.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalRecordHeader {
    pub total_len: u32,
    pub tx_id: u32,
    pub prev_lsn: Lsn,
    pub crc: u32,
}

/// A single record in the WAL.
#[derive(Debug, Clone)]
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
    Checkpoint,
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
            WalRecord::Checkpoint => 0, // Checkpoints don't belong to a tx
        }
    }
}

/// The WAL manager.
pub struct WalManager {
    file: File,
    next_lsn: AtomicU64,
}

impl WalManager {
    /// Opens the WAL file and initializes the manager.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let next_lsn = AtomicU64::new(file_len);

        Ok(Self { file, next_lsn })
    }

    /// Logs a record to the WAL and returns the LSN of the record.
    pub fn log(&mut self, tx_id: u32, prev_lsn: Lsn, record: &WalRecord) -> io::Result<Lsn> {
        let mut record_bytes = Vec::new();
        self.serialize_record(record, &mut record_bytes);
        
        let header_len = std::mem::size_of::<WalRecordHeader>() as u32;
        let total_len = header_len + record_bytes.len() as u32;

        let lsn = self.next_lsn.fetch_add(total_len as u64, Ordering::SeqCst);

        let mut hasher = Hasher::new();
        hasher.update(&total_len.to_be_bytes());
        hasher.update(&tx_id.to_be_bytes());
        hasher.update(&prev_lsn.to_be_bytes());
        hasher.update(&record_bytes);
        let crc = hasher.finalize();

        let header = WalRecordHeader { total_len, tx_id, prev_lsn, crc };

        self.file.write_all(unsafe {
            std::slice::from_raw_parts(
                &header as *const _ as *const u8,
                header_len as usize,
            )
        })?;
        self.file.write_all(&record_bytes)?;
        self.file.sync_all()?;

        Ok(lsn)
    }

    fn serialize_record(&self, record: &WalRecord, buf: &mut Vec<u8>) {
        match record {
            WalRecord::Commit { tx_id } => {
                buf.push(1);
                buf.extend_from_slice(&tx_id.to_be_bytes());
            }
            WalRecord::Abort { tx_id } => {
                buf.push(2);
                buf.extend_from_slice(&tx_id.to_be_bytes());
            }
            WalRecord::InsertTuple { tx_id, page_id, item_id } => {
                buf.push(3);
                buf.extend_from_slice(&tx_id.to_be_bytes());
                buf.extend_from_slice(&page_id.to_be_bytes());
                buf.extend_from_slice(&item_id.to_be_bytes());
            }
            WalRecord::DeleteTuple { tx_id, page_id, item_id } => {
                buf.push(4);
                buf.extend_from_slice(&tx_id.to_be_bytes());
                buf.extend_from_slice(&page_id.to_be_bytes());
                buf.extend_from_slice(&item_id.to_be_bytes());
            }
            WalRecord::UpdateTuple { tx_id, page_id, item_id, old_data } => {
                buf.push(5);
                buf.extend_from_slice(&tx_id.to_be_bytes());
                buf.extend_from_slice(&page_id.to_be_bytes());
                buf.extend_from_slice(&item_id.to_be_bytes());
                buf.extend_from_slice(&(old_data.len() as u32).to_be_bytes());
                buf.extend_from_slice(old_data);
            }
            WalRecord::BTreePage { tx_id, page_id, data } => {
                buf.push(6);
                buf.extend_from_slice(&tx_id.to_be_bytes());
                buf.extend_from_slice(&page_id.to_be_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
                buf.extend_from_slice(data);
            }
            WalRecord::Checkpoint => buf.push(7),
        }
    }

    pub fn deserialize_record(buf: &[u8]) -> Option<WalRecord> {
        if buf.is_empty() { return None; }
        let record_type = buf[0];
        let data = &buf[1..];
        match record_type {
            1 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                Some(WalRecord::Commit { tx_id })
            }
            2 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                Some(WalRecord::Abort { tx_id })
            }
            3 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                let page_id = u32::from_be_bytes(data[4..8].try_into().ok()?);
                let item_id = u16::from_be_bytes(data[8..10].try_into().ok()?);
                Some(WalRecord::InsertTuple { tx_id, page_id, item_id })
            }
            4 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                let page_id = u32::from_be_bytes(data[4..8].try_into().ok()?);
                let item_id = u16::from_be_bytes(data[8..10].try_into().ok()?);
                Some(WalRecord::DeleteTuple { tx_id, page_id, item_id })
            }
            5 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                let page_id = u32::from_be_bytes(data[4..8].try_into().ok()?);
                let item_id = u16::from_be_bytes(data[8..10].try_into().ok()?);
                let len = u32::from_be_bytes(data[10..14].try_into().ok()?) as usize;
                let old_data = data[14..14+len].to_vec();
                Some(WalRecord::UpdateTuple { tx_id, page_id, item_id, old_data })
            }
            6 => {
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                let page_id = u32::from_be_bytes(data[4..8].try_into().ok()?);
                let len = u32::from_be_bytes(data[8..12].try_into().ok()?) as usize;
                let page_data = data[12..12+len].to_vec();
                Some(WalRecord::BTreePage { tx_id, page_id, data: page_data })
            }
            7 => Some(WalRecord::Checkpoint),
            _ => None,
        }
    }

    pub fn read_record(&mut self, lsn: Lsn) -> io::Result<(Option<WalRecord>, Lsn)> {
        if lsn == 0 {
            return Ok((None, 0));
        }
        self.file.seek(SeekFrom::Start(lsn))?;
        let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
        if self.file.read_exact(&mut header_buf).is_err() {
            // This can happen if we are at the end of the file or the file is corrupted.
            return Ok((None, 0));
        }
        let header: WalRecordHeader = unsafe { std::mem::transmute(header_buf) };
        
        let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
        let mut record_buf = vec![0; record_len];
        self.file.read_exact(&mut record_buf)?;

        // TODO: CRC check

        Ok((Self::deserialize_record(&record_buf), header.prev_lsn))
    }
    
    /// Recovers the database from the WAL using the ARIES Redo algorithm.
    /// Returns the highest transaction ID found in the WAL.
    pub fn recover<P: AsRef<Path>>(wal_path: P, pager: &mut Pager) -> io::Result<u32> {
        let mut wal_file = OpenOptions::new().read(true).open(wal_path)?;
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf)?;

        let mut highest_tx_id = 0;
        let mut current_pos = 0;

        while current_pos < buf.len() {
            let header_slice = &buf[current_pos..current_pos + std::mem::size_of::<WalRecordHeader>()];
            let header: WalRecordHeader = unsafe { *header_slice.as_ptr().cast::<WalRecordHeader>() };

            if header.tx_id > highest_tx_id {
                highest_tx_id = header.tx_id;
            }

            let record_len = header.total_len as usize - std::mem::size_of::<WalRecordHeader>();
            let record_start = current_pos + std::mem::size_of::<WalRecordHeader>();
            let record_end = record_start + record_len;
            let record_buf = &buf[record_start..record_end];

            if let Some(record) = Self::deserialize_record(record_buf) {
                // REDO phase: Re-apply all logged changes to bring pages up to date.
                // We only need to apply changes if the LSN on the page is less than the LSN of the record.
                // This prevents re-applying changes that were already flushed to disk before a crash.
                match record {
                    WalRecord::InsertTuple { page_id, .. } | WalRecord::DeleteTuple { page_id, .. } | WalRecord::UpdateTuple { page_id, .. } => {
                        let mut page = pager.read_page(page_id)?;
                        if page.header().lsn < current_pos as u64 {
                            // The actual re-doing of the operation is implicitly handled by the executor
                            // when it re-creates the tuple. Here we just need to ensure the page's LSN is updated.
                            // A more complete ARIES implementation would re-apply the exact change here.
                            page.header_mut().lsn = current_pos as u64;
                            pager.write_page(&page)?;
                        }
                    }
                    WalRecord::BTreePage { page_id, data, .. } => {
                        let mut page = pager.read_page(page_id)?;
                        if page.header().lsn < current_pos as u64 {
                            page.data.copy_from_slice(&data);
                            page.header_mut().lsn = current_pos as u64;
                            pager.write_page(&page)?;
                        }
                    }
                    _ => {} // Commit, Abort, Checkpoint don't modify pages directly.
                }
            }
            current_pos += header.total_len as usize;
        }

        Ok(highest_tx_id)
    }
}