//! The Write-Ahead Log manager.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use crc32fast::Hasher;
use crate::{PageId, PAGE_SIZE};
use crate::pager::Pager;

pub type Lsn = u64;

/// A single record in the WAL.
#[derive(Debug)]
pub enum WalRecord {
    /// Indicates the commit of a transaction.
    Commit { tx_id: u32 },
    /// A full physical page image.
    SetPage {
        tx_id: u32,
        page_id: PageId,
        data: Box<[u8; PAGE_SIZE]>,
    },
    /// A checkpoint record.
    Checkpoint,
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
            .append(true) // Ensure all writes go to the end of the file.
            .open(path)?;

        let file_len = file.metadata()?.len();
        let next_lsn = AtomicU64::new(file_len);

        Ok(Self { file, next_lsn })
    }

    /// Logs a record to the WAL and returns the LSN of the record.
    pub fn log(&mut self, record: &WalRecord) -> io::Result<Lsn> {
        let mut record_bytes = Vec::new();
        self.serialize_record(record, &mut record_bytes);
        let len = record_bytes.len() as u32;
        let record_len = (4 + 4 + 8 + len) as u64;

        let lsn = self.next_lsn.fetch_add(record_len, Ordering::SeqCst);

        let mut hasher = Hasher::new();
        hasher.update(&len.to_be_bytes());
        hasher.update(&lsn.to_be_bytes());
        hasher.update(&record_bytes);
        let crc = hasher.finalize();

        self.file.write_all(&len.to_be_bytes())?;
        self.file.write_all(&crc.to_be_bytes())?;
        self.file.write_all(&lsn.to_be_bytes())?;
        self.file.write_all(&record_bytes)?;
        self.file.sync_all()?;

        Ok(lsn)
    }

    fn serialize_record(&self, record: &WalRecord, buf: &mut Vec<u8>) {
        match record {
            WalRecord::Commit { tx_id } => {
                buf.push(1); // Record type
                buf.extend_from_slice(&tx_id.to_be_bytes());
            }
            WalRecord::SetPage { tx_id, page_id, data } => {
                buf.push(2); // Record type
                buf.extend_from_slice(&tx_id.to_be_bytes());
                buf.extend_from_slice(&page_id.to_be_bytes());
                buf.extend_from_slice(data.as_ref());
            }
            WalRecord::Checkpoint => {
                buf.push(3); // Record type
            }
        }
    }

    fn deserialize_record(buf: &[u8]) -> Option<WalRecord> {
        if buf.is_empty() { return None; }
        let record_type = buf[0];
        let data = &buf[1..];
        match record_type {
            1 => { // Commit
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                Some(WalRecord::Commit { tx_id })
            }
            2 => { // SetPage
                if data.len() < 8 + PAGE_SIZE { return None; }
                let tx_id = u32::from_be_bytes(data[0..4].try_into().ok()?);
                let page_id = u32::from_be_bytes(data[4..8].try_into().ok()?);
                let mut page_data = Box::new([0u8; PAGE_SIZE]);
                page_data.copy_from_slice(&data[8..8 + PAGE_SIZE]);
                Some(WalRecord::SetPage { tx_id, page_id, data: page_data })
            }
            3 => Some(WalRecord::Checkpoint),
            _ => None,
        }
    }

    /// Recovers the database from the WAL using the ARIES Redo algorithm.
    /// Returns the highest transaction ID found in the WAL.
    pub fn recover<P: AsRef<Path>>(wal_path: P, pager: &mut Pager) -> io::Result<u32> {
        let mut file = match OpenOptions::new().read(true).open(wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut cursor = 0;
        let mut highest_tx_id = 0;

        while cursor < buffer.len() {
            if cursor + 16 > buffer.len() { break; }
            let len = u32::from_be_bytes(buffer[cursor..cursor+4].try_into().unwrap()) as usize;
            let record_start = cursor + 16;
            let record_end = record_start + len;
            if record_end > buffer.len() { break; }

            let lsn = u64::from_be_bytes(buffer[cursor+8..cursor+16].try_into().unwrap());
            let record_data = &buffer[record_start..record_end];
            let record = match Self::deserialize_record(record_data) {
                Some(r) => r,
                None => { cursor = record_end; continue; }
            };

            let current_tx_id = match record {
                WalRecord::SetPage { tx_id, .. } => tx_id,
                WalRecord::Commit { tx_id } => tx_id,
                _ => 0,
            };
            if current_tx_id > highest_tx_id {
                highest_tx_id = current_tx_id;
            }

            if let WalRecord::SetPage { page_id, ref data, .. } = record {
                let mut page = pager.read_page(page_id)?;
                let page_lsn = page.header().lsn;
                if lsn >= page_lsn {
                    page.data.copy_from_slice(&**data);
                    pager.write_page(&page)?;
                }
            }
            cursor = record_end;
        }
        Ok(highest_tx_id)
    }
}