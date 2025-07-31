use bedrock::wal::{WalManager, WalRecord};
use std::fs::OpenOptions;
use std::io::{Seek, Write};
use tempfile::tempdir;

#[test]
fn test_wal_crc_check_ok() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().join("test.wal");
    let mut wal = WalManager::open(&wal_path).unwrap();

    let record = WalRecord::Commit { tx_id: 1 };
    let lsn = wal.log(1, 0, &record).unwrap();
    
    drop(wal);
    let mut wal = WalManager::open(&wal_path).unwrap();

    let (read_record, _) = wal.read_record(lsn).unwrap();
    assert!(read_record.is_some());
}

#[test]
fn test_wal_crc_check_fail() {
    let temp_dir = tempdir().unwrap();
    let wal_path = temp_dir.path().join("test.wal");
    let mut wal = WalManager::open(&wal_path).unwrap();

    let record = WalRecord::Commit { tx_id: 1 };
    let lsn = wal.log(1, 0, &record).unwrap();

    // Corrupt the record
    drop(wal);
    let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
    file.seek(std::io::SeekFrom::Start(lsn + 16)).unwrap(); // Seek to CRC
    file.write_all(&[0, 0, 0, 0]).unwrap(); // Corrupt CRC

    let mut wal = WalManager::open(&wal_path).unwrap();
    let result = wal.read_record(lsn);
    assert!(result.is_err());
    assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::InvalidData);
}