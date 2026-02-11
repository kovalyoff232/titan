use crate::buffer_pool::BufferPoolManager;
use crate::page::TransactionId;
use crate::wal::{Lsn, WalManager, WalRecord};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: TransactionId,

    pub xmax: TransactionId,

    pub active_transactions: Arc<HashSet<TransactionId>>,
}

impl Snapshot {
    pub fn is_visible(&self, tx_id: TransactionId) -> bool {
        if tx_id >= self.xmax {
            return false;
        }
        if tx_id < self.xmin {
            return true;
        }
        !self.active_transactions.contains(&tx_id)
    }
}

#[derive(Debug, Default)]
struct TransactionManagerState {
    next_transaction_id: AtomicU32,

    active_transactions: Mutex<HashSet<TransactionId>>,

    last_lsns: Mutex<HashMap<TransactionId, Lsn>>,

    next_oid: AtomicU32,
}

#[derive(Debug, Clone)]
pub struct TransactionManager {
    state: Arc<TransactionManagerState>,
}

impl TransactionManager {
    pub fn new(initial_tx_id: TransactionId) -> Self {
        println!(
            "[TM::new] Initializing with next_transaction_id = {}",
            initial_tx_id
        );
        let state = TransactionManagerState {
            next_transaction_id: AtomicU32::new(initial_tx_id),
            active_transactions: Mutex::new(HashSet::new()),
            last_lsns: Mutex::new(HashMap::new()),
            next_oid: AtomicU32::new(100),
        };
        Self {
            state: Arc::new(state),
        }
    }

    pub fn get_next_oid(&self) -> u32 {
        let oid = self.state.next_oid.fetch_add(1, Ordering::SeqCst);
        println!("[TM::get_next_oid] Vending new OID: {}", oid);
        oid
    }

    pub fn begin(&self) -> TransactionId {
        let tx_id = self
            .state
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        self.state.active_transactions.lock().unwrap().insert(tx_id);
        self.state.last_lsns.lock().unwrap().insert(tx_id, 0);
        println!(
            "[TM::begin] Started tx_id: {}. Active transactions: {:?}",
            tx_id,
            self.state.active_transactions.lock().unwrap()
        );
        tx_id
    }

    fn finalize_commit(&self, tx_id: TransactionId) {
        self.state
            .active_transactions
            .lock()
            .unwrap()
            .remove(&tx_id);
        self.state.last_lsns.lock().unwrap().remove(&tx_id);
        println!(
            "[TM::commit] Committed tx_id: {}. Active transactions: {:?}",
            tx_id,
            self.state.active_transactions.lock().unwrap()
        );
    }

    pub fn commit(&self, tx_id: TransactionId) {
        self.finalize_commit(tx_id);
    }

    pub fn commit_with_wal(
        &self,
        tx_id: TransactionId,
        wal: &mut WalManager,
    ) -> std::io::Result<()> {
        let prev_lsn = self.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wal.log(tx_id, prev_lsn, &WalRecord::Commit { tx_id })?;
        self.set_last_lsn(tx_id, lsn);
        self.finalize_commit(tx_id);
        Ok(())
    }

    pub fn get_last_lsn(&self, tx_id: TransactionId) -> Option<Lsn> {
        self.state.last_lsns.lock().unwrap().get(&tx_id).cloned()
    }

    pub fn set_last_lsn(&self, tx_id: TransactionId, lsn: Lsn) {
        self.state.last_lsns.lock().unwrap().insert(tx_id, lsn);
    }

    pub fn abort(
        &self,
        tx_id: TransactionId,
        wal: &mut WalManager,
        bpm: &Arc<BufferPoolManager>,
    ) -> std::io::Result<()> {
        println!("[TM::abort] Aborting tx_id: {}", tx_id);

        let mut current_lsn = self.get_last_lsn(tx_id).unwrap_or(0);

        while current_lsn > 0 {
            let (record, prev_lsn) = wal.read_record(current_lsn)?;

            let rec = match record {
                Some(r) => r,
                None => {
                    break;
                }
            };

            if rec.tx_id() != tx_id {
                current_lsn = prev_lsn;
                continue;
            }

            let clr = WalRecord::CompensationLogRecord {
                tx_id,
                page_id: 0,
                item_id: 0,
                undo_next_lsn: prev_lsn,
            };

            let last_lsn = self.get_last_lsn(tx_id).unwrap_or(0);
            let clr_lsn = wal.log(tx_id, last_lsn, &clr)?;
            self.set_last_lsn(tx_id, clr_lsn);

            match rec {
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
                    let page_guard = bpm.acquire_page(page_id)?;
                    let mut page = page_guard.write();
                    if before_page.len() == page.data.len() {
                        page.data.copy_from_slice(&before_page);
                    }
                    let mut header = page.read_header();
                    header.lsn = clr_lsn;
                    page.write_header(&header);
                }

                WalRecord::CompensationLogRecord { undo_next_lsn, .. } => {
                    current_lsn = undo_next_lsn;
                    continue;
                }
                _ => {}
            }

            current_lsn = prev_lsn;
        }

        let last_lsn = self.get_last_lsn(tx_id).unwrap_or(0);
        wal.log(tx_id, last_lsn, &WalRecord::Abort { tx_id })?;

        self.state
            .active_transactions
            .lock()
            .unwrap()
            .remove(&tx_id);
        self.state.last_lsns.lock().unwrap().remove(&tx_id);

        println!(
            "[TM::abort] Finished abort for tx_id: {}. Active transactions: {:?}",
            tx_id,
            self.state.active_transactions.lock().unwrap()
        );
        Ok(())
    }

    pub fn create_snapshot(&self, _current_tx_id: TransactionId) -> Snapshot {
        let active_txns = self.state.active_transactions.lock().unwrap();

        let xmin = active_txns
            .iter()
            .min()
            .cloned()
            .unwrap_or_else(|| self.state.next_transaction_id.load(Ordering::SeqCst));

        let xmax = self.state.next_transaction_id.load(Ordering::SeqCst);

        let snapshot = Snapshot {
            xmin,
            xmax,
            active_transactions: Arc::new(active_txns.clone()),
        };
        println!(
            "[TM::create_snapshot] Created for tx {}: {:?}",
            _current_tx_id, snapshot
        );
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_snapshot() {
        let tm = TransactionManager::new(0);

        let snapshot1 = tm.create_snapshot(0);
        assert_eq!(snapshot1.xmin, 0);
        assert_eq!(snapshot1.xmax, 0);
        assert!(snapshot1.active_transactions.is_empty());

        let tx1 = tm.begin();
        let tx2 = tm.begin();

        let snapshot2 = tm.create_snapshot(tx2);
        assert_eq!(snapshot2.xmin, 0);
        assert_eq!(snapshot2.xmax, 2);
        assert!(snapshot2.active_transactions.contains(&0));
        assert!(snapshot2.active_transactions.contains(&1));
        assert_eq!(snapshot2.active_transactions.len(), 2);

        tm.commit(tx1);

        let snapshot3 = tm.create_snapshot(tx2);
        assert_eq!(snapshot3.xmin, 1);
        assert_eq!(snapshot3.xmax, 2);
        assert!(!snapshot3.active_transactions.contains(&0));
        assert!(snapshot3.active_transactions.contains(&1));
        assert_eq!(snapshot3.active_transactions.len(), 1);
    }

    #[test]
    fn test_snapshot_visibility() {
        let snapshot = Snapshot {
            xmin: 10,
            xmax: 20,
            active_transactions: Arc::new([12, 15].iter().cloned().collect()),
        };

        assert!(snapshot.is_visible(5));

        assert!(!snapshot.is_visible(25));

        assert!(!snapshot.is_visible(12));
        assert!(!snapshot.is_visible(15));

        assert!(snapshot.is_visible(11));
        assert!(snapshot.is_visible(19));
    }
}
