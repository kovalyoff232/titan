//! Transaction management.

use crate::buffer_pool::BufferPoolManager;
use crate::page::TransactionId;
use crate::wal::{Lsn, WalManager, WalRecord};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// A snapshot of the database state.
/// It contains the set of active transactions.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub active_transactions: Arc<HashSet<TransactionId>>,
}

impl Snapshot {
    /// Returns true if the given transaction is visible in this snapshot.
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

/// The state of the transaction manager.
#[derive(Debug, Default)]
struct TransactionManagerState {
    next_transaction_id: AtomicU32,
    active_transactions: Mutex<HashSet<TransactionId>>,
    last_lsns: Mutex<HashMap<TransactionId, Lsn>>,
    next_oid: AtomicU32,
}

/// The transaction manager. Designed to be shared across threads.
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
            next_oid: AtomicU32::new(100), // Start OIDs from a higher number
        };
        Self {
            state: Arc::new(state),
        }
    }

    /// Gets the next available Object ID.
    pub fn get_next_oid(&self) -> u32 {
        let oid = self.state.next_oid.fetch_add(1, Ordering::SeqCst);
        println!("[TM::get_next_oid] Vending new OID: {}", oid);
        oid
    }

    /// Begins a new transaction and returns its ID.
    pub fn begin(&self) -> TransactionId {
        let tx_id = self
            .state
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        self.state.active_transactions.lock().unwrap().insert(tx_id);
        self.state.last_lsns.lock().unwrap().insert(tx_id, 0); // Initialize LSN to 0
        println!(
            "[TM::begin] Started tx_id: {}. Active transactions: {:?}",
            tx_id,
            self.state.active_transactions.lock().unwrap()
        );
        tx_id
    }

    /// Commits a transaction.
    pub fn commit(&self, tx_id: TransactionId) {
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

    pub fn get_last_lsn(&self, tx_id: TransactionId) -> Option<Lsn> {
        self.state.last_lsns.lock().unwrap().get(&tx_id).cloned()
    }

    pub fn set_last_lsn(&self, tx_id: TransactionId, lsn: Lsn) {
        self.state.last_lsns.lock().unwrap().insert(tx_id, lsn);
    }

    /// Aborts a transaction, rolling back its changes using ARIES-style UNDO.
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
                    // Reached end or corruption
                    break;
                }
            };

            // We only care about records from the transaction we are aborting
            if rec.tx_id() != tx_id {
                current_lsn = prev_lsn;
                continue;
            }

            // Generate and log a Compensation Log Record (CLR) *before* undoing the change.
            // The CLR's `undo_next_lsn` points to the `prev_lsn` of the record we are undoing.
            // This makes UNDO operations idempotent. If we crash during UNDO, we can restart
            // and the CLRs will tell us which actions have already been undone.
            let clr = WalRecord::CompensationLogRecord {
                tx_id,
                page_id: 0, // Placeholder, specific to record type
                item_id: 0, // Placeholder
                undo_next_lsn: prev_lsn,
            };

            let last_lsn = self.get_last_lsn(tx_id).unwrap_or(0);
            let clr_lsn = wal.log(tx_id, last_lsn, &clr)?;
            self.set_last_lsn(tx_id, clr_lsn);

            // Now, perform the physical UNDO operation.
            match rec {
                WalRecord::InsertTuple {
                    page_id, item_id, ..
                } => {
                    let page_guard = bpm.acquire_page(page_id)?;
                    let mut page = page_guard.write();
                    if let Some(header) = page.get_tuple_header_mut(item_id) {
                        // Physically mark the tuple as deleted by this abort.
                        header.xmax = tx_id;
                    }
                    let mut header = page.read_header();
                    header.lsn = clr_lsn;
                    page.write_header(&header);
                }
                WalRecord::DeleteTuple {
                    page_id, item_id, ..
                } => {
                    let page_guard = bpm.acquire_page(page_id)?;
                    let mut page = page_guard.write();
                    if let Some(header) = page.get_tuple_header_mut(item_id) {
                        // Undo the deletion by clearing the xmax.
                        if header.xmax == tx_id {
                            header.xmax = 0;
                        }
                    }
                    let mut header = page.read_header();
                    header.lsn = clr_lsn;
                    page.write_header(&header);
                }
                WalRecord::UpdateTuple {
                    page_id,
                    item_id,
                    old_data,
                    ..
                } => {
                    let page_guard = bpm.acquire_page(page_id)?;
                    let mut page = page_guard.write();
                    if let Some(tuple) = page.get_raw_tuple_mut(item_id) {
                        // Restore the old version of the tuple.
                        tuple.copy_from_slice(&old_data);
                    }
                    let mut header = page.read_header();
                    header.lsn = clr_lsn;
                    page.write_header(&header);
                }
                // CLRs are not undone. We just follow their `undo_next_lsn` pointer.
                WalRecord::CompensationLogRecord { undo_next_lsn, .. } => {
                    current_lsn = undo_next_lsn;
                    continue;
                }
                _ => {}
            }

            // Move to the previous record in this transaction's chain.
            current_lsn = prev_lsn;
        }

        // After undoing all changes, write an Abort record to the WAL.
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

    /// Creates a new snapshot of the current database state.
    pub fn create_snapshot(&self, _current_tx_id: TransactionId) -> Snapshot {
        let active_txns = self.state.active_transactions.lock().unwrap();

        // xmin is the lowest transaction ID that is currently active.
        // Any transaction with an ID less than xmin is guaranteed to be either committed or aborted.
        let xmin = active_txns
            .iter()
            .min()
            .cloned()
            .unwrap_or_else(|| self.state.next_transaction_id.load(Ordering::SeqCst));

        // xmax is the next transaction ID to be allocated. Any transaction with this ID or
        // higher has not yet started and is therefore not visible.
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

        // No active transactions
        let snapshot1 = tm.create_snapshot(0);
        assert_eq!(snapshot1.xmin, 0);
        assert_eq!(snapshot1.xmax, 0);
        assert!(snapshot1.active_transactions.is_empty());

        // Start some transactions
        let tx1 = tm.begin(); // 0
        let tx2 = tm.begin(); // 1

        let snapshot2 = tm.create_snapshot(tx2);
        assert_eq!(snapshot2.xmin, 0); // Smallest active is 0
        assert_eq!(snapshot2.xmax, 2); // Next tx id is 2
        assert!(snapshot2.active_transactions.contains(&0));
        assert!(snapshot2.active_transactions.contains(&1));
        assert_eq!(snapshot2.active_transactions.len(), 2);

        tm.commit(tx1);

        let snapshot3 = tm.create_snapshot(tx2);
        assert_eq!(snapshot3.xmin, 1); // Smallest active is now 1
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

        // Past transactions are visible
        assert!(snapshot.is_visible(5));
        // Future transactions are not visible
        assert!(!snapshot.is_visible(25));
        // Active transactions are not visible
        assert!(!snapshot.is_visible(12));
        assert!(!snapshot.is_visible(15));
        // Committed transactions in the range are visible
        assert!(snapshot.is_visible(11));
        assert!(snapshot.is_visible(19));
    }
}
