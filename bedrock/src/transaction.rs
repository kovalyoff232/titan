//! Transaction management.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use crate::page::TransactionId;

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
    next_oid: AtomicU32,
}

/// The transaction manager. Designed to be shared across threads.
#[derive(Debug, Clone)]
pub struct TransactionManager {
    state: Arc<TransactionManagerState>,
}

impl TransactionManager {
    pub fn new(initial_tx_id: TransactionId) -> Self {
        println!("[TM::new] Initializing with next_transaction_id = {}", initial_tx_id);
        let state = TransactionManagerState {
            next_transaction_id: AtomicU32::new(initial_tx_id),
            active_transactions: Mutex::new(HashSet::new()),
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
        let tx_id = self.state.next_transaction_id.fetch_add(1, Ordering::SeqCst);
        self.state.active_transactions.lock().unwrap().insert(tx_id);
        println!("[TM::begin] Started tx_id: {}. Active transactions: {:?}", tx_id, self.state.active_transactions.lock().unwrap());
        tx_id
    }

    /// Commits a transaction.
    pub fn commit(&self, tx_id: TransactionId) {
        self.state.active_transactions.lock().unwrap().remove(&tx_id);
        println!("[TM::commit] Committed tx_id: {}. Active transactions: {:?}", tx_id, self.state.active_transactions.lock().unwrap());
    }

    /// Aborts a transaction.
    pub fn abort(&self, tx_id: TransactionId) {
        // For now, aborting is the same as committing as we don't have UNDO logs yet.
        self.state.active_transactions.lock().unwrap().remove(&tx_id);
        println!("[TM::abort] Aborted tx_id: {}. Active transactions: {:?}", tx_id, self.state.active_transactions.lock().unwrap());
    }

    /// Creates a new snapshot of the current database state.
    pub fn create_snapshot(&self, current_tx_id: TransactionId) -> Snapshot {
        let active_txns = self.state.active_transactions.lock().unwrap();
        
        let xmin = active_txns.iter()
            .filter(|&&tx| tx != current_tx_id)
            .min()
            .cloned()
            .unwrap_or_else(|| self.state.next_transaction_id.load(Ordering::SeqCst));

        let xmax = self.state.next_transaction_id.load(Ordering::SeqCst);
        
        let snapshot = Snapshot {
            xmin,
            xmax,
            active_transactions: Arc::new(active_txns.clone()),
        };
        println!("[TM::create_snapshot] Created for tx {}: {:?}", current_tx_id, snapshot);
        snapshot
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_snapshot() {
        let tm = TransactionManager::new();
        
        // No active transactions
        let snapshot1 = tm.create_snapshot();
        assert_eq!(snapshot1.xmin, 0);
        assert_eq!(snapshot1.xmax, 0);
        assert!(snapshot1.active_transactions.is_empty());

        // Start some transactions
        let tx1 = tm.begin(); // 0
        let tx2 = tm.begin(); // 1
        
        let snapshot2 = tm.create_snapshot();
        assert_eq!(snapshot2.xmin, 0); // Smallest active is 0
        assert_eq!(snapshot2.xmax, 2); // Next tx id is 2
        assert!(snapshot2.active_transactions.contains(&0));
        assert!(snapshot2.active_transactions.contains(&1));
        assert_eq!(snapshot2.active_transactions.len(), 2);

        tm.commit(tx1);

        let snapshot3 = tm.create_snapshot();
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
