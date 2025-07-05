//! Manages locks on database resources.

use crate::page::TransactionId;
use crate::TupleId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};

/// Represents the different modes of locking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
    ReadForUpdate,
}

/// Represents a resource that can be locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockableResource {
    Table(u32), // OID of the table
    Tuple(TupleId),
}

/// A request for a lock by a transaction.
#[derive(Debug, Clone)]
struct LockRequest {
    tx_id: TransactionId,
    mode: LockMode,
    granted: bool,
}

/// A queue of lock requests for a specific resource.
#[derive(Debug, Default)]
struct LockRequestQueue {
    requests: VecDeque<LockRequest>,
    // To avoid iterating the whole queue, we can cache the number of granted locks of each type.
    shared_granted: usize,
    exclusive_granted: bool,
}

/// The main lock manager struct.
#[derive(Debug, Default)]
pub struct LockManager {
    /// The main table mapping resources to their lock queues.
    lock_table: Mutex<HashMap<LockableResource, LockRequestQueue>>,
    /// A condition variable to allow transactions to wait for locks.
    cvar: Condvar,
    /// The waits-for graph for deadlock detection.
    /// Maps a waiting transaction to the set of transactions it's waiting for.
    waits_for: Mutex<HashMap<TransactionId, HashSet<TransactionId>>>,
}

#[derive(Debug)]
pub enum LockError {
    Deadlock,
}

impl LockManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(
        &self,
        tx_id: TransactionId,
        resource: LockableResource,
        mode: LockMode,
    ) -> Result<(), LockError> {
        let mut lock_table = self.lock_table.lock().unwrap();

        // Check if this transaction already holds a lock.
        // If so, we might need to upgrade it. For now, we don't support lock upgrades.
        // We'll just assume a transaction doesn't re-lock a resource it already has.

        let queue = lock_table.entry(resource).or_default();
        queue.requests.push_back(LockRequest {
            tx_id,
            mode,
            granted: false,
        });

        loop {
            // Try to grant locks. This function will handle the logic of checking compatibility.
            self.try_grant_locks(resource, &mut lock_table);

            // Check if our request was granted.
            let our_request_granted = lock_table
                .get(&resource)
                .and_then(|q| q.requests.iter().find(|r| r.tx_id == tx_id))
                .map_or(false, |r| r.granted);

            if our_request_granted {
                self.remove_from_waits_for(tx_id, &mut lock_table);
                return Ok(());
            }

            // If not granted, we must wait. First, update the waits-for graph and check for deadlocks.
            self.update_waits_for_graph(tx_id, &lock_table);
            if self.detect_deadlock(tx_id, &lock_table) {
                // Deadlock detected. We need to abort this transaction.
                // Remove its request from the queue.
                let queue = lock_table.get_mut(&resource).unwrap();
                queue.requests.retain(|r| r.tx_id != tx_id);
                if queue.requests.is_empty() {
                    lock_table.remove(&resource);
                }
                self.remove_from_waits_for(tx_id, &mut lock_table);
                self.cvar.notify_all(); // Wake up others
                return Err(LockError::Deadlock);
            }

            // Wait for another transaction to release a lock.
            lock_table = self.cvar.wait(lock_table).unwrap();
        }
    }

    pub fn unlock_all(&self, tx_id: TransactionId) {
        let mut lock_table = self.lock_table.lock().unwrap();
        let mut affected_resources = Vec::new();

        lock_table.retain(|resource, queue| {
            let old_len = queue.requests.len();
            queue.requests.retain(|req| req.tx_id != tx_id);
            if queue.requests.len() < old_len {
                affected_resources.push(*resource);
            }
            !queue.requests.is_empty()
        });

        self.remove_from_waits_for(tx_id, &mut lock_table);

        for resource in affected_resources {
            if let Some(queue) = lock_table.get_mut(&resource) {
                // Recalculate granted counts after unlocks
                queue.shared_granted = queue
                    .requests
                    .iter()
                    .filter(|r| r.granted && r.mode == LockMode::Shared)
                    .count();
                queue.exclusive_granted = queue
                    .requests
                    .iter()
                    .any(|r| r.granted && (r.mode == LockMode::Exclusive || r.mode == LockMode::ReadForUpdate));
            }
            self.try_grant_locks(resource, &mut lock_table);
        }

        self.cvar.notify_all();
    }

    fn try_grant_locks(
        &self,
        resource: LockableResource,
        lock_table: &mut HashMap<LockableResource, LockRequestQueue>,
    ) {
        let queue = match lock_table.get_mut(&resource) {
            Some(q) => q,
            None => return,
        };

        for req in queue.requests.iter_mut() {
            if req.granted {
                continue;
            }

            // ReadForUpdate acts like an exclusive lock for compatibility checks.
            let compatible = match req.mode {
                LockMode::Shared => !queue.exclusive_granted,
                LockMode::Exclusive | LockMode::ReadForUpdate => queue.shared_granted == 0 && !queue.exclusive_granted,
            };

            if compatible {
                req.granted = true;
                match req.mode {
                    LockMode::Shared => queue.shared_granted += 1,
                    LockMode::Exclusive | LockMode::ReadForUpdate => queue.exclusive_granted = true,
                }
            } else {
                // First incompatible request stops further grants to maintain FIFO order.
                break;
            }
        }
    }

    fn update_waits_for_graph(
        &self,
        waiter_tx_id: TransactionId,
        lock_table: &HashMap<LockableResource, LockRequestQueue>,
    ) {
        let mut waits_for = self.waits_for.lock().unwrap();
        let waiting_for_set = waits_for.entry(waiter_tx_id).or_default();
        waiting_for_set.clear();

        for queue in lock_table.values() {
            // Find the request of the waiting transaction
            if let Some(waiter_req) = queue.requests.iter().find(|r| r.tx_id == waiter_tx_id && !r.granted) {
                // The waiter is waiting for all transactions that hold an incompatible lock on this resource.
                for holder in queue.requests.iter().filter(|r| r.granted) {
                    let compatible = match waiter_req.mode {
                        LockMode::Shared => holder.mode != LockMode::Exclusive && holder.mode != LockMode::ReadForUpdate,
                        LockMode::Exclusive | LockMode::ReadForUpdate => false,
                    };
                    if !compatible && holder.tx_id != waiter_tx_id {
                        waiting_for_set.insert(holder.tx_id);
                    }
                }
            }
        }
    }

    fn remove_from_waits_for(
        &self,
        tx_id: TransactionId,
        _lock_table: &mut HashMap<LockableResource, LockRequestQueue>,
    ) {
        let mut waits_for = self.waits_for.lock().unwrap();
        waits_for.remove(&tx_id);
        for (_, waiting_set) in waits_for.iter_mut() {
            waiting_set.remove(&tx_id);
        }
    }

    fn detect_deadlock(
        &self,
        start_tx_id: TransactionId,
        _lock_table: &HashMap<LockableResource, LockRequestQueue>,
    ) -> bool {
        let waits_for = self.waits_for.lock().unwrap();
        let mut visited = HashSet::new();
        let mut path = HashSet::new();
        self.dfs_detect(start_tx_id, &waits_for, &mut visited, &mut path)
    }

    fn dfs_detect(
        &self,
        current_tx_id: TransactionId,
        waits_for: &HashMap<TransactionId, HashSet<TransactionId>>,
        visited: &mut HashSet<TransactionId>,
        path: &mut HashSet<TransactionId>,
    ) -> bool {
        visited.insert(current_tx_id);
        path.insert(current_tx_id);

        if let Some(waits_for_set) = waits_for.get(&current_tx_id) {
            for &next_tx_id in waits_for_set {
                if path.contains(&next_tx_id) {
                    return true; // Cycle detected
                }
                if !visited.contains(&next_tx_id) {
                    if self.dfs_detect(next_tx_id, waits_for, visited, path) {
                        return true;
                    }
                }
            }
        }

        path.remove(&current_tx_id);
        false
    }
}
