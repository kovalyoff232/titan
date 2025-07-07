//! Manages locks on database resources.

use crate::page::TransactionId;
use crate::TupleId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockableResource {
    Table(u32),
    Tuple(TupleId),
}

#[derive(Debug)]
struct LockRequest {
    tx_id: TransactionId,
    mode: LockMode,
}

#[derive(Debug, Default)]
struct LockQueue {
    queue: VecDeque<LockRequest>,
    sharing: HashSet<TransactionId>,
    exclusive: Option<TransactionId>,
}

impl LockQueue {
    /// Returns a list of transaction IDs that are holding a lock that conflicts with the given request.
    fn get_conflicting_holders(&self, request: &LockRequest) -> Vec<TransactionId> {
        let mut holders = Vec::new();
        match request.mode {
            LockMode::Shared => {
                if let Some(ex_holder) = self.exclusive {
                    if ex_holder != request.tx_id {
                        holders.push(ex_holder);
                    }
                }
            }
            LockMode::Exclusive => {
                for holder in &self.sharing {
                    if *holder != request.tx_id {
                        holders.push(*holder);
                    }
                }
                if let Some(ex_holder) = self.exclusive {
                    if ex_holder != request.tx_id {
                        holders.push(ex_holder);
                    }
                }
            }
        }
        holders
    }
}

#[derive(Debug, Default)]
struct WaitQueue {
    queue: Mutex<LockQueue>,
    cvar: Condvar,
}

#[derive(Debug, Default)]
pub struct LockManager {
    table: Mutex<HashMap<LockableResource, Arc<WaitQueue>>>,
    waits_for: Mutex<HashMap<TransactionId, Vec<TransactionId>>>,
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
        let wait_queue = {
            let mut table = self.table.lock().unwrap();
            table.entry(resource).or_default().clone()
        };

        let mut guard = wait_queue.queue.lock().unwrap();

        // If we already hold the lock in a compatible or stronger mode, we are good.
        if (mode == LockMode::Shared
            && (guard.sharing.contains(&tx_id) || guard.exclusive == Some(tx_id)))
            || (mode == LockMode::Exclusive && guard.exclusive == Some(tx_id))
        {
            return Ok(());
        }

        let request = LockRequest { tx_id, mode };
        guard.queue.push_back(request);

        loop {
            if self.try_acquire(&mut guard, tx_id, mode) {
                // Lock acquired, remove from waits-for graph
                self.waits_for.lock().unwrap().remove(&tx_id);
                return Ok(());
            }

            // --- Deadlock Detection ---
            if let Err(e) = self.update_waits_for_graph(&guard, tx_id) {
                // A deadlock was detected. This transaction is the victim.
                // We need to remove our request from the queue and return the error.
                guard.queue.retain(|req| req.tx_id != tx_id);
                return Err(e);
            }

            guard = wait_queue.cvar.wait(guard).unwrap();
        }
    }

    fn update_waits_for_graph(
        &self,
        queue: &LockQueue,
        waiting_tx_id: TransactionId,
    ) -> Result<(), LockError> {
        let mut waits_for_map = self.waits_for.lock().unwrap();
        waits_for_map.remove(&waiting_tx_id); // Clear old dependencies for this tx

        // Find the request for the current transaction in the queue
        if let Some(my_request) = queue.queue.iter().find(|req| req.tx_id == waiting_tx_id) {
            // Find who holds the lock that I am waiting for
            let holders = queue.get_conflicting_holders(my_request);
            if !holders.is_empty() {
                waits_for_map.insert(waiting_tx_id, holders);
            }
        }

        // Check for cycles
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();
        for tx_id in waits_for_map.keys() {
            if self.has_cycle_util(*tx_id, &mut visited, &mut recursion_stack, &waits_for_map) {
                // Cycle detected, abort the current transaction
                waits_for_map.remove(&waiting_tx_id);
                return Err(LockError::Deadlock);
            }
        }

        Ok(())
    }

    fn has_cycle_util(
        &self,
        tx_id: TransactionId,
        visited: &mut HashSet<TransactionId>,
        recursion_stack: &mut HashSet<TransactionId>,
        waits_for: &HashMap<TransactionId, Vec<TransactionId>>,
    ) -> bool {
        if recursion_stack.contains(&tx_id) {
            return true; // Cycle detected
        }
        if visited.contains(&tx_id) {
            return false; // Already checked this node
        }

        visited.insert(tx_id);
        recursion_stack.insert(tx_id);

        if let Some(waits_for_txs) = waits_for.get(&tx_id) {
            for waited_tx in waits_for_txs {
                if self.has_cycle_util(*waited_tx, visited, recursion_stack, waits_for) {
                    return true;
                }
            }
        }

        recursion_stack.remove(&tx_id);
        false
    }

    fn try_acquire(&self, queue: &mut LockQueue, tx_id: TransactionId, mode: LockMode) -> bool {
        if self.is_locked_for(queue, tx_id, mode) {
            return false;
        }

        // Check if it's our turn in the queue
        if let Some(first) = queue.queue.front() {
            if first.tx_id != tx_id {
                return false;
            }
        } else {
            return false;
        }

        // Grant the lock
        queue.queue.pop_front();
        match mode {
            LockMode::Shared => {
                queue.sharing.insert(tx_id);
            }
            LockMode::Exclusive => {
                queue.exclusive = Some(tx_id);
            }
        }
        true
    }

    fn is_locked_for(&self, queue: &LockQueue, tx_id: TransactionId, mode: LockMode) -> bool {
        match mode {
            LockMode::Shared => {
                if let Some(ex_tx) = queue.exclusive {
                    return ex_tx != tx_id;
                }
            }
            LockMode::Exclusive => {
                if let Some(ex_tx) = queue.exclusive {
                    return ex_tx != tx_id;
                }
                if !queue.sharing.is_empty()
                    && (queue.sharing.len() > 1 || !queue.sharing.contains(&tx_id))
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn unlock_all(&self, tx_id: TransactionId) {
        let table = self.table.lock().unwrap();
        let mut to_notify = Vec::new();

        // Also remove the transaction from the waits-for graph
        self.waits_for.lock().unwrap().remove(&tx_id);

        for (resource, wait_queue) in table.iter() {
            let mut queue = wait_queue.queue.lock().unwrap();
            let mut changed = false;
            if queue.exclusive == Some(tx_id) {
                queue.exclusive = None;
                changed = true;
            }
            if queue.sharing.remove(&tx_id) {
                changed = true;
            }
            queue.queue.retain(|req| req.tx_id != tx_id);

            if changed {
                to_notify.push(resource.clone());
            }
        }

        // We must release the table lock before notifying
        drop(table);

        for resource in to_notify {
            let table = self.table.lock().unwrap();
            if let Some(wait_queue) = table.get(&resource) {
                wait_queue.cvar.notify_all();
            }
        }
    }
}
