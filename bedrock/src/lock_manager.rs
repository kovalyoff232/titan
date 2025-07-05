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

#[derive(Debug, Default)]
struct WaitQueue {
    queue: Mutex<LockQueue>,
    cvar: Condvar,
}

#[derive(Debug, Default)]
pub struct LockManager {
    table: Mutex<HashMap<LockableResource, Arc<WaitQueue>>>,
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

        if guard.exclusive == Some(tx_id) || guard.sharing.contains(&tx_id) {
            return Ok(());
        }

        guard.queue.push_back(LockRequest { tx_id, mode });

        loop {
            if self.try_acquire(&mut guard, tx_id, mode) {
                return Ok(());
            }
            // TODO: Deadlock detection
            guard = wait_queue.cvar.wait(guard).unwrap();
        }
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
            // Should not happen if we just pushed to the queue
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
