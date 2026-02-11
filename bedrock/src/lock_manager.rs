use crate::TupleId;
use crate::page::TransactionId;
use dashmap::DashMap;
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
    table: DashMap<LockableResource, Arc<WaitQueue>>,

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
        let wait_queue = self.table.entry(resource).or_default().clone();

        let mut guard = wait_queue.queue.lock().unwrap();

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
                self.waits_for.lock().unwrap().remove(&tx_id);
                return Ok(());
            }

            if let Err(e) = self.update_waits_for_graph(&guard, tx_id) {
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
        waits_for_map.remove(&waiting_tx_id);

        if let Some(my_request) = queue.queue.iter().find(|req| req.tx_id == waiting_tx_id) {
            let holders = queue.get_conflicting_holders(my_request);
            if !holders.is_empty() {
                waits_for_map.insert(waiting_tx_id, holders);
            }
        }

        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();
        for tx_id in waits_for_map.keys() {
            if self.has_cycle_util(*tx_id, &mut visited, &mut recursion_stack, &waits_for_map) {
                waits_for_map.remove(&waiting_tx_id);
                return Err(LockError::Deadlock);
            }
        }

        Ok(())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn has_cycle_util(
        &self,
        tx_id: TransactionId,
        visited: &mut HashSet<TransactionId>,
        recursion_stack: &mut HashSet<TransactionId>,
        waits_for: &HashMap<TransactionId, Vec<TransactionId>>,
    ) -> bool {
        if recursion_stack.contains(&tx_id) {
            return true;
        }
        if visited.contains(&tx_id) {
            return false;
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

        if let Some(first) = queue.queue.front() {
            if first.tx_id != tx_id {
                return false;
            }
        } else {
            return false;
        }

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
        self.waits_for.lock().unwrap().remove(&tx_id);

        for entry in self.table.iter() {
            let wait_queue = entry.value();
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
                wait_queue.cvar.notify_all();
            }
        }
    }
}
