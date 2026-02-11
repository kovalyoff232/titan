use super::{Executor, Row, evaluate_expr_for_row, parse_tuple};
use crate::errors::ExecutionError;
use crate::parser::{Expression, LiteralValue};
use crate::types::Column;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::Snapshot;
use bedrock::{PageId, btree};
use std::sync::Arc;

pub(super) struct TableScanExecutor<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    lm: &'a Arc<LockManager>,
    schema: Vec<Column>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    for_update: bool,
    filter: Option<Expression>,
    current_page_id: PageId,
    current_item_id: u16,
}

impl<'a> TableScanExecutor<'a> {
    pub(super) fn new(
        bpm: &'a Arc<BufferPoolManager>,
        lm: &'a Arc<LockManager>,
        first_page_id: PageId,
        schema: Vec<Column>,
        tx_id: u32,
        snapshot: &'a Snapshot,
        for_update: bool,
        filter: Option<Expression>,
    ) -> Self {
        Self {
            bpm,
            lm,
            schema,
            tx_id,
            snapshot,
            for_update,
            filter,
            current_page_id: first_page_id,
            current_item_id: 0,
        }
    }
}

impl<'a> Executor for TableScanExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            if self.current_page_id == INVALID_PAGE_ID {
                return Ok(None);
            }

            let page_guard = self.bpm.acquire_page(self.current_page_id)?;
            let page = page_guard.read();

            if self.current_item_id >= page.get_tuple_count() {
                self.current_page_id = page.read_header().next_page_id;
                self.current_item_id = 0;
                continue;
            }

            let item_id = self.current_item_id;
            self.current_item_id += 1;

            if page.is_visible(self.snapshot, self.tx_id, item_id) {
                if self.for_update {
                    self.lm.lock(
                        self.tx_id,
                        LockableResource::Tuple((self.current_page_id, item_id)),
                        LockMode::Exclusive,
                    )?;
                }
                if let Some(tuple_data) = page.get_tuple(item_id) {
                    let parsed_row = parse_tuple(tuple_data, &self.schema);
                    if let Some(predicate) = &self.filter {
                        if !evaluate_expr_for_row(predicate, &parsed_row)? {
                            continue;
                        }
                    }
                    let row: Row = self
                        .schema
                        .iter()
                        .map(|col| {
                            let val = parsed_row.get(&col.name).unwrap();
                            match val {
                                LiteralValue::Bool(b) => (if *b { "t" } else { "f" }).to_string(),
                                _ => val.to_string(),
                            }
                        })
                        .collect();
                    return Ok(Some(row));
                }
            }
        }
    }
}

pub(super) struct IndexScanExecutor<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    schema: Vec<Column>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    tuple_id: Option<(PageId, u16)>,
    done: bool,
}

impl<'a> IndexScanExecutor<'a> {
    pub(super) fn new(
        bpm: &'a Arc<BufferPoolManager>,
        schema: Vec<Column>,
        index_root_page_id: PageId,
        key: i32,
        tx_id: u32,
        snapshot: &'a Snapshot,
    ) -> Self {
        let tuple_id = btree::btree_search(bpm, index_root_page_id, key).unwrap_or(None);
        Self {
            bpm,
            schema,
            tx_id,
            snapshot,
            tuple_id,
            done: false,
        }
    }
}

impl<'a> Executor for IndexScanExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        if self.done || self.tuple_id.is_none() {
            return Ok(None);
        }
        self.done = true;

        let (page_id, item_id) = self.tuple_id.unwrap();
        let page_guard = self.bpm.acquire_page(page_id)?;
        let page = page_guard.read();

        if page.is_visible(self.snapshot, self.tx_id, item_id) {
            if let Some(tuple_data) = page.get_tuple(item_id) {
                let parsed = parse_tuple(tuple_data, &self.schema);
                let row = self
                    .schema
                    .iter()
                    .map(|col| parsed.get(&col.name).unwrap().to_string())
                    .collect();
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}
