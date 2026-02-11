use super::eval::evaluate_expr_for_row;
use super::helpers::parse_tuple;
use super::{Executor, Row};
use crate::errors::ExecutionError;
use crate::parser::{Expression, LiteralValue};
use crate::types::Column;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::Snapshot;
use bedrock::{PageId, btree};
use std::collections::HashMap;
use std::sync::Arc;

fn materialize_row(
    schema: &[Column],
    parsed_row: &HashMap<String, LiteralValue>,
) -> Result<Row, ExecutionError> {
    let mut row = Vec::with_capacity(schema.len());
    for col in schema {
        let value = parsed_row.get(&col.name).ok_or_else(|| {
            ExecutionError::GenericError(format!(
                "Tuple decode mismatch: missing column {} in scanned row",
                col.name
            ))
        })?;
        match value {
            LiteralValue::Bool(b) => row.push((if *b { "t" } else { "f" }).to_string()),
            _ => row.push(value.to_string()),
        }
    }
    Ok(row)
}

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

pub(super) struct TableScanConfig<'a> {
    pub(super) first_page_id: PageId,
    pub(super) schema: Vec<Column>,
    pub(super) tx_id: u32,
    pub(super) snapshot: &'a Snapshot,
    pub(super) for_update: bool,
    pub(super) filter: Option<Expression>,
}

impl<'a> TableScanExecutor<'a> {
    pub(super) fn new(
        bpm: &'a Arc<BufferPoolManager>,
        lm: &'a Arc<LockManager>,
        config: TableScanConfig<'a>,
    ) -> Self {
        Self {
            bpm,
            lm,
            schema: config.schema,
            tx_id: config.tx_id,
            snapshot: config.snapshot,
            for_update: config.for_update,
            filter: config.filter,
            current_page_id: config.first_page_id,
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
                    let row = materialize_row(&self.schema, &parsed_row)?;
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
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let Some((page_id, item_id)) = self.tuple_id else {
            return Ok(None);
        };
        let page_guard = self.bpm.acquire_page(page_id)?;
        let page = page_guard.read();

        if page.is_visible(self.snapshot, self.tx_id, item_id) {
            if let Some(tuple_data) = page.get_tuple(item_id) {
                let parsed = parse_tuple(tuple_data, &self.schema);
                let row = materialize_row(&self.schema, &parsed)?;
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::materialize_row;
    use crate::parser::LiteralValue;
    use crate::types::Column;
    use std::collections::HashMap;

    #[test]
    fn materialize_row_returns_error_for_missing_column() {
        let schema = vec![
            Column {
                name: "id".to_string(),
                type_id: 23,
            },
            Column {
                name: "name".to_string(),
                type_id: 25,
            },
        ];
        let mut parsed = HashMap::new();
        parsed.insert("id".to_string(), LiteralValue::Number("1".to_string()));

        let result = materialize_row(&schema, &parsed);
        assert!(result.is_err());
    }
}
