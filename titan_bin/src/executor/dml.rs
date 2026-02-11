use super::eval::{evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use super::helpers::{
    IntIndexInfo, extract_i32_key, load_convention_int_indexes, parse_tuple,
    update_index_root_if_needed,
};
use super::{Executor, Row};
use crate::catalog::{SystemCatalog, update_pg_class_page_id};
use crate::errors::ExecutionError;
use crate::parser::{DeleteStatement, Expression, InsertStatement, LiteralValue, UpdateStatement};
use crate::types::Column;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::{WalManager, WalRecord};
use bedrock::{PageId, btree};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub(super) struct DmlCtx<'a> {
    pub(super) bpm: &'a Arc<BufferPoolManager>,
    pub(super) tm: &'a Arc<TransactionManager>,
    pub(super) lm: &'a Arc<LockManager>,
    pub(super) wm: &'a Arc<Mutex<WalManager>>,
    pub(super) tx_id: u32,
    pub(super) snapshot: &'a Snapshot,
    pub(super) system_catalog: &'a Arc<Mutex<SystemCatalog>>,
}

pub(super) struct InsertPathCtx<'a> {
    pub(super) bpm: &'a Arc<BufferPoolManager>,
    pub(super) tm: &'a Arc<TransactionManager>,
    pub(super) wm: &'a Arc<Mutex<WalManager>>,
    pub(super) tx_id: u32,
    pub(super) snapshot: &'a Snapshot,
}

pub(super) type ParsedRow = HashMap<String, LiteralValue>;
pub(super) type ScannedRow = (PageId, u16, ParsedRow);

fn lock_system_catalog<'a>(
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
) -> Result<MutexGuard<'a, SystemCatalog>, ExecutionError> {
    system_catalog
        .lock()
        .map_err(|_| ExecutionError::GenericError("system catalog lock poisoned".to_string()))
}

fn lock_wal<'a>(
    wm: &'a Arc<Mutex<WalManager>>,
) -> Result<MutexGuard<'a, WalManager>, ExecutionError> {
    wm.lock()
        .map_err(|_| ExecutionError::GenericError("wal lock poisoned".to_string()))
}

fn parse_i32_literal(value: &str, context: &str) -> Result<i32, ExecutionError> {
    value.parse::<i32>().map_err(|_| {
        ExecutionError::GenericError(format!("Invalid integer value in {}: {}", context, value))
    })
}

fn epoch_date() -> Result<NaiveDate, ExecutionError> {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .ok_or_else(|| ExecutionError::GenericError("failed to construct epoch date".to_string()))
}

pub(super) fn execute_insert(
    stmt: &InsertStatement,
    ctx: &DmlCtx<'_>,
) -> Result<u32, ExecutionError> {
    let (table_oid, mut first_page_id) = lock_system_catalog(ctx.system_catalog)?
        .find_table(&stmt.table_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

    ctx.lm.lock(
        ctx.tx_id,
        LockableResource::Table(table_oid),
        LockMode::Shared,
    )?;

    let tuple_data = serialize_expressions(&stmt.values)?;

    insert_tuple_and_update_indexes(ctx, table_oid, &mut first_page_id, &tuple_data)?;

    Ok(1)
}

fn insert_tuple_and_update_indexes(
    ctx: &DmlCtx<'_>,
    table_oid: u32,
    first_page_id: &mut PageId,
    tuple_data: &[u8],
) -> Result<(), ExecutionError> {
    let insert_ctx = InsertPathCtx {
        bpm: ctx.bpm,
        tm: ctx.tm,
        wm: ctx.wm,
        tx_id: ctx.tx_id,
        snapshot: ctx.snapshot,
    };
    let page_id =
        find_or_create_page_for_insert(&insert_ctx, table_oid, first_page_id, tuple_data.len())?;

    let item_id = insert_tuple_and_log(ctx.bpm, ctx.tm, ctx.wm, ctx.tx_id, page_id, tuple_data)?;

    let schema = lock_system_catalog(ctx.system_catalog)?.get_table_schema(
        ctx.bpm,
        table_oid,
        ctx.tx_id,
        ctx.snapshot,
    )?;
    let parsed_tuple = parse_tuple(tuple_data, &schema);
    let mut indexes = load_convention_int_indexes(
        ctx.bpm,
        ctx.tx_id,
        ctx.snapshot,
        ctx.system_catalog,
        &schema,
    )?;

    for index in &mut indexes {
        let Some(key) = extract_i32_key(&parsed_tuple, &index.column_name) else {
            continue;
        };

        let new_root_page_id = btree::btree_insert(
            ctx.bpm,
            ctx.tm,
            ctx.wm,
            ctx.tx_id,
            index.root_page_id,
            key,
            (page_id, item_id),
        )?;
        update_index_root_if_needed(
            ctx.bpm,
            ctx.tm,
            ctx.wm,
            ctx.tx_id,
            ctx.snapshot,
            index,
            new_root_page_id,
        )?;
    }

    Ok(())
}

pub(super) fn execute_update(
    stmt: &UpdateStatement,
    ctx: &DmlCtx<'_>,
) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = lock_system_catalog(ctx.system_catalog)?
        .find_table(&stmt.table_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(0);
    }

    let schema = lock_system_catalog(ctx.system_catalog)?.get_table_schema(
        ctx.bpm,
        table_oid,
        ctx.tx_id,
        ctx.snapshot,
    )?;
    let mut indexes = load_convention_int_indexes(
        ctx.bpm,
        ctx.tx_id,
        ctx.snapshot,
        ctx.system_catalog,
        &schema,
    )?;
    let mut rows_affected = 0;
    let all_rows = scan_table(
        ctx.bpm,
        ctx.lm,
        first_page_id,
        &schema,
        ctx.tx_id,
        ctx.snapshot,
        false,
    )?;
    let mut rows_to_update = Vec::new();
    for row_entry in all_rows {
        let should_update = if let Some(predicate) = &stmt.where_clause {
            evaluate_expr_for_row(predicate, &row_entry.2)?
        } else {
            true
        };
        if should_update {
            rows_to_update.push(row_entry);
        }
    }

    for (page_id, item_id, old_parsed) in rows_to_update {
        ctx.lm.lock(
            ctx.tx_id,
            LockableResource::Tuple((page_id, item_id)),
            LockMode::Exclusive,
        )?;
        let page_guard = ctx.bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();
        if !page.is_visible(ctx.snapshot, ctx.tx_id, item_id)
            || page
                .get_item_id_data(item_id)
                .map(|d| page.read_tuple_header(d.offset).xmax != 0)
                .unwrap_or(true)
        {
            ctx.lm.unlock_all(ctx.tx_id);
            return Err(ExecutionError::SerializationFailure);
        }

        let mut new_parsed = old_parsed.clone();
        for (col_name, expr) in &stmt.assignments {
            new_parsed.insert(
                col_name.clone(),
                evaluate_expr_for_row_to_val(expr, &old_parsed)?,
            );
        }

        let new_data = serialize_literal_map(&new_parsed, &schema)?;

        let before_page = page.data.to_vec();
        let item_id_data = page.get_item_id_data(item_id).ok_or_else(|| {
            ExecutionError::GenericError("missing item metadata for visible tuple".to_string())
        })?;
        let mut header = page.read_tuple_header(item_id_data.offset);
        header.xmax = ctx.tx_id;
        page.write_tuple_header(item_id_data.offset, &header);

        let Some(new_item_id) = page.add_tuple(&new_data, ctx.tx_id, 0) else {
            let mut header = page.read_tuple_header(item_id_data.offset);
            header.xmax = 0;
            page.write_tuple_header(item_id_data.offset, &header);
            ctx.lm.unlock_all(ctx.tx_id);
            return Err(ExecutionError::SerializationFailure);
        };
        let after_page = page.data.to_vec();

        rows_affected += 1;
        let prev_lsn = ctx.tm.get_last_lsn(ctx.tx_id).unwrap_or(0);
        let lsn = lock_wal(ctx.wm)?.log(
            ctx.tx_id,
            prev_lsn,
            &WalRecord::UpdateTuple {
                tx_id: ctx.tx_id,
                page_id,
                item_id: new_item_id,
                before_page,
                after_page,
            },
        )?;
        ctx.tm.set_last_lsn(ctx.tx_id, lsn);
        let mut page_header = page.read_header();
        page_header.lsn = lsn;
        page.write_header(&page_header);
        drop(page);

        for index in &mut indexes {
            let old_key = extract_i32_key(&old_parsed, &index.column_name);
            let new_key = extract_i32_key(&new_parsed, &index.column_name);

            if old_key == new_key {
                continue;
            }

            if let Some(key) = old_key {
                let new_root_page_id = btree::btree_delete(
                    ctx.bpm,
                    ctx.tm,
                    ctx.wm,
                    ctx.tx_id,
                    index.root_page_id,
                    key,
                )?;
                update_index_root_if_needed(
                    ctx.bpm,
                    ctx.tm,
                    ctx.wm,
                    ctx.tx_id,
                    ctx.snapshot,
                    index,
                    new_root_page_id,
                )?;
            }

            if let Some(key) = new_key {
                let new_root_page_id = btree::btree_insert(
                    ctx.bpm,
                    ctx.tm,
                    ctx.wm,
                    ctx.tx_id,
                    index.root_page_id,
                    key,
                    (page_id, new_item_id),
                )?;
                update_index_root_if_needed(
                    ctx.bpm,
                    ctx.tm,
                    ctx.wm,
                    ctx.tx_id,
                    ctx.snapshot,
                    index,
                    new_root_page_id,
                )?;
            }
        }
    }
    Ok(rows_affected)
}

pub(super) fn execute_delete(
    stmt: &DeleteStatement,
    ctx: &DmlCtx<'_>,
) -> Result<u32, ExecutionError> {
    let mut executor = DeleteExecutor::new(stmt, ctx)?;
    let result = executor.next()?.ok_or_else(|| {
        ExecutionError::GenericError("delete executor did not produce result row".to_string())
    })?;
    let rows_deleted = result
        .first()
        .ok_or_else(|| ExecutionError::GenericError("missing rows_deleted field".to_string()))?
        .parse::<u32>()
        .map_err(|_| ExecutionError::GenericError("invalid rows_deleted value".to_string()))?;
    Ok(rows_deleted)
}

pub(super) fn scan_table(
    bpm: &Arc<BufferPoolManager>,
    lm: &Arc<LockManager>,
    first_page_id: PageId,
    schema: &[Column],
    tx_id: u32,
    snapshot: &Snapshot,
    for_update: bool,
) -> Result<Vec<ScannedRow>, ExecutionError> {
    let mut rows = Vec::new();
    if first_page_id == INVALID_PAGE_ID {
        return Ok(rows);
    }
    let mut current_page_id = first_page_id;
    while current_page_id != INVALID_PAGE_ID {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let page = page_guard.read();
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if for_update {
                    lm.lock(
                        tx_id,
                        LockableResource::Tuple((current_page_id, i)),
                        LockMode::Exclusive,
                    )?;
                }
                if let Some(tuple_data) = page.get_tuple(i) {
                    rows.push((current_page_id, i, parse_tuple(tuple_data, schema)));
                }
            }
        }
        current_page_id = page.read_header().next_page_id;
    }
    Ok(rows)
}

struct DeleteExecutor<'a> {
    stmt: &'a DeleteStatement,
    bpm: &'a Arc<BufferPoolManager>,
    tm: &'a Arc<TransactionManager>,
    lm: &'a Arc<LockManager>,
    wm: &'a Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    table_schema: Vec<Column>,
    indexes: Vec<IntIndexInfo>,
    output_schema: Vec<Column>,
    current_page_id: PageId,
    current_item_id: u16,
    rows_affected: u32,
    done: bool,
}

impl<'a> DeleteExecutor<'a> {
    fn new(stmt: &'a DeleteStatement, ctx: &'a DmlCtx<'a>) -> Result<Self, ExecutionError> {
        let (table_oid, first_page_id) = lock_system_catalog(ctx.system_catalog)?
            .find_table(&stmt.table_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
            .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

        let table_schema = lock_system_catalog(ctx.system_catalog)?.get_table_schema(
            ctx.bpm,
            table_oid,
            ctx.tx_id,
            ctx.snapshot,
        )?;
        let indexes = load_convention_int_indexes(
            ctx.bpm,
            ctx.tx_id,
            ctx.snapshot,
            ctx.system_catalog,
            &table_schema,
        )?;

        let output_schema = vec![Column {
            name: "rows_deleted".to_string(),
            type_id: 23,
        }];

        Ok(Self {
            stmt,
            bpm: ctx.bpm,
            tm: ctx.tm,
            lm: ctx.lm,
            wm: ctx.wm,
            tx_id: ctx.tx_id,
            snapshot: ctx.snapshot,
            table_schema,
            indexes,
            output_schema,
            current_page_id: first_page_id,
            current_item_id: 0,
            rows_affected: 0,
            done: false,
        })
    }
}

impl<'a> Executor for DeleteExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.output_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        if self.done {
            return Ok(None);
        }

        loop {
            if self.current_page_id == INVALID_PAGE_ID {
                self.done = true;
                return Ok(Some(vec![self.rows_affected.to_string()]));
            }

            let page_guard = self.bpm.acquire_page(self.current_page_id)?;
            let mut page = page_guard.write();

            if self.current_item_id >= page.get_tuple_count() {
                self.current_page_id = page.read_header().next_page_id;
                self.current_item_id = 0;
                continue;
            }

            let item_id = self.current_item_id;
            self.current_item_id += 1;

            if page.is_visible(self.snapshot, self.tx_id, item_id) {
                if let Some(tuple_data) = page.get_tuple(item_id) {
                    let parsed_row = parse_tuple(tuple_data, &self.table_schema);
                    let should_delete = if let Some(predicate) = &self.stmt.where_clause {
                        evaluate_expr_for_row(predicate, &parsed_row)?
                    } else {
                        true
                    };
                    if should_delete {
                        self.lm.lock(
                            self.tx_id,
                            LockableResource::Tuple((self.current_page_id, item_id)),
                            LockMode::Exclusive,
                        )?;

                        if let Some(item_id_data) = page.get_item_id_data(item_id) {
                            let before_page = page.data.to_vec();
                            let mut header = page.read_tuple_header(item_id_data.offset);
                            if header.xmax != 0 {
                                return Err(ExecutionError::SerializationFailure);
                            }
                            header.xmax = self.tx_id;
                            page.write_tuple_header(item_id_data.offset, &header);
                            self.rows_affected += 1;
                            let after_page = page.data.to_vec();

                            let prev_lsn = self.tm.get_last_lsn(self.tx_id).unwrap_or(0);
                            let lsn = lock_wal(self.wm)?.log(
                                self.tx_id,
                                prev_lsn,
                                &WalRecord::DeleteTuple {
                                    tx_id: self.tx_id,
                                    page_id: self.current_page_id,
                                    item_id,
                                    before_page,
                                    after_page,
                                },
                            )?;
                            self.tm.set_last_lsn(self.tx_id, lsn);
                            let mut page_header = page.read_header();
                            page_header.lsn = lsn;
                            page.write_header(&page_header);
                            drop(page);

                            for index in &mut self.indexes {
                                let Some(key) = extract_i32_key(&parsed_row, &index.column_name)
                                else {
                                    continue;
                                };
                                let new_root_page_id = btree::btree_delete(
                                    self.bpm,
                                    self.tm,
                                    self.wm,
                                    self.tx_id,
                                    index.root_page_id,
                                    key,
                                )?;
                                update_index_root_if_needed(
                                    self.bpm,
                                    self.tm,
                                    self.wm,
                                    self.tx_id,
                                    self.snapshot,
                                    index,
                                    new_root_page_id,
                                )?;
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn serialize_expressions(values: &[Expression]) -> Result<Vec<u8>, ExecutionError> {
    let mut tuple_data = Vec::new();
    let epoch = epoch_date()?;
    for value in values {
        match value {
            Expression::Literal(LiteralValue::Number(n)) => tuple_data
                .extend_from_slice(&parse_i32_literal(n, "insert expression")?.to_be_bytes()),
            Expression::Literal(LiteralValue::String(s)) => {
                tuple_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                tuple_data.extend_from_slice(s.as_bytes());
            }
            Expression::Literal(LiteralValue::Bool(b)) => tuple_data.push(if *b { 1 } else { 0 }),
            Expression::Literal(LiteralValue::Date(s)) => {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| ExecutionError::GenericError(e.to_string()))?;
                let days = date.signed_duration_since(epoch).num_days() as i32;
                tuple_data.extend_from_slice(&days.to_be_bytes());
            }
            Expression::Literal(LiteralValue::Null) => {
                tuple_data.extend_from_slice(&0i32.to_be_bytes());
            }
            _ => {
                return Err(ExecutionError::GenericError(
                    "Unsupported expression type for insert".to_string(),
                ));
            }
        }
    }
    Ok(tuple_data)
}

pub(super) fn serialize_literal_map(
    map: &HashMap<String, LiteralValue>,
    schema: &[Column],
) -> Result<Vec<u8>, ExecutionError> {
    let mut new_data = Vec::new();
    let epoch = epoch_date()?;
    for col in schema {
        if let Some(val) = map.get(&col.name) {
            match val {
                LiteralValue::Number(n) => new_data
                    .extend_from_slice(&parse_i32_literal(n, "row serialization")?.to_be_bytes()),
                LiteralValue::String(s) => {
                    new_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    new_data.extend_from_slice(s.as_bytes());
                }
                LiteralValue::Bool(b) => new_data.push(if *b { 1 } else { 0 }),
                LiteralValue::Date(s) => {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map_err(|e| ExecutionError::GenericError(e.to_string()))?;
                    let days = date.signed_duration_since(epoch).num_days() as i32;
                    new_data.extend_from_slice(&days.to_be_bytes());
                }
                LiteralValue::Null => {
                    new_data.extend_from_slice(&0i32.to_be_bytes());
                }
            }
        }
    }
    Ok(new_data)
}

pub(super) fn find_or_create_page_for_insert(
    ctx: &InsertPathCtx<'_>,
    table_oid: u32,
    first_page_id: &mut PageId,
    tuple_len: usize,
) -> Result<PageId, ExecutionError> {
    if *first_page_id == INVALID_PAGE_ID {
        let new_page_guard = ctx.bpm.new_page()?;
        *first_page_id = new_page_guard.read().id;
        update_pg_class_page_id(
            ctx.bpm,
            ctx.tm,
            ctx.wm,
            ctx.tx_id,
            ctx.snapshot,
            table_oid,
            *first_page_id,
        )?;
        return Ok(*first_page_id);
    }

    let mut current_page_id = *first_page_id;
    loop {
        let page_guard = ctx.bpm.acquire_page(current_page_id)?;
        let has_space = {
            let page = page_guard.read();
            let needed = tuple_len
                + std::mem::size_of::<bedrock::page::HeapTupleHeaderData>()
                + std::mem::size_of::<bedrock::page::ItemIdData>();
            let header = page.read_header();
            header.upper_offset.saturating_sub(header.lower_offset) >= needed as u16
        };

        if has_space {
            return Ok(current_page_id);
        }

        let next_page_id = page_guard.read().read_header().next_page_id;
        if next_page_id == INVALID_PAGE_ID {
            let new_page_guard = ctx.bpm.new_page()?;
            let new_page_id = new_page_guard.read().id;
            let mut page = page_guard.write();
            let mut header = page.read_header();
            header.next_page_id = new_page_id;
            page.write_header(&header);
            return Ok(new_page_id);
        }
        current_page_id = next_page_id;
    }
}

pub(super) fn insert_tuple_and_log(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    page_id: PageId,
    tuple_data: &[u8],
) -> Result<u16, ExecutionError> {
    let page_guard = bpm.acquire_page(page_id)?;
    let mut page = page_guard.write();
    let before_page = page.data.to_vec();
    let item_id = page
        .add_tuple(tuple_data, tx_id, 0)
        .ok_or_else(|| ExecutionError::GenericError("Failed to add tuple".to_string()))?;
    let after_page = page.data.to_vec();

    let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
    let lsn = lock_wal(wm)?.log(
        tx_id,
        prev_lsn,
        &WalRecord::InsertTuple {
            tx_id,
            page_id,
            item_id,
            before_page,
            after_page,
        },
    )?;
    tm.set_last_lsn(tx_id, lsn);
    let mut header = page.read_header();
    header.lsn = lsn;
    page.write_header(&header);
    Ok(item_id)
}

#[cfg(test)]
mod tests {
    use super::{serialize_expressions, serialize_literal_map};
    use crate::parser::{Expression, LiteralValue};
    use crate::types::Column;
    use std::collections::HashMap;

    #[test]
    fn serialize_expressions_rejects_invalid_integer_literal() {
        let values = vec![Expression::Literal(LiteralValue::Number(
            "1.25".to_string(),
        ))];
        let result = serialize_expressions(&values);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_literal_map_rejects_invalid_integer_literal() {
        let mut map = HashMap::new();
        map.insert("id".to_string(), LiteralValue::Number("abc".to_string()));
        let schema = vec![Column {
            name: "id".to_string(),
            type_id: 23,
        }];

        let result = serialize_literal_map(&map, &schema);
        assert!(result.is_err());
    }
}
