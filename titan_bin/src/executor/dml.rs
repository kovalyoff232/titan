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
use std::sync::{Arc, Mutex};

pub(super) fn execute_insert(
    stmt: &InsertStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<u32, ExecutionError> {
    let (table_oid, mut first_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

    lm.lock(tx_id, LockableResource::Table(table_oid), LockMode::Shared)?;

    let tuple_data = serialize_expressions(&stmt.values)?;

    insert_tuple_and_update_indexes(
        bpm,
        tm,
        wm,
        tx_id,
        snapshot,
        system_catalog,
        table_oid,
        &mut first_page_id,
        &tuple_data,
    )?;

    Ok(1)
}

fn insert_tuple_and_update_indexes(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
    table_oid: u32,
    first_page_id: &mut PageId,
    tuple_data: &[u8],
) -> Result<(), ExecutionError> {
    let page_id = find_or_create_page_for_insert(
        bpm,
        tm,
        wm,
        tx_id,
        snapshot,
        table_oid,
        first_page_id,
        tuple_data.len(),
    )?;

    let item_id = insert_tuple_and_log(bpm, tm, wm, tx_id, page_id, tuple_data)?;

    let schema = system_catalog
        .lock()
        .unwrap()
        .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let parsed_tuple = parse_tuple(tuple_data, &schema);
    let mut indexes = load_convention_int_indexes(bpm, tx_id, snapshot, system_catalog, &schema)?;

    for index in &mut indexes {
        let Some(key) = extract_i32_key(&parsed_tuple, &index.column_name) else {
            continue;
        };

        let new_root_page_id = btree::btree_insert(
            bpm,
            tm,
            wm,
            tx_id,
            index.root_page_id,
            key,
            (page_id, item_id),
        )?;
        update_index_root_if_needed(bpm, tm, wm, tx_id, snapshot, index, new_root_page_id)?;
    }

    Ok(())
}

pub(super) fn execute_update(
    stmt: &UpdateStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(0);
    }

    let schema = system_catalog
        .lock()
        .unwrap()
        .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut indexes = load_convention_int_indexes(bpm, tx_id, snapshot, system_catalog, &schema)?;
    let mut rows_affected = 0;
    let all_rows = scan_table(bpm, lm, first_page_id, &schema, tx_id, snapshot, false)?;
    let rows_to_update: Vec<_> = all_rows
        .into_iter()
        .filter(|(_, _, parsed)| {
            stmt.where_clause
                .as_ref()
                .map_or(true, |p| evaluate_expr_for_row(p, parsed).unwrap_or(false))
        })
        .collect();

    for (page_id, item_id, old_parsed) in rows_to_update {
        lm.lock(
            tx_id,
            LockableResource::Tuple((page_id, item_id)),
            LockMode::Exclusive,
        )?;
        let page_guard = bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();
        if !page.is_visible(snapshot, tx_id, item_id)
            || page
                .get_item_id_data(item_id)
                .map(|d| page.read_tuple_header(d.offset).xmax != 0)
                .unwrap_or(true)
        {
            lm.unlock_all(tx_id);
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
        let item_id_data = page.get_item_id_data(item_id).unwrap();
        let mut header = page.read_tuple_header(item_id_data.offset);
        header.xmax = tx_id;
        page.write_tuple_header(item_id_data.offset, &header);

        let Some(new_item_id) = page.add_tuple(&new_data, tx_id, 0) else {
            let mut header = page.read_tuple_header(item_id_data.offset);
            header.xmax = 0;
            page.write_tuple_header(item_id_data.offset, &header);
            lm.unlock_all(tx_id);
            return Err(ExecutionError::SerializationFailure);
        };
        let after_page = page.data.to_vec();

        rows_affected += 1;
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::UpdateTuple {
                tx_id,
                page_id,
                item_id: new_item_id,
                before_page,
                after_page,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
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
                let new_root_page_id =
                    btree::btree_delete(bpm, tm, wm, tx_id, index.root_page_id, key)?;
                update_index_root_if_needed(bpm, tm, wm, tx_id, snapshot, index, new_root_page_id)?;
            }

            if let Some(key) = new_key {
                let new_root_page_id = btree::btree_insert(
                    bpm,
                    tm,
                    wm,
                    tx_id,
                    index.root_page_id,
                    key,
                    (page_id, new_item_id),
                )?;
                update_index_root_if_needed(bpm, tm, wm, tx_id, snapshot, index, new_root_page_id)?;
            }
        }
    }
    Ok(rows_affected)
}

pub(super) fn execute_delete(
    stmt: &DeleteStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<u32, ExecutionError> {
    let mut executor = DeleteExecutor::new(stmt, bpm, tm, lm, wm, tx_id, snapshot, system_catalog)?;
    let result = executor.next()?.unwrap();
    Ok(result[0].parse().unwrap())
}

pub(super) fn scan_table(
    bpm: &Arc<BufferPoolManager>,
    lm: &Arc<LockManager>,
    first_page_id: PageId,
    schema: &[Column],
    tx_id: u32,
    snapshot: &Snapshot,
    for_update: bool,
) -> Result<Vec<(PageId, u16, HashMap<String, LiteralValue>)>, ExecutionError> {
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
    schema: Vec<Column>,
    indexes: Vec<IntIndexInfo>,
    output_schema: Vec<Column>,
    current_page_id: PageId,
    current_item_id: u16,
    rows_affected: u32,
    done: bool,
}

impl<'a> DeleteExecutor<'a> {
    fn new(
        stmt: &'a DeleteStatement,
        bpm: &'a Arc<BufferPoolManager>,
        tm: &'a Arc<TransactionManager>,
        lm: &'a Arc<LockManager>,
        wm: &'a Arc<Mutex<WalManager>>,
        tx_id: u32,
        snapshot: &'a Snapshot,
        system_catalog: &'a Arc<Mutex<SystemCatalog>>,
    ) -> Result<Self, ExecutionError> {
        let (table_oid, first_page_id) = system_catalog
            .lock()
            .unwrap()
            .find_table(&stmt.table_name, bpm, tx_id, snapshot)?
            .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

        let schema = system_catalog
            .lock()
            .unwrap()
            .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
        let indexes = load_convention_int_indexes(bpm, tx_id, snapshot, system_catalog, &schema)?;

        let output_schema = vec![Column {
            name: "rows_deleted".to_string(),
            type_id: 23,
        }];

        Ok(Self {
            stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            schema,
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
                    let parsed_row = parse_tuple(tuple_data, &self.schema);
                    if self.stmt.where_clause.as_ref().map_or(true, |p| {
                        evaluate_expr_for_row(p, &parsed_row).unwrap_or(false)
                    }) {
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
                            let lsn = self.wm.lock().unwrap().log(
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
    for value in values {
        match value {
            Expression::Literal(LiteralValue::Number(n)) => {
                tuple_data.extend_from_slice(&n.parse::<i32>().unwrap_or(0).to_be_bytes())
            }
            Expression::Literal(LiteralValue::String(s)) => {
                tuple_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                tuple_data.extend_from_slice(s.as_bytes());
            }
            Expression::Literal(LiteralValue::Bool(b)) => tuple_data.push(if *b { 1 } else { 0 }),
            Expression::Literal(LiteralValue::Date(s)) => {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| ExecutionError::GenericError(e.to_string()))?;
                let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
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
    for col in schema {
        if let Some(val) = map.get(&col.name) {
            match val {
                LiteralValue::Number(n) => {
                    new_data.extend_from_slice(&n.parse::<i32>().unwrap().to_be_bytes())
                }
                LiteralValue::String(s) => {
                    new_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    new_data.extend_from_slice(s.as_bytes());
                }
                LiteralValue::Bool(b) => new_data.push(if *b { 1 } else { 0 }),
                LiteralValue::Date(s) => {
                    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map_err(|e| ExecutionError::GenericError(e.to_string()))?;
                    let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
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
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    table_oid: u32,
    first_page_id: &mut PageId,
    tuple_len: usize,
) -> Result<PageId, ExecutionError> {
    if *first_page_id == INVALID_PAGE_ID {
        let new_page_guard = bpm.new_page()?;
        *first_page_id = new_page_guard.read().id;
        update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, table_oid, *first_page_id)?;
        return Ok(*first_page_id);
    }

    let mut current_page_id = *first_page_id;
    loop {
        let page_guard = bpm.acquire_page(current_page_id)?;
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
            let new_page_guard = bpm.new_page()?;
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
    let lsn = wm.lock().unwrap().log(
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
