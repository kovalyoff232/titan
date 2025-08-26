//! The query executor.
//!
//! This module is responsible for taking a physical query plan and executing it
//! against the database. It handles all the details of data retrieval, modification,
//! and expression evaluation.

use crate::catalog::{
    update_pg_class_page_id, PG_ATTRIBUTE_TABLE_OID, PG_CLASS_TABLE_OID, SystemCatalog,
};
use crate::errors::ExecutionError;
use crate::optimizer::{self, PhysicalPlan};
use crate::parser::{
    BinaryOperator, CreateIndexStatement, CreateTableStatement, DataType, DeleteStatement,
    Expression, InsertStatement, LiteralValue, SelectItem, SelectStatement, Statement,
    UpdateStatement,
};
use crate::planner;
use crate::types::{Column, ExecuteResult, ResultSet};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::{WalManager, WalRecord};
use bedrock::{btree, PageId};
use chrono::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};



// --- Type Aliases ---
type Row = Vec<String>;

// --- Executor Trait ---
pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError>;
    fn schema(&self) -> &Vec<Column>;
}

// --- Helper Functions ---

pub fn parse_tuple(tuple_data: &[u8], schema: &Vec<Column>) -> HashMap<String, LiteralValue> {
    let mut offset = 0;
    let mut parsed_tuple = HashMap::new();

    for col in schema {
        if offset >= tuple_data.len() {
            break;
        }
        match col.type_id {
            16 => {
                // BOOLEAN
                if offset + 1 > tuple_data.len() {
                    break;
                }
                parsed_tuple.insert(
                    col.name.clone(),
                    LiteralValue::Bool(tuple_data[offset] != 0),
                );
                offset += 1;
            }
            23 => {
                // INT
                if offset + 4 > tuple_data.len() {
                    break;
                }
                let val = i32::from_be_bytes(tuple_data[offset..offset + 4].try_into().unwrap());
                parsed_tuple.insert(col.name.clone(), LiteralValue::Number(val.to_string()));
                offset += 4;
            }
            25 => {
                // TEXT
                if offset + 4 > tuple_data.len() {
                    break;
                }
                let len =
                    u32::from_be_bytes(tuple_data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > tuple_data.len() {
                    break;
                }
                let val = String::from_utf8_lossy(&tuple_data[offset..offset + len]);
                parsed_tuple.insert(col.name.clone(), LiteralValue::String(val.into_owned()));
                offset += len;
            }
            1082 => {
                // DATE
                if offset + 4 > tuple_data.len() {
                    break;
                }
                let days = i32::from_be_bytes(tuple_data[offset..offset + 4].try_into().unwrap());
                let date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()
                    + chrono::Duration::days(days as i64);
                parsed_tuple.insert(
                    col.name.clone(),
                    LiteralValue::Date(date.format("%Y-%m-%d").to_string()),
                );
                offset += 4;
            }
            _ => {}
        }
    }
    parsed_tuple
}

fn row_vec_to_map(
    row_vec: &[String],
    columns: &[Column],
    table_name: Option<&str>,
) -> HashMap<String, LiteralValue> {
    let mut map = HashMap::new();
    for (i, col) in columns.iter().enumerate() {
        let val_str = &row_vec[i];
        let literal = match col.type_id {
            16 => LiteralValue::Bool(val_str == "t"),
            23 => LiteralValue::Number(val_str.clone()),
            25 => LiteralValue::String(val_str.clone()),
            1082 => LiteralValue::Date(val_str.clone()),
            _ => LiteralValue::String(val_str.clone()),
        };
        if let Some(table) = table_name {
            map.insert(format!("{}.{}", table, col.name), literal.clone());
        }
        map.insert(col.name.clone(), literal);
    }
    map
}

// --- Orchestrator ---

pub fn execute(
    stmt: &Statement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<ExecuteResult, ExecutionError> {
    match stmt {
        Statement::Select(select_stmt) => {
            let logical_plan =
                planner::create_logical_plan(select_stmt, bpm, tx_id, snapshot, system_catalog)?;
            let physical_plan =
                optimizer::optimize(logical_plan, bpm, tm, tx_id, snapshot, system_catalog)
                    .map_err(|_| {
                        ExecutionError::PlanningError("Failed to create physical plan".to_string())
                    })?;

            println!("[Executor] Physical Plan: {:?}", physical_plan);

            let mut executor = create_executor(
                &physical_plan,
                bpm,
                lm,
                tx_id,
                snapshot,
                select_stmt,
                system_catalog,
            )?;
            let schema = executor.schema().clone();
            let mut rows = Vec::new();
            while let Some(row) = executor.next()? {
                rows.push(row);
            }

            Ok(ExecuteResult::ResultSet(ResultSet {
                columns: schema,
                rows,
            }))
        }
        Statement::CreateTable(create_stmt) => {
            execute_create_table(create_stmt, bpm, tm, wm, tx_id).map(|_| ExecuteResult::Ddl)
        }
        Statement::CreateIndex(create_stmt) => {
            execute_create_index(
                create_stmt,
                bpm,
                tm,
                lm,
                wm,
                tx_id,
                snapshot,
                system_catalog,
            )
            .map(|_| ExecuteResult::Ddl)
        }
        Statement::Insert(insert_stmt) => execute_insert(
            insert_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Insert),
        Statement::Update(update_stmt) => execute_update(
            update_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Update),
        Statement::Delete(delete_stmt) => execute_delete(
            delete_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Delete),
        Statement::Vacuum(table_name) => {
            execute_vacuum(
                table_name,
                bpm,
                tm,
                lm,
                wm,
                tx_id,
                snapshot,
                system_catalog,
            )
            .map(|_| ExecuteResult::Ddl)
        }
        Statement::Analyze(table_name) => {
            execute_analyze(
                table_name,
                bpm,
                tm,
                lm,
                wm,
                tx_id,
                snapshot,
                system_catalog,
            )
            .map(|_| ExecuteResult::Ddl)
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(ExecuteResult::Ddl),
        _ => Err(ExecutionError::GenericError(
            "Unsupported statement type".to_string(),
        )),
    }
}

// --- Executor Creation ---

fn create_executor<'a>(
    plan: &'a PhysicalPlan,
    bpm: &'a Arc<BufferPoolManager>,
    lm: &'a Arc<LockManager>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    select_stmt: &'a SelectStatement,
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
) -> Result<Box<dyn Executor + 'a>, ExecutionError> {
    match plan {
        PhysicalPlan::TableScan { table_name, filter } => {
            let (table_oid, first_page_id) = system_catalog
                .lock()
                .unwrap()
                .find_table(table_name, bpm, tx_id, snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = system_catalog
                .lock()
                .unwrap()
                .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
            let scan_executor = Box::new(TableScanExecutor::new(
                bpm,
                lm,
                first_page_id,
                schema,
                tx_id,
                snapshot,
                select_stmt.for_update,
                None, // Filter is handled by FilterExecutor
            ));

            if let Some(predicate) = filter {
                Ok(Box::new(FilterExecutor::new(
                    scan_executor,
                    predicate.clone(),
                )))
            } else {
                Ok(scan_executor)
            }
        }
        PhysicalPlan::IndexScan {
            table_name, key, ..
        } => {
            let (table_oid, _first_page_id) = system_catalog
                .lock()
                .unwrap()
                .find_table(table_name, bpm, tx_id, snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = system_catalog
                .lock()
                .unwrap()
                .get_table_schema(bpm, table_oid, tx_id, snapshot)?;

            let index_name = "idx_id".to_string(); // Simplified
            let (_index_oid, index_root_page_id) = system_catalog
                .lock()
                .unwrap()
                .find_table(&index_name, bpm, tx_id, snapshot)?
                .ok_or(ExecutionError::TableNotFound(index_name))?;

            Ok(Box::new(IndexScanExecutor::new(
                bpm,
                schema,
                index_root_page_id,
                *key,
                tx_id,
                snapshot,
            )))
        }
        PhysicalPlan::Filter { input, predicate } => {
            let input_executor =
                create_executor(input, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            Ok(Box::new(FilterExecutor::new(
                input_executor,
                predicate.clone(),
            )))
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
        } => {
            let left_executor =
                create_executor(left, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            let right_executor =
                create_executor(right, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.clone())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.clone())
            } else {
                None
            };
            Ok(Box::new(NestedLoopJoinExecutor::new(
                left_executor,
                right_executor,
                condition.clone(),
                left_table_name,
                right_table_name,
            )))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_executor =
                create_executor(left, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            let right_executor =
                create_executor(right, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.clone())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.clone())
            } else {
                None
            };
            Ok(Box::new(HashJoinExecutor::new(
                left_executor,
                right_executor,
                left_key.clone(),
                right_key.clone(),
                left_table_name,
                right_table_name,
            )?))
        }
        PhysicalPlan::Projection { input, expressions } => {
            let input_executor =
                create_executor(input, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            Ok(Box::new(ProjectionExecutor::new(
                input_executor,
                expressions.clone(),
            )))
        }
        PhysicalPlan::Sort { input, order_by } => {
            let input_executor =
                create_executor(input, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)?;
            Ok(Box::new(SortExecutor::new(
                input_executor,
                order_by.clone(),
            )?))
        }
        PhysicalPlan::MergeJoin { .. } => {
            Err(ExecutionError::GenericError(
                "MergeJoin is not supported by the iterator executor yet".to_string(),
            ))
        }
        PhysicalPlan::HashAggregate { .. } => {
            Err(ExecutionError::GenericError(
                "HashAggregate not yet fully implemented".to_string(),
            ))
        }
        PhysicalPlan::StreamAggregate { .. } => {
            Err(ExecutionError::GenericError(
                "StreamAggregate not yet fully implemented".to_string(),
            ))
        }
        PhysicalPlan::Window { .. } => {
            Err(ExecutionError::GenericError(
                "Window functions not yet fully implemented".to_string(),
            ))
        }
        PhysicalPlan::MaterializeCTE { .. } => {
            Err(ExecutionError::GenericError(
                "MaterializeCTE not yet implemented".to_string(),
            ))
        }
        PhysicalPlan::CTEScan { .. } => {
            Err(ExecutionError::GenericError(
                "CTEScan not yet implemented".to_string(),
            ))
        }
        PhysicalPlan::Limit { input, .. } => {
            // For now, just pass through to child executor
            // TODO: Implement proper LimitExecutor
            create_executor(input, bpm, lm, tx_id, snapshot, select_stmt, system_catalog)
        }
    }
}

// --- Physical Plan Executors ---

// TableScanExecutor
struct TableScanExecutor<'a> {
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
    fn new(
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

// IndexScanExecutor
struct IndexScanExecutor<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    schema: Vec<Column>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    tuple_id: Option<(PageId, u16)>,
    done: bool,
}

impl<'a> IndexScanExecutor<'a> {
    fn new(
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

// FilterExecutor
struct FilterExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    predicate: Expression,
}

impl<'a> FilterExecutor<'a> {
    fn new(input: Box<dyn Executor + 'a>, predicate: Expression) -> Self {
        Self { input, predicate }
    }
}

impl<'a> Executor for FilterExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            match self.input.next()? {
                Some(row) => {
                    let row_map = row_vec_to_map(&row, self.schema(), None);
                    if evaluate_expr_for_row(&self.predicate, &row_map)? {
                        return Ok(Some(row));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

// ProjectionExecutor
struct ProjectionExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    expressions: Vec<SelectItem>,
    projected_schema: Vec<Column>,
}

impl<'a> ProjectionExecutor<'a> {
    fn new(input: Box<dyn Executor + 'a>, expressions: Vec<SelectItem>) -> Self {
        let mut projected_schema = Vec::new();
        if expressions
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            projected_schema = input.schema().clone();
        } else {
            for (i, item) in expressions.iter().enumerate() {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => alias.clone(),
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expression::Column(name) => name.clone(),
                        Expression::QualifiedColumn(_, col) => col.clone(),
                        _ => format!("?column?_{}", i),
                    },
                    SelectItem::Wildcard => "*".to_string(),
                    SelectItem::QualifiedWildcard(table_name) => format!("{}.*", table_name),
                };
                projected_schema.push(Column { name, type_id: 25 }); // Simplified type
            }
        }
        Self {
            input,
            expressions,
            projected_schema,
        }
    }
}

impl<'a> Executor for ProjectionExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.projected_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        let next_row = self.input.next()?;
        if next_row.is_none() {
            return Ok(None);
        }
        let row = next_row.unwrap();

        if self
            .expressions
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            return Ok(Some(row));
        }

        let mut row_map = HashMap::new();
        if self.input.schema().len() == row.len() {
            row_map = row_vec_to_map(&row, self.input.schema(), None);
        } else {
            // This is a hack for joins. A proper solution would involve executors
            // passing down table information.
            let left_len = self.input.schema().iter().filter(|c| c.name.starts_with("users.")).count();
            let (left_row, right_row) = row.split_at(left_len);
            let (left_schema, right_schema) = self.input.schema().split_at(left_len);
            row_map.extend(row_vec_to_map(left_row, left_schema, Some("users")));
            row_map.extend(row_vec_to_map(right_row, right_schema, Some("orders")));
        }
        let mut projected_row = Vec::new();
        for item in &self.expressions {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                projected_row.push(val.to_string());
            }
        }
        Ok(Some(projected_row))
    }
}

// NestedLoopJoinExecutor
struct NestedLoopJoinExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    right: Box<dyn Executor + 'a>,
    condition: Expression,
    joined_schema: Vec<Column>,
    left_row: Option<Row>,
    right_rows: Vec<Row>,
    right_cursor: usize,
    left_table_name: Option<String>,
    right_table_name: Option<String>,
}

impl<'a> NestedLoopJoinExecutor<'a> {
    fn new(
        mut left: Box<dyn Executor + 'a>,
        mut right: Box<dyn Executor + 'a>,
        condition: Expression,
        left_table_name: Option<String>,
        right_table_name: Option<String>,
    ) -> Self {
        let mut joined_schema = Vec::new();
        let left_schema = left.schema();
        let right_schema = right.schema();
        if let Some(table_name) = &left_table_name {
            for col in left_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(left_schema);
        }
        if let Some(table_name) = &right_table_name {
            for col in right_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(right_schema);
        }

        // Materialize the right side
        let mut right_rows = Vec::new();
        while let Ok(Some(row)) = right.next() {
            right_rows.push(row);
        }

        let left_row = left.next().unwrap_or(None);

        Self {
            left,
            right,
            condition,
            joined_schema,
            left_row,
            right_rows,
            right_cursor: 0,
            left_table_name,
            right_table_name,
        }
    }
}

impl<'a> Executor for NestedLoopJoinExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.joined_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            if self.left_row.is_none() {
                return Ok(None);
            }

            while self.right_cursor < self.right_rows.len() {
                let right_row = &self.right_rows[self.right_cursor];
                self.right_cursor += 1;

                let left_row = self.left_row.as_ref().unwrap();
                let left_map = row_vec_to_map(
                    left_row,
                    self.left.schema(),
                    self.left_table_name.as_deref(),
                );
                let right_map = row_vec_to_map(
                    right_row,
                    self.right.schema(),
                    self.right_table_name.as_deref(),
                );
                let combined_map: HashMap<String, LiteralValue> =
                    left_map.into_iter().chain(right_map).collect();

                if evaluate_expr_for_row(&self.condition, &combined_map)? {
                    let mut joined_row = left_row.clone();
                    joined_row.extend(right_row.clone());
                    return Ok(Some(joined_row));
                }
            }

            // Move to the next left row and reset the right cursor
            self.left_row = self.left.next()?;
            self.right_cursor = 0;
        }
    }
}

// HashJoinExecutor
struct HashJoinExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    left_key: Expression,
    joined_schema: Vec<Column>,
    hash_table: HashMap<LiteralValue, Vec<Row>>,
    current_left_row: Option<Row>,
    current_matches: Vec<Row>,
    left_table_name: Option<String>,
    right_table_name: Option<String>,
}

impl<'a> HashJoinExecutor<'a> {
    fn new(
        left: Box<dyn Executor + 'a>,
        mut right: Box<dyn Executor + 'a>,
        left_key: Expression,
        right_key: Expression,
        left_table_name: Option<String>,
        right_table_name: Option<String>,
    ) -> Result<Self, ExecutionError> {
        let mut joined_schema = Vec::new();
        let left_schema = left.schema();
        let right_schema = right.schema();
        if let Some(table_name) = &left_table_name {
            for col in left_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(left_schema);
        }
        if let Some(table_name) = &right_table_name {
            for col in right_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(right_schema);
        }

        // Build phase
        let mut hash_table: HashMap<LiteralValue, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next()? {
            let row_map = row_vec_to_map(&row, right.schema(), right_table_name.as_deref());
            let key = evaluate_expr_for_row_to_val(&right_key, &row_map)?;
            hash_table.entry(key).or_default().push(row);
        }

        Ok(Self {
            left,
            left_key,
            joined_schema,
            hash_table,
            current_left_row: None,
            current_matches: Vec::new(),
            left_table_name,
            right_table_name,
        })
    }
}

impl<'a> Executor for HashJoinExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.joined_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            // If we have matches for the current left row, return one
            if let Some(match_row) = self.current_matches.pop() {
                let mut joined_row = self.current_left_row.as_ref().unwrap().clone();
                joined_row.extend(match_row);
                return Ok(Some(joined_row));
            }

            // Otherwise, get the next left row and find matches
            let left_row = self.left.next()?;
            if left_row.is_none() {
                return Ok(None); // Left side is exhausted
            }
            self.current_left_row = left_row;
            let left_row_ref = self.current_left_row.as_ref().unwrap();

            let left_map = row_vec_to_map(
                left_row_ref,
                self.left.schema(),
                self.left_table_name.as_deref(),
            );
            let key = evaluate_expr_for_row_to_val(&self.left_key, &left_map)?;

            if let Some(matching_rows) = self.hash_table.get(&key) {
                self.current_matches = matching_rows.clone();
                self.current_matches.reverse(); // To pop from the back
            } else {
                self.current_matches.clear();
            }
        }
    }
}

// SortExecutor
struct SortExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    order_by: Vec<Expression>,
    sorted_rows: Vec<Row>,
    cursor: usize,
}

impl<'a> SortExecutor<'a> {
    fn new(mut input: Box<dyn Executor + 'a>, order_by: Vec<Expression>) -> Result<Self, ExecutionError> {
        let mut rows = Vec::new();
        while let Some(row) = input.next()? {
            rows.push(row);
        }

        if order_by.len() != 1 {
            return Err(ExecutionError::GenericError(
                "ORDER BY only supports a single column".to_string(),
            ));
        }
        let sort_expr = &order_by[0];

        let (sort_col_idx, sort_col_type) = if let Expression::Column(name) = sort_expr {
            input
                .schema()
                .iter()
                .position(|c| c.name == *name || c.name.ends_with(&format!(".{}", name)))
                .map(|i| (i, input.schema()[i].type_id))
                .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone()))?
        } else {
            return Err(ExecutionError::GenericError(
                "ORDER BY only supports column names".to_string(),
            ));
        };

        rows.sort_by(|a, b| {
            let val_a = &a[sort_col_idx];
            let val_b = &b[sort_col_idx];
            match sort_col_type {
                23 => {
                    let num_a = val_a.parse::<i32>().unwrap_or(0);
                    let num_b = val_b.parse::<i32>().unwrap_or(0);
                    num_a.cmp(&num_b)
                }
                25 => val_a.cmp(val_b),
                1082 => {
                    let date_a = NaiveDate::parse_from_str(val_a, "%Y-%m-%d").unwrap();
                    let date_b = NaiveDate::parse_from_str(val_b, "%Y-%m-%d").unwrap();
                    date_a.cmp(&date_b)
                }
                16 => {
                    let bool_a = val_a == "t";
                    let bool_b = val_b == "t";
                    bool_a.cmp(&bool_b)
                }
                _ => std::cmp::Ordering::Equal,
            }
        });

        Ok(Self {
            input,
            order_by,
            sorted_rows: rows,
            cursor: 0,
        })
    }
}

impl<'a> Executor for SortExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        if self.cursor >= self.sorted_rows.len() {
            return Ok(None);
        }
        let row = self.sorted_rows[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(row))
    }
}

// --- DDL ---

fn execute_create_table(
    stmt: &CreateTableStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
) -> Result<(), ExecutionError> {
    let new_table_oid = tm.get_next_oid();
    let mut tuple_data = Vec::new();
    tuple_data.extend_from_slice(&new_table_oid.to_be_bytes());
    tuple_data.extend_from_slice(&INVALID_PAGE_ID.to_be_bytes());
    tuple_data.push(stmt.table_name.len() as u8);
    tuple_data.extend_from_slice(stmt.table_name.as_bytes());

    let pg_class_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
    let mut pg_class_page = pg_class_guard.write();
    let item_id = pg_class_page
        .add_tuple(&tuple_data, tx_id, 0)
        .ok_or_else(|| {
            ExecutionError::GenericError("Failed to insert into pg_class".to_string())
        })?;

    let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
    let lsn = wm.lock().unwrap().log(
        tx_id,
        prev_lsn,
        &WalRecord::InsertTuple {
            tx_id,
            page_id: PG_CLASS_TABLE_OID,
            item_id,
        },
    )?;
    tm.set_last_lsn(tx_id, lsn);
    let mut header = pg_class_page.read_header();
    header.lsn = lsn;
    pg_class_page.write_header(&header);
    drop(pg_class_page);
    drop(pg_class_guard);

    let pg_attr_guard = bpm.acquire_page(PG_ATTRIBUTE_TABLE_OID)?;
    let mut pg_attr_page = pg_attr_guard.write();
    for (i, col) in stmt.columns.iter().enumerate() {
        let mut attr_tuple_data = Vec::new();
        attr_tuple_data.extend_from_slice(&new_table_oid.to_be_bytes());
        attr_tuple_data.extend_from_slice(&(i as u16).to_be_bytes());
        let type_id: u32 = match col.data_type {
            DataType::Int => 23,
            DataType::Text => 25,
            DataType::Bool => 16,
            DataType::Date => 1082,
        };
        attr_tuple_data.extend_from_slice(&type_id.to_be_bytes());
        attr_tuple_data.push(col.name.len() as u8);
        attr_tuple_data.extend_from_slice(col.name.as_bytes());
        let item_id = pg_attr_page
            .add_tuple(&attr_tuple_data, tx_id, 0)
            .ok_or_else(|| {
                ExecutionError::GenericError("Failed to insert into pg_attribute".to_string())
            })?;

        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: PG_ATTRIBUTE_TABLE_OID,
                item_id,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = pg_attr_page.read_header();
        header.lsn = lsn;
        pg_attr_page.write_header(&header);
    }
    Ok(())
}

fn execute_create_index(
    stmt: &CreateIndexStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    let (table_oid, table_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

    lm.lock(
        tx_id,
        LockableResource::Table(table_oid),
        LockMode::Exclusive,
    )?;

    let root_page_guard = bpm.new_page()?;
    let mut root_page_id = root_page_guard.read().id;
    root_page_guard.write().as_btree_leaf_page();
    drop(root_page_guard);

    let index_oid = tm.get_next_oid();
    {
        let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut page = page_guard.write();
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&index_oid.to_be_bytes());
        tuple_data.extend_from_slice(&root_page_id.to_be_bytes());
        tuple_data.push(stmt.index_name.len() as u8);
        tuple_data.extend_from_slice(stmt.index_name.as_bytes());
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).unwrap();
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: PG_CLASS_TABLE_OID,
                item_id,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = page.read_header();
        header.lsn = lsn;
        page.write_header(&header);
    }

    if table_page_id != INVALID_PAGE_ID {
        let schema = system_catalog
            .lock()
            .unwrap()
            .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
        let rows_with_ids = scan_table(
            bpm,
            lm,
            table_page_id,
            &schema,
            tx_id,
            snapshot,
            false,
        )?;
        for (page_id, item_id, parsed_tuple) in rows_with_ids {
            if let Some(LiteralValue::Number(key_str)) = parsed_tuple.get(&stmt.column_name) {
                if let Ok(key) = key_str.parse::<i32>() {
                    root_page_id = btree::btree_insert(
                        bpm,
                        tm,
                        wm,
                        tx_id,
                        root_page_id,
                        key,
                        (page_id, item_id),
                    )?;
                }
            }
        }
    }

    update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, index_oid, root_page_id)?;
    Ok(())
}

// --- DML ---

fn execute_insert(
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

    let _page_id = find_or_create_page_for_insert(
        bpm,
        tm,
        wm,
        tx_id,
        snapshot,
        table_oid,
        &mut first_page_id,
        tuple_data.len(),
    )?;

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
    _system_catalog: &Arc<Mutex<SystemCatalog>>,
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

    insert_tuple_and_log(bpm, tm, wm, tx_id, page_id, tuple_data)
}

fn execute_update(
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
    let mut rows_affected = 0;
    let all_rows = scan_table(
        bpm,
        lm,
        first_page_id,
        &schema,
        tx_id,
        snapshot,
        false,
    )?;
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

        let old_raw = page.get_raw_tuple(item_id).unwrap().to_vec();
        let item_id_data = page.get_item_id_data(item_id).unwrap();
        let mut header = page.read_tuple_header(item_id_data.offset);
        header.xmax = tx_id;
        page.write_tuple_header(item_id_data.offset, &header);

        if let Some(new_item_id) = page.add_tuple(&new_data, tx_id, 0) {
            rows_affected += 1;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::UpdateTuple {
                    tx_id,
                    page_id,
                    item_id: new_item_id,
                    old_data: old_raw,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = page.read_header();
            header.lsn = lsn;
            page.write_header(&header);
        } else {
            let mut header = page.read_tuple_header(item_id_data.offset);
            header.xmax = 0;
            page.write_tuple_header(item_id_data.offset, &header);
            lm.unlock_all(tx_id);
            return Err(ExecutionError::SerializationFailure);
        }
    }
    Ok(rows_affected)
}

fn execute_delete(
    stmt: &DeleteStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<u32, ExecutionError> {
    let mut executor = DeleteExecutor::new(
        stmt,
        bpm,
        tm,
        lm,
        wm,
        tx_id,
        snapshot,
        system_catalog,
    )?;
    let result = executor.next()?.unwrap();
    Ok(result[0].parse().unwrap())
}

/// Scans a table and returns a vector of rows, where each row is a map of column name to value.
pub fn scan_table(
    bpm: &Arc<BufferPoolManager>,
    lm: &Arc<LockManager>,
    first_page_id: PageId,
    schema: &Vec<Column>,
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
                    // Lock the tuple before reading it. This will block if another transaction
                    // holds an exclusive lock.
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

fn evaluate_expr_for_row(
    expr: &Expression,
    row: &HashMap<String, LiteralValue>,
) -> Result<bool, ExecutionError> {
    match evaluate_expr_for_row_to_val(expr, row)? {
        LiteralValue::Bool(b) => Ok(b),
        _ => Err(ExecutionError::GenericError(
            "Expression did not evaluate to a boolean".to_string(),
        )),
    }
}

fn evaluate_expr_for_row_to_val<'a>(
    expr: &'a Expression,
    row: &HashMap<String, LiteralValue>,
) -> Result<LiteralValue, ExecutionError> {
    match expr {
        Expression::Literal(lit) => Ok(lit.clone()),
        Expression::Column(name) => {
            // Try direct match first
            if let Some(val) = row.get(name) {
                return Ok(val.clone());
            }
            // Try to find an unambiguous qualified match
            let mut found: Option<LiteralValue> = None;
            let mut ambiguous = false;
            for (key, val) in row.iter() {
                if key.ends_with(&format!(".{}", name)) {
                    if found.is_some() {
                        ambiguous = true;
                        break;
                    }
                    found = Some(val.clone());
                }
            }

            if ambiguous {
                return Err(ExecutionError::GenericError(format!(
                    "Column {} is ambiguous",
                    name
                )));
            }

            found.ok_or_else(|| ExecutionError::ColumnNotFound(name.clone()))
        }
        Expression::QualifiedColumn(table, col) => {
            let qname = format!("{}.{}", table, col);
            row.get(&qname)
                .cloned()
                .ok_or_else(|| ExecutionError::ColumnNotFound(qname))
        }
        Expression::Binary { left, op, right } => {
            let lval = evaluate_expr_for_row_to_val(left, row)?;
            let rval = evaluate_expr_for_row_to_val(right, row)?;
            match (lval, rval) {
                (LiteralValue::Number(l), LiteralValue::Number(r)) => {
                    let lnum = l.parse::<i32>().unwrap();
                    let rnum = r.parse::<i32>().unwrap();
                    match op {
                        BinaryOperator::Plus => Ok(LiteralValue::Number((lnum + rnum).to_string())),
                        BinaryOperator::Minus => {
                            Ok(LiteralValue::Number((lnum - rnum).to_string()))
                        }
                        BinaryOperator::Eq => Ok(LiteralValue::Bool(lnum == rnum)),
                        BinaryOperator::NotEq => Ok(LiteralValue::Bool(lnum != rnum)),
                        BinaryOperator::Lt => Ok(LiteralValue::Bool(lnum < rnum)),
                        BinaryOperator::LtEq => Ok(LiteralValue::Bool(lnum <= rnum)),
                        BinaryOperator::Gt => Ok(LiteralValue::Bool(lnum > rnum)),
                        BinaryOperator::GtEq => Ok(LiteralValue::Bool(lnum >= rnum)),
                        BinaryOperator::And => Ok(LiteralValue::Bool(lnum != 0 && rnum != 0)),
                        BinaryOperator::Or => Ok(LiteralValue::Bool(lnum != 0 || rnum != 0)),
                    }
                }
                (LiteralValue::Bool(l), LiteralValue::Bool(r)) => match op {
                    BinaryOperator::Eq => Ok(LiteralValue::Bool(l == r)),
                    BinaryOperator::NotEq => Ok(LiteralValue::Bool(l != r)),
                    BinaryOperator::And => Ok(LiteralValue::Bool(l && r)),
                    BinaryOperator::Or => Ok(LiteralValue::Bool(l || r)),
                    _ => Err(ExecutionError::GenericError(
                        "Unsupported operator for boolean".to_string(),
                    )),
                },
                (LiteralValue::Date(l), LiteralValue::Date(r)) => {
                    let ldate = NaiveDate::parse_from_str(&l, "%Y-%m-%d").unwrap();
                    let rdate = NaiveDate::parse_from_str(&r, "%Y-%m-%d").unwrap();
                    match op {
                        BinaryOperator::Eq => Ok(LiteralValue::Bool(ldate == rdate)),
                        BinaryOperator::NotEq => Ok(LiteralValue::Bool(ldate != rdate)),
                        BinaryOperator::Lt => Ok(LiteralValue::Bool(ldate < rdate)),
                        BinaryOperator::LtEq => Ok(LiteralValue::Bool(ldate <= rdate)),
                        BinaryOperator::Gt => Ok(LiteralValue::Bool(ldate > rdate)),
                        BinaryOperator::GtEq => Ok(LiteralValue::Bool(ldate >= rdate)),
                        _ => Err(ExecutionError::GenericError(
                            "Unsupported operator for date".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutionError::GenericError(
                    "Type mismatch in binary expression".to_string(),
                )),
            }
        }
        Expression::Unary { op, expr } => {
            let val = evaluate_expr_for_row_to_val(expr, row)?;
            match op {
                crate::parser::UnaryOperator::Not => match val {
                    LiteralValue::Bool(b) => Ok(LiteralValue::Bool(!b)),
                    _ => Err(ExecutionError::GenericError(
                        "NOT operator requires a boolean expression".to_string(),
                    )),
                },
            }
        }
        Expression::WindowFunction { .. } => {
            // Window functions are handled by WindowFunctionExecutor
            Err(ExecutionError::GenericError(
                "Window functions cannot be evaluated in WHERE clause".to_string(),
            ))
        }
        Expression::Function { name, args } => {
            // Simple function evaluation (limited support for now)
            match name.to_uppercase().as_str() {
                "COUNT" => Ok(LiteralValue::Number("1".to_string())),
                "SUM" | "AVG" | "MIN" | "MAX" => {
                    if !args.is_empty() {
                        evaluate_expr_for_row_to_val(&args[0], row)
                    } else {
                        Ok(LiteralValue::Null)
                    }
                }
                _ => Err(ExecutionError::GenericError(
                    format!("Unsupported function: {}", name),
                ))
            }
        }
        Expression::Case { operand, when_clauses, else_clause } => {
            // CASE expression evaluation
            for (condition, result) in when_clauses {
                let cond_result = if let Some(op) = operand {
                    let op_val = evaluate_expr_for_row_to_val(op, row)?;
                    let cond_val = evaluate_expr_for_row_to_val(condition, row)?;
                    op_val == cond_val
                } else {
                    match evaluate_expr_for_row_to_val(condition, row)? {
                        LiteralValue::Bool(b) => b,
                        _ => false,
                    }
                };
                
                if cond_result {
                    return evaluate_expr_for_row_to_val(result, row);
                }
            }
            
            if let Some(else_expr) = else_clause {
                evaluate_expr_for_row_to_val(else_expr, row)
            } else {
                Ok(LiteralValue::Null)
            }
        }
        Expression::Subquery(_) => {
            // Subqueries require special handling with executor context
            Err(ExecutionError::GenericError(
                "Subqueries are not yet supported in this context".to_string(),
            ))
        }
    }
}

fn execute_vacuum(
    table_name: &str,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    let (table_oid, old_first_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;
    lm.lock(
        tx_id,
        LockableResource::Table(table_oid),
        LockMode::Exclusive,
    )?;
    if old_first_page_id == INVALID_PAGE_ID {
        return Ok(());
    }

    let schema = system_catalog
        .lock()
        .unwrap()
        .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let live_tuples: Vec<_> = scan_table(
        bpm,
        lm,
        old_first_page_id,
        &schema,
        tx_id,
        snapshot,
        false,
    )?
    .into_iter()
    .map(|(_, _, parsed)| parsed)
    .collect();

    let mut new_first_page_id = INVALID_PAGE_ID;
    if !live_tuples.is_empty() {
        let first_page_guard = bpm.new_page()?;
        new_first_page_id = first_page_guard.read().id;
        drop(first_page_guard);
        let mut current_new_page_id = new_first_page_id;

        for tuple in live_tuples {
            let tuple_data = serialize_literal_map(&tuple, &schema)?;

            let mut inserted = false;
            while !inserted {
                let page_guard = bpm.acquire_page(current_new_page_id)?;
                let mut page = page_guard.write();
                if page.add_tuple(&tuple_data, tx_id, 0).is_some() {
                    inserted = true;
                } else {
                    let new_page_guard = bpm.new_page()?;
                    let next_id = new_page_guard.read().id;
                    let mut header = page.read_header();
                    header.next_page_id = next_id;
                    page.write_header(&header);
                    current_new_page_id = next_id;
                }
            }
        }
    }

    update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, table_oid, new_first_page_id)?;
    Ok(())
}

use rand::thread_rng;

fn execute_analyze(
    table_name: &str,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    analyze_table_and_update_stats(table_name, bpm, tm, lm, wm, tx_id, snapshot, system_catalog)
}

/// Analyzes a table, computes statistics, and updates the system catalog.
/// This function is a helper for `execute_analyze`.
fn analyze_table_and_update_stats(
    table_name: &str,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    const RESERVOIR_SIZE: usize = 1000;
    const MCV_LIST_SIZE: usize = 10;
    const HISTOGRAM_BINS: usize = 100;

    // Invalidate cache at the beginning of ANALYZE to get fresh data.

    let (table_oid, first_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(());
    }

    lm.lock(tx_id, LockableResource::Table(table_oid), LockMode::Shared)?;

    let schema = system_catalog
        .lock()
        .unwrap()
        .get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let all_rows = scan_table(
        bpm,
        lm,
        first_page_id,
        &schema,
        tx_id,
        snapshot,
        false,
    )?;
    let total_rows = all_rows.len();

    // Pre-fetch pg_statistic info to avoid re-fetching in the loop
    let (pg_stat_oid, pg_stat_first_page_id) =
        if let Some(info) = system_catalog
            .lock()
            .unwrap()
            .find_table("pg_statistic", bpm, tx_id, snapshot)?
        {
            info
        } else {
            // This should not happen in a properly initialized database
            return Err(ExecutionError::TableNotFound("pg_statistic".to_string()));
        };
    let pg_stat_schema = system_catalog
        .lock()
        .unwrap()
        .get_table_schema(bpm, pg_stat_oid, tx_id, snapshot)?;

    // --- Delete all old statistics for the entire table in one go ---
    let mut current_page_id = pg_stat_first_page_id;
    while current_page_id != bedrock::page::INVALID_PAGE_ID {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let mut page = page_guard.write();
        let mut modified = false;

        for item_id in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, item_id) {
                if let Some(tuple_data) = page.get_tuple(item_id) {
                    let parsed = parse_tuple(tuple_data, &pg_stat_schema);
                    if let Some(relid_val) = parsed.get("starelid") {
                        if relid_val.to_string() == table_oid.to_string() {
                            // Match found, mark as deleted
                            if let Some(item_id_data) = page.get_item_id_data(item_id) {
                                let offset = item_id_data.offset;
                                let mut header = page.read_tuple_header(offset);
                                if header.xmax == 0 {
                                    header.xmax = tx_id;
                                    page.write_tuple_header(offset, &header);
                                    modified = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        if modified {
            // In a real system, we'd log this change to the WAL.
        }
        current_page_id = page.read_header().next_page_id;
    }

    // Invalidate again after deletion to ensure subsequent inserts see the clean state.

    for (i, column) in schema.iter().enumerate() {
        let mut column_values: Vec<LiteralValue> = all_rows
            .iter()
            .filter_map(|(_, _, parsed_row)| parsed_row.get(&column.name).cloned())
            .collect();

        // --- 1. N_DISTINCT ---
        let n_distinct = column_values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len() as i32;
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&table_oid.to_be_bytes()); // starelid
        tuple_data.extend_from_slice(&(i as u32).to_be_bytes()); // staattnum
        tuple_data.extend_from_slice(&n_distinct.to_be_bytes()); // stadistinct
        tuple_data.extend_from_slice(&1i32.to_be_bytes()); // stakind = 1 (n_distinct)
        tuple_data.extend_from_slice(&0i32.to_be_bytes()); // staop
        let empty_text = "";
        tuple_data.extend_from_slice(&(empty_text.len() as u32).to_be_bytes()); // stanumbers
        tuple_data.extend_from_slice(empty_text.as_bytes());
        tuple_data.extend_from_slice(&(empty_text.len() as u32).to_be_bytes()); // stavalues
        tuple_data.extend_from_slice(empty_text.as_bytes());
        insert_tuple_into_system_table(
            &tuple_data,
            "pg_statistic",
            bpm,
            tm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )?;

        if total_rows == 0 {
            continue;
        }

        // --- Reservoir Sampling ---
        let mut reservoir = Vec::with_capacity(RESERVOIR_SIZE);
        let mut rng = thread_rng();
        for (idx, val) in column_values.iter().enumerate() {
            if idx < RESERVOIR_SIZE {
                reservoir.push(val.clone());
            } else {
                let j = rand::Rng::gen_range(&mut rng, 0..=idx);
                if j < RESERVOIR_SIZE {
                    reservoir[j] = val.clone();
                }
            }
        }

        // --- 2. MCV (Most Common Values) ---
        let mut counts = HashMap::new();
        for val in &reservoir {
            *counts.entry(val).or_insert(0) += 1;
        }
        let mut mcv_list: Vec<_> = counts.into_iter().collect();
        mcv_list.sort_by(|a, b| b.1.cmp(&a.1));
        let mcvs: Vec<String> = mcv_list
            .iter()
            .take(MCV_LIST_SIZE)
            .map(|(val, _)| val.to_string())
            .collect();

        if !mcvs.is_empty() {
            let mcv_str = mcvs.join(",");
            let mut tuple_data = Vec::new();
            tuple_data.extend_from_slice(&table_oid.to_be_bytes());
            tuple_data.extend_from_slice(&(i as u32).to_be_bytes());
            tuple_data.extend_from_slice(&0i32.to_be_bytes()); // stadistinct
            tuple_data.extend_from_slice(&2i32.to_be_bytes()); // stakind = 2 (mcv)
            tuple_data.extend_from_slice(&0i32.to_be_bytes()); // staop
            tuple_data.extend_from_slice(&(empty_text.len() as u32).to_be_bytes());
            tuple_data.extend_from_slice(empty_text.as_bytes());
            tuple_data.extend_from_slice(&(mcv_str.len() as u32).to_be_bytes());
            tuple_data.extend_from_slice(mcv_str.as_bytes());
            insert_tuple_into_system_table(
                &tuple_data,
                "pg_statistic",
                bpm,
                tm,
                wm,
                tx_id,
                snapshot,
                system_catalog,
            )?;
        }

        // --- 3. HISTOGRAM ---
        column_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mut histogram_bounds = Vec::new();
        if column_values.len() > 1 {
            let step = column_values.len() / (HISTOGRAM_BINS + 1);
            if step > 0 {
                for j in 1..=HISTOGRAM_BINS {
                    if let Some(val) = column_values.get(j * step) {
                        histogram_bounds.push(val.to_string());
                    }
                }
            }
        }

        if !histogram_bounds.is_empty() {
            let hist_str = histogram_bounds.join(",");
            let mut tuple_data = Vec::new();
            tuple_data.extend_from_slice(&table_oid.to_be_bytes());
            tuple_data.extend_from_slice(&(i as u32).to_be_bytes());
            tuple_data.extend_from_slice(&0i32.to_be_bytes());
            tuple_data.extend_from_slice(&3i32.to_be_bytes()); // stakind = 3 (histogram)
            tuple_data.extend_from_slice(&0i32.to_be_bytes());
            tuple_data.extend_from_slice(&(hist_str.len() as u32).to_be_bytes());
            tuple_data.extend_from_slice(hist_str.as_bytes());
            tuple_data.extend_from_slice(&(empty_text.len() as u32).to_be_bytes());
            tuple_data.extend_from_slice(empty_text.as_bytes());
            insert_tuple_into_system_table(
                &tuple_data,
                "pg_statistic",
                bpm,
                tm,
                wm,
                tx_id,
                snapshot,
                system_catalog,
            )?;
        }
    }

    Ok(())
}

/// A generic helper to insert a raw tuple into a system catalog.
/// This avoids the overhead of the full `execute_insert` path.
fn insert_tuple_into_system_table(
    tuple_data: &[u8],
    table_name: &str,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    let (table_oid, mut first_page_id) = system_catalog
        .lock()
        .unwrap()
        .find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;

    let page_id = find_or_create_page_for_insert(
        bpm,
        tm,
        wm,
        tx_id,
        snapshot,
        table_oid,
        &mut first_page_id,
        tuple_data.len(),
    )?;

    insert_tuple_and_log(bpm, tm, wm, tx_id, page_id, tuple_data)
}

struct DeleteExecutor<'a> {
    stmt: &'a DeleteStatement,
    bpm: &'a Arc<BufferPoolManager>,
    tm: &'a Arc<TransactionManager>,
    lm: &'a Arc<LockManager>,
    wm: &'a Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
    schema: Vec<Column>,
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

        let output_schema = vec![Column {
            name: "rows_deleted".to_string(),
            type_id: 23, // INT
        }];

        Ok(Self {
            stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
            schema,
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
                    if self
                        .stmt
                        .where_clause
                        .as_ref()
                        .map_or(true, |p| evaluate_expr_for_row(p, &parsed_row).unwrap_or(false))
                    {
                        // Lock and delete
                        self.lm.lock(
                            self.tx_id,
                            LockableResource::Tuple((self.current_page_id, item_id)),
                            LockMode::Exclusive,
                        )?;

                        if let Some(item_id_data) = page.get_item_id_data(item_id) {
                            let mut header = page.read_tuple_header(item_id_data.offset);
                            if header.xmax != 0 {
                                return Err(ExecutionError::SerializationFailure);
                            }
                            header.xmax = self.tx_id;
                            page.write_tuple_header(item_id_data.offset, &header);
                            self.rows_affected += 1;

                            // WAL
                            let prev_lsn = self.tm.get_last_lsn(self.tx_id).unwrap_or(0);
                            let lsn = self.wm.lock().unwrap().log(
                                self.tx_id,
                                prev_lsn,
                                &WalRecord::DeleteTuple {
                                    tx_id: self.tx_id,
                                    page_id: self.current_page_id,
                                    item_id,
                                },
                            )?;
                            self.tm.set_last_lsn(self.tx_id, lsn);
                            let mut page_header = page.read_header();
                            page_header.lsn = lsn;
                            page.write_header(&page_header);
                        }
                    }
                }
            }
        }
    }
}

struct UpdateExecutor<'a> {
    stmt: &'a UpdateStatement,
    bpm: &'a Arc<BufferPoolManager>,
    tm: &'a Arc<TransactionManager>,
    lm: &'a Arc<LockManager>,
    wm: &'a Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
    schema: Vec<Column>,
    output_schema: Vec<Column>,
    current_page_id: PageId,
    current_item_id: u16,
    rows_affected: u32,
    done: bool,
}

impl<'a> UpdateExecutor<'a> {
    fn new(
        stmt: &'a UpdateStatement,
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

        let output_schema = vec![Column {
            name: "rows_updated".to_string(),
            type_id: 23, // INT
        }];

        Ok(Self {
            stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
            schema,
            output_schema,
            current_page_id: first_page_id,
            current_item_id: 0,
            rows_affected: 0,
            done: false,
        })
    }
}

impl<'a> Executor for UpdateExecutor<'a> {
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
            
            if self.current_item_id >= page_guard.read().get_tuple_count() {
                self.current_page_id = page_guard.read().read_header().next_page_id;
                self.current_item_id = 0;
                continue;
            }

            let item_id = self.current_item_id;
            self.current_item_id += 1;

            let old_parsed = {
                let page = page_guard.read();
                 if !page.is_visible(self.snapshot, self.tx_id, item_id) {
                    continue;
                }
                if let Some(tuple_data) = page.get_tuple(item_id) {
                    parse_tuple(tuple_data, &self.schema)
                } else {
                    continue;
                }
            };

            if self
                .stmt
                .where_clause
                .as_ref()
                .map_or(true, |p| evaluate_expr_for_row(p, &old_parsed).unwrap_or(false))
            {
                self.lm.lock(
                    self.tx_id,
                    LockableResource::Tuple((self.current_page_id, item_id)),
                    LockMode::Exclusive,
                )?;

                let mut page = page_guard.write();

                if !page.is_visible(self.snapshot, self.tx_id, item_id)
                    || page
                        .get_item_id_data(item_id)
                        .map(|d| page.read_tuple_header(d.offset).xmax != 0)
                        .unwrap_or(true)
                {
                    self.lm.unlock_all(self.tx_id);
                    return Err(ExecutionError::SerializationFailure);
                }

                let mut new_parsed = old_parsed.clone();
                for (col_name, expr) in &self.stmt.assignments {
                    new_parsed.insert(
                        col_name.clone(),
                        evaluate_expr_for_row_to_val(expr, &old_parsed)?,
                    );
                }

                let mut new_data = Vec::new();
                for col in &self.schema {
                    if let Some(val) = new_parsed.get(&col.name) {
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
                                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                                let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                                let days = date.signed_duration_since(epoch).num_days() as i32;
                                new_data.extend_from_slice(&days.to_be_bytes());
                            }
                            LiteralValue::Null => {
                                // For NULL values, we could either skip or write a special marker
                                // For now, let's write 4 zero bytes to represent NULL
                                new_data.extend_from_slice(&0i32.to_be_bytes());
                            }
                        }
                    }
                }

                let old_raw = page.get_raw_tuple(item_id).unwrap().to_vec();
                let item_id_data = page.get_item_id_data(item_id).unwrap();
                let mut header = page.read_tuple_header(item_id_data.offset);
                header.xmax = self.tx_id;
                page.write_tuple_header(item_id_data.offset, &header);

                if let Some(new_item_id) = page.add_tuple(&new_data, self.tx_id, 0) {
                    self.rows_affected += 1;
                    let prev_lsn = self.tm.get_last_lsn(self.tx_id).unwrap_or(0);
                    let lsn = self.wm.lock().unwrap().log(
                        self.tx_id,
                        prev_lsn,
                        &WalRecord::UpdateTuple {
                            tx_id: self.tx_id,
                            page_id: self.current_page_id,
                            item_id: new_item_id,
                            old_data: old_raw,
                        },
                    )?;
                    self.tm.set_last_lsn(self.tx_id, lsn);
                    let mut page_header = page.read_header();
                    page_header.lsn = lsn;
                    page.write_header(&page_header);
                } else {
                    let mut header = page.read_tuple_header(item_id_data.offset);
                    header.xmax = 0;
                    page.write_tuple_header(item_id_data.offset, &header);
                    self.lm.unlock_all(self.tx_id);
                    return Err(ExecutionError::SerializationFailure);
                }
            }
        }
    }
}

// --- DML Helper Functions ---

fn serialize_expressions(values: &[Expression]) -> Result<Vec<u8>, ExecutionError> {
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
                // For NULL values, write 4 zero bytes
                tuple_data.extend_from_slice(&0i32.to_be_bytes());
            }
            _ => {
                return Err(ExecutionError::GenericError(
                    "Unsupported expression type for insert".to_string(),
                ))
            }
        }
    }
    Ok(tuple_data)
}

fn serialize_literal_map(
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
                    // For NULL values, write a special marker
                    new_data.extend_from_slice(&0i32.to_be_bytes());
                }
            }
        }
    }
    Ok(new_data)
}

fn find_or_create_page_for_insert(
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
            header
                .upper_offset
                .saturating_sub(header.lower_offset)
                >= needed as u16
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

fn insert_tuple_and_log(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    page_id: PageId,
    tuple_data: &[u8],
) -> Result<(), ExecutionError> {
    let page_guard = bpm.acquire_page(page_id)?;
    let mut page = page_guard.write();
    let item_id = page
        .add_tuple(tuple_data, tx_id, 0)
        .ok_or_else(|| ExecutionError::GenericError("Failed to add tuple".to_string()))?;

    let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
    let lsn = wm.lock().unwrap().log(
        tx_id,
        prev_lsn,
        &WalRecord::InsertTuple {
            tx_id,
            page_id,
            item_id,
        },
    )?;
    tm.set_last_lsn(tx_id, lsn);
    let mut header = page.read_header();
    header.lsn = lsn;
    page.write_header(&header);
    Ok(())
}
