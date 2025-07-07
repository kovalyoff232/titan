//! The query executor.

use crate::catalog::{
    find_table, get_table_schema, update_pg_class_page_id, PG_ATTRIBUTE_TABLE_OID,
    PG_CLASS_TABLE_OID,
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



// --- Helper Functions ---

fn parse_tuple(tuple_data: &[u8], schema: &Vec<Column>) -> HashMap<String, LiteralValue> {
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
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<ExecuteResult, ExecutionError> {
    match stmt {
        Statement::Select(select_stmt) => {
            let logical_plan = planner::create_logical_plan(select_stmt, bpm, tx_id, snapshot)
                .map_err(|_| {
                    ExecutionError::PlanningError("Failed to create logical plan".to_string())
                })?;
            let physical_plan = optimizer::optimize(logical_plan, bpm, tm, tx_id, snapshot)
                .map_err(|_| {
                    ExecutionError::PlanningError("Failed to create physical plan".to_string())
                })?;

            println!("[Executor] Physical Plan: {:?}", physical_plan);

            execute_physical_plan(&physical_plan, bpm, lm, tx_id, snapshot, select_stmt)
                .map(ExecuteResult::ResultSet)
        }
        Statement::CreateTable(create_stmt) => {
            execute_create_table(create_stmt, bpm, tm, wm, tx_id).map(|_| ExecuteResult::Ddl)
        }
        Statement::CreateIndex(create_stmt) => {
            execute_create_index(create_stmt, bpm, tm, lm, wm, tx_id, snapshot)
                .map(|_| ExecuteResult::Ddl)
        }
        Statement::Insert(insert_stmt) => {
            execute_insert(insert_stmt, bpm, tm, lm, wm, tx_id, snapshot).map(ExecuteResult::Insert)
        }
        Statement::Update(update_stmt) => {
            execute_update(update_stmt, bpm, tm, lm, wm, tx_id, snapshot).map(ExecuteResult::Update)
        }
        Statement::Delete(delete_stmt) => {
            execute_delete(delete_stmt, bpm, tm, lm, wm, tx_id, snapshot).map(ExecuteResult::Delete)
        }
        Statement::Vacuum(table_name) => {
            execute_vacuum(table_name, bpm, tm, lm, wm, tx_id, snapshot).map(|_| ExecuteResult::Ddl)
        }
        Statement::Analyze(table_name) => {
            execute_analyze(table_name, bpm, tm, lm, wm, tx_id, snapshot)
                .map(|_| ExecuteResult::Ddl)
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(ExecuteResult::Ddl),
        _ => Err(ExecutionError::GenericError(
            "Unsupported statement type".to_string(),
        )),
    }
}

// --- Physical Plan Executor ---

fn execute_physical_plan(
    plan: &PhysicalPlan,
    bpm: &Arc<BufferPoolManager>,
    lm: &Arc<LockManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    select_stmt: &SelectStatement,
) -> Result<ResultSet, ExecutionError> {
    match plan {
        PhysicalPlan::TableScan { table_name, filter } => {
            let (table_oid, first_page_id) = find_table(table_name, bpm, tx_id, snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
            let mut rows_with_ids = scan_table(
                bpm,
                lm,
                first_page_id,
                &schema,
                tx_id,
                snapshot,
                select_stmt.for_update,
            )?;

            if let Some(predicate) = filter {
                rows_with_ids
                    .retain(|(_, _, row)| evaluate_expr_for_row(predicate, row).unwrap_or(false));
            }

            let rows: Vec<Vec<String>> = rows_with_ids
                .into_iter()
                .map(|(_, _, row)| {
                    schema
                        .iter()
                        .map(|col| {
                            let val = row.get(&col.name).unwrap();
                            match val {
                                LiteralValue::Bool(b) => {
                                    if *b {
                                        "t".to_string()
                                    } else {
                                        "f".to_string()
                                    }
                                }
                                _ => val.to_string(),
                            }
                        })
                        .collect()
                })
                .collect();
            Ok(ResultSet {
                columns: schema,
                rows,
            })
        }
        PhysicalPlan::IndexScan {
            table_name, key, ..
        } => {
            let (table_oid, _first_page_id) = find_table(table_name, bpm, tx_id, snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;

            let index_name = "idx_id".to_string(); // Simplified
            let (_index_oid, index_root_page_id) =
                find_table(&index_name, bpm, tx_id, snapshot)?
                    .ok_or(ExecutionError::TableNotFound(index_name))?;

            let mut rows = Vec::new();
            if let Some(tuple_id) = btree::btree_search(bpm, index_root_page_id, *key)? {
                let (page_id, item_id) = tuple_id;
                let page_guard = bpm.acquire_page(page_id)?;
                let page = page_guard.read();
                if page.is_visible(snapshot, tx_id, item_id) {
                    if let Some(tuple_data) = page.get_tuple(item_id) {
                        let parsed = parse_tuple(tuple_data, &schema);
                        rows.push(
                            schema
                                .iter()
                                .map(|col| parsed.get(&col.name).unwrap().to_string())
                                .collect(),
                        );
                    }
                }
            }
            Ok(ResultSet {
                columns: schema,
                rows,
            })
        }
        PhysicalPlan::Filter { input, predicate } => {
            let input_result = execute_physical_plan(input, bpm, lm, tx_id, snapshot, select_stmt)?;
            let mut filtered_rows = Vec::new();
            for row_vec in &input_result.rows {
                let row_map = row_vec_to_map(row_vec, &input_result.columns, None);
                if evaluate_expr_for_row(predicate, &row_map)? {
                    filtered_rows.push(row_vec.clone());
                }
            }
            Ok(ResultSet {
                columns: input_result.columns,
                rows: filtered_rows,
            })
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
        } => {
            let left_result = execute_physical_plan(left, bpm, lm, tx_id, snapshot, select_stmt)?;
            let right_result = execute_physical_plan(right, bpm, lm, tx_id, snapshot, select_stmt)?;

            let mut joined_rows = Vec::new();
            let mut joined_columns = left_result.columns.clone();
            joined_columns.extend(right_result.columns.clone());

            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.as_str())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.as_str())
            } else {
                None
            };

            for left_row_vec in &left_result.rows {
                for right_row_vec in &right_result.rows {
                    let left_map =
                        row_vec_to_map(left_row_vec, &left_result.columns, left_table_name);
                    let right_map =
                        row_vec_to_map(right_row_vec, &right_result.columns, right_table_name);
                    let combined_map: HashMap<String, LiteralValue> =
                        left_map.into_iter().chain(right_map).collect();

                    if evaluate_expr_for_row(condition, &combined_map)? {
                        let mut joined_row_vec = left_row_vec.clone();
                        joined_row_vec.extend(right_row_vec.clone());
                        joined_rows.push(joined_row_vec);
                    }
                }
            }
            Ok(ResultSet {
                columns: joined_columns,
                rows: joined_rows,
            })
        }
        PhysicalPlan::MergeJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_result =
                execute_physical_plan(left, bpm, lm, tx_id, snapshot, select_stmt)?;
            let right_result =
                execute_physical_plan(right, bpm, lm, tx_id, snapshot, select_stmt)?;

            let mut joined_rows = Vec::new();
            let mut joined_columns = left_result.columns.clone();
            joined_columns.extend(right_result.columns.clone());

            let left_key_idx = left_result
                .columns
                .iter()
                .position(|c| c.name == left_key.to_string().split('.').next_back().unwrap())
                .unwrap();
            let right_key_idx = right_result
                .columns
                .iter()
                .position(|c| c.name == right_key.to_string().split('.').next_back().unwrap())
                .unwrap();

            let mut i = 0;
            let mut j = 0;
            while i < left_result.rows.len() && j < right_result.rows.len() {
                match left_result.rows[i][left_key_idx].cmp(&right_result.rows[j][right_key_idx]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        let j_start = j;
                        while j < right_result.rows.len()
                            && left_result.rows[i][left_key_idx]
                                == right_result.rows[j][right_key_idx]
                        {
                            let mut joined_row = left_result.rows[i].clone();
                            joined_row.extend(right_result.rows[j].clone());
                            joined_rows.push(joined_row);
                            j += 1;
                        }
                        i += 1;
                        j = j_start;
                    }
                }
            }

            Ok(ResultSet {
                columns: joined_columns,
                rows: joined_rows,
            })
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_result = execute_physical_plan(left, bpm, lm, tx_id, snapshot, select_stmt)?;
            let right_result = execute_physical_plan(right, bpm, lm, tx_id, snapshot, select_stmt)?;

            let mut joined_rows = Vec::new();
            let mut joined_columns = left_result.columns.clone();
            joined_columns.extend(right_result.columns.clone());

            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.as_str())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.as_str())
            } else {
                None
            };

            // Build phase
            let mut hash_table: HashMap<LiteralValue, Vec<&Vec<String>>> = HashMap::new();
            for right_row_vec in &right_result.rows {
                let right_row_map =
                    row_vec_to_map(right_row_vec, &right_result.columns, right_table_name);
                let key = evaluate_expr_for_row_to_val(right_key, &right_row_map)?;
                hash_table.entry(key).or_default().push(right_row_vec);
            }

            // Probe phase
            for left_row_vec in &left_result.rows {
                let left_row_map =
                    row_vec_to_map(left_row_vec, &left_result.columns, left_table_name);
                let key = evaluate_expr_for_row_to_val(left_key, &left_row_map)?;
                if let Some(matching_rows) = hash_table.get(&key) {
                    for right_row_vec in matching_rows {
                        let mut joined_row = left_row_vec.clone();
                        joined_row.extend((*right_row_vec).clone());
                        joined_rows.push(joined_row);
                    }
                }
            }
            Ok(ResultSet {
                columns: joined_columns,
                rows: joined_rows,
            })
        }
        PhysicalPlan::Projection { input, expressions } => {
            let input_result = execute_physical_plan(input, bpm, lm, tx_id, snapshot, select_stmt)?;
            if expressions
                .iter()
                .any(|item| matches!(item, SelectItem::Wildcard))
            {
                return Ok(input_result);
            }

            let mut projected_rows = Vec::new();
            let mut projected_columns = Vec::new();

            // Determine projected column names and types (simplified)
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
                // This is a simplification. We should ideally resolve the type from the input.
                projected_columns.push(Column { name, type_id: 25 });
            }

            for row_vec in &input_result.rows {
                let row_map: HashMap<String, LiteralValue> =
                    if let PhysicalPlan::HashJoin { left, right, .. }
                    | PhysicalPlan::NestedLoopJoin { left, right, .. } = &**input
                    {
                        // HACK: This is inefficient as it may re-fetch schema info.
                        // It's also brittle as it assumes the direct children of the join are table scans.
                        // A proper fix would involve a larger refactor of the ResultSet or execution context.
                        fn get_child_schema(
                            plan: &PhysicalPlan,
                            bpm: &Arc<BufferPoolManager>,
                            tx_id: u32,
                            snapshot: &Snapshot,
                        ) -> Result<Vec<Column>, ExecutionError> {
                            if let PhysicalPlan::TableScan { table_name, .. } = plan {
                                let (oid, _) = find_table(table_name, bpm, tx_id, snapshot)?
                                    .ok_or_else(|| {
                                        ExecutionError::TableNotFound(table_name.clone())
                                    })?;
                                get_table_schema(bpm, oid, tx_id, snapshot)
                            } else {
                                // This is not robust. We can't easily get the schema for a more complex child plan.
                                Ok(vec![])
                            }
                        }

                        let left_schema = get_child_schema(left, bpm, tx_id, snapshot)?;
                        let left_col_count = left_schema.len();

                        let (left_cols, right_cols) = input_result.columns.split_at(left_col_count);
                        let (left_vec, right_vec) = row_vec.split_at(left_col_count);

                        let left_table_name =
                            if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                                Some(table_name.as_str())
                            } else {
                                None
                            };
                        let right_table_name =
                            if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                                Some(table_name.as_str())
                            } else {
                                None
                            };

                        let left_map = row_vec_to_map(left_vec, left_cols, left_table_name);
                        let right_map = row_vec_to_map(right_vec, right_cols, right_table_name);
                        left_map.into_iter().chain(right_map).collect()
                    } else if let PhysicalPlan::Sort {
                        input: sort_input, ..
                    } = &**input
                    {
                        if let PhysicalPlan::HashJoin { left, right, .. }
                        | PhysicalPlan::NestedLoopJoin { left, right, .. } = &**sort_input
                        {
                            fn get_child_schema(
                                plan: &PhysicalPlan,
                                bpm: &Arc<BufferPoolManager>,
                                tx_id: u32,
                                snapshot: &Snapshot,
                            ) -> Result<Vec<Column>, ExecutionError> {
                                if let PhysicalPlan::TableScan { table_name, .. } = plan {
                                    let (oid, _) = find_table(table_name, bpm, tx_id, snapshot)?
                                        .ok_or_else(|| {
                                            ExecutionError::TableNotFound(table_name.clone())
                                        })?;
                                    get_table_schema(bpm, oid, tx_id, snapshot)
                                } else {
                                    Ok(vec![])
                                }
                            }

                            let left_schema = get_child_schema(left, bpm, tx_id, snapshot)?;
                            let left_col_count = left_schema.len();

                            let (left_cols, right_cols) =
                                input_result.columns.split_at(left_col_count);
                            let (left_vec, right_vec) = row_vec.split_at(left_col_count);

                            let left_table_name =
                                if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                                    Some(table_name.as_str())
                                } else {
                                    None
                                };
                            let right_table_name =
                                if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                                    Some(table_name.as_str())
                                } else {
                                    None
                                };

                            let left_map = row_vec_to_map(left_vec, left_cols, left_table_name);
                            let right_map = row_vec_to_map(right_vec, right_cols, right_table_name);
                            left_map.into_iter().chain(right_map).collect()
                        } else {
                            let table_name =
                                if let PhysicalPlan::TableScan { table_name, .. } = &**sort_input {
                                    Some(table_name.as_str())
                                } else {
                                    None
                                };
                            row_vec_to_map(&row_vec, &input_result.columns, table_name)
                        }
                    } else {
                        let table_name =
                            if let PhysicalPlan::TableScan { table_name, .. } = &**input {
                                Some(table_name.as_str())
                            } else {
                                None
                            };
                        row_vec_to_map(&row_vec, &input_result.columns, table_name)
                    };

                let mut projected_row = Vec::new();
                for item in expressions {
                    if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } =
                        item
                    {
                        let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                        projected_row.push(val.to_string());
                    }
                }
                projected_rows.push(projected_row);
            }

            Ok(ResultSet {
                columns: projected_columns,
                rows: projected_rows,
            })
        }
        PhysicalPlan::Sort { input, order_by } => {
            let mut input_result =
                execute_physical_plan(input, bpm, lm, tx_id, snapshot, select_stmt)?;

            // For simplicity, we only support sorting by a single column expression for now.
            if order_by.len() != 1 {
                return Err(ExecutionError::GenericError(
                    "ORDER BY only supports a single column".to_string(),
                ));
            }
            let sort_expr = &order_by[0];

            // Find the index and type of the sort key column
            let (sort_col_idx, sort_col_type) = if let Expression::Column(name) = sort_expr {
                input_result
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .map(|i| (i, input_result.columns[i].type_id))
                    .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone()))?
            } else {
                return Err(ExecutionError::GenericError(
                    "ORDER BY only supports column names".to_string(),
                ));
            };

            // Sort the rows
            input_result.rows.sort_by(|a, b| {
                let val_a = &a[sort_col_idx];
                let val_b = &b[sort_col_idx];
                match sort_col_type {
                    23 => {
                        // INT
                        let num_a = val_a.parse::<i32>().unwrap_or(0);
                        let num_b = val_b.parse::<i32>().unwrap_or(0);
                        num_a.cmp(&num_b)
                    }
                    25 => val_a.cmp(val_b), // TEXT
                    1082 => {
                        // DATE
                        let date_a = NaiveDate::parse_from_str(val_a, "%Y-%m-%d").unwrap();
                        let date_b = NaiveDate::parse_from_str(val_b, "%Y-%m-%d").unwrap();
                        date_a.cmp(&date_b)
                    }
                    16 => {
                        // BOOL
                        let bool_a = val_a == "t";
                        let bool_b = val_b == "t";
                        bool_a.cmp(&bool_b)
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });

            Ok(input_result)
        }
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
) -> Result<(), ExecutionError> {
    let (table_oid, table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?
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
        let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
        let rows_with_ids = scan_table(bpm, lm, table_page_id, &schema, tx_id, snapshot, false)?;
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
) -> Result<u32, ExecutionError> {
    let (table_oid, mut first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;

    lm.lock(tx_id, LockableResource::Table(table_oid), LockMode::Shared)?;

    let mut tuple_data = Vec::new();
    for value in &stmt.values {
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
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                let days = date.signed_duration_since(epoch).num_days() as i32;
                tuple_data.extend_from_slice(&days.to_be_bytes());
            }
            _ => {}
        }
    }

    if first_page_id == INVALID_PAGE_ID {
        let new_page_guard = bpm.new_page()?;
        first_page_id = new_page_guard.read().id;
        let mut page = new_page_guard.write();
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).unwrap();
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: first_page_id,
                item_id,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = page.read_header();
        header.lsn = lsn;
        page.write_header(&header);
        update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, table_oid, first_page_id)?;
        return Ok(1);
    }

    let mut current_page_id = first_page_id;
    loop {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let has_space = {
            let page = page_guard.read();
            let needed = tuple_data.len()
                + std::mem::size_of::<bedrock::page::HeapTupleHeaderData>()
                + std::mem::size_of::<bedrock::page::ItemIdData>();
            let header = page.read_header();
            header
                .upper_offset
                .saturating_sub(header.lower_offset)
                >= needed as u16
        };

        if has_space {
            let mut page = page_guard.write();
            let item_id = page.add_tuple(&tuple_data, tx_id, 0).unwrap();
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::InsertTuple {
                    tx_id,
                    page_id: current_page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = page.read_header();
            header.lsn = lsn;
            page.write_header(&header);
            return Ok(1);
        }

        let next_page_id = page_guard.read().read_header().next_page_id;
        if next_page_id == INVALID_PAGE_ID {
            let new_page_guard = bpm.new_page()?;
            let new_page_id = new_page_guard.read().id;
            let mut page = page_guard.write();
            let mut header = page.read_header();
            header.next_page_id = new_page_id;
            page.write_header(&header);
            let mut new_page = new_page_guard.write();
            let item_id = new_page.add_tuple(&tuple_data, tx_id, 0).unwrap();
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::InsertTuple {
                    tx_id,
                    page_id: new_page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = new_page.read_header();
            header.lsn = lsn;
            new_page.write_header(&header);
            return Ok(1);
        }
        current_page_id = next_page_id;
    }
}

fn execute_update(
    stmt: &UpdateStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(0);
    }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
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
                .get_tuple_header_mut(item_id)
                .map_or(true, |h| h.xmax != 0)
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

        let mut new_data = Vec::new();
        for col in &schema {
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
                }
            }
        }

        let old_raw = page.get_raw_tuple(item_id).unwrap().to_vec();
        page.get_tuple_header_mut(item_id).unwrap().xmax = tx_id;

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
            page.get_tuple_header_mut(item_id).unwrap().xmax = 0;
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
) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(0);
    }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let all_rows = scan_table(bpm, lm, first_page_id, &schema, tx_id, snapshot, false)?;
    let to_delete: Vec<_> = all_rows
        .into_iter()
        .filter(|(_, _, parsed)| {
            stmt.where_clause
                .as_ref()
                .map_or(true, |p| evaluate_expr_for_row(p, parsed).unwrap_or(false))
        })
        .collect();

    for (page_id, item_id, _) in to_delete {
        lm.lock(
            tx_id,
            LockableResource::Tuple((page_id, item_id)),
            LockMode::Exclusive,
        )?;
        let page_guard = bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();
        if !page.is_visible(snapshot, tx_id, item_id) {
            continue;
        }
        if let Some(header) = page.get_tuple_header_mut(item_id) {
            if header.xmax != 0 {
                return Err(ExecutionError::SerializationFailure);
            }
            header.xmax = tx_id;
            rows_affected += 1;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::DeleteTuple {
                    tx_id,
                    page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = page.read_header();
            header.lsn = lsn;
            page.write_header(&header);
        }
    }
    Ok(rows_affected)
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
        Expression::Column(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone())),
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
                    }
                }
                (LiteralValue::Bool(l), LiteralValue::Bool(r)) => match op {
                    BinaryOperator::Eq => Ok(LiteralValue::Bool(l == r)),
                    BinaryOperator::NotEq => Ok(LiteralValue::Bool(l != r)),
                    BinaryOperator::And => Ok(LiteralValue::Bool(l && r)),
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
) -> Result<(), ExecutionError> {
    let (table_oid, old_first_page_id) = find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;
    lm.lock(
        tx_id,
        LockableResource::Table(table_oid),
        LockMode::Exclusive,
    )?;
    if old_first_page_id == INVALID_PAGE_ID {
        return Ok(());
    }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let live_tuples: Vec<_> =
        scan_table(bpm, lm, old_first_page_id, &schema, tx_id, snapshot, false)?
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
            let mut tuple_data = Vec::new();
            for col in &schema {
                if let Some(val) = tuple.get(&col.name) {
                    match val {
                        LiteralValue::Number(n) => {
                            tuple_data.extend_from_slice(&n.parse::<i32>().unwrap().to_be_bytes())
                        }
                        LiteralValue::String(s) => {
                            tuple_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                            tuple_data.extend_from_slice(s.as_bytes());
                        }
                        LiteralValue::Bool(b) => tuple_data.push(if *b { 1 } else { 0 }),
                        LiteralValue::Date(s) => {
                            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                            let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
                            let days = date.signed_duration_since(epoch).num_days() as i32;
                            tuple_data.extend_from_slice(&days.to_be_bytes());
                        }
                    }
                }
            }

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
) -> Result<(), ExecutionError> {
    const RESERVOIR_SIZE: usize = 1000;
    const MCV_LIST_SIZE: usize = 10;
    const HISTOGRAM_BINS: usize = 100;

    let (table_oid, first_page_id) = find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(());
    }

    lm.lock(tx_id, LockableResource::Table(table_oid), LockMode::Shared)?;

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let all_rows = scan_table(bpm, lm, first_page_id, &schema, tx_id, snapshot, false)?;
    let total_rows = all_rows.len();

    for (i, column) in schema.iter().enumerate() {
        // --- Delete old statistics for this column ---
        let delete_stmt = DeleteStatement {
            table_name: "pg_statistic".to_string(),
            where_clause: Some(Expression::Binary {
                left: Box::new(Expression::Binary {
                    left: Box::new(Expression::Column("starelid".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(LiteralValue::Number(
                        table_oid.to_string(),
                    ))),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expression::Binary {
                    left: Box::new(Expression::Column("staattnum".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(LiteralValue::Number(i.to_string()))),
                }),
            }),
        };
        execute_delete_internal(&delete_stmt, bpm, tm, lm, wm, tx_id, snapshot)?;

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
        insert_tuple_into_system_table(&tuple_data, "pg_statistic", bpm, tm, wm, tx_id, snapshot)?;

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
) -> Result<(), ExecutionError> {
    let (table_oid, mut first_page_id) = find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;

    if first_page_id == INVALID_PAGE_ID {
        let new_page_guard = bpm.new_page()?;
        first_page_id = new_page_guard.read().id;
        let mut page = new_page_guard.write();
        let item_id = page.add_tuple(tuple_data, tx_id, 0).unwrap();

        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: first_page_id,
                item_id,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = page.read_header();
        header.lsn = lsn;
        page.write_header(&header);

        update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, table_oid, first_page_id)?;
        return Ok(());
    }

    let mut current_page_id = first_page_id;
    loop {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let has_space = {
            let page = page_guard.read();
            let needed = tuple_data.len()
                + std::mem::size_of::<bedrock::page::HeapTupleHeaderData>()
                + std::mem::size_of::<bedrock::page::ItemIdData>();
            let header = page.read_header();
            header
                .upper_offset
                .saturating_sub(header.lower_offset)
                >= needed as u16
        };

        if has_space {
            let mut page = page_guard.write();
            let item_id = page.add_tuple(tuple_data, tx_id, 0).unwrap();
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::InsertTuple {
                    tx_id,
                    page_id: current_page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = page.read_header();
            header.lsn = lsn;
            page.write_header(&header);
            return Ok(());
        }

        let next_page_id = page_guard.read().read_header().next_page_id;
        if next_page_id == INVALID_PAGE_ID {
            let new_page_guard = bpm.new_page()?;
            let new_page_id = new_page_guard.read().id;
            let mut page = page_guard.write();
            let mut header = page.read_header();
            header.next_page_id = new_page_id;
            page.write_header(&header);
            let mut new_page = new_page_guard.write();
            let item_id = new_page.add_tuple(tuple_data, tx_id, 0).unwrap();
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::InsertTuple {
                    tx_id,
                    page_id: new_page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = new_page.read_header();
            header.lsn = lsn;
            new_page.write_header(&header);
            return Ok(());
        }
        current_page_id = next_page_id;
    }
}

fn execute_delete_internal(
    stmt: &DeleteStatement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(stmt.table_name.clone()))?;
    if first_page_id == INVALID_PAGE_ID {
        return Ok(0);
    }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let all_rows = scan_table(bpm, lm, first_page_id, &schema, tx_id, snapshot, false)?;
    let to_delete: Vec<_> = all_rows
        .into_iter()
        .filter(|(_, _, parsed)| {
            stmt.where_clause
                .as_ref()
                .map_or(true, |p| evaluate_expr_for_row(p, parsed).unwrap_or(false))
        })
        .collect();

    for (page_id, item_id, _) in to_delete {
        lm.lock(
            tx_id,
            LockableResource::Tuple((page_id, item_id)),
            LockMode::Exclusive,
        )?;
        let page_guard = bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();
        if !page.is_visible(snapshot, tx_id, item_id) {
            continue;
        }
        if let Some(header) = page.get_tuple_header_mut(item_id) {
            if header.xmax != 0 {
                return Err(ExecutionError::SerializationFailure);
            }
            header.xmax = tx_id;
            rows_affected += 1;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &WalRecord::DeleteTuple {
                    tx_id,
                    page_id,
                    item_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
            let mut header = page.read_header();
            header.lsn = lsn;
            page.write_header(&header);
        }
    }
    Ok(rows_affected)
}
