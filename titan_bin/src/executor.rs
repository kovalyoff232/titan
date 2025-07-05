//! The query executor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bedrock::buffer_pool::{BufferPoolManager};
use crate::parser::{CreateTableStatement, CreateIndexStatement, Expression, InsertStatement, UpdateStatement, DeleteStatement, LiteralValue, SelectItem, SelectStatement, Statement, DataType, BinaryOperator};
use bedrock::{PageId, TupleId};
use bedrock::wal::{WalManager, WalRecord};
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::btree;

// --- Result Enums ---

#[derive(Debug)]
pub enum ExecuteResult {
    ResultSet(ResultSet),
    Insert(u32),
    Delete(u32),
    Update(u32),
    Ddl,
}

#[derive(Clone, Debug)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub type_id: u32,
}

#[derive(Debug)]
pub enum ExecutionError {
    IoError(()),
    TableNotFound(()),
    ColumnNotFound(()),
    GenericError(()),
}

impl From<std::io::Error> for ExecutionError {
    fn from(err: std::io::Error) -> Self {
        println!("[ExecutionError] IO Error: {}", err);
        ExecutionError::IoError(())
    }
}

// --- Constants ---

const PG_CLASS_TABLE_OID: PageId = 0;
const PG_ATTRIBUTE_TABLE_OID: PageId = 1;

// --- Helper Functions ---

fn find_table(name: &str, bpm: &Arc<BufferPoolManager>, tx_id: u32, snapshot: &Snapshot) -> Result<Option<(u32, u32)>, ExecutionError> {
    println!("[find_table] Searching for table '{}' with tx_id: {} and snapshot: {:?}", name, tx_id, snapshot);
    let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
    let page = page_guard.read();
    let mut best_candidate: Option<(bedrock::page::TransactionId, u32, u32)> = None;

    for i in 0..page.get_tuple_count() {
        if page.is_visible(snapshot, tx_id, i) {
            if let (Some(tuple_data), Some(item_id_data)) = (page.get_tuple(i), page.get_item_id_data(i)) {
                let header = page.tuple_header(item_id_data.offset);
                let name_len = tuple_data[8] as usize;
                if tuple_data.len() >= 9 + name_len {
                    let table_name = String::from_utf8_lossy(&tuple_data[9..9 + name_len]);
                    if table_name == name {
                        println!("[find_table] Checking item_id: {}, xmin: {}, xmax: {}, visible: true, name: {}", i, header.xmin, header.xmax, table_name);
                        let table_oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                        let table_page_id = u32::from_be_bytes(tuple_data[4..8].try_into().unwrap());
                        
                        // We want the most recent committed version that is visible to us.
                        if best_candidate.is_none() || header.xmin > best_candidate.unwrap().0 {
                            best_candidate = Some((header.xmin, table_oid, table_page_id));
                            println!("[find_table] Found candidate for '{}': oid={}, page_id={}, xmin={}", name, table_oid, table_page_id, header.xmin);
                        }
                    }
                }
            }
        }
    }
    if let Some((xmin, oid, page_id)) = best_candidate {
        println!("[find_table] Selected best candidate for '{}': oid={}, page_id={}, xmin={}", name, oid, page_id, xmin);
        Ok(Some((oid, page_id)))
    } else {
        println!("[find_table] No visible table named '{}' found.", name);
        Ok(None)
    }
}

fn get_table_schema(bpm: &Arc<BufferPoolManager>, table_oid: u32, tx_id: u32, snapshot: &Snapshot) -> Result<Vec<Column>, ExecutionError> {
    let mut schema_cols = Vec::new();
    let attr_page_guard = bpm.acquire_page(PG_ATTRIBUTE_TABLE_OID)?;
    let attr_page = attr_page_guard.read();
    for i in 0..attr_page.get_tuple_count() {
        if attr_page.is_visible(snapshot, tx_id, i) {
            if let Some(tuple_data) = attr_page.get_tuple(i) {
                let rel_oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                if rel_oid == table_oid {
                    let attnum = u16::from_be_bytes(tuple_data[4..6].try_into().unwrap());
                    let type_id = u32::from_be_bytes(tuple_data[6..10].try_into().unwrap());
                    let name_len = tuple_data[10] as usize;
                    let name = String::from_utf8_lossy(&tuple_data[11..11 + name_len]).to_string();
                    schema_cols.push((attnum, Column { name, type_id }));
                }
            }
        }
    }
    // Sort columns by attribute number to ensure correct order
    schema_cols.sort_by_key(|(attnum, _)| *attnum);
    Ok(schema_cols.into_iter().map(|(_, col)| col).collect())
}



fn parse_tuple(tuple_data: &[u8], schema: &Vec<Column>) -> HashMap<String, LiteralValue> {
    let mut offset = 0;
    let mut parsed_tuple = HashMap::new();

    for col in schema {
        if offset >= tuple_data.len() {
            break;
        }
        match col.type_id {
            23 => { // INT
                if offset + 4 > tuple_data.len() { break; }
                let val = i32::from_be_bytes(tuple_data[offset..offset+4].try_into().unwrap());
                offset += 4;
                parsed_tuple.insert(col.name.clone(), LiteralValue::Number(val.to_string()));
            }
            25 => { // TEXT
                if offset + 4 > tuple_data.len() { break; }
                let len = u32::from_be_bytes(tuple_data[offset..offset+4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > tuple_data.len() { break; }
                let val = String::from_utf8_lossy(&tuple_data[offset..offset+len]);
                offset += len;
                parsed_tuple.insert(col.name.clone(), LiteralValue::String(val.into_owned()));
            }
            _ => {}
        }
    }
    parsed_tuple
}

// --- Main Executor ---

pub fn execute(stmt: &Statement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<ExecuteResult, ExecutionError> {
    match stmt {
        Statement::Select(select_stmt) => execute_select(select_stmt, bpm, tx_id, snapshot).map(ExecuteResult::ResultSet),
        Statement::CreateTable(create_stmt) => execute_create_table(create_stmt, bpm, tm, wm, tx_id).map(|_| ExecuteResult::Ddl),
        Statement::CreateIndex(create_stmt) => execute_create_index(create_stmt, bpm, tm, wm, tx_id, snapshot).map(|_| ExecuteResult::Ddl),
        Statement::Insert(insert_stmt) => execute_insert(insert_stmt, bpm, tm, wm, tx_id, snapshot).map(ExecuteResult::Insert),
        Statement::Update(update_stmt) => execute_update(update_stmt, bpm, tm, wm, tx_id, snapshot).map(ExecuteResult::Update),
        Statement::Delete(delete_stmt) => execute_delete(delete_stmt, bpm, tm, wm, tx_id, snapshot).map(ExecuteResult::Delete),
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(ExecuteResult::Ddl),
        _ => Ok(ExecuteResult::Ddl),
    }
}

// --- DDL ---

fn execute_create_table(stmt: &CreateTableStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32) -> Result<(), ExecutionError> {
    println!("[execute_create_table] Creating table '{}' in tx_id {}", stmt.table_name, tx_id);
    let new_table_oid = tm.get_next_oid();
    
    // Insert into pg_class
    {
        println!("[execute_create_table] Acquiring lock on PG_CLASS_TABLE_OID");
        let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut page = page_guard.write();
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&new_table_oid.to_be_bytes());
        // PageId is initially 0, will be updated on first insert
        tuple_data.extend_from_slice(&0u32.to_be_bytes()); 
        tuple_data.push(stmt.table_name.len() as u8);
        tuple_data.extend_from_slice(stmt.table_name.as_bytes());
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: PG_CLASS_TABLE_OID, item_id })?;
        tm.set_last_lsn(tx_id, lsn);
        page.header_mut().lsn = lsn;
        println!("[execute_create_table] Added to pg_class with oid {}", new_table_oid);
    }

    // Insert into pg_attribute
    {
        println!("[execute_create_table] Acquiring lock on PG_ATTRIBUTE_TABLE_OID");
        let page_guard = bpm.acquire_page(PG_ATTRIBUTE_TABLE_OID)?;
        let mut page = page_guard.write();
        for (i, col) in stmt.columns.iter().enumerate() {
            let mut tuple_data = Vec::new();
            tuple_data.extend_from_slice(&new_table_oid.to_be_bytes());
            tuple_data.extend_from_slice(&(i as u16).to_be_bytes());
            let type_id: u32 = match col.data_type { DataType::Int => 23, DataType::Text => 25 };
            tuple_data.extend_from_slice(&type_id.to_be_bytes());
            tuple_data.push(col.name.len() as u8);
            tuple_data.extend_from_slice(col.name.as_bytes());
            let item_id = page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: PG_ATTRIBUTE_TABLE_OID, item_id })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
        }
        println!("[execute_create_table] Added {} columns to pg_attribute for oid {}", stmt.columns.len(), new_table_oid);
    }

    Ok(())
}

fn execute_create_index(stmt: &CreateIndexStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<(), ExecutionError> {
    let (table_oid, table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    
    // Create a new B-Tree root page
    let root_page_guard = bpm.new_page()?;
    let mut root_page_id = root_page_guard.read().id;
    root_page_guard.write().as_btree_leaf_page();
    drop(root_page_guard);

    // Create a new relation for the index in pg_class
    let index_oid = tm.get_next_oid();
    {
        let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut page = page_guard.write();
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&index_oid.to_be_bytes());
        tuple_data.extend_from_slice(&root_page_id.to_be_bytes());
        tuple_data.push(stmt.index_name.len() as u8);
        tuple_data.extend_from_slice(stmt.index_name.as_bytes());
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: PG_CLASS_TABLE_OID, item_id })?;
        tm.set_last_lsn(tx_id, lsn);
        page.header_mut().lsn = lsn;
    }

    // Populate the index by scanning the entire table
    println!("[create_index] Populating index '{}' for table '{}'...", stmt.index_name, stmt.table_name);
    if table_page_id != 0 {
        let page_guard = bpm.acquire_page(table_page_id)?;
        let page = page_guard.read();
        let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
        
        let _col_idx = schema.iter().position(|c| c.name == stmt.column_name).ok_or(ExecutionError::ColumnNotFound(()))?;

        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let parsed_tuple = parse_tuple(tuple_data, &schema);
                    if let Some(LiteralValue::Number(key_str)) = parsed_tuple.get(&stmt.column_name) {
                        if let Ok(key) = key_str.parse::<i32>() {
                             let tuple_id: TupleId = (page.id, i);
                             root_page_id = btree::btree_insert(bpm, tm, wm, tx_id, root_page_id, key, tuple_id)?;
                        }
                    }
                }
            }
        }
        println!("[create_index] Finished populating index.");
    } else {
        println!("[create_index] Table is empty, no data to populate.");
    }

    // Update the root_page_id in pg_class
    {
        let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut page = page_guard.write();
        for i in 0..page.get_tuple_count() {
            if let Some(tuple_data) = page.get_tuple(i) {
                let oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                if oid == index_oid {
                    if let Some(tuple) = page.get_raw_tuple_mut(i) {
                        tuple[4..8].copy_from_slice(&root_page_id.to_be_bytes());
                    }
                    break;
                }
            }
        }
    }

    Ok(())
}

// --- DML ---

fn execute_insert(stmt: &InsertStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    println!("[INSERT] table: '{}', tx_id: {}", stmt.table_name, tx_id);
    let (table_oid, mut table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    
    // This block handles the very first insertion into a table, allocating its first page.
    if table_page_id == 0 {
        println!("[INSERT] First insert for table '{}' (oid {}). Allocating a new page.", stmt.table_name, table_oid);
        let new_page_guard = bpm.new_page()?;
        let new_page_id = new_page_guard.read().id;
        table_page_id = new_page_id; // Use this new page for the insert.
        println!("[INSERT] Allocated new page {} for table '{}'", new_page_id, stmt.table_name);

        // Now, we must update the pg_class entry for this table to point to the newly allocated page.
        // This is a critical step for persistence. We create a new version of the pg_class row.
        let pg_class_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut pg_class_page = pg_class_guard.write();
        
        let mut old_item_id = None;
        // Find the old entry (where page_id was 0) and mark it as deleted by the current transaction.
        for i in 0..pg_class_page.get_tuple_count() {
            if let Some(tuple_data) = pg_class_page.get_tuple(i) {
                let oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                let page_id = u32::from_be_bytes(tuple_data[4..8].try_into().unwrap());
                // We need to find the specific version of the row for this table that has page_id 0
                // and is visible to our current transaction.
                if oid == table_oid && page_id == 0 && pg_class_page.is_visible(snapshot, tx_id, i) {
                    if let Some(header) = pg_class_page.get_tuple_header_mut(i) {
                        header.xmax = tx_id;
                        old_item_id = Some(i);
                        println!("[INSERT] Marked initial pg_class entry for oid {} (item_id {}) as deleted by tx {}", table_oid, i, tx_id);
                    }
                    break;
                }
            }
        }

        // Add the new entry with the correct page_id.
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&table_oid.to_be_bytes());
        tuple_data.extend_from_slice(&table_page_id.to_be_bytes());
        tuple_data.push(stmt.table_name.len() as u8);
        tuple_data.extend_from_slice(stmt.table_name.as_bytes());
        let new_item_id = pg_class_page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        println!("[INSERT] Added new pg_class entry for oid {} with page_id {} at item_id {}", table_oid, table_page_id, new_item_id);

        // Log the changes to pg_class to the WAL.
        if let Some(old_id) = old_item_id {
             let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
             let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::DeleteTuple { tx_id, page_id: PG_CLASS_TABLE_OID, item_id: old_id })?;
             tm.set_last_lsn(tx_id, lsn);
        }
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: PG_CLASS_TABLE_OID, item_id: new_item_id })?;
        tm.set_last_lsn(tx_id, lsn);
        pg_class_page.header_mut().lsn = lsn;
    }

    // This block executes for ALL inserts (first or subsequent).
    {
        println!("[INSERT] Inserting tuple into page_id: {}", table_page_id);
        let page_guard = bpm.acquire_page(table_page_id)?;
        let mut page = page_guard.write();
        let mut tuple_data = Vec::new();
        for value in &stmt.values {
            match value {
                Expression::Literal(LiteralValue::Number(n)) => tuple_data.extend_from_slice(&n.parse::<i32>().unwrap_or(0).to_be_bytes()),
                Expression::Literal(LiteralValue::String(s)) => {
                    tuple_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    tuple_data.extend_from_slice(s.as_bytes());
                }
                _ => {}
            }
        }
        // TODO: Handle page full scenario. For now, we assume it fits.
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        // Log the specific insert operation to the WAL.
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: table_page_id, item_id })?;
        tm.set_last_lsn(tx_id, lsn);
        page.header_mut().lsn = lsn;
        println!("[INSERT] Successfully inserted tuple with item_id {} into page {}", item_id, table_page_id);
    }

    Ok(1)
}

fn execute_update(stmt: &UpdateStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    let (table_oid, table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    if table_page_id == 0 { return Ok(0); }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let page_guard = bpm.acquire_page(table_page_id)?;
    let mut page = page_guard.write();
    let mut to_update = Vec::new();

    for i in 0..page.get_tuple_count() {
        if page.is_visible(snapshot, tx_id, i) {
            if let Some(where_clause) = &stmt.where_clause {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let parsed_tuple = parse_tuple(tuple_data, &schema);
                    if evaluate_expr_for_row(where_clause, &parsed_tuple)? {
                        to_update.push(i);
                    }
                }
            } else {
                to_update.push(i);
            }
        }
    }

    for item_id in to_update {
        if let Some(old_tuple_data) = page.get_tuple(item_id) {
            let mut parsed_tuple = parse_tuple(old_tuple_data, &schema);

            // Apply assignments
            for (col_name, expr) in &stmt.assignments {
                // For simplicity, this example assumes the expression is a literal.
                // A real implementation would evaluate the expression.
                if let Expression::Literal(val) = expr {
                    parsed_tuple.insert(col_name.clone(), val.clone());
                }
            }

            // Serialize the new tuple
            let mut new_tuple_data = Vec::new();
            for col in &schema {
                if let Some(val) = parsed_tuple.get(&col.name) {
                    match val {
                        LiteralValue::Number(n) => new_tuple_data.extend_from_slice(&n.parse::<i32>().unwrap_or(0).to_be_bytes()),
                        LiteralValue::String(s) => {
                            new_tuple_data.extend_from_slice(&(s.len() as u32).to_be_bytes());
                            new_tuple_data.extend_from_slice(s.as_bytes());
                        }
                    }
                }
            }
            
            // Mark old tuple as deleted and add the new one
            if let Some(header) = page.get_tuple_header_mut(item_id) {
                header.xmax = tx_id;
            }
            let old_tuple_raw = page.get_raw_tuple(item_id).unwrap().to_vec();
            let new_item_id = page.add_tuple(&new_tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
            rows_affected += 1;

            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::UpdateTuple { tx_id, page_id: table_page_id, item_id: new_item_id, old_data: old_tuple_raw })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
        }
    }

    Ok(rows_affected)
}

fn execute_delete(stmt: &DeleteStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    let (table_oid, table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    if table_page_id == 0 { return Ok(0); }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let page_guard = bpm.acquire_page(table_page_id)?;
    let mut page = page_guard.write();
    let mut to_delete = Vec::new();

    for i in 0..page.get_tuple_count() {
        if page.is_visible(snapshot, tx_id, i) {
            if let Some(where_clause) = &stmt.where_clause {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let parsed_tuple = parse_tuple(tuple_data, &schema);
                    if evaluate_expr_for_row(where_clause, &parsed_tuple)? {
                        to_delete.push(i);
                    }
                }
            } else {
                to_delete.push(i);
            }
        }
    }

    for item_id in to_delete {
        if let Some(header) = page.get_tuple_header_mut(item_id) {
            header.xmax = tx_id;
            rows_affected += 1;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::DeleteTuple { tx_id, page_id: table_page_id, item_id })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
        }
    }

    Ok(rows_affected)
}

fn execute_select(stmt: &SelectStatement, bpm: &Arc<BufferPoolManager>, tx_id: u32, snapshot: &Snapshot) -> Result<ResultSet, ExecutionError> {
    let from_table = stmt.from_table.as_ref().ok_or(ExecutionError::GenericError(()))?;
    println!("[SELECT] from_table: {}, tx_id: {}", from_table, tx_id);
    let (table_oid, table_page_id) = find_table(from_table, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    println!("[SELECT] table_oid: {}, table_page_id: {}", table_oid, table_page_id);
    
    let table_schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    if table_schema.is_empty() {
        println!("[SELECT] schema not found or empty for table_oid: {}", table_oid);
        return Ok(ResultSet { columns: vec![], rows: vec![] });
    }

    let projected_columns = if stmt.select_list.len() == 1 && matches!(stmt.select_list[0], SelectItem::Wildcard) {
        table_schema.clone()
    } else {
        let mut proj_cols = Vec::new();
        for item in &stmt.select_list {
            if let SelectItem::UnnamedExpr(Expression::Column(name)) = item {
                if let Some(col) = table_schema.iter().find(|c| c.name == *name) {
                    proj_cols.push(col.clone());
                } else {
                    return Err(ExecutionError::ColumnNotFound(()));
                }
            }
            // TODO: Handle expressions and other select items
        }
        proj_cols
    };

    let mut rows = Vec::new();

    // Simple optimization: If there is a WHERE clause on an indexed column, use an index scan.
    let mut used_index = false;
    if let Some(Expression::Binary { left, op: BinaryOperator::Eq, right }) = &stmt.where_clause {
        if let (Expression::Column(col_name), Expression::Literal(LiteralValue::Number(val_str))) = (&**left, &**right) {
            // This is a simplified check. A real system would check pg_index.
            if col_name == "id" {
                let index_name = format!("idx_{}", col_name);
                if let Ok(Some((_index_oid, index_root_page_id))) = find_table(&index_name, bpm, tx_id, snapshot) {
                    if index_root_page_id != 0 {
                        let key = val_str.parse::<i32>().unwrap_or(0);
                        if let Some(tuple_id) = btree::btree_search(bpm, index_root_page_id, key)? {
                            let (page_id, item_id) = tuple_id;
                            let page_guard = bpm.acquire_page(page_id)?;
                            let page = page_guard.read();
                            if page.is_visible(snapshot, tx_id, item_id) {
                                if let Some(tuple_data) = page.get_tuple(item_id) {
                                     let parsed_tuple = parse_tuple(tuple_data, &table_schema);
                                     let mut row = Vec::new();
                                     for col in &projected_columns {
                                         if let Some(value) = parsed_tuple.get(&col.name) {
                                             row.push(value.to_string());
                                         } else {
                                             row.push("".to_string());
                                         }
                                     }
                                     rows.push(row);
                                }
                            }
                            used_index = true;
                        }
                    }
                }
            }
        }
    }

    if !used_index && table_page_id != 0 {
        let page_guard = bpm.acquire_page(table_page_id)?;
        let page = page_guard.read();
        println!("[SELECT] scanning page_id: {}, tuple_count: {}", page.id, page.get_tuple_count());
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let parsed_tuple = parse_tuple(tuple_data, &table_schema);

                    // Filter rows based on WHERE clause if it exists
                    let should_include = if let Some(where_clause) = &stmt.where_clause {
                        evaluate_expr_for_row(where_clause, &parsed_tuple).unwrap_or(false)
                    } else {
                        true // No WHERE clause means include all rows
                    };

                    if should_include {
                        let mut row = Vec::new();
                        for col in &projected_columns {
                            if let Some(value) = parsed_tuple.get(&col.name) {
                                row.push(value.to_string());
                            } else {
                                row.push("".to_string()); 
                            }
                        }
                        rows.push(row);
                    }
                }
            }
        }
    }
    println!("[SELECT] found {} rows", rows.len());
    Ok(ResultSet { columns: projected_columns, rows })
}

fn evaluate_expr_for_row(expr: &Expression, row: &HashMap<String, LiteralValue>) -> Result<bool, ExecutionError> {
    match expr {
        Expression::Binary { left, op, right } => {
            let left_val = evaluate_expr_for_row_to_val(left, row)?;
            let right_val = evaluate_expr_for_row_to_val(right, row)?;
            
            let left_num = match left_val {
                LiteralValue::Number(s) => s.parse::<i32>().map_err(|_| ExecutionError::GenericError(()))?,
                _ => return Err(ExecutionError::GenericError(())),
            };
            let right_num = match right_val {
                LiteralValue::Number(s) => s.parse::<i32>().map_err(|_| ExecutionError::GenericError(()))?,
                _ => return Err(ExecutionError::GenericError(())),
            };

            let result = match op {
                BinaryOperator::Eq => left_num == right_num,
                BinaryOperator::NotEq => left_num != right_num,
                BinaryOperator::Lt => left_num < right_num,
                BinaryOperator::LtEq => left_num <= right_num,
                BinaryOperator::Gt => left_num > right_num,
                BinaryOperator::GtEq => left_num >= right_num,
            };
            Ok(result)
        }
        _ => Err(ExecutionError::GenericError(())),
    }
}

fn evaluate_expr_for_row_to_val<'a>(expr: &'a Expression, row: &HashMap<String, LiteralValue>) -> Result<LiteralValue, ExecutionError> {
    match expr {
        Expression::Literal(literal) => Ok(literal.clone()),
        Expression::Column(name) => {
            println!("[evaluate_expr] Looking for column '{}' in row: {:?}", name, row.keys());
            row.get(name).cloned().ok_or_else(|| ExecutionError::ColumnNotFound(()))
        },
        _ => Err(ExecutionError::GenericError(())),
    }
}
