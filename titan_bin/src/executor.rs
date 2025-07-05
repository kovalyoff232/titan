//! The query executor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bedrock::buffer_pool::{BufferPoolManager};
use crate::parser::{CreateTableStatement, CreateIndexStatement, Expression, InsertStatement, UpdateStatement, DeleteStatement, LiteralValue, SelectItem, SelectStatement, Statement, DataType, BinaryOperator, TableReference};
use bedrock::{PageId, TupleId};
use bedrock::page::INVALID_PAGE_ID;
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
    let (table_oid, mut first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;

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

    // Handle the very first insertion into a table.
    if first_page_id == INVALID_PAGE_ID {
        println!("[INSERT] First insert for table '{}' (oid {}). Allocating a new page.", stmt.table_name, table_oid);
        let new_page_guard = bpm.new_page()?;
        let new_page_id = new_page_guard.read().id;
        first_page_id = new_page_id;
        println!("[INSERT] Allocated new page {} for table '{}'", new_page_id, stmt.table_name);

        let mut page = new_page_guard.write();
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: first_page_id, item_id })?;
        tm.set_last_lsn(tx_id, lsn);
        page.header_mut().lsn = lsn;

        // Update pg_class to point to the new page.
        update_pg_class_page_id(bpm, tm, wm, tx_id, snapshot, table_oid, first_page_id)?;
        return Ok(1);
    }

    // Find a page with enough space in the chain.
    let mut current_page_id = first_page_id;
    loop {
        let page_guard = bpm.acquire_page(current_page_id)?;
        
        // Check for space first with a read lock.
        let has_space = {
            let page = page_guard.read();
            let tuple_header_len = std::mem::size_of::<bedrock::page::HeapTupleHeaderData>();
            let tuple_len = tuple_data.len() + tuple_header_len;
            let item_id_len = std::mem::size_of::<bedrock::page::ItemIdData>();
            let needed_space = tuple_len + item_id_len;
            page.header().upper_offset.saturating_sub(page.header().lower_offset) >= needed_space as u16
        };

        if has_space {
            let mut page = page_guard.write();
            let item_id = page.add_tuple(&tuple_data, tx_id, 0).unwrap(); // Should not fail
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: current_page_id, item_id })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
            println!("[INSERT] Successfully inserted tuple with item_id {} into page {}", item_id, current_page_id);
            return Ok(1);
        }

        // If no space, move to the next page or allocate a new one.
        let (next_page_id, page_id_to_link) = {
            let page = page_guard.read();
            (page.header().next_page_id, page.id)
        };

        if next_page_id == INVALID_PAGE_ID {
            let new_page_guard = bpm.new_page()?;
            let new_page_id = new_page_guard.read().id;
            
            { // Link the old page to the new one
                let mut current_page = page_guard.write();
                current_page.header_mut().next_page_id = new_page_id;
                
                let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
                let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::SetNextPageId { tx_id, page_id: page_id_to_link, next_page_id: new_page_id })?;
                tm.set_last_lsn(tx_id, lsn);
                current_page.header_mut().lsn = lsn;
            }

            let mut new_page = new_page_guard.write();
            let item_id = new_page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::InsertTuple { tx_id, page_id: new_page_id, item_id })?;
            tm.set_last_lsn(tx_id, lsn);
            new_page.header_mut().lsn = lsn;
            println!("[INSERT] Allocated new page {} and inserted tuple.", new_page_id);
            return Ok(1);
        }
        current_page_id = next_page_id;
    }
}

/// Helper to update the page_id in the pg_class table for a given table OID.
fn update_pg_class_page_id(bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot, table_oid: u32, new_page_id: PageId) -> Result<(), ExecutionError> {
    let pg_class_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
    let mut pg_class_page = pg_class_guard.write();
    
    let mut old_item_id = None;
    let mut old_tuple_data = None;

    // Find the old entry and mark it as deleted.
    for i in 0..pg_class_page.get_tuple_count() {
        if pg_class_page.is_visible(snapshot, tx_id, i) {
            let oid = {
                let tuple_data = pg_class_page.get_tuple(i).unwrap();
                u32::from_be_bytes(tuple_data[0..4].try_into().unwrap())
            };
            if oid == table_oid {
                let tuple_data_vec = pg_class_page.get_tuple(i).unwrap().to_vec();
                if let Some(header) = pg_class_page.get_tuple_header_mut(i) {
                    header.xmax = tx_id;
                    old_item_id = Some(i);
                    old_tuple_data = Some(tuple_data_vec);
                    println!("[update_pg_class] Marked pg_class entry for oid {} (item_id {}) as deleted by tx {}", table_oid, i, tx_id);
                }
                break;
            }
        }
    }

    // Add the new entry with the correct page_id.
    if let Some(mut tuple_data) = old_tuple_data {
        tuple_data[4..8].copy_from_slice(&new_page_id.to_be_bytes());
        let new_item_id = pg_class_page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        println!("[update_pg_class] Added new pg_class entry for oid {} with page_id {} at item_id {}", table_oid, new_page_id, new_item_id);

        // Log changes to WAL
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
    Ok(())
}


fn execute_update(stmt: &UpdateStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    if first_page_id == INVALID_PAGE_ID { return Ok(0); }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let mut to_update: Vec<(PageId, u16)> = Vec::new();

    // First, collect all tuples to be updated
    let mut current_page_id = first_page_id;
    while current_page_id != INVALID_PAGE_ID {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let page = page_guard.read();
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(where_clause) = &stmt.where_clause {
                    if let Some(tuple_data) = page.get_tuple(i) {
                        let parsed_tuple = parse_tuple(tuple_data, &schema);
                        if evaluate_expr_for_row(where_clause, &parsed_tuple)? {
                            to_update.push((current_page_id, i));
                        }
                    }
                } else {
                    to_update.push((current_page_id, i));
                }
            }
        }
        current_page_id = page.header().next_page_id;
    }

    // Now, perform the updates
    for (page_id, item_id) in to_update {
        let page_guard = bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();

        if let Some(old_tuple_data) = page.get_tuple(item_id) {
            let mut parsed_tuple = parse_tuple(old_tuple_data, &schema);

            for (col_name, expr) in &stmt.assignments {
                if let Expression::Literal(val) = expr {
                    parsed_tuple.insert(col_name.clone(), val.clone());
                }
            }

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
            
            if let Some(header) = page.get_tuple_header_mut(item_id) {
                header.xmax = tx_id;
            }
            let old_tuple_raw = page.get_raw_tuple(item_id).unwrap().to_vec();
            
            // TODO: This could insert to a different page if the current one is full.
            // For now, assume it fits on the same page.
            let new_item_id = page.add_tuple(&new_tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
            rows_affected += 1;

            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::UpdateTuple { tx_id, page_id, item_id: new_item_id, old_data: old_tuple_raw })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
        }
    }

    Ok(rows_affected)
}

fn execute_delete(stmt: &DeleteStatement, bpm: &Arc<BufferPoolManager>, tm: &Arc<TransactionManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    let (table_oid, first_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    if first_page_id == INVALID_PAGE_ID { return Ok(0); }

    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    let mut rows_affected = 0;
    let mut to_delete: Vec<(PageId, u16)> = Vec::new();

    // First, collect all tuples to be deleted
    let mut current_page_id = first_page_id;
    while current_page_id != INVALID_PAGE_ID {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let page = page_guard.read();
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(where_clause) = &stmt.where_clause {
                    if let Some(tuple_data) = page.get_tuple(i) {
                        let parsed_tuple = parse_tuple(tuple_data, &schema);
                        if evaluate_expr_for_row(where_clause, &parsed_tuple)? {
                            to_delete.push((current_page_id, i));
                        }
                    }
                } else {
                    to_delete.push((current_page_id, i));
                }
            }
        }
        current_page_id = page.header().next_page_id;
    }

    // Now, perform the deletions
    for (page_id, item_id) in to_delete {
        let page_guard = bpm.acquire_page(page_id)?;
        let mut page = page_guard.write();
        if let Some(header) = page.get_tuple_header_mut(item_id) {
            header.xmax = tx_id;
            rows_affected += 1;
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(tx_id, prev_lsn, &WalRecord::DeleteTuple { tx_id, page_id, item_id })?;
            tm.set_last_lsn(tx_id, lsn);
            page.header_mut().lsn = lsn;
        }
    }

    Ok(rows_affected)
}

fn execute_select(stmt: &SelectStatement, bpm: &Arc<BufferPoolManager>, tx_id: u32, snapshot: &Snapshot) -> Result<ResultSet, ExecutionError> {
    if stmt.from.is_empty() {
        return Err(ExecutionError::GenericError(())); // SELECT without FROM is not supported
    }

    // For now, we only support a single table or a single JOIN clause.
    let (left_table_name, right_table_name, on_condition) = match &stmt.from[0] {
        TableReference::Table { name } => (name.clone(), None, None),
        TableReference::Join { left, right, on_condition } => {
            let left_name = if let TableReference::Table { name } = &**left { name.clone() } else { return Err(ExecutionError::GenericError(())); };
            let right_name = if let TableReference::Table { name } = &**right { name.clone() } else { return Err(ExecutionError::GenericError(())); };
            (left_name, Some(right_name), Some(on_condition.clone()))
        }
    };

    // --- Data Loading ---
    let (left_oid, left_page_id) = find_table(&left_table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    let left_schema = get_table_schema(bpm, left_oid, tx_id, snapshot)?;
    let left_rows = scan_table(bpm, left_page_id, &left_schema, tx_id, snapshot)?;

    let (right_schema, right_rows) = if let Some(right_table_name_val) = &right_table_name {
        let (right_oid, right_page_id) = find_table(right_table_name_val, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
        let schema = get_table_schema(bpm, right_oid, tx_id, snapshot)?;
        let rows = scan_table(bpm, right_page_id, &schema, tx_id, snapshot)?;
        (Some(schema), Some(rows))
    } else {
        (None, None)
    };

    // --- Column Projection ---
    let mut projected_columns: Vec<Column> = Vec::new();
    if right_table_name.is_none() { // Single table case
        if stmt.select_list.len() == 1 && matches!(stmt.select_list[0], SelectItem::Wildcard) {
            projected_columns.extend(left_schema.clone());
        } else {
            for item in &stmt.select_list {
                if let SelectItem::UnnamedExpr(Expression::Column(name)) = item {
                    if let Some(col) = left_schema.iter().find(|c| &c.name == name) {
                        projected_columns.push(col.clone());
                    } else {
                        return Err(ExecutionError::ColumnNotFound(()));
                    }
                }
            }
        }
    } else { // JOIN case
        for item in &stmt.select_list {
            match item {
                SelectItem::Wildcard => {
                    projected_columns.extend(left_schema.iter().map(|c| Column { name: format!("{}.{}", left_table_name, c.name), ..c.clone() }));
                    if let Some(schema) = &right_schema {
                        projected_columns.extend(schema.iter().map(|c| Column { name: format!("{}.{}", right_table_name.as_ref().unwrap(), c.name), ..c.clone() }));
                    }
                }
                SelectItem::QualifiedWildcard(table_name) => {
                     if table_name == &left_table_name {
                        projected_columns.extend(left_schema.iter().map(|c| Column { name: format!("{}.{}", left_table_name, c.name), ..c.clone() }));
                     } else if right_table_name.as_ref() == Some(table_name) {
                        projected_columns.extend(right_schema.as_ref().unwrap().iter().map(|c| Column { name: format!("{}.{}", right_table_name.as_ref().unwrap(), c.name), ..c.clone() }));
                     }
                }
                SelectItem::UnnamedExpr(expr) => {
                     match expr {
                        Expression::Column(col_name) => {
                            if let Some(col) = left_schema.iter().find(|c| &c.name == col_name) {
                                projected_columns.push(Column { name: format!("{}.{}", left_table_name, col.name), ..col.clone() });
                            } else if let Some(schema) = &right_schema {
                                if let Some(col) = schema.iter().find(|c| &c.name == col_name) {
                                     projected_columns.push(Column { name: format!("{}.{}", right_table_name.as_ref().unwrap(), col.name), ..col.clone() });
                                }
                            }
                        }
                        Expression::QualifiedColumn(table_name, col_name) => {
                            if table_name == &left_table_name {
                                if let Some(col) = left_schema.iter().find(|c| &c.name == col_name) {
                                    projected_columns.push(Column { name: format!("{}.{}", table_name, col.name), ..col.clone() });
                                }
                            } else if right_table_name.as_ref() == Some(table_name) {
                                if let Some(col) = right_schema.as_ref().unwrap().iter().find(|c| &c.name == col_name) {
                                    projected_columns.push(Column { name: format!("{}.{}", table_name, col.name), ..col.clone() });
                                }
                            }
                        }
                        _ => {}
                     }
                }
                _ => {}
            }
        }
    }
    
    // --- Execution ---
    let mut result_rows = Vec::new();
    if let (Some(right_rows), Some(on_condition)) = (right_rows, on_condition) {
        // Nested Loop Join
        for left_row in &left_rows {
            for right_row in &right_rows {
                let mut combined_row = HashMap::new();
                for (k, v) in left_row { combined_row.insert(format!("{}.{}", left_table_name, k), v.clone()); }
                for (k, v) in right_row { combined_row.insert(format!("{}.{}", right_table_name.as_ref().unwrap(), k), v.clone()); }

                if evaluate_join_condition(&on_condition, &combined_row)? {
                    let mut row_vec = Vec::new();
                    for col in &projected_columns {
                        if let Some(val) = combined_row.get(&col.name) {
                            row_vec.push(val.to_string());
                        }
                    }
                    result_rows.push(row_vec);
                }
            }
        }
    } else { 
        // Single Table Scan
        let mut final_rows = Vec::new();
        let mut used_index = false;

        // Index Scan optimization
        if let Some(Expression::Binary { left, op: BinaryOperator::Eq, right }) = &stmt.where_clause {
            if let (Expression::Column(col_name), Expression::Literal(LiteralValue::Number(val_str))) = (&**left, &**right) {
                let index_name = format!("idx_{}", col_name);
                if let Ok(Some((_index_oid, index_root_page_id))) = find_table(&index_name, bpm, tx_id, snapshot) {
                    if index_root_page_id != INVALID_PAGE_ID {
                        let key = val_str.parse::<i32>().unwrap_or(0);
                        if let Some(tuple_id) = btree::btree_search(bpm, index_root_page_id, key)? {
                            let (page_id, item_id) = tuple_id;
                            let page_guard = bpm.acquire_page(page_id)?;
                            let page = page_guard.read();
                            if page.is_visible(snapshot, tx_id, item_id) {
                                if let Some(tuple_data) = page.get_tuple(item_id) {
                                     let parsed_tuple = parse_tuple(tuple_data, &left_schema);
                                     final_rows.push(parsed_tuple);
                                }
                            }
                            used_index = true;
                        }
                    }
                }
            }
        }

        // Full Table Scan if index is not used
        if !used_index {
            for row_map in left_rows {
                let should_include = if let Some(where_clause) = &stmt.where_clause {
                    evaluate_expr_for_row(where_clause, &row_map)?
                } else {
                    true
                };

                if should_include {
                    final_rows.push(row_map);
                }
            }
        }
        
        for row_map in final_rows {
             let mut row_vec = Vec::new();
             for col in &projected_columns {
                 if let Some(val) = row_map.get(&col.name) {
                     row_vec.push(val.to_string());
                 }
             }
             result_rows.push(row_vec);
        }
    }

    println!("[SELECT] found {} rows", result_rows.len());
    Ok(ResultSet { columns: projected_columns, rows: result_rows })
}

/// Scans a table and returns a vector of rows, where each row is a map of column name to value.
fn scan_table(bpm: &Arc<BufferPoolManager>, first_page_id: PageId, schema: &Vec<Column>, tx_id: u32, snapshot: &Snapshot) -> Result<Vec<HashMap<String, LiteralValue>>, ExecutionError> {
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
                if let Some(tuple_data) = page.get_tuple(i) {
                    rows.push(parse_tuple(tuple_data, schema));
                }
            }
        }
        current_page_id = page.header().next_page_id;
    }
    Ok(rows)
}

fn evaluate_join_condition(expr: &Expression, row: &HashMap<String, LiteralValue>) -> Result<bool, ExecutionError> {
     match expr {
        Expression::Binary { left, op, right } => {
            let left_val = evaluate_expr_for_row_to_val(left, row)?;
            let right_val = evaluate_expr_for_row_to_val(right, row)?;
            
            // This is a simplified comparison, assumes strings or numbers that can be compared as strings.
            // A real implementation would handle types properly.
            let result = match op {
                BinaryOperator::Eq => left_val.to_string() == right_val.to_string(),
                _ => false // Only equality is supported for now
            };
            Ok(result)
        }
        _ => Err(ExecutionError::GenericError(())),
    }
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
            row.get(name).cloned().ok_or_else(|| ExecutionError::ColumnNotFound(()))
        },
        Expression::QualifiedColumn(table, col) => {
            let qualified_name = format!("{}.{}", table, col);
            row.get(&qualified_name).cloned().ok_or_else(|| ExecutionError::ColumnNotFound(()))
        }
        _ => Err(ExecutionError::GenericError(())),
    }
}