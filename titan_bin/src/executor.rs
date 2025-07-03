//! The query executor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bedrock::buffer_pool::{BufferPoolManager};
use crate::parser::{CreateTableStatement, CreateIndexStatement, Expression, InsertStatement, UpdateStatement, DeleteStatement, LiteralValue, SelectItem, SelectStatement, Statement, DataType, BinaryOperator};
use bedrock::{PageId, PAGE_SIZE, TupleId};
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
    let mut schema = Vec::new();
    let attr_page_guard = bpm.acquire_page(PG_ATTRIBUTE_TABLE_OID)?;
    let attr_page = attr_page_guard.read();
    for i in 0..attr_page.get_tuple_count() {
        if attr_page.is_visible(snapshot, tx_id, i) {
            if let Some(tuple_data) = attr_page.get_tuple(i) {
                let rel_oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                if rel_oid == table_oid {
                    let type_id = u32::from_be_bytes(tuple_data[6..10].try_into().unwrap());
                    let name_len = tuple_data[10] as usize;
                    let name = String::from_utf8_lossy(&tuple_data[11..11 + name_len]).to_string();
                    schema.push(Column { name, type_id });
                }
            }
        }
    }
    Ok(schema)
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
        Statement::CreateIndex(create_stmt) => execute_create_index(create_stmt, bpm, wm, tx_id, snapshot).map(|_| ExecuteResult::Ddl),
        Statement::Insert(insert_stmt) => execute_insert(insert_stmt, bpm, wm, tx_id, snapshot).map(ExecuteResult::Insert),
        Statement::Update(update_stmt) => execute_update(update_stmt, bpm, wm, tx_id, snapshot).map(ExecuteResult::Update),
        Statement::Delete(delete_stmt) => execute_delete(delete_stmt, bpm, wm, tx_id, snapshot).map(ExecuteResult::Delete),
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
        page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: PG_CLASS_TABLE_OID, data: page_data_box })?;
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
            page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        }
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: PG_ATTRIBUTE_TABLE_OID, data: page_data_box })?;
        page.header_mut().lsn = lsn;
        println!("[execute_create_table] Added {} columns to pg_attribute for oid {}", stmt.columns.len(), new_table_oid);
    }

    Ok(())
}

fn execute_create_index(stmt: &CreateIndexStatement, bpm: &Arc<BufferPoolManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<(), ExecutionError> {
    let (_table_oid, table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    let index_file_name = format!("titan_bin/{}.idx", stmt.index_name);
    let mut index_pager = bedrock::pager::Pager::open(&index_file_name)?;
    let mut root_page_id = index_pager.allocate_page()?;
    
    {
        let page_guard = bpm.acquire_page(root_page_id)?;
        let mut page = page_guard.write();
        page.as_btree_leaf_page();
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: root_page_id, data: page_data_box })?;
        page.header_mut().lsn = lsn;
    }

    if table_page_id != 0 {
        let page_guard = bpm.acquire_page(table_page_id)?;
        let page = page_guard.read();
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let key = i32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                    let tuple_id: TupleId = (page.id, i);
                    root_page_id = btree::btree_insert(&mut index_pager, root_page_id, key, tuple_id)?;
                }
            }
        }
    }
    Ok(())
}

// --- DML ---

fn execute_insert(stmt: &InsertStatement, bpm: &Arc<BufferPoolManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
    println!("[INSERT] table: {}, tx_id: {}", stmt.table_name, tx_id);
    let (table_oid, mut table_page_id) = find_table(&stmt.table_name, bpm, tx_id, snapshot)?.ok_or_else(|| ExecutionError::TableNotFound(()))?;
    
    if table_page_id == 0 {
        // This is the first insert into this table, so we need to allocate a real page for it.
        let new_page_guard = bpm.new_page()?;
        let new_page_id = new_page_guard.read().id;
        // CRITICAL FIX: Assign the new_page_id to table_page_id for the subsequent insert.
        table_page_id = new_page_id;
        println!("[INSERT] Allocated new page {} for table '{}'", new_page_id, stmt.table_name);

        // Now, update the pg_class entry for this table to point to the new page.
        // This is a critical step. We create a new version of the pg_class row.
        let pg_class_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let mut pg_class_page = pg_class_guard.write();
        
        // Find the old entry and mark it as deleted by the current transaction.
        for i in 0..pg_class_page.get_tuple_count() {
            // We need a fresh snapshot here to see the just-created table entry.
            // But for simplicity in this step, we just look for the oid.
            if let Some(tuple_data) = pg_class_page.get_tuple(i) {
                let oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                let page_id = u32::from_be_bytes(tuple_data[4..8].try_into().unwrap());
                if oid == table_oid && page_id == 0 { // Find the initial entry
                    if let Some(header) = pg_class_page.get_tuple_header_mut(i) {
                        header.xmax = tx_id;
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

        // Log the change to pg_class to the WAL.
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&pg_class_page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: PG_CLASS_TABLE_OID, data: page_data_box })?;
        pg_class_page.header_mut().lsn = lsn;
    }

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
        page.add_tuple(&tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
        
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: table_page_id, data: page_data_box })?;
        page.header_mut().lsn = lsn;
    }

    Ok(1)
}

fn execute_update(stmt: &UpdateStatement, bpm: &Arc<BufferPoolManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
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
            page.add_tuple(&new_tuple_data, tx_id, 0).ok_or(ExecutionError::GenericError(()))?;
            rows_affected += 1;
        }
    }

    if rows_affected > 0 {
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: table_page_id, data: page_data_box })?;
        page.header_mut().lsn = lsn;
    }

    Ok(rows_affected)
}

fn execute_delete(stmt: &DeleteStatement, bpm: &Arc<BufferPoolManager>, wm: &Arc<Mutex<WalManager>>, tx_id: u32, snapshot: &Snapshot) -> Result<u32, ExecutionError> {
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
        }
    }

    if rows_affected > 0 {
        let mut page_data_box = Box::new([0u8; PAGE_SIZE]);
        page_data_box.copy_from_slice(&page.data);
        let lsn = wm.lock().unwrap().log(&WalRecord::SetPage { tx_id, page_id: table_page_id, data: page_data_box })?;
        page.header_mut().lsn = lsn;
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
        // TODO: Handle specific column selection
        table_schema.clone() 
    };

    let mut rows = Vec::new();
    if table_page_id != 0 {
        let page_guard = bpm.acquire_page(table_page_id)?;
        let page = page_guard.read();
        println!("[SELECT] scanning page_id: {}, tuple_count: {}", page.id, page.get_tuple_count());
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(tuple_data) = page.get_tuple(i) {
                    println!("[SELECT] visible tuple data on page {}: {:?}", page.id, tuple_data);
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
