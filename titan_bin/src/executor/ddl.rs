use super::dml::scan_table;
use crate::catalog::{
    PG_ATTRIBUTE_TABLE_OID, PG_CLASS_TABLE_OID, SystemCatalog, update_pg_class_page_id,
};
use crate::errors::ExecutionError;
use crate::parser::{CreateIndexStatement, CreateTableStatement, DataType, LiteralValue};
use bedrock::btree;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::{WalManager, WalRecord};
use std::sync::{Arc, Mutex};

pub(super) fn execute_create_table(
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
    let before_page = pg_class_page.data.to_vec();
    let item_id = pg_class_page
        .add_tuple(&tuple_data, tx_id, 0)
        .ok_or_else(|| {
            ExecutionError::GenericError("Failed to insert into pg_class".to_string())
        })?;
    let after_page = pg_class_page.data.to_vec();

    let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
    let lsn = wm.lock().unwrap().log(
        tx_id,
        prev_lsn,
        &WalRecord::InsertTuple {
            tx_id,
            page_id: PG_CLASS_TABLE_OID,
            item_id,
            before_page,
            after_page,
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
        let before_page = pg_attr_page.data.to_vec();
        let item_id = pg_attr_page
            .add_tuple(&attr_tuple_data, tx_id, 0)
            .ok_or_else(|| {
                ExecutionError::GenericError("Failed to insert into pg_attribute".to_string())
            })?;
        let after_page = pg_attr_page.data.to_vec();

        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: PG_ATTRIBUTE_TABLE_OID,
                item_id,
                before_page,
                after_page,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = pg_attr_page.read_header();
        header.lsn = lsn;
        pg_attr_page.write_header(&header);
    }
    Ok(())
}

pub(super) fn execute_create_index(
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
        let before_page = page.data.to_vec();
        let item_id = page.add_tuple(&tuple_data, tx_id, 0).unwrap();
        let after_page = page.data.to_vec();
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &WalRecord::InsertTuple {
                tx_id,
                page_id: PG_CLASS_TABLE_OID,
                item_id,
                before_page,
                after_page,
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
