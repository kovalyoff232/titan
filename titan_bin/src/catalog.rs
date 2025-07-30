//! Manages the system catalogs, which store metadata about the database.
//!
//! The system catalogs are special tables that store information about tables, columns,
//! and other database objects. This module provides functions for accessing and
//! manipulating the system catalogs.

use crate::errors::ExecutionError;
use crate::types::Column;
use bedrock::buffer_pool::BufferPoolManager;

use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::WalManager;
use bedrock::PageId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The object ID of the `pg_class` table, which stores information about tables.
pub const PG_CLASS_TABLE_OID: PageId = 0;
/// The object ID of the `pg_attribute` table, which stores information about columns.
pub const PG_ATTRIBUTE_TABLE_OID: PageId = 1;

pub struct SystemCatalog {}

impl SystemCatalog {
    pub fn new() -> Self {
        Self {}
    }

    /// Finds a table by name in the `pg_class` catalog.
    ///
    /// This function searches the `pg_class` table for a table with the given name.
    /// It returns the table's object ID and the page ID of its first page.
    pub fn find_table(
        &mut self,
        name: &str,
        bpm: &Arc<BufferPoolManager>,
        tx_id: u32,
        snapshot: &Snapshot,
    ) -> Result<Option<(u32, u32)>, ExecutionError> {
        if name == "pg_class" {
            return Ok(Some((PG_CLASS_TABLE_OID, PG_CLASS_TABLE_OID)));
        }
        if name == "pg_attribute" {
            return Ok(Some((PG_ATTRIBUTE_TABLE_OID, PG_ATTRIBUTE_TABLE_OID)));
        }

        let page_guard = bpm.acquire_page(PG_CLASS_TABLE_OID)?;
        let page = page_guard.read();
        let mut best_candidate: Option<(bedrock::page::TransactionId, u32, u32)> = None;

        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let (Some(tuple_data), Some(item_id_data)) =
                    (page.get_tuple(i), page.get_item_id_data(i))
                {
                    let header = page.read_tuple_header(item_id_data.offset);
                    let name_len = tuple_data[8] as usize;
                    if tuple_data.len() >= 9 + name_len {
                        let table_name = String::from_utf8_lossy(&tuple_data[9..9 + name_len]);
                        if table_name == name {
                            println!(
                                "[find_table] Checking item_id: {}, xmin: {}, xmax: {}, visible: true, name: {}",
                                i, header.xmin, header.xmax, table_name
                            );
                            let table_oid =
                                u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                            let table_page_id =
                                u32::from_be_bytes(tuple_data[4..8].try_into().unwrap());

                            // We need the latest committed version that is visible to us.
                            if best_candidate.is_none() || header.xmin > best_candidate.unwrap().0 {
                                best_candidate = Some((header.xmin, table_oid, table_page_id));
                                println!(
                                    "[find_table] Found candidate for '{}': oid={}, page_id={}, xmin={}",
                                    name, table_oid, table_page_id, header.xmin
                                );
                            }
                        }
                    }
                }
            }
        }

        if let Some((xmin, oid, page_id)) = best_candidate {
            println!(
                "[find_table] Selected best candidate for '{}': oid={}, page_id={}, xmin={}",
                name, oid, page_id, xmin
            );
            Ok(Some((oid, page_id)))
        } else {
            println!("[find_table] No visible table named '{}' found.", name);
            Ok(None)
        }
    }

    pub fn get_table_schema(
        &mut self,
        bpm: &Arc<BufferPoolManager>,
        table_oid: u32,
        tx_id: u32,
        snapshot: &Snapshot,
    ) -> Result<Vec<Column>, ExecutionError> {
        let mut schema_cols = Vec::new();
        let mut current_page_id = PG_ATTRIBUTE_TABLE_OID;

        while current_page_id != bedrock::page::INVALID_PAGE_ID {
            let attr_page_guard = bpm.acquire_page(current_page_id)?;
            let attr_page = attr_page_guard.read();
            for i in 0..attr_page.get_tuple_count() {
                if attr_page.is_visible(snapshot, tx_id, i) {
                    if let Some(tuple_data) = attr_page.get_tuple(i) {
                        let rel_oid = u32::from_be_bytes(tuple_data[0..4].try_into().unwrap());
                        if rel_oid == table_oid {
                            let attnum = u16::from_be_bytes(tuple_data[4..6].try_into().unwrap());
                            let type_id = u32::from_be_bytes(tuple_data[6..10].try_into().unwrap());
                            let name_len = tuple_data[10] as usize;
                            let name =
                                String::from_utf8_lossy(&tuple_data[11..11 + name_len]).to_string();
                            schema_cols.push((attnum, Column { name, type_id }));
                        }
                    }
                }
            }
            current_page_id = attr_page.read_header().next_page_id;
        }

        schema_cols.sort_by_key(|(attnum, _)| *attnum);
        let schema: Vec<Column> = schema_cols.into_iter().map(|(_, col)| col).collect();
        Ok(schema)
    }
}

pub fn update_pg_class_page_id(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    table_oid: u32,
    new_page_id: PageId,
) -> Result<(), ExecutionError> {
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
                if let Some(item_id_data) = pg_class_page.get_item_id_data(i) {
                    let mut header = pg_class_page.read_tuple_header(item_id_data.offset);
                    header.xmax = tx_id;
                    pg_class_page.write_tuple_header(item_id_data.offset, &header);
                    old_item_id = Some(i);
                    old_tuple_data = Some(tuple_data_vec);
                }
                break;
            }
        }
    }

    // Add the new entry with the correct page_id.
    if let Some(mut tuple_data) = old_tuple_data {
        tuple_data[4..8].copy_from_slice(&new_page_id.to_be_bytes());
        let new_item_id = pg_class_page
            .add_tuple(&tuple_data, tx_id, 0)
            .ok_or_else(|| {
                ExecutionError::GenericError("Failed to insert into pg_class".to_string())
            })?;

        if let Some(old_id) = old_item_id {
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = wm.lock().unwrap().log(
                tx_id,
                prev_lsn,
                &bedrock::wal::WalRecord::DeleteTuple {
                    tx_id,
                    page_id: PG_CLASS_TABLE_OID,
                    item_id: old_id,
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
        }
        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = wm.lock().unwrap().log(
            tx_id,
            prev_lsn,
            &bedrock::wal::WalRecord::InsertTuple {
                tx_id,
                page_id: PG_CLASS_TABLE_OID,
                item_id: new_item_id,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = pg_class_page.read_header();
        header.lsn = lsn;
        pg_class_page.write_header(&header);
    }
    Ok(())
}
