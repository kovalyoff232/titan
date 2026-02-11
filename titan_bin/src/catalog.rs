use crate::errors::ExecutionError;
use crate::optimizer::TableStats;
use crate::types::Column;
use bedrock::buffer_pool::BufferPoolManager;

use bedrock::PageId;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::WalManager;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub const PG_CLASS_TABLE_OID: PageId = 0;

pub const PG_ATTRIBUTE_TABLE_OID: PageId = 1;

pub struct SystemCatalog {
    stats_cache: Mutex<HashMap<String, Arc<TableStats>>>,
    schema_cache: Mutex<HashMap<String, Arc<Vec<Column>>>>,
}

fn read_u32(data: &[u8], start: usize) -> Option<u32> {
    data.get(start..start + 4)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(u32::from_be_bytes)
}

fn read_u16(data: &[u8], start: usize) -> Option<u16> {
    data.get(start..start + 2)
        .and_then(|b| <[u8; 2]>::try_from(b).ok())
        .map(u16::from_be_bytes)
}

fn lock_wal<'a>(
    wm: &'a Arc<Mutex<WalManager>>,
) -> Result<MutexGuard<'a, WalManager>, ExecutionError> {
    wm.lock()
        .map_err(|_| ExecutionError::GenericError("wal lock poisoned".to_string()))
}

impl SystemCatalog {
    pub fn new() -> Self {
        Self {
            stats_cache: Mutex::new(HashMap::new()),
            schema_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_statistics(&self, table_name: &str) -> Option<Arc<TableStats>> {
        self.stats_cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(table_name).cloned())
    }

    pub fn add_statistics(&self, table_name: String, stats: Arc<TableStats>) {
        if let Ok(mut cache) = self.stats_cache.lock() {
            cache.insert(table_name, stats);
        } else {
            eprintln!("[catalog] failed to acquire stats_cache lock");
        }
    }

    pub fn get_schema(&self, table_name: &str) -> Option<Arc<Vec<Column>>> {
        self.schema_cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(table_name).cloned())
    }

    pub fn add_schema(&self, table_name: String, schema: Arc<Vec<Column>>) {
        if let Ok(mut cache) = self.schema_cache.lock() {
            cache.insert(table_name, schema);
        } else {
            eprintln!("[catalog] failed to acquire schema_cache lock");
        }
    }

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
                    if tuple_data.len() < 9 {
                        continue;
                    }
                    let header = page.read_tuple_header(item_id_data.offset);
                    let name_len = tuple_data[8] as usize;
                    if tuple_data.len() >= 9 + name_len {
                        let table_name = String::from_utf8_lossy(&tuple_data[9..9 + name_len]);
                        if table_name == name {
                            println!(
                                "[find_table] Checking item_id: {}, xmin: {}, xmax: {}, visible: true, name: {}",
                                i, header.xmin, header.xmax, table_name
                            );
                            if let (Some(table_oid), Some(table_page_id)) =
                                (read_u32(tuple_data, 0), read_u32(tuple_data, 4))
                            {
                                if best_candidate
                                    .as_ref()
                                    .map_or(true, |(xmin, _, _)| header.xmin > *xmin)
                                {
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
                        let Some(rel_oid) = read_u32(tuple_data, 0) else {
                            continue;
                        };
                        if rel_oid == table_oid {
                            let Some(attnum) = read_u16(tuple_data, 4) else {
                                continue;
                            };
                            let Some(type_id) = read_u32(tuple_data, 6) else {
                                continue;
                            };
                            let Some(name_len) = tuple_data.get(10).copied().map(|b| b as usize)
                            else {
                                continue;
                            };
                            if tuple_data.len() < 11 + name_len {
                                continue;
                            }
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
    let mut delete_before_page: Option<Vec<u8>> = None;
    let mut delete_after_page: Option<Vec<u8>> = None;

    for i in 0..pg_class_page.get_tuple_count() {
        if pg_class_page.is_visible(snapshot, tx_id, i) {
            let oid = {
                let Some(tuple_data) = pg_class_page.get_tuple(i) else {
                    continue;
                };
                let Some(oid) = read_u32(tuple_data, 0) else {
                    continue;
                };
                oid
            };
            if oid == table_oid {
                let Some(tuple_data_vec) = pg_class_page.get_tuple(i).map(|d| d.to_vec()) else {
                    continue;
                };
                if let Some(item_id_data) = pg_class_page.get_item_id_data(i) {
                    delete_before_page = Some(pg_class_page.data.to_vec());
                    let mut header = pg_class_page.read_tuple_header(item_id_data.offset);
                    header.xmax = tx_id;
                    pg_class_page.write_tuple_header(item_id_data.offset, &header);
                    delete_after_page = Some(pg_class_page.data.to_vec());
                    old_item_id = Some(i);
                    old_tuple_data = Some(tuple_data_vec);
                }
                break;
            }
        }
    }

    if let Some(mut tuple_data) = old_tuple_data {
        if let Some(old_id) = old_item_id {
            let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
            let lsn = lock_wal(wm)?.log(
                tx_id,
                prev_lsn,
                &bedrock::wal::WalRecord::DeleteTuple {
                    tx_id,
                    page_id: PG_CLASS_TABLE_OID,
                    item_id: old_id,
                    before_page: delete_before_page.unwrap_or_default(),
                    after_page: delete_after_page.unwrap_or_default(),
                },
            )?;
            tm.set_last_lsn(tx_id, lsn);
        }

        let before_insert_page = pg_class_page.data.to_vec();
        tuple_data[4..8].copy_from_slice(&new_page_id.to_be_bytes());
        let new_item_id = pg_class_page
            .add_tuple(&tuple_data, tx_id, 0)
            .ok_or_else(|| {
                ExecutionError::GenericError("Failed to insert into pg_class".to_string())
            })?;
        let after_insert_page = pg_class_page.data.to_vec();

        let prev_lsn = tm.get_last_lsn(tx_id).unwrap_or(0);
        let lsn = lock_wal(wm)?.log(
            tx_id,
            prev_lsn,
            &bedrock::wal::WalRecord::InsertTuple {
                tx_id,
                page_id: PG_CLASS_TABLE_OID,
                item_id: new_item_id,
                before_page: before_insert_page,
                after_page: after_insert_page,
            },
        )?;
        tm.set_last_lsn(tx_id, lsn);
        let mut header = pg_class_page.read_header();
        header.lsn = lsn;
        pg_class_page.write_header(&header);
    }
    Ok(())
}
