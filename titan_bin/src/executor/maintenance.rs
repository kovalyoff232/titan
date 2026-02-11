use super::dml::{
    find_or_create_page_for_insert, insert_tuple_and_log, scan_table, serialize_literal_map,
};
use super::helpers::parse_tuple;
use crate::catalog::{SystemCatalog, update_pg_class_page_id};
use crate::errors::ExecutionError;
use crate::parser::LiteralValue;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::{LockManager, LockMode, LockableResource};
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::WalManager;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(super) fn execute_vacuum(
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

pub(super) fn execute_analyze(
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
    let all_rows = scan_table(bpm, lm, first_page_id, &schema, tx_id, snapshot, false)?;
    let total_rows = all_rows.len();

    let (pg_stat_oid, pg_stat_first_page_id) = if let Some(info) = system_catalog
        .lock()
        .unwrap()
        .find_table("pg_statistic", bpm, tx_id, snapshot)?
    {
        info
    } else {
        return Err(ExecutionError::TableNotFound("pg_statistic".to_string()));
    };
    let pg_stat_schema =
        system_catalog
            .lock()
            .unwrap()
            .get_table_schema(bpm, pg_stat_oid, tx_id, snapshot)?;

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
        if modified {}
        current_page_id = page.read_header().next_page_id;
    }

    for (i, column) in schema.iter().enumerate() {
        let mut column_values: Vec<LiteralValue> = all_rows
            .iter()
            .filter_map(|(_, _, parsed_row)| parsed_row.get(&column.name).cloned())
            .collect();

        let n_distinct = column_values
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len() as i32;
        let mut tuple_data = Vec::new();
        tuple_data.extend_from_slice(&table_oid.to_be_bytes());
        tuple_data.extend_from_slice(&(i as u32).to_be_bytes());
        tuple_data.extend_from_slice(&n_distinct.to_be_bytes());
        tuple_data.extend_from_slice(&1i32.to_be_bytes());
        tuple_data.extend_from_slice(&0i32.to_be_bytes());
        let empty_text = "";
        tuple_data.extend_from_slice(&(empty_text.len() as u32).to_be_bytes());
        tuple_data.extend_from_slice(empty_text.as_bytes());
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

        if total_rows == 0 {
            continue;
        }

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
            tuple_data.extend_from_slice(&0i32.to_be_bytes());
            tuple_data.extend_from_slice(&2i32.to_be_bytes());
            tuple_data.extend_from_slice(&0i32.to_be_bytes());
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
            tuple_data.extend_from_slice(&3i32.to_be_bytes());
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

    let _ = insert_tuple_and_log(bpm, tm, wm, tx_id, page_id, tuple_data)?;
    Ok(())
}
