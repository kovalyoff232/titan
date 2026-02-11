use super::TableStats;
use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::executor::parse_tuple;
use crate::parser::LiteralValue;
use crate::planner::LogicalPlan;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::Snapshot;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub(super) fn get_table_names(plan: &LogicalPlan) -> HashSet<String> {
    let mut tables = HashSet::new();
    match plan {
        LogicalPlan::Scan { table_name, .. } => {
            tables.insert(table_name.clone());
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Limit { input, .. } => {
            tables.extend(get_table_names(input));
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOperation { left, right, .. } => {
            tables.extend(get_table_names(left));
            tables.extend(get_table_names(right));
        }
        LogicalPlan::CteRef { name } => {
            tables.insert(format!("cte_{}", name));
        }
        LogicalPlan::WithCte { input, .. } => {
            tables.extend(get_table_names(input));
        }
    }
    tables
}

pub(super) fn load_statistics_for_table(
    table_name: &str,
    stats: &mut HashMap<String, Arc<TableStats>>,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    let mut catalog = system_catalog
        .lock()
        .map_err(|_| ExecutionError::GenericError("system catalog lock poisoned".to_string()))?;
    if catalog.get_statistics(table_name).is_some() && catalog.get_schema(table_name).is_some() {
        return Ok(());
    }

    let (table_oid, _) = catalog
        .find_table(table_name, bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))?;

    let (pg_stat_oid, pg_stat_first_page_id) = catalog
        .find_table("pg_statistic", bpm, tx_id, snapshot)?
        .ok_or_else(|| ExecutionError::TableNotFound("pg_statistic".to_string()))?;

    if pg_stat_first_page_id == INVALID_PAGE_ID {
        return Ok(());
    }

    let table_schema = catalog.get_table_schema(bpm, table_oid, tx_id, snapshot)?;
    catalog.add_schema(table_name.to_string(), Arc::new(table_schema));

    let pg_stat_schema = catalog.get_table_schema(bpm, pg_stat_oid, tx_id, snapshot)?;

    let mut table_stats = TableStats::default();
    let mut current_page_id = pg_stat_first_page_id;

    while current_page_id != INVALID_PAGE_ID {
        let page_guard = bpm.acquire_page(current_page_id)?;
        let page = page_guard.read();
        for i in 0..page.get_tuple_count() {
            if page.is_visible(snapshot, tx_id, i) {
                if let Some(tuple_data) = page.get_tuple(i) {
                    let parsed = parse_tuple(tuple_data, &pg_stat_schema);
                    let relid = parsed
                        .get("starelid")
                        .and_then(|v| v.to_string().parse::<u32>().ok());

                    if relid == Some(table_oid) {
                        let attnum = parsed
                            .get("staattnum")
                            .and_then(|v| v.to_string().parse::<usize>().ok())
                            .unwrap_or(0);
                        let kind = parsed
                            .get("stakind")
                            .and_then(|v| v.to_string().parse::<i32>().ok())
                            .unwrap_or(0);

                        let col_stats = table_stats.column_stats.entry(attnum).or_default();

                        match kind {
                            1 => {
                                if let Some(LiteralValue::Number(n)) = parsed.get("stadistinct") {
                                    col_stats.n_distinct = n.parse().unwrap_or(0.0);
                                }
                            }
                            2 => {
                                if let Some(LiteralValue::String(s)) = parsed.get("stavalues") {
                                    col_stats.most_common_vals = s
                                        .split(',')
                                        .map(|v| LiteralValue::String(v.to_string()))
                                        .collect();
                                }
                            }
                            3 => {
                                if let Some(LiteralValue::String(s)) = parsed.get("stanumbers") {
                                    col_stats.histogram_bounds = s
                                        .split(',')
                                        .map(|v| LiteralValue::String(v.to_string()))
                                        .collect();
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        current_page_id = page.read_header().next_page_id;
    }

    let table_stats_arc = Arc::new(table_stats);
    stats.insert(table_name.to_string(), table_stats_arc.clone());
    catalog.add_statistics(table_name.to_string(), table_stats_arc);

    Ok(())
}
