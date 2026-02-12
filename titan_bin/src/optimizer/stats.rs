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
        | LogicalPlan::Filter { input, .. }
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
    if let (Some(cached_stats), Some(_schema)) = (
        catalog.get_statistics(table_name),
        catalog.get_schema(table_name),
    ) {
        stats.insert(table_name.to_string(), cached_stats);
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

    let table_schema = Arc::new(catalog.get_table_schema(bpm, table_oid, tx_id, snapshot)?);
    catalog.add_schema(table_name.to_string(), Arc::clone(&table_schema));

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
                        let col_type_id = table_schema
                            .get(attnum)
                            .map(|col| col.type_id)
                            .unwrap_or(25);

                        match kind {
                            0 => {
                                if let Some(LiteralValue::Number(n)) = parsed.get("stadistinct") {
                                    if let Ok(total_rows) = n.parse::<f64>() {
                                        table_stats.total_rows = total_rows.max(0.0);
                                    }
                                }
                            }
                            1 => {
                                let col_stats = table_stats.column_stats.entry(attnum).or_default();
                                if let Some(LiteralValue::Number(n)) = parsed.get("stadistinct") {
                                    col_stats.n_distinct = n.parse().unwrap_or(0.0);
                                }
                            }
                            2 => {
                                let col_stats = table_stats.column_stats.entry(attnum).or_default();
                                if let Some(LiteralValue::String(s)) = parsed.get("stavalues") {
                                    col_stats.most_common_vals =
                                        parse_stat_literal_list(s, col_type_id);
                                }
                            }
                            3 => {
                                let col_stats = table_stats.column_stats.entry(attnum).or_default();
                                if let Some(LiteralValue::String(s)) = parsed.get("stanumbers") {
                                    col_stats.histogram_bounds =
                                        parse_stat_literal_list(s, col_type_id);
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

fn parse_stat_literal_list(raw: &str, type_id: u32) -> Vec<LiteralValue> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(',')
        .map(|value| parse_stat_literal(value, type_id))
        .collect()
}

fn parse_stat_literal(raw: &str, type_id: u32) -> LiteralValue {
    let trimmed = raw.trim();
    match type_id {
        16 => match trimmed.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => LiteralValue::Bool(true),
            "f" | "false" | "0" => LiteralValue::Bool(false),
            _ => LiteralValue::String(trimmed.to_string()),
        },
        23 => {
            if trimmed.parse::<f64>().is_ok() {
                LiteralValue::Number(trimmed.to_string())
            } else {
                LiteralValue::String(trimmed.to_string())
            }
        }
        1082 => LiteralValue::Date(trimmed.to_string()),
        25 => LiteralValue::String(trimmed.to_string()),
        _ => LiteralValue::String(trimmed.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Column;
    use bedrock::pager::Pager;
    use tempfile::tempdir;

    #[test]
    fn load_statistics_uses_cache_to_populate_local_stats_map() {
        let dir = tempdir().expect("temp dir");
        let db_path = dir.path().join("stats_cache.db");
        let pager = Pager::open(&db_path).expect("open pager");
        let bpm = Arc::new(BufferPoolManager::new(pager));
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));

        let cached = TableStats {
            total_rows: 42.0,
            ..TableStats::default()
        };
        let cached_arc = Arc::new(cached);

        {
            let catalog_guard = catalog.lock().expect("catalog lock");
            catalog_guard.add_schema(
                "users".to_string(),
                Arc::new(vec![Column {
                    name: "id".to_string(),
                    type_id: 23,
                }]),
            );
            catalog_guard.add_statistics("users".to_string(), cached_arc.clone());
        }

        let snapshot = Snapshot {
            xmin: 0,
            xmax: 1,
            active_transactions: Arc::new(HashSet::new()),
        };
        let mut local_stats: HashMap<String, Arc<TableStats>> = HashMap::new();
        load_statistics_for_table("users", &mut local_stats, &bpm, 1, &snapshot, &catalog)
            .expect("load cached stats");

        let loaded = local_stats.get("users").expect("stats must be populated");
        assert!(Arc::ptr_eq(loaded, &cached_arc));
        assert_eq!(loaded.total_rows, 42.0);
    }

    #[test]
    fn parse_stat_literal_list_keeps_numeric_values_as_numbers() {
        let parsed = parse_stat_literal_list("1,2,10,100", 23);
        assert_eq!(
            parsed,
            vec![
                LiteralValue::Number("1".to_string()),
                LiteralValue::Number("2".to_string()),
                LiteralValue::Number("10".to_string()),
                LiteralValue::Number("100".to_string()),
            ]
        );
    }

    #[test]
    fn parse_stat_literal_list_parses_boolean_values() {
        let parsed = parse_stat_literal_list("true,false,t,f,1,0", 16);
        assert_eq!(
            parsed,
            vec![
                LiteralValue::Bool(true),
                LiteralValue::Bool(false),
                LiteralValue::Bool(true),
                LiteralValue::Bool(false),
                LiteralValue::Bool(true),
                LiteralValue::Bool(false),
            ]
        );
    }

    #[test]
    fn parse_stat_literal_list_keeps_dates_as_date_literals() {
        let parsed = parse_stat_literal_list("2026-01-01,2026-12-31", 1082);
        assert_eq!(
            parsed,
            vec![
                LiteralValue::Date("2026-01-01".to_string()),
                LiteralValue::Date("2026-12-31".to_string()),
            ]
        );
    }
}
