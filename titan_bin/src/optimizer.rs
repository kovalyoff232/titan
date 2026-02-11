use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::executor::parse_tuple;
use crate::parser::{BinaryOperator, Expression, LiteralValue, SelectItem};
use crate::planner::LogicalPlan;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::page::INVALID_PAGE_ID;
use bedrock::transaction::{Snapshot, TransactionManager};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub n_distinct: f64,
    pub most_common_vals: Vec<LiteralValue>,
    pub histogram_bounds: Vec<LiteralValue>,
}

#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub total_rows: f64,
    pub column_stats: HashMap<usize, ColumnStats>,
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    TableScan {
        table_name: String,
        filter: Option<Expression>,
    },
    IndexScan {
        table_name: String,
        index_name: String,
        key: i32,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expression,
    },
    Projection {
        input: Box<PhysicalPlan>,
        expressions: Vec<SelectItem>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: Expression,
        right_key: Expression,
    },
    MergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: Expression,
        right_key: Expression,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expression,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<Expression>,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expression>,
        aggregates: Vec<crate::planner::AggregateExpr>,
        having: Option<Expression>,
    },
    StreamAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expression>,
        aggregates: Vec<crate::planner::AggregateExpr>,
        having: Option<Expression>,
    },
    Window {
        input: Box<PhysicalPlan>,
        window_functions: Vec<crate::planner::WindowFunctionPlan>,
    },
    MaterializeCTE {
        name: String,
        plan: Box<PhysicalPlan>,
    },
    CTEScan {
        name: String,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<i64>,
        offset: Option<i64>,
    },
}

pub fn optimize(
    plan: LogicalPlan,
    bpm: &Arc<BufferPoolManager>,
    _tm: &Arc<TransactionManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<PhysicalPlan, ExecutionError> {
    let mut stats = HashMap::new();
    let table_names = get_table_names(&plan);

    for table_name in table_names {
        load_statistics_for_table(
            &table_name,
            &mut stats,
            bpm,
            tx_id,
            snapshot,
            system_catalog,
        )?;
    }

    Ok(create_physical_plan(
        plan,
        &stats,
        bpm,
        tx_id,
        snapshot,
        system_catalog,
    ))
}

fn get_table_names(plan: &LogicalPlan) -> HashSet<String> {
    use crate::planner::LogicalPlan;
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

fn load_statistics_for_table(
    table_name: &str,
    stats: &mut HashMap<String, Arc<TableStats>>,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<(), ExecutionError> {
    let mut catalog = system_catalog.lock().unwrap();
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

#[derive(Clone, Debug)]
struct PlanInfo {
    plan: Arc<PhysicalPlan>,
    cost: f64,
    cardinality: f64,
}

type PlanCache = HashMap<u32, Vec<PlanInfo>>;

fn create_physical_plan(
    plan: LogicalPlan,
    stats: &HashMap<String, Arc<TableStats>>,
    _bpm: &Arc<BufferPoolManager>,
    _tx_id: u32,
    _snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> PhysicalPlan {
    let mut cache: PlanCache = HashMap::new();
    let table_names = get_table_names(&plan);
    let tables: Vec<_> = table_names.iter().collect();

    for (i, table_name) in tables.iter().enumerate() {
        let relation_mask = 1 << i;
        let mut plans_for_rel = Vec::new();

        let table_stats = stats.get(*table_name);
        let base_cardinality = table_stats.map_or(1.0, |s| s.total_rows);

        let seq_scan_plan = PhysicalPlan::TableScan {
            table_name: table_name.to_string(),
            filter: None,
        };
        let seq_scan_cost = cost_seq_scan(table_stats, base_cardinality);
        plans_for_rel.push(PlanInfo {
            plan: Arc::new(seq_scan_plan),
            cost: seq_scan_cost,
            cardinality: base_cardinality,
        });

        if let LogicalPlan::Scan {
            filter: Some(predicate),
            ..
        } = &plan
        {
            if let Expression::Binary {
                left,
                op: BinaryOperator::Eq,
                right,
            } = predicate
            {
                if let (
                    Expression::Column(col_name),
                    Expression::Literal(LiteralValue::Number(val_str)),
                ) = (&**left, &**right)
                {
                    let schema = system_catalog
                        .lock()
                        .unwrap()
                        .get_schema(table_name)
                        .unwrap();
                    if let Some(col_idx) = schema.iter().position(|c| c.name == *col_name) {
                        if col_idx == 0 {
                            let index_scan_plan = PhysicalPlan::IndexScan {
                                table_name: table_name.to_string(),
                                index_name: format!("idx_{}", col_name),
                                key: val_str.parse().unwrap_or(0),
                            };

                            let index_cardinality = 1.0;
                            let index_cost = cost_index_scan(table_stats, index_cardinality);
                            plans_for_rel.push(PlanInfo {
                                plan: Arc::new(index_scan_plan),
                                cost: index_cost,
                                cardinality: index_cardinality,
                            });
                        }
                    }
                }
            }
        }

        cache.insert(relation_mask, plans_for_rel);
    }

    let num_tables = tables.len();
    for i in 2..=num_tables {
        for subset_mask in (0..(1 << num_tables)).filter(|m: &u32| m.count_ones() == i as u32) {
            let mut best_plans_for_subset = Vec::new();

            for sub_subset_mask in (1..subset_mask).filter(|m| (subset_mask & m) == *m) {
                let other_sub_mask = subset_mask ^ sub_subset_mask;
                if cache.contains_key(&sub_subset_mask) && cache.contains_key(&other_sub_mask) {
                    let plans1 = cache.get(&sub_subset_mask).unwrap();
                    let plans2 = cache.get(&other_sub_mask).unwrap();

                    for p1 in plans1 {
                        for p2 in plans2 {
                            if let Some((left_key, right_key)) = find_join_condition(&plan, p1, p2)
                            {
                                let hj_plan = PhysicalPlan::HashJoin {
                                    left: Box::new((*p1.plan).clone()),
                                    right: Box::new((*p2.plan).clone()),
                                    left_key: left_key.clone(),
                                    right_key: right_key.clone(),
                                };

                                let hj_card = estimate_join_cardinality(
                                    p1,
                                    p2,
                                    &left_key,
                                    &right_key,
                                    stats,
                                    system_catalog,
                                )
                                .unwrap_or(1.0);
                                let hj_cost = cost_hash_join(p1, p2);
                                best_plans_for_subset.push(PlanInfo {
                                    plan: Arc::new(hj_plan),
                                    cost: hj_cost,
                                    cardinality: hj_card,
                                });

                                let nlj_plan = PhysicalPlan::NestedLoopJoin {
                                    left: Box::new((*p1.plan).clone()),
                                    right: Box::new((*p2.plan).clone()),
                                    condition: Expression::Binary {
                                        left: Box::new(left_key.clone()),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(right_key.clone()),
                                    },
                                };
                                let nlj_card = p1.cardinality * p2.cardinality;
                                let nlj_cost = p1.cost + p1.cardinality * p2.cost;
                                best_plans_for_subset.push(PlanInfo {
                                    plan: Arc::new(nlj_plan),
                                    cost: nlj_cost,
                                    cardinality: nlj_card,
                                });
                            }
                        }
                    }
                }
            }
            if !best_plans_for_subset.is_empty() {
                best_plans_for_subset.sort_by(|a, b| a.cost.partial_cmp(&b.cost).unwrap());
                cache.insert(subset_mask, best_plans_for_subset);
            }
        }
    }

    let final_mask = (1 << num_tables) - 1;
    let best_plan_info = cache.get(&final_mask).and_then(|v| v.get(0));

    if let Some(info) = best_plan_info {
        let mut final_plan_info = info.clone();
        let mut final_plan = (*final_plan_info.plan).clone();

        if let Some(filter_predicate) = get_filter_from_logical_plan(&plan) {
            final_plan = PhysicalPlan::Filter {
                input: Box::new(final_plan),
                predicate: filter_predicate.clone(),
            };
            final_plan_info.cost += cost_filter(&final_plan_info);
            final_plan_info.cardinality *= estimate_filter_selectivity_for_plan(
                &filter_predicate,
                &final_plan,
                stats,
                system_catalog,
            );
        }

        if let LogicalPlan::Projection { expressions, .. } = &plan {
            final_plan = PhysicalPlan::Projection {
                input: Box::new(final_plan),
                expressions: expressions.clone(),
            };
            final_plan_info.cost += cost_projection(&final_plan_info, expressions.len());
        }
        if let LogicalPlan::Sort { order_by, .. } = &plan {
            final_plan = PhysicalPlan::Sort {
                input: Box::new(final_plan),
                order_by: order_by.clone(),
            };
            final_plan_info.cost += cost_sort(&final_plan_info);
        }
        final_plan
    } else {
        create_simple_physical_plan(plan)
    }
}

fn create_simple_physical_plan(plan: LogicalPlan) -> PhysicalPlan {
    use crate::planner::LogicalPlan;
    match plan {
        LogicalPlan::Scan {
            table_name, filter, ..
        } => PhysicalPlan::TableScan { table_name, filter },
        LogicalPlan::Projection { input, expressions } => PhysicalPlan::Projection {
            input: Box::new(create_simple_physical_plan(*input)),
            expressions,
        },
        LogicalPlan::Join {
            left,
            right,
            condition,
        } => {
            let left_physical = create_simple_physical_plan(*left);
            let right_physical = create_simple_physical_plan(*right);
            if let Expression::Binary {
                left: left_key,
                op: BinaryOperator::Eq,
                right: right_key,
            } = &condition
            {
                return PhysicalPlan::HashJoin {
                    left: Box::new(left_physical),
                    right: Box::new(right_physical),
                    left_key: *left_key.clone(),
                    right_key: *right_key.clone(),
                };
            }
            PhysicalPlan::NestedLoopJoin {
                left: Box::new(left_physical),
                right: Box::new(right_physical),
                condition,
            }
        }
        LogicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(create_simple_physical_plan(*input)),
            order_by,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(create_simple_physical_plan(*input)),
            group_by,
            aggregates,
            having,
        },
        LogicalPlan::Window {
            input,
            window_functions,
        } => PhysicalPlan::Window {
            input: Box::new(create_simple_physical_plan(*input)),
            window_functions,
        },
        LogicalPlan::CteRef { name } => PhysicalPlan::CTEScan { name },
        LogicalPlan::WithCte { cte_list, input } => {
            let mut result = create_simple_physical_plan(*input);
            for cte in cte_list.iter().rev() {
                result = PhysicalPlan::MaterializeCTE {
                    name: cte.name.clone(),
                    plan: Box::new(result),
                };
            }
            result
        }
        LogicalPlan::SetOperation {
            op: _,
            all: _,
            left,
            right,
        } => {
            let left_physical = create_simple_physical_plan(*left);
            let right_physical = create_simple_physical_plan(*right);

            PhysicalPlan::HashJoin {
                left: Box::new(left_physical),
                right: Box::new(right_physical),
                left_key: Expression::Literal(LiteralValue::Number("1".to_string())),
                right_key: Expression::Literal(LiteralValue::Number("1".to_string())),
            }
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(create_simple_physical_plan(*input)),
            limit,
            offset,
        },
    }
}

const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 1.1;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_OPERATOR_COST: f64 = 0.0025;

#[allow(dead_code)]
fn estimate_cardinality(
    plan: &PhysicalPlan,
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> f64 {
    match plan {
        PhysicalPlan::TableScan { table_name, filter } => {
            let table_stats = match stats.get(table_name) {
                Some(s) => s,
                None => return 1.0,
            };
            let base_card = table_stats.total_rows;
            if let Some(f) = filter {
                base_card * estimate_filter_selectivity(f, table_name, table_stats, system_catalog)
            } else {
                base_card
            }
        }
        PhysicalPlan::Filter { input, predicate } => {
            let input_card = estimate_cardinality(input, stats, system_catalog);

            if let Some(table_name) = find_first_table(input) {
                if let Some(table_stats) = stats.get(&table_name) {
                    return input_card
                        * estimate_filter_selectivity(
                            predicate,
                            &table_name,
                            table_stats,
                            system_catalog,
                        );
                }
            }
            input_card * 0.5
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_card = estimate_cardinality(left, stats, system_catalog);
            let right_card = estimate_cardinality(right, stats, system_catalog);

            let left_table = find_first_table(left).unwrap();
            let right_table = find_first_table(right).unwrap();
            let left_col = get_col_name(left_key).unwrap();
            let right_col = get_col_name(right_key).unwrap();

            let left_stats = stats.get(&left_table).unwrap();
            let right_stats = stats.get(&right_table).unwrap();

            let left_schema = system_catalog
                .lock()
                .unwrap()
                .get_schema(&left_table)
                .unwrap();
            let right_schema = system_catalog
                .lock()
                .unwrap()
                .get_schema(&right_table)
                .unwrap();

            let left_col_idx = left_schema.iter().position(|c| c.name == left_col).unwrap();
            let right_col_idx = right_schema
                .iter()
                .position(|c| c.name == right_col)
                .unwrap();

            let left_n_distinct = left_stats
                .column_stats
                .get(&left_col_idx)
                .map_or(1.0, |cs| cs.n_distinct);
            let right_n_distinct = right_stats
                .column_stats
                .get(&right_col_idx)
                .map_or(1.0, |cs| cs.n_distinct);

            (left_card * right_card) / (left_n_distinct).max(right_n_distinct)
        }
        PhysicalPlan::Projection { input, .. } => {
            estimate_cardinality(input, stats, system_catalog)
        }
        PhysicalPlan::Sort { input, .. } => estimate_cardinality(input, stats, system_catalog),
        PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            let left_card = estimate_cardinality(left, stats, system_catalog);
            let right_card = estimate_cardinality(right, stats, system_catalog);
            left_card * right_card * 0.1
        }
        _ => 1.0,
    }
}

fn estimate_filter_selectivity_for_plan(
    predicate: &Expression,
    plan: &PhysicalPlan,
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> f64 {
    if let Some(table_name) = find_first_table(plan) {
        if let Some(table_stats) = stats.get(&table_name) {
            return estimate_filter_selectivity(
                predicate,
                &table_name,
                table_stats,
                system_catalog,
            );
        }
    }
    0.5
}

fn estimate_filter_selectivity(
    filter: &Expression,
    table_name: &str,
    table_stats: &TableStats,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> f64 {
    match filter {
        Expression::Binary { left, op, right } => {
            if *op == BinaryOperator::And {
                return estimate_filter_selectivity(left, table_name, table_stats, system_catalog)
                    * estimate_filter_selectivity(right, table_name, table_stats, system_catalog);
            }
            if *op == BinaryOperator::Or {
                let s1 = estimate_filter_selectivity(left, table_name, table_stats, system_catalog);
                let s2 =
                    estimate_filter_selectivity(right, table_name, table_stats, system_catalog);
                return s1 + s2 - (s1 * s2);
            }

            let (col_name, literal) = if let Expression::Column(name) = &**left {
                if let Expression::Literal(lit) = &**right {
                    (name, lit)
                } else {
                    return 0.33;
                }
            } else {
                return 0.33;
            };

            let schema = system_catalog
                .lock()
                .unwrap()
                .get_schema(table_name)
                .unwrap();
            let col_idx = schema.iter().position(|c| c.name == *col_name);

            if col_idx.is_none() {
                return 0.5;
            }
            let col_idx = col_idx.unwrap();

            let col_stats = table_stats.column_stats.get(&col_idx);

            if col_stats.is_none() {
                return 0.5;
            }
            let col_stats = col_stats.unwrap();

            match op {
                BinaryOperator::Eq => {
                    if col_stats.most_common_vals.contains(literal) {
                        1.0 / col_stats.n_distinct.max(1.0)
                    } else {
                        let mcv_count = col_stats.most_common_vals.len() as f64;
                        let non_mcv_distinct_count = (col_stats.n_distinct - mcv_count).max(1.0);

                        1.0 / non_mcv_distinct_count
                    }
                }
                BinaryOperator::Lt
                | BinaryOperator::Gt
                | BinaryOperator::LtEq
                | BinaryOperator::GtEq => estimate_range_selectivity(col_stats, literal, op),
                _ => 0.33,
            }
        }
        Expression::Unary { op, expr } => {
            if matches!(op, crate::parser::UnaryOperator::Not) {
                1.0 - estimate_filter_selectivity(expr, table_name, table_stats, system_catalog)
            } else {
                0.5
            }
        }
        _ => 0.5,
    }
}

fn estimate_range_selectivity(
    col_stats: &ColumnStats,
    literal: &LiteralValue,
    op: &BinaryOperator,
) -> f64 {
    if col_stats.histogram_bounds.is_empty() {
        return 0.33;
    }

    let literal_str = literal.to_string();
    let bounds_str: Vec<String> = col_stats
        .histogram_bounds
        .iter()
        .map(|v| v.to_string())
        .collect();

    let num_buckets = bounds_str.len() - 1;
    if num_buckets <= 0 {
        return 0.33;
    }

    let bucket_idx = bounds_str
        .iter()
        .position(|b| *b > literal_str)
        .unwrap_or(num_buckets);

    match op {
        BinaryOperator::Lt | BinaryOperator::LtEq => (bucket_idx as f64) / (num_buckets as f64),
        BinaryOperator::Gt | BinaryOperator::GtEq => {
            ((num_buckets - bucket_idx) as f64) / (num_buckets as f64)
        }
        _ => 0.33,
    }
}

fn find_first_table(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::TableScan { table_name, .. } => Some(table_name.clone()),
        PhysicalPlan::IndexScan { table_name, .. } => Some(table_name.clone()),
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::StreamAggregate { input, .. }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Limit { input, .. } => find_first_table(input),
        PhysicalPlan::HashJoin { left, .. }
        | PhysicalPlan::MergeJoin { left, .. }
        | PhysicalPlan::NestedLoopJoin { left, .. } => find_first_table(left),
        PhysicalPlan::MaterializeCTE { plan, .. } => find_first_table(plan),
        PhysicalPlan::CTEScan { name } => Some(format!("cte_{}", name)),
    }
}

fn find_join_condition(
    plan: &LogicalPlan,
    p1: &PlanInfo,
    p2: &PlanInfo,
) -> Option<(Expression, Expression)> {
    use crate::planner::LogicalPlan;
    match plan {
        LogicalPlan::Join {
            left,
            right,
            condition,
        } => {
            if let Expression::Binary {
                left: cond_left,
                op: BinaryOperator::Eq,
                right: cond_right,
            } = condition
            {
                let p1_table = find_first_table(&p1.plan);
                let p2_table = find_first_table(&p2.plan);
                let cond_left_table = get_table_name_from_expr(cond_left);
                let cond_right_table = get_table_name_from_expr(cond_right);

                if cond_left_table == p1_table && cond_right_table == p2_table {
                    return Some((*(cond_left.clone()), *(cond_right.clone())));
                }
                if cond_left_table == p2_table && cond_right_table == p1_table {
                    return Some((*(cond_right.clone()), *(cond_left.clone())));
                }
            }

            find_join_condition(left, p1, p2).or_else(|| find_join_condition(right, p1, p2))
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Limit { input, .. } => find_join_condition(input, p1, p2),
        LogicalPlan::Scan { .. } | LogicalPlan::CteRef { .. } => None,
        LogicalPlan::WithCte { input, .. } => find_join_condition(input, p1, p2),
        LogicalPlan::SetOperation { left, right, .. } => {
            find_join_condition(left, p1, p2).or_else(|| find_join_condition(right, p1, p2))
        }
    }
}

fn get_table_name_from_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::QualifiedColumn(table, _) => Some(table.clone()),
        _ => None,
    }
}

fn get_filter_from_logical_plan(plan: &LogicalPlan) -> Option<&Expression> {
    use crate::planner::LogicalPlan;
    match plan {
        LogicalPlan::Scan { filter, .. } => filter.as_ref(),
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Limit { input, .. } => get_filter_from_logical_plan(input),
        LogicalPlan::WithCte { input, .. } => get_filter_from_logical_plan(input),

        LogicalPlan::Join { .. }
        | LogicalPlan::SetOperation { .. }
        | LogicalPlan::CteRef { .. } => None,
    }
}

#[allow(dead_code)]
fn get_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table_name, .. } => Some(table_name.clone()),
        _ => None,
    }
}

fn get_col_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Column(name) => Some(name.clone()),
        Expression::QualifiedColumn(_, name) => Some(name.clone()),
        _ => None,
    }
}

#[allow(dead_code)]
fn get_col_idx(
    table_name: &str,
    col_name: &str,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Option<usize> {
    let mut catalog = system_catalog.lock().unwrap();
    let (table_oid, _) = catalog
        .find_table(table_name, bpm, tx_id, snapshot)
        .ok()??;
    let schema = catalog
        .get_table_schema(bpm, table_oid, tx_id, snapshot)
        .ok()?;
    schema.iter().position(|c| c.name == col_name)
}

fn cost_seq_scan(_stats: Option<&Arc<TableStats>>, cardinality: f64) -> f64 {
    let n_pages = (cardinality * 100.0 / 4096.0).max(1.0);
    (n_pages * SEQ_PAGE_COST) + (cardinality * CPU_TUPLE_COST)
}

fn cost_index_scan(_stats: Option<&Arc<TableStats>>, selected_tuples: f64) -> f64 {
    let b_tree_height = 3.0;
    let n_index_leaf_pages = (selected_tuples / 100.0).max(1.0);
    (b_tree_height * RANDOM_PAGE_COST)
        + (n_index_leaf_pages * SEQ_PAGE_COST)
        + (selected_tuples * CPU_TUPLE_COST)
}

fn cost_hash_join(left: &PlanInfo, right: &PlanInfo) -> f64 {
    let build_cost = right.cardinality * CPU_TUPLE_COST;
    let probe_cost = left.cardinality * CPU_TUPLE_COST;
    let hash_cost = (left.cardinality + right.cardinality) * CPU_OPERATOR_COST;
    left.cost + right.cost + build_cost + probe_cost + hash_cost
}

fn cost_sort(input: &PlanInfo) -> f64 {
    let n_tuples = input.cardinality;
    if n_tuples < 1.0 {
        return 0.0;
    }
    let n_pages = (n_tuples * 100.0 / 4096.0).max(1.0);
    (n_pages * SEQ_PAGE_COST) + (n_tuples * n_tuples.log2() * CPU_OPERATOR_COST)
}

fn cost_projection(input: &PlanInfo, num_expressions: usize) -> f64 {
    input.cardinality * (num_expressions as f64) * CPU_OPERATOR_COST
}

fn cost_filter(input: &PlanInfo) -> f64 {
    input.cardinality * CPU_OPERATOR_COST
}

fn estimate_join_cardinality(
    left_plan: &PlanInfo,
    right_plan: &PlanInfo,
    left_key: &Expression,
    right_key: &Expression,
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Option<f64> {
    let left_card = left_plan.cardinality;
    let right_card = right_plan.cardinality;

    let left_table = find_first_table(&left_plan.plan)?;
    let right_table = find_first_table(&right_plan.plan)?;
    let left_col = get_col_name(left_key)?;
    let right_col = get_col_name(right_key)?;

    let left_stats = stats.get(&left_table)?;
    let right_stats = stats.get(&right_table)?;

    let catalog = system_catalog.lock().unwrap();
    let left_schema = catalog.get_schema(&left_table)?;
    let right_schema = catalog.get_schema(&right_table)?;

    let left_col_idx = left_schema.iter().position(|c| c.name == left_col)?;
    let right_col_idx = right_schema.iter().position(|c| c.name == right_col)?;

    let left_n_distinct = left_stats
        .column_stats
        .get(&left_col_idx)
        .map_or(left_card, |cs| cs.n_distinct);
    let right_n_distinct = right_stats
        .column_stats
        .get(&right_col_idx)
        .map_or(right_card, |cs| cs.n_distinct);

    Some((left_card * right_card) / (left_n_distinct.max(right_n_distinct)).max(1.0))
}
