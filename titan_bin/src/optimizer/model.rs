use super::{ColumnStats, PhysicalPlan, PlanInfo, TableStats};
use crate::catalog::SystemCatalog;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use crate::planner::LogicalPlan;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 1.1;
const CPU_TUPLE_COST: f64 = 0.01;
const CPU_OPERATOR_COST: f64 = 0.0025;

pub(super) fn estimate_filter_selectivity_for_plan(
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

            let Some(schema) = system_catalog
                .lock()
                .ok()
                .and_then(|catalog| catalog.get_schema(table_name))
            else {
                return 0.5;
            };
            let Some(col_idx) = schema.iter().position(|c| c.name == *col_name) else {
                return 0.5;
            };

            let Some(col_stats) = table_stats.column_stats.get(&col_idx) else {
                return 0.5;
            };

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

pub(super) fn find_join_condition(
    plan: &LogicalPlan,
    p1: &PlanInfo,
    p2: &PlanInfo,
) -> Option<(Expression, Expression)> {
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

pub(super) fn get_filter_from_logical_plan(plan: &LogicalPlan) -> Option<&Expression> {
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

fn get_col_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Column(name) => Some(name.clone()),
        Expression::QualifiedColumn(_, name) => Some(name.clone()),
        _ => None,
    }
}

pub(super) fn cost_seq_scan(_stats: Option<&Arc<TableStats>>, cardinality: f64) -> f64 {
    let n_pages = (cardinality * 100.0 / 4096.0).max(1.0);
    (n_pages * SEQ_PAGE_COST) + (cardinality * CPU_TUPLE_COST)
}

pub(super) fn cost_index_scan(_stats: Option<&Arc<TableStats>>, selected_tuples: f64) -> f64 {
    let b_tree_height = 3.0;
    let n_index_leaf_pages = (selected_tuples / 100.0).max(1.0);
    (b_tree_height * RANDOM_PAGE_COST)
        + (n_index_leaf_pages * SEQ_PAGE_COST)
        + (selected_tuples * CPU_TUPLE_COST)
}

pub(super) fn cost_hash_join(left: &PlanInfo, right: &PlanInfo) -> f64 {
    let build_cost = right.cardinality * CPU_TUPLE_COST;
    let probe_cost = left.cardinality * CPU_TUPLE_COST;
    let hash_cost = (left.cardinality + right.cardinality) * CPU_OPERATOR_COST;
    left.cost + right.cost + build_cost + probe_cost + hash_cost
}

pub(super) fn cost_sort(input: &PlanInfo) -> f64 {
    let n_tuples = input.cardinality;
    if n_tuples < 1.0 {
        return 0.0;
    }
    let n_pages = (n_tuples * 100.0 / 4096.0).max(1.0);
    (n_pages * SEQ_PAGE_COST) + (n_tuples * n_tuples.log2() * CPU_OPERATOR_COST)
}

pub(super) fn cost_projection(input: &PlanInfo, num_expressions: usize) -> f64 {
    input.cardinality * (num_expressions as f64) * CPU_OPERATOR_COST
}

pub(super) fn cost_filter(input: &PlanInfo) -> f64 {
    input.cardinality * CPU_OPERATOR_COST
}

pub(super) fn estimate_join_cardinality(
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

    let catalog = system_catalog.lock().ok()?;
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
