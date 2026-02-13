use super::{ColumnStats, PhysicalPlan, PlanInfo, TableStats};
use crate::catalog::SystemCatalog;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use crate::planner::LogicalPlan;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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

            let (col_name, literal, comparison_op) = match (&**left, &**right) {
                (Expression::Column(name), Expression::Literal(lit))
                | (Expression::QualifiedColumn(_, name), Expression::Literal(lit)) => {
                    (name.as_str(), lit, op.clone())
                }
                (Expression::Literal(lit), Expression::Column(name))
                | (Expression::Literal(lit), Expression::QualifiedColumn(_, name)) => {
                    let Some(reversed) = reverse_comparison_operator(op) else {
                        return 0.33;
                    };
                    (name.as_str(), lit, reversed)
                }
                _ => return 0.33,
            };

            let Some(schema) = system_catalog
                .lock()
                .ok()
                .and_then(|catalog| catalog.get_schema(table_name))
            else {
                return 0.5;
            };
            let Some(col_idx) = schema.iter().position(|c| c.name == col_name) else {
                return 0.5;
            };

            let Some(col_stats) = table_stats.column_stats.get(&col_idx) else {
                return 0.5;
            };

            match comparison_op {
                BinaryOperator::Eq => {
                    if matches!(literal, LiteralValue::Null) {
                        return 0.0;
                    }
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
                | BinaryOperator::GtEq => {
                    estimate_range_selectivity(col_stats, literal, &comparison_op)
                }
                _ => 0.33,
            }
        }
        Expression::IsNull { expr, negated } => {
            let Some(col_name) = get_col_name(expr) else {
                return if *negated { 0.9 } else { 0.1 };
            };
            let Some(schema) = system_catalog
                .lock()
                .ok()
                .and_then(|catalog| catalog.get_schema(table_name))
            else {
                return if *negated { 0.9 } else { 0.1 };
            };
            let Some(col_idx) = schema.iter().position(|c| c.name == col_name) else {
                return if *negated { 0.9 } else { 0.1 };
            };
            let Some(col_stats) = table_stats.column_stats.get(&col_idx) else {
                return if *negated { 0.9 } else { 0.1 };
            };
            let null_selectivity = if col_stats.most_common_vals.contains(&LiteralValue::Null) {
                1.0 / col_stats.n_distinct.max(1.0)
            } else {
                0.1
            };
            if *negated {
                (1.0 - null_selectivity).clamp(0.0, 1.0)
            } else {
                null_selectivity.clamp(0.0, 1.0)
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

fn reverse_comparison_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        _ => None,
    }
}

fn parse_number_literal(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Number(raw) | LiteralValue::String(raw) | LiteralValue::Date(raw) => {
            raw.parse::<f64>().ok()
        }
        _ => None,
    }
}

fn literal_cmp(left: &LiteralValue, right: &LiteralValue) -> Option<Ordering> {
    match (left, right) {
        (LiteralValue::Null, _) | (_, LiteralValue::Null) => None,
        (LiteralValue::Bool(a), LiteralValue::Bool(b)) => Some(a.cmp(b)),
        (LiteralValue::String(a), LiteralValue::String(b))
        | (LiteralValue::Date(a), LiteralValue::Date(b))
        | (LiteralValue::String(a), LiteralValue::Date(b))
        | (LiteralValue::Date(a), LiteralValue::String(b)) => Some(a.cmp(b)),
        _ => {
            let left_num = parse_number_literal(left)?;
            let right_num = parse_number_literal(right)?;
            left_num.partial_cmp(&right_num)
        }
    }
}

fn estimate_range_selectivity(
    col_stats: &ColumnStats,
    literal: &LiteralValue,
    op: &BinaryOperator,
) -> f64 {
    if matches!(literal, LiteralValue::Null) {
        return 0.0;
    }
    if col_stats.histogram_bounds.is_empty() {
        return 0.33;
    }

    if col_stats
        .histogram_bounds
        .iter()
        .any(|bound| literal_cmp(bound, literal).is_none())
    {
        return 0.33;
    }

    let boundary_count = col_stats.histogram_bounds.len();
    let lt_boundary_idx = col_stats
        .histogram_bounds
        .iter()
        .position(|bound| {
            matches!(
                literal_cmp(bound, literal),
                Some(Ordering::Equal) | Some(Ordering::Greater)
            )
        })
        .unwrap_or(boundary_count);
    let lte_boundary_idx = col_stats
        .histogram_bounds
        .iter()
        .position(|bound| matches!(literal_cmp(bound, literal), Some(Ordering::Greater)))
        .unwrap_or(boundary_count);

    let denominator = boundary_count as f64;
    let lt_selectivity = (lt_boundary_idx as f64 / denominator).clamp(0.0, 1.0);
    let lte_selectivity = (lte_boundary_idx as f64 / denominator).clamp(0.0, 1.0);

    match op {
        BinaryOperator::Lt => lt_selectivity,
        BinaryOperator::LtEq => lte_selectivity,
        BinaryOperator::Gt => (1.0 - lte_selectivity).clamp(0.0, 1.0),
        BinaryOperator::GtEq => (1.0 - lt_selectivity).clamp(0.0, 1.0),
        _ => 0.33,
    }
}

fn find_first_table(plan: &PhysicalPlan) -> Option<String> {
    match plan {
        PhysicalPlan::TableScan { table_name, .. } => Some(table_name.clone()),
        PhysicalPlan::IndexScan { table_name, .. } => Some(table_name.clone()),
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Distinct { input }
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

fn collect_plan_tables(plan: &PhysicalPlan, out: &mut HashSet<String>) {
    match plan {
        PhysicalPlan::TableScan {
            table_name, alias, ..
        } => {
            out.insert(table_name.clone());
            if let Some(alias_name) = alias {
                out.insert(alias_name.clone());
            }
        }
        PhysicalPlan::IndexScan { table_name, .. } => {
            out.insert(table_name.clone());
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::StreamAggregate { input, .. }
        | PhysicalPlan::Window { input, .. }
        | PhysicalPlan::Limit { input, .. } => collect_plan_tables(input, out),
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::MergeJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
            collect_plan_tables(left, out);
            collect_plan_tables(right, out);
        }
        PhysicalPlan::MaterializeCTE { plan, .. } => collect_plan_tables(plan, out),
        PhysicalPlan::CTEScan { name } => {
            out.insert(format!("cte_{name}"));
        }
    }
}

fn plan_table_set(plan: &PhysicalPlan) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_plan_tables(plan, &mut tables);
    tables
}

pub(super) fn find_join_condition(
    plan: &LogicalPlan,
    p1: &PlanInfo,
    p2: &PlanInfo,
) -> Option<(Expression, Expression)> {
    let p1_tables = plan_table_set(&p1.plan);
    let p2_tables = plan_table_set(&p2.plan);

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
                let cond_left_table = get_table_name_from_expr(cond_left);
                let cond_right_table = get_table_name_from_expr(cond_right);

                if let (Some(left_table), Some(right_table)) = (cond_left_table, cond_right_table) {
                    if p1_tables.contains(&left_table) && p2_tables.contains(&right_table) {
                        return Some((*(cond_left.clone()), *(cond_right.clone())));
                    }
                    if p1_tables.contains(&right_table) && p2_tables.contains(&left_table) {
                        return Some((*(cond_right.clone()), *(cond_left.clone())));
                    }
                }
            }

            find_join_condition(left, p1, p2).or_else(|| find_join_condition(right, p1, p2))
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
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
        LogicalPlan::Filter { predicate, .. } => Some(predicate),
        LogicalPlan::Scan { filter, .. } => filter.as_ref(),
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Distinct { input }
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

    let left_table =
        get_table_name_from_expr(left_key).or_else(|| find_first_table(&left_plan.plan))?;
    let right_table =
        get_table_name_from_expr(right_key).or_else(|| find_first_table(&right_plan.plan))?;
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

#[cfg(test)]
mod tests {
    use super::{estimate_filter_selectivity, estimate_range_selectivity};
    use crate::catalog::SystemCatalog;
    use crate::optimizer::{ColumnStats, TableStats};
    use crate::parser::{BinaryOperator, Expression, LiteralValue};
    use crate::types::Column;
    use std::sync::{Arc, Mutex};

    fn build_int_table_stats(bounds: &[i32]) -> TableStats {
        let mut table_stats = TableStats::default();
        let histogram_bounds = bounds
            .iter()
            .map(|v| LiteralValue::Number(v.to_string()))
            .collect::<Vec<_>>();
        table_stats.column_stats.insert(
            0,
            ColumnStats {
                n_distinct: 100.0,
                most_common_vals: Vec::new(),
                histogram_bounds,
            },
        );
        table_stats
    }

    #[test]
    fn estimate_range_selectivity_uses_numeric_order() {
        let col_stats = ColumnStats {
            n_distinct: 100.0,
            most_common_vals: Vec::new(),
            histogram_bounds: vec![
                LiteralValue::Number("2".to_string()),
                LiteralValue::Number("10".to_string()),
                LiteralValue::Number("20".to_string()),
                LiteralValue::Number("30".to_string()),
            ],
        };
        let selectivity = estimate_range_selectivity(
            &col_stats,
            &LiteralValue::Number("9".to_string()),
            &BinaryOperator::Lt,
        );

        assert!((selectivity - 0.25).abs() < 1e-9);
    }

    #[test]
    fn estimate_filter_selectivity_supports_literal_on_left_side() {
        let table_stats = build_int_table_stats(&[10, 20, 30, 40]);
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));
        {
            let catalog_guard = catalog.lock().expect("catalog lock");
            catalog_guard.add_schema(
                "users".to_string(),
                Arc::new(vec![Column {
                    name: "id".to_string(),
                    type_id: 23,
                }]),
            );
        }

        let filter = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("25".to_string()))),
            op: BinaryOperator::Lt,
            right: Box::new(Expression::Column("id".to_string())),
        };
        let selectivity = estimate_filter_selectivity(&filter, "users", &table_stats, &catalog);

        assert!((selectivity - 0.5).abs() < 1e-9);
    }

    #[test]
    fn estimate_filter_selectivity_handles_qualified_column_with_literal_on_left_side() {
        let table_stats = build_int_table_stats(&[10, 20, 30, 40]);
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));
        {
            let catalog_guard = catalog.lock().expect("catalog lock");
            catalog_guard.add_schema(
                "users".to_string(),
                Arc::new(vec![Column {
                    name: "id".to_string(),
                    type_id: 23,
                }]),
            );
        }

        let filter = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("15".to_string()))),
            op: BinaryOperator::LtEq,
            right: Box::new(Expression::QualifiedColumn(
                "users".to_string(),
                "id".to_string(),
            )),
        };
        let selectivity = estimate_filter_selectivity(&filter, "users", &table_stats, &catalog);

        assert!((selectivity - 0.75).abs() < 1e-9);
    }
}
