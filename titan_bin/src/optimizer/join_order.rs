use super::{PhysicalPlan, PlanInfo, TableStats, compare_plan_info_stable};
use crate::catalog::SystemCatalog;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use crate::planner::LogicalPlan;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::model::{
    cost_hash_join, cost_index_scan, cost_seq_scan, estimate_join_cardinality, find_join_condition,
};

type PlanCache = HashMap<u32, Vec<PlanInfo>>;
const DP_JOIN_RELATION_LIMIT: usize = 8;

fn find_scan_filter<'a>(plan: &'a LogicalPlan, table_name: &str) -> Option<&'a Expression> {
    match plan {
        LogicalPlan::Scan {
            table_name: scan_table,
            filter,
            ..
        } if scan_table == table_name => filter.as_ref(),
        LogicalPlan::Scan { .. } | LogicalPlan::CteRef { .. } => None,
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::WithCte { input, .. } => find_scan_filter(input, table_name),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOperation { left, right, .. } => {
            find_scan_filter(left, table_name).or_else(|| find_scan_filter(right, table_name))
        }
    }
}

fn indexable_column_name(expr: &Expression, table_name: &str) -> Option<String> {
    match expr {
        Expression::Column(name) => Some(name.clone()),
        Expression::QualifiedColumn(source_table, name) if source_table == table_name => {
            Some(name.clone())
        }
        _ => None,
    }
}

fn parse_i32_literal(expr: &Expression) -> Option<i32> {
    match expr {
        Expression::Literal(LiteralValue::Number(value)) => value.parse::<i32>().ok(),
        _ => None,
    }
}

fn extract_eq_filter_for_index(predicate: &Expression, table_name: &str) -> Option<(String, i32)> {
    match predicate {
        Expression::Binary {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_eq_filter_for_index(left, table_name)
            .or_else(|| extract_eq_filter_for_index(right, table_name)),
        Expression::Binary {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let (Some(column), Some(value)) = (
                indexable_column_name(left, table_name),
                parse_i32_literal(right),
            ) {
                return Some((column, value));
            }
            if let (Some(value), Some(column)) = (
                parse_i32_literal(left),
                indexable_column_name(right, table_name),
            ) {
                return Some((column, value));
            }
            None
        }
        _ => None,
    }
}

fn estimate_index_cardinality(
    table_stats: Option<&Arc<TableStats>>,
    base_cardinality: f64,
    column_idx: usize,
) -> f64 {
    let fallback = base_cardinality.max(1.0);
    let Some(table_stats) = table_stats else {
        return fallback;
    };

    let total_rows = table_stats.total_rows.max(fallback);
    let Some(column_stats) = table_stats.column_stats.get(&column_idx) else {
        return fallback;
    };

    let n_distinct = column_stats.n_distinct.max(1.0);
    (total_rows / n_distinct).clamp(1.0, total_rows)
}

fn build_single_relation_cache(
    plan: &LogicalPlan,
    table_names: &[String],
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> PlanCache {
    let mut cache: PlanCache = HashMap::new();

    for (i, table_name) in table_names.iter().enumerate() {
        let relation_mask = 1u32 << i;
        let mut plans_for_rel = Vec::new();

        let table_stats = stats.get(table_name);
        let base_cardinality = table_stats.map_or(1.0, |s| s.total_rows.max(1.0));

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

        if let Some(predicate) = find_scan_filter(plan, table_name) {
            if let Some((column_name, key)) = extract_eq_filter_for_index(predicate, table_name) {
                let schema = system_catalog
                    .lock()
                    .ok()
                    .and_then(|catalog| catalog.get_schema(table_name));
                if let Some(schema) = schema {
                    if let Some(col_idx) = schema.iter().position(|c| c.name == column_name) {
                        if col_idx == 0 {
                            let index_scan_plan = PhysicalPlan::IndexScan {
                                table_name: table_name.to_string(),
                                index_name: format!("idx_{}", column_name),
                                key,
                            };

                            let index_cardinality =
                                estimate_index_cardinality(table_stats, base_cardinality, col_idx);
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

        plans_for_rel.sort_by(compare_plan_info_stable);
        cache.insert(relation_mask, plans_for_rel);
    }

    cache
}

fn best_single_relation_plan(cache: &PlanCache, relation_idx: usize) -> Option<PlanInfo> {
    cache
        .get(&(1u32 << relation_idx))
        .and_then(|plans| plans.first().cloned())
}

fn choose_greedy_fallback_plan(
    plan: &LogicalPlan,
    table_names: &[String],
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
    cache: &PlanCache,
) -> Option<PlanInfo> {
    struct Candidate {
        relation_idx: usize,
        plan: PlanInfo,
    }

    let mut remaining = (0..table_names.len()).collect::<Vec<_>>();
    let first_idx = remaining.first().copied()?;
    remaining.remove(0);

    let mut current = best_single_relation_plan(cache, first_idx)?;

    while !remaining.is_empty() {
        let mut candidates = Vec::new();

        for &next_idx in &remaining {
            let Some(next_plan) = best_single_relation_plan(cache, next_idx) else {
                continue;
            };

            let candidate_plan = if let Some((left_key, right_key)) =
                find_join_condition(plan, &current, &next_plan)
            {
                let joined_plan = PhysicalPlan::HashJoin {
                    left: Box::new((*current.plan).clone()),
                    right: Box::new((*next_plan.plan).clone()),
                    left_key: left_key.clone(),
                    right_key: right_key.clone(),
                };
                let estimated_cardinality = estimate_join_cardinality(
                    &current,
                    &next_plan,
                    &left_key,
                    &right_key,
                    stats,
                    system_catalog,
                )
                .unwrap_or((current.cardinality * next_plan.cardinality).max(1.0));
                PlanInfo {
                    plan: Arc::new(joined_plan),
                    cost: cost_hash_join(&current, &next_plan),
                    cardinality: estimated_cardinality,
                }
            } else {
                let joined_plan = PhysicalPlan::NestedLoopJoin {
                    left: Box::new((*current.plan).clone()),
                    right: Box::new((*next_plan.plan).clone()),
                    condition: Expression::Literal(LiteralValue::Bool(true)),
                };
                PlanInfo {
                    plan: Arc::new(joined_plan),
                    cost: current.cost + (current.cardinality * next_plan.cost),
                    cardinality: (current.cardinality * next_plan.cardinality).max(1.0),
                }
            };

            candidates.push(Candidate {
                relation_idx: next_idx,
                plan: candidate_plan,
            });
        }

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by(|a, b| compare_plan_info_stable(&a.plan, &b.plan));
        let selected = candidates.remove(0);
        current = selected.plan;
        remaining.retain(|idx| *idx != selected.relation_idx);
    }

    Some(current)
}

pub(super) fn choose_best_join_plan(
    plan: &LogicalPlan,
    table_names: &[String],
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Option<PlanInfo> {
    let num_tables = table_names.len();
    if num_tables == 0 {
        return None;
    }

    let mut cache = build_single_relation_cache(plan, table_names, stats, system_catalog);

    if num_tables > DP_JOIN_RELATION_LIMIT {
        return choose_greedy_fallback_plan(plan, table_names, stats, system_catalog, &cache);
    }

    for i in 2..=num_tables {
        for subset_mask in (0..(1u32 << num_tables)).filter(|m| m.count_ones() == i as u32) {
            let mut best_plans_for_subset = Vec::new();

            for sub_subset_mask in (1..subset_mask).filter(|m| (subset_mask & m) == *m) {
                let other_sub_mask = subset_mask ^ sub_subset_mask;
                if cache.contains_key(&sub_subset_mask) && cache.contains_key(&other_sub_mask) {
                    let (Some(plans1), Some(plans2)) =
                        (cache.get(&sub_subset_mask), cache.get(&other_sub_mask))
                    else {
                        continue;
                    };

                    for p1 in plans1 {
                        for p2 in plans2 {
                            if let Some((left_key, right_key)) = find_join_condition(plan, p1, p2) {
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
                best_plans_for_subset.sort_by(compare_plan_info_stable);
                cache.insert(subset_mask, best_plans_for_subset);
            }
        }
    }

    let final_mask = (1u32 << num_tables) - 1;
    cache
        .get(&final_mask)
        .and_then(|plans| plans.first().cloned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::physical_plan_stability_key;
    use crate::types::Column;
    use std::collections::HashMap;

    fn prepare_single_table_stats(
        total_rows: f64,
        n_distinct: f64,
    ) -> HashMap<String, Arc<TableStats>> {
        let mut stats = HashMap::new();
        let mut table_stats = TableStats {
            total_rows,
            ..TableStats::default()
        };
        table_stats.column_stats.insert(
            0,
            crate::optimizer::ColumnStats {
                n_distinct,
                ..crate::optimizer::ColumnStats::default()
            },
        );
        stats.insert("users".to_string(), Arc::new(table_stats));
        stats
    }

    fn prepare_catalog_with_users_schema() -> Arc<Mutex<SystemCatalog>> {
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));
        catalog.lock().expect("catalog lock poisoned").add_schema(
            "users".to_string(),
            Arc::new(vec![Column {
                name: "id".to_string(),
                type_id: 0,
            }]),
        );
        catalog
    }

    fn build_two_table_equi_join() -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "a".to_string(),
                alias: None,
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "b".to_string(),
                alias: None,
                filter: None,
            }),
            condition: Expression::Binary {
                left: Box::new(Expression::QualifiedColumn(
                    "a".to_string(),
                    "id".to_string(),
                )),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::QualifiedColumn(
                    "b".to_string(),
                    "id".to_string(),
                )),
            },
        }
    }

    fn build_equi_join_chain(table_count: usize) -> LogicalPlan {
        let mut plan = LogicalPlan::Scan {
            table_name: "t0".to_string(),
            alias: None,
            filter: None,
        };

        for i in 1..table_count {
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(LogicalPlan::Scan {
                    table_name: format!("t{}", i),
                    alias: None,
                    filter: None,
                }),
                condition: Expression::Binary {
                    left: Box::new(Expression::QualifiedColumn(
                        format!("t{}", i - 1),
                        "id".to_string(),
                    )),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::QualifiedColumn(
                        format!("t{}", i),
                        "id".to_string(),
                    )),
                },
            };
        }

        plan
    }

    #[test]
    fn stable_compare_breaks_equal_cost_ties_by_plan_key() {
        let mut candidates = vec![
            PlanInfo {
                plan: Arc::new(PhysicalPlan::TableScan {
                    table_name: "z_table".to_string(),
                    filter: None,
                }),
                cost: 1.0,
                cardinality: 10.0,
            },
            PlanInfo {
                plan: Arc::new(PhysicalPlan::TableScan {
                    table_name: "a_table".to_string(),
                    filter: None,
                }),
                cost: 1.0,
                cardinality: 10.0,
            },
        ];

        candidates.sort_by(compare_plan_info_stable);
        let first_key = physical_plan_stability_key(&candidates[0].plan);
        let second_key = physical_plan_stability_key(&candidates[1].plan);
        assert!(first_key < second_key);
    }

    #[test]
    fn choose_best_join_plan_is_stable_across_invocations() {
        let logical = build_two_table_equi_join();
        let table_names = vec!["a".to_string(), "b".to_string()];
        let stats: HashMap<String, Arc<TableStats>> = HashMap::new();
        let system_catalog = Arc::new(Mutex::new(SystemCatalog::new()));

        let expected = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .map(|p| physical_plan_stability_key(&p.plan))
            .expect("expected a plan");

        for _ in 0..50 {
            let current = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
                .map(|p| physical_plan_stability_key(&p.plan))
                .expect("expected a plan");
            assert_eq!(current, expected);
        }
    }

    #[test]
    fn chooses_index_scan_through_projection_wrapped_scan() {
        let logical = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
                filter: Some(Expression::Binary {
                    left: Box::new(Expression::Column("id".to_string())),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(LiteralValue::Number("42".to_string()))),
                }),
            }),
            expressions: vec![],
        };
        let table_names = vec!["users".to_string()];
        let stats = prepare_single_table_stats(10_000.0, 10_000.0);
        let system_catalog = prepare_catalog_with_users_schema();

        let best = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .expect("expected a plan");

        match &*best.plan {
            PhysicalPlan::IndexScan {
                table_name,
                index_name,
                key,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "idx_id");
                assert_eq!(*key, 42);
                assert_eq!(best.cardinality, 1.0);
            }
            other => panic!("expected index scan, got {other:?}"),
        }
    }

    #[test]
    fn chooses_index_scan_through_multi_wrapped_scan() {
        let logical = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Projection {
                    input: Box::new(LogicalPlan::Scan {
                        table_name: "users".to_string(),
                        alias: None,
                        filter: Some(Expression::Binary {
                            left: Box::new(Expression::Column("id".to_string())),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expression::Literal(LiteralValue::Number(
                                "9".to_string(),
                            ))),
                        }),
                    }),
                    expressions: vec![],
                }),
                order_by: vec![],
            }),
            limit: Some(5),
            offset: Some(0),
        };
        let table_names = vec!["users".to_string()];
        let stats = prepare_single_table_stats(10_000.0, 10_000.0);
        let system_catalog = prepare_catalog_with_users_schema();

        let expected_key = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .map(|p| physical_plan_stability_key(&p.plan))
            .expect("expected a plan");

        for _ in 0..50 {
            let best = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
                .expect("expected a plan");
            let current_key = physical_plan_stability_key(&best.plan);
            assert_eq!(current_key, expected_key);
            match &*best.plan {
                PhysicalPlan::IndexScan { key, .. } => {
                    assert_eq!(*key, 9);
                    assert_eq!(best.cardinality, 1.0);
                }
                other => panic!("expected index scan, got {other:?}"),
            }
        }
    }

    #[test]
    fn extracts_index_predicate_with_qualified_column_and_literal_on_left() {
        let logical = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: None,
            filter: Some(Expression::Binary {
                left: Box::new(Expression::Literal(LiteralValue::Number("7".to_string()))),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::QualifiedColumn(
                    "users".to_string(),
                    "id".to_string(),
                )),
            }),
        };
        let table_names = vec!["users".to_string()];
        let stats = prepare_single_table_stats(20_000.0, 10_000.0);
        let system_catalog = prepare_catalog_with_users_schema();

        let best = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .expect("expected a plan");

        match &*best.plan {
            PhysicalPlan::IndexScan { key, .. } => {
                assert_eq!(*key, 7);
                assert_eq!(best.cardinality, 2.0);
            }
            other => panic!("expected index scan, got {other:?}"),
        }
    }

    #[test]
    fn does_not_choose_index_scan_for_non_i32_key_literal() {
        let logical = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: None,
            filter: Some(Expression::Binary {
                left: Box::new(Expression::Column("id".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::Literal(LiteralValue::Number(
                    "99999999999999999999".to_string(),
                ))),
            }),
        };
        let table_names = vec!["users".to_string()];
        let stats = prepare_single_table_stats(10_000.0, 10_000.0);
        let system_catalog = prepare_catalog_with_users_schema();

        let best = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .expect("expected a plan");
        assert!(matches!(&*best.plan, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn uses_greedy_fallback_when_relation_count_exceeds_dp_limit() {
        let relation_count = DP_JOIN_RELATION_LIMIT + 1;
        let logical = build_equi_join_chain(relation_count);
        let table_names = (0..(DP_JOIN_RELATION_LIMIT + 1))
            .map(|i| format!("t{}", i))
            .collect::<Vec<_>>();
        let stats: HashMap<String, Arc<TableStats>> = HashMap::new();
        let system_catalog = Arc::new(Mutex::new(SystemCatalog::new()));

        let expected = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
            .map(|p| physical_plan_stability_key(&p.plan))
            .expect("expected greedy fallback plan");

        for _ in 0..20 {
            let current = choose_best_join_plan(&logical, &table_names, &stats, &system_catalog)
                .map(|p| physical_plan_stability_key(&p.plan))
                .expect("expected greedy fallback plan");
            assert_eq!(current, expected);
        }
    }
}
