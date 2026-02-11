use super::{PhysicalPlan, PlanInfo, TableStats};
use crate::catalog::SystemCatalog;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use crate::planner::LogicalPlan;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::model::{
    cost_hash_join, cost_index_scan, cost_seq_scan, estimate_join_cardinality, find_join_condition,
};

type PlanCache = HashMap<u32, Vec<PlanInfo>>;

pub(super) fn choose_best_join_plan(
    plan: &LogicalPlan,
    table_names: &[String],
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Option<PlanInfo> {
    if table_names.is_empty() {
        return None;
    }

    let mut cache: PlanCache = HashMap::new();

    for (i, table_name) in table_names.iter().enumerate() {
        let relation_mask = 1u32 << i;
        let mut plans_for_rel = Vec::new();

        let table_stats = stats.get(table_name);
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
        } = plan
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
                        .ok()
                        .and_then(|catalog| catalog.get_schema(table_name));
                    if let Some(schema) = schema {
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
        }

        cache.insert(relation_mask, plans_for_rel);
    }

    let num_tables = table_names.len();
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
                best_plans_for_subset
                    .sort_by(|a, b| a.cost.partial_cmp(&b.cost).unwrap_or(Ordering::Equal));
                cache.insert(subset_mask, best_plans_for_subset);
            }
        }
    }

    let final_mask = (1u32 << num_tables) - 1;
    cache
        .get(&final_mask)
        .and_then(|plans| plans.first().cloned())
}
