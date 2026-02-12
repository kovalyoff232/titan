use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::parser::{Expression, LiteralValue, OrderByExpr, SelectItem};
use crate::planner::LogicalPlan;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod convert;
mod join_order;
mod model;
mod stats;
use convert::create_simple_physical_plan;
use join_order::choose_best_join_plan;
use model::{
    cost_filter, cost_projection, cost_sort, estimate_filter_selectivity_for_plan,
    get_filter_from_logical_plan,
};
use stats::{get_table_names, load_statistics_for_table};

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
        order_by: Vec<OrderByExpr>,
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
    let table_names = sorted_table_names(&plan);

    for table_name in &table_names {
        load_statistics_for_table(table_name, &mut stats, bpm, tx_id, snapshot, system_catalog)?;
    }

    Ok(create_physical_plan(
        plan,
        &table_names,
        &stats,
        system_catalog,
    ))
}

fn sorted_table_names(plan: &LogicalPlan) -> Vec<String> {
    let mut table_names: Vec<_> = get_table_names(plan).into_iter().collect();
    table_names.sort();
    table_names
}

#[derive(Clone, Debug)]
struct PlanInfo {
    plan: Arc<PhysicalPlan>,
    cost: f64,
    cardinality: f64,
}

fn compare_plan_info_stable(a: &PlanInfo, b: &PlanInfo) -> Ordering {
    match a.cost.partial_cmp(&b.cost).unwrap_or(Ordering::Equal) {
        Ordering::Equal => {}
        ord => return ord,
    }

    match a
        .cardinality
        .partial_cmp(&b.cardinality)
        .unwrap_or(Ordering::Equal)
    {
        Ordering::Equal => {}
        ord => return ord,
    }

    physical_plan_stability_key(&a.plan).cmp(&physical_plan_stability_key(&b.plan))
}

pub(crate) fn physical_plan_stability_key(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::TableScan { table_name, filter } => {
            format!("scan:{table_name}:{filter:?}")
        }
        PhysicalPlan::IndexScan {
            table_name,
            index_name,
            key,
        } => format!("idx:{table_name}:{index_name}:{key}"),
        PhysicalPlan::Filter { input, predicate } => {
            format!(
                "filter:{}:{predicate:?}",
                physical_plan_stability_key(input)
            )
        }
        PhysicalPlan::Projection { input, expressions } => {
            format!(
                "proj:{}:{expressions:?}",
                physical_plan_stability_key(input)
            )
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => format!(
            "hjoin:{}:{}:{left_key:?}:{right_key:?}",
            physical_plan_stability_key(left),
            physical_plan_stability_key(right)
        ),
        PhysicalPlan::MergeJoin {
            left,
            right,
            left_key,
            right_key,
        } => format!(
            "mjoin:{}:{}:{left_key:?}:{right_key:?}",
            physical_plan_stability_key(left),
            physical_plan_stability_key(right)
        ),
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
        } => format!(
            "nlj:{}:{}:{condition:?}",
            physical_plan_stability_key(left),
            physical_plan_stability_key(right)
        ),
        PhysicalPlan::Sort { input, order_by } => {
            format!("sort:{}:{order_by:?}", physical_plan_stability_key(input))
        }
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            having,
        } => format!(
            "hagg:{}:{group_by:?}:{aggregates:?}:{having:?}",
            physical_plan_stability_key(input)
        ),
        PhysicalPlan::StreamAggregate {
            input,
            group_by,
            aggregates,
            having,
        } => format!(
            "sagg:{}:{group_by:?}:{aggregates:?}:{having:?}",
            physical_plan_stability_key(input)
        ),
        PhysicalPlan::Window {
            input,
            window_functions,
        } => format!(
            "window:{}:{window_functions:?}",
            physical_plan_stability_key(input)
        ),
        PhysicalPlan::MaterializeCTE { name, plan } => {
            format!("matcte:{name}:{}", physical_plan_stability_key(plan))
        }
        PhysicalPlan::CTEScan { name } => format!("ctescan:{name}"),
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => format!(
            "limit:{}:{limit:?}:{offset:?}",
            physical_plan_stability_key(input)
        ),
    }
}

fn create_physical_plan(
    plan: LogicalPlan,
    table_names: &[String],
    stats: &HashMap<String, Arc<TableStats>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> PhysicalPlan {
    if let Some(mut final_plan_info) =
        choose_best_join_plan(&plan, table_names, stats, system_catalog)
    {
        let mut final_plan = (*final_plan_info.plan).clone();

        if let Some(filter_predicate) = get_filter_from_logical_plan(&plan) {
            final_plan = PhysicalPlan::Filter {
                input: Box::new(final_plan),
                predicate: filter_predicate.clone(),
            };
            final_plan_info.cost += cost_filter(&final_plan_info);
            final_plan_info.cardinality *= estimate_filter_selectivity_for_plan(
                filter_predicate,
                &final_plan,
                stats,
                system_catalog,
            );
        }

        final_plan = apply_non_filter_wrappers(&plan, final_plan, &mut final_plan_info);

        final_plan
    } else {
        create_simple_physical_plan(plan)
    }
}

fn apply_non_filter_wrappers(
    logical: &LogicalPlan,
    base: PhysicalPlan,
    plan_info: &mut PlanInfo,
) -> PhysicalPlan {
    match logical {
        LogicalPlan::Projection { input, expressions } => {
            let wrapped = apply_non_filter_wrappers(input, base, plan_info);
            plan_info.cost += cost_projection(plan_info, expressions.len());
            PhysicalPlan::Projection {
                input: Box::new(wrapped),
                expressions: expressions.clone(),
            }
        }
        LogicalPlan::Sort { input, order_by } => {
            let wrapped = apply_non_filter_wrappers(input, base, plan_info);
            plan_info.cost += cost_sort(plan_info);
            PhysicalPlan::Sort {
                input: Box::new(wrapped),
                order_by: order_by.clone(),
            }
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let wrapped = apply_non_filter_wrappers(input, base, plan_info);
            PhysicalPlan::Limit {
                input: Box::new(wrapped),
                limit: *limit,
                offset: *offset,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => {
            let wrapped = apply_non_filter_wrappers(input, base, plan_info);
            PhysicalPlan::HashAggregate {
                input: Box::new(wrapped),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                having: having.clone(),
            }
        }
        LogicalPlan::Window {
            input,
            window_functions,
        } => {
            let wrapped = apply_non_filter_wrappers(input, base, plan_info);
            PhysicalPlan::Window {
                input: Box::new(wrapped),
                window_functions: window_functions.clone(),
            }
        }
        LogicalPlan::Filter { input, .. } | LogicalPlan::WithCte { input, .. } => {
            apply_non_filter_wrappers(input, base, plan_info)
        }
        LogicalPlan::SetOperation { .. }
        | LogicalPlan::CteRef { .. }
        | LogicalPlan::Scan { .. }
        | LogicalPlan::Join { .. } => base,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PhysicalPlan, create_physical_plan, create_simple_physical_plan, sorted_table_names,
    };
    use crate::catalog::SystemCatalog;
    use crate::parser::{BinaryOperator, Expression, LiteralValue, OrderByExpr, SelectItem};
    use crate::planner::LogicalPlan;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn build_join_logical_plan() -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
                filter: None,
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
                alias: None,
                filter: None,
            }),
            condition: Expression::Binary {
                left: Box::new(Expression::QualifiedColumn(
                    "users".to_string(),
                    "id".to_string(),
                )),
                op: BinaryOperator::Eq,
                right: Box::new(Expression::QualifiedColumn(
                    "orders".to_string(),
                    "user_id".to_string(),
                )),
            },
        }
    }

    #[test]
    fn sorted_table_names_are_stable() {
        let plan = build_join_logical_plan();
        let table_names = sorted_table_names(&plan);

        assert_eq!(table_names, vec!["orders".to_string(), "users".to_string()]);
    }

    #[test]
    fn simple_physical_plan_is_stable_for_same_input() {
        let plan = LogicalPlan::Projection {
            input: Box::new(build_join_logical_plan()),
            expressions: vec![SelectItem::Wildcard],
        };

        let left = create_simple_physical_plan(plan.clone());
        let right = create_simple_physical_plan(plan);

        assert_eq!(format!("{:?}", left), format!("{:?}", right));
    }

    #[test]
    fn create_physical_plan_preserves_projection_sort_wrapper_order() {
        let logical = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    filter: None,
                }),
                order_by: vec![OrderByExpr {
                    expr: Expression::Column("id".to_string()),
                    asc: true,
                    nulls_first: None,
                }],
            }),
            expressions: vec![SelectItem::Wildcard],
        };
        let table_names = vec!["users".to_string()];
        let stats = HashMap::new();
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));

        let physical = create_physical_plan(logical, &table_names, &stats, &catalog);

        match physical {
            PhysicalPlan::Projection { input, .. } => match *input {
                PhysicalPlan::Sort { input, .. } => {
                    assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
                }
                other => panic!("expected Sort under Projection, got {other:?}"),
            },
            other => panic!("expected Projection at root, got {other:?}"),
        }
    }

    #[test]
    fn create_physical_plan_keeps_scan_filter_under_sort_and_projection() {
        let logical = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table_name: "users".to_string(),
                    alias: None,
                    filter: Some(Expression::Binary {
                        left: Box::new(Expression::Column("id".to_string())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expression::Literal(LiteralValue::Number("7".to_string()))),
                    }),
                }),
                order_by: vec![OrderByExpr {
                    expr: Expression::Column("id".to_string()),
                    asc: true,
                    nulls_first: None,
                }],
            }),
            expressions: vec![SelectItem::Wildcard],
        };
        let table_names = vec!["users".to_string()];
        let stats = HashMap::new();
        let catalog = Arc::new(Mutex::new(SystemCatalog::new()));

        let physical = create_physical_plan(logical, &table_names, &stats, &catalog);

        match physical {
            PhysicalPlan::Projection { input, .. } => match *input {
                PhysicalPlan::Sort { input, .. } => {
                    assert!(matches!(*input, PhysicalPlan::Filter { .. }));
                }
                other => panic!("expected Sort under Projection, got {other:?}"),
            },
            other => panic!("expected Projection at root, got {other:?}"),
        }
    }
}
