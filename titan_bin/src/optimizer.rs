//! The query optimizer.
//!
//! This module is responsible for converting a logical plan into an efficient physical plan.

use crate::catalog::{SystemCatalog};
use crate::errors::ExecutionError;
use crate::executor;
use crate::parser::{BinaryOperator, Expression, LiteralValue, SelectItem};
use crate::planner::{LogicalPlan};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
}

#[derive(Default, Debug)]
struct ColumnStats {
    n_distinct: f64,
    most_common_vals: Vec<LiteralValue>,
    histogram_bounds: Vec<LiteralValue>,
}

#[derive(Default, Debug)]
struct TableStats {
    total_rows: f64,
    column_stats: HashMap<usize, ColumnStats>,
}

pub fn optimize(
    plan: LogicalPlan,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<PhysicalPlan, ExecutionError> {
    let stats = load_statistics(bpm, tm, tx_id, snapshot, system_catalog)?;
    Ok(create_physical_plan(
        plan,
        &stats,
        bpm,
        tx_id,
        snapshot,
        system_catalog,
    ))
}

fn load_statistics(
    _bpm: &Arc<BufferPoolManager>,
    _tm: &Arc<TransactionManager>,
    _tx_id: u32,
    _snapshot: &Snapshot,
    _system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<HashMap<String, TableStats>, ExecutionError> {
    // FIXME: This is a temporary fix to avoid recursive calls to the executor
    // when loading statistics. A proper solution would involve a way to load
    // statistics without triggering the full query planning and optimization pipeline.
    Ok(HashMap::new())
}

fn create_physical_plan(
    plan: LogicalPlan,
    stats: &HashMap<String, TableStats>,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan {
            table_name,
            filter,
            ..
        } => PhysicalPlan::TableScan {
            table_name,
            filter,
        },
        LogicalPlan::Projection { input, expressions } => {
            let physical_input =
                create_physical_plan(*input, stats, bpm, tx_id, snapshot, system_catalog);
            PhysicalPlan::Projection {
                input: Box::new(physical_input),
                expressions,
            }
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
        } => {
            let left_physical =
                create_physical_plan(*left, stats, bpm, tx_id, snapshot, system_catalog);
            let right_physical =
                create_physical_plan(*right, stats, bpm, tx_id, snapshot, system_catalog);

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
        LogicalPlan::Sort { input, order_by } => {
            let physical_input =
                create_physical_plan(*input, stats, bpm, tx_id, snapshot, system_catalog);
            PhysicalPlan::Sort {
                input: Box::new(physical_input),
                order_by,
            }
        }
    }
}

fn estimate_cardinality(
    plan: &LogicalPlan,
    stats: &HashMap<String, TableStats>,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> f64 {
    match &plan {
        LogicalPlan::Scan { table_name, .. } => {
            let (table_oid, first_page_id) = system_catalog
                .lock()
                .unwrap()
                .find_table(&table_name, bpm, tx_id, snapshot)
                .unwrap()
                .unwrap();
            let schema = system_catalog
                .lock()
                .unwrap()
                .get_table_schema(bpm, table_oid, tx_id, snapshot)
                .unwrap();
            let num_rows = executor::scan_table(
                bpm,
                &Arc::new(bedrock::lock_manager::LockManager::new()),
                first_page_id,
                &schema,
                tx_id,
                snapshot,
                false,
            )
            .unwrap()
            .len();
            num_rows as f64
        }
        LogicalPlan::Projection { input, .. } => {
            estimate_cardinality(input, stats, bpm, tx_id, snapshot, system_catalog)
        }
        LogicalPlan::Join { left, right, .. } => {
            let left_card = estimate_cardinality(left, stats, bpm, tx_id, snapshot, system_catalog);
            let right_card =
                estimate_cardinality(right, stats, bpm, tx_id, snapshot, system_catalog);
            left_card * right_card // Simplified assumption
        }
        LogicalPlan::Projection { input, .. } => {
            estimate_cardinality(input, stats, bpm, tx_id, snapshot, system_catalog)
        }
        LogicalPlan::Sort { input, .. } => {
            estimate_cardinality(input, stats, bpm, tx_id, snapshot, system_catalog)
        }
    }
}

fn estimate_filter_selectivity(
    plan: &LogicalPlan,
    filter: &Expression,
    stats: &HashMap<String, TableStats>,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> f64 {
    let base_card = estimate_cardinality(plan, stats, bpm, tx_id, snapshot, system_catalog);
    match filter {
        Expression::Binary {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (col_expr, lit_val) = if let Expression::Column(name) = &**left {
                if let Expression::Literal(lit) = &**right {
                    (name, lit)
                } else {
                    return base_card / 3.0;
                }
            } else {
                return base_card / 3.0;
            };

            if let LogicalPlan::Scan { table_name, .. } = plan {
                let (table_oid, _) = system_catalog
                    .lock()
                    .unwrap()
                    .find_table(table_name, bpm, tx_id, snapshot)
                    .unwrap()
                    .unwrap();
                let schema = system_catalog
                    .lock()
                    .unwrap()
                    .get_table_schema(bpm, table_oid, tx_id, snapshot)
                    .unwrap();
                let col_idx = schema.iter().position(|c| &c.name == col_expr).unwrap();

                if let Some(table_stats) = stats.get(table_name) {
                    if let Some(col_stats) = table_stats.column_stats.get(&col_idx) {
                        // Check MCV list
                        if col_stats
                            .most_common_vals
                            .iter()
                            .any(|mcv| mcv == lit_val)
                        {
                            return base_card / col_stats.most_common_vals.len() as f64;
                        }
                        // Check histogram
                        let pos = col_stats
                            .histogram_bounds
                            .binary_search(&lit_val)
                            .unwrap_or_else(|x| x);
                        return base_card / (pos + 1) as f64;
                    }
                }
                base_card / 10.0
            } else {
                base_card / 3.0
            }
        }
        _ => base_card,
    }
}
