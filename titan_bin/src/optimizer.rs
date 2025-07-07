//! The query optimizer.
//!
//! This module is responsible for converting a logical plan into an efficient physical plan.

use crate::catalog::{self, get_table_schema};
use crate::executor;
use crate::parser::{BinaryOperator, Expression, LiteralValue, SelectItem};
use crate::planner::LogicalPlan;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use std::collections::HashMap;
use std::sync::Arc;

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

struct Statistics {
    n_distinct: f64,
    mcv: Vec<String>,
    histogram: Vec<String>,
}

impl Statistics {
    fn load(
        table_oid: u32,
        col_idx: usize,
        bpm: &Arc<BufferPoolManager>,
        tx_id: u32,
        snapshot: &Snapshot,
    ) -> Result<Self, ()> {
        let (stat_oid, stat_page_id) = catalog::find_table("pg_statistic", bpm, tx_id, snapshot)
            .unwrap()
            .unwrap();
        let stat_schema = catalog::get_table_schema(bpm, stat_oid, tx_id, snapshot).unwrap();
        let stat_rows = executor::scan_table(
            bpm,
            &Arc::new(Default::default()),
            stat_page_id,
            &stat_schema,
            tx_id,
            snapshot,
            false,
        )
        .unwrap();

        let mut n_distinct = 0.0;
        let mut mcv = Vec::new();
        let mut histogram = Vec::new();

        for (_, _, row) in stat_rows {
            let starelid = row
                .get("starelid")
                .unwrap()
                .to_string()
                .parse::<u32>()
                .unwrap();
            let staattnum = row
                .get("staattnum")
                .unwrap()
                .to_string()
                .parse::<usize>()
                .unwrap();

            if starelid == table_oid && staattnum == col_idx {
                let stakind = row
                    .get("stakind")
                    .unwrap()
                    .to_string()
                    .parse::<i32>()
                    .unwrap();
                match stakind {
                    1 => {
                        n_distinct = row
                            .get("stadistinct")
                            .unwrap()
                            .to_string()
                            .parse::<f64>()
                            .unwrap()
                    }
                    2 => {
                        mcv = row
                            .get("stavalues")
                            .unwrap()
                            .to_string()
                            .split(',')
                            .map(String::from)
                            .collect()
                    }
                    3 => {
                        histogram = row
                            .get("stanumbers")
                            .unwrap()
                            .to_string()
                            .split(',')
                            .map(String::from)
                            .collect()
                    }
                    _ => {}
                }
            }
        }
        Ok(Self {
            n_distinct,
            mcv,
            histogram,
        })
    }
}

fn estimate_seq_scan_cost(num_pages: u32) -> f64 {
    num_pages as f64 * 1.0 // Simple I/O cost
}

fn estimate_index_scan_cost(stats: &Statistics, total_rows: f64, predicate_val: &str) -> f64 {
    let mut selectivity = 1.0 / stats.n_distinct.max(1.0); // Default selectivity

    if stats.mcv.contains(&predicate_val.to_string()) {
        selectivity = 1.0 / stats.mcv.len() as f64 / 2.0; // More selective if in MCV
    } else if !stats.histogram.is_empty() {
        let val = predicate_val.parse::<f64>().unwrap_or(0.0);
        let mut count = 0;
        for bound in &stats.histogram {
            if val <= bound.parse::<f64>().unwrap_or(f64::MAX) {
                count += 1;
            }
        }
        selectivity = count as f64 / stats.histogram.len() as f64;
    }

    let estimated_rows = total_rows * selectivity;
    estimated_rows * 1.2 // I/O cost + CPU cost for index traversal
}

pub fn optimize(
    plan: LogicalPlan,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<PhysicalPlan, ()> {
    match plan {
        LogicalPlan::Scan {
            table_name,
            filter,
            total_rows,
            alias: _,
        } => {
            let (table_oid, first_page_id) = catalog::find_table(&table_name, bpm, tx_id, snapshot)
                .unwrap()
                .unwrap();
            let num_pages = if first_page_id == bedrock::page::INVALID_PAGE_ID {
                0
            } else {
                bpm.pager.lock().unwrap().num_pages
            };

            let seq_scan_cost = estimate_seq_scan_cost(num_pages);
            let mut best_plan = PhysicalPlan::TableScan {
                table_name: table_name.clone(),
                filter: filter.clone(),
            };
            let mut best_cost = seq_scan_cost;

            if let Some(Expression::Binary {
                left,
                op: BinaryOperator::Eq,
                right,
            }) = &filter
            {
                if let (Expression::Column(col_name), Expression::Literal(lit_val)) =
                    (&**left, &**right)
                {
                    let schema = get_table_schema(bpm, table_oid, tx_id, snapshot).unwrap();
                    if let Some(col_idx) = schema.iter().position(|c| &c.name == col_name) {
                        let index_name = format!("idx_{}", col_name);
                        if catalog::find_table(&index_name, bpm, tx_id, snapshot)
                            .unwrap()
                            .is_some()
                        {
                            if let Ok(stats) =
                                Statistics::load(table_oid, col_idx, bpm, tx_id, snapshot)
                            {
                                let index_scan_cost = estimate_index_scan_cost(
                                    &stats,
                                    total_rows as f64,
                                    &lit_val.to_string(),
                                );
                                if index_scan_cost < best_cost {
                                    if let LiteralValue::Number(key_str) = lit_val {
                                        best_plan = PhysicalPlan::IndexScan {
                                            table_name,
                                            index_name,
                                            key: key_str.parse().unwrap(),
                                        };
                                        best_cost = index_scan_cost;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(best_plan)
        }
        LogicalPlan::Projection { input, expressions } => {
            let physical_input = optimize(*input, bpm, tm, tx_id, snapshot)?;
            Ok(PhysicalPlan::Projection {
                input: Box::new(physical_input),
                expressions,
            })
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
        } => {
            let physical_left = optimize(*left, bpm, tm, tx_id, snapshot)?;
            let physical_right = optimize(*right, bpm, tm, tx_id, snapshot)?;

            if let Expression::Binary {
                left: left_expr,
                op: BinaryOperator::Eq,
                right: right_expr,
            } = &condition
            {
                // Check if inputs are sorted on join keys
                let left_sorted = is_sorted_by(&physical_left, left_expr);
                let right_sorted = is_sorted_by(&physical_right, right_expr);

                if left_sorted && right_sorted {
                    return Ok(PhysicalPlan::MergeJoin {
                        left: Box::new(physical_left),
                        right: Box::new(physical_right),
                        left_key: *left_expr.clone(),
                        right_key: *right_expr.clone(),
                    });
                }

                if let (Expression::QualifiedColumn(..), Expression::QualifiedColumn(..)) =
                    (&**left_expr, &**right_expr)
                {
                    return Ok(PhysicalPlan::HashJoin {
                        left: Box::new(physical_left),
                        right: Box::new(physical_right),
                        left_key: *left_expr.clone(),
                        right_key: *right_expr.clone(),
                    });
                }
            }

            Ok(PhysicalPlan::NestedLoopJoin {
                left: Box::new(physical_left),
                right: Box::new(physical_right),
                condition,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let physical_input = optimize(*input, bpm, tm, tx_id, snapshot)?;
            Ok(PhysicalPlan::Sort {
                input: Box::new(physical_input),
                order_by,
            })
        }
    }
}

fn is_sorted_by(plan: &PhysicalPlan, key: &Expression) -> bool {
    match plan {
        PhysicalPlan::Sort { order_by, .. } => !order_by.is_empty() && &order_by[0] == key,
        _ => false,
    }
}
