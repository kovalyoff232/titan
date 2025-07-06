//! The query optimizer.
//!
//! This module is responsible for converting a logical plan into an efficient physical plan.

use crate::parser::{BinaryOperator, Expression, SelectItem};
use crate::planner::LogicalPlan;
use crate::catalog;
use std::sync::Arc;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};

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

/// A simple rule-based optimizer.
pub fn optimize(
    plan: LogicalPlan,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<PhysicalPlan, ()> {
    match plan {
        LogicalPlan::Scan { table_name, filter, .. } => {
            if let Some(Expression::Binary { left, op: BinaryOperator::Eq, right }) = &filter {
                if let (Expression::Column(col_name), Expression::Literal(crate::parser::LiteralValue::Number(key_str))) = (&**left, &**right) {
                    let index_name = format!("idx_{}", col_name);
                    if catalog::find_table(&index_name, bpm, tx_id, snapshot).unwrap().is_some() {
                        let key = key_str.parse::<i32>().unwrap();
                        return Ok(PhysicalPlan::IndexScan { table_name, index_name, key });
                    }
                }
            }
            Ok(PhysicalPlan::TableScan { table_name, filter })
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
