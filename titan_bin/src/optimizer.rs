//! The query optimizer.
//!
//! This module is responsible for converting a logical plan into an efficient physical plan.

use crate::parser::{BinaryOperator, Expression, SelectItem};
use crate::planner::LogicalPlan;

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
pub fn optimize(plan: LogicalPlan) -> Result<PhysicalPlan, ()> {
    match plan {
        LogicalPlan::Scan { table_name, filter, .. } => {
            // For now, we will always use a table scan.
            // A future improvement would be to check for available indexes and choose
            // an IndexScan if a suitable one exists.
            Ok(PhysicalPlan::TableScan { table_name, filter })
        }
        LogicalPlan::Projection { input, expressions } => {
            let physical_input = optimize(*input)?;
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
            let physical_left = optimize(*left)?;
            let physical_right = optimize(*right)?;

            // Rule: Choose Join Algorithm
            // If it's a simple equality on qualified columns, use HashJoin.
            // Otherwise, fall back to NestedLoopJoin.
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

            // Fallback for non-equi joins or complex conditions
            Ok(PhysicalPlan::NestedLoopJoin {
                left: Box::new(physical_left),
                right: Box::new(physical_right),
                condition,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let physical_input = optimize(*input)?;
            Ok(PhysicalPlan::Sort {
                input: Box::new(physical_input),
                order_by,
            })
        }
    }
}
