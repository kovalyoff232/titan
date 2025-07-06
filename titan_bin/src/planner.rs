//! The query planner.
//!
//! This module is responsible for converting the AST from the parser into a logical plan.

use crate::parser::{Expression, SelectItem, SelectStatement, TableReference};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<SelectItem>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expression,
        // For now, we only support INNER joins.
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<Expression>,
    },
}

pub fn create_logical_plan(stmt: &SelectStatement) -> Result<LogicalPlan, ()> {
    // For now, we only support a single table or a single JOIN clause.
    let from_plan = match &stmt.from[0] {
        TableReference::Table { name } => LogicalPlan::Scan {
            table_name: name.clone(),
            alias: None,
            filter: stmt.where_clause.clone(),
        },
        TableReference::Join {
            left,
            right,
            on_condition,
        } => {
            let left_plan = build_plan_from_table_ref(left)?;
            let right_plan = build_plan_from_table_ref(right)?;
            LogicalPlan::Join {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                condition: on_condition.clone(),
            }
        }
    };

    let mut plan = from_plan;

    if let Some(order_by) = &stmt.order_by {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: order_by.clone(),
        };
    }

    plan = LogicalPlan::Projection {
        input: Box::new(plan),
        expressions: stmt.select_list.clone(),
    };

    Ok(plan)
}

fn build_plan_from_table_ref(table_ref: &TableReference) -> Result<LogicalPlan, ()> {
    match table_ref {
        TableReference::Table { name } => Ok(LogicalPlan::Scan {
            table_name: name.clone(),
            alias: None,
            filter: None,
        }),
        _ => Err(()), // Nested joins not supported yet
    }
}
