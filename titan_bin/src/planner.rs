//! The query planner.
//!
//! This module is responsible for converting the AST from the parser into a logical plan.

use crate::errors::ExecutionError;
use crate::parser::{Expression, SelectItem, SelectStatement, TableReference};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot};
use crate::catalog::SystemCatalog;
use std::sync::{Arc, Mutex};

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

pub fn create_logical_plan(
    stmt: &SelectStatement,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<LogicalPlan, ExecutionError> {
    // For now, we only support a single table or a single JOIN clause.
    let from_plan = match &stmt.from[0] {
        TableReference::Table { name } => {
            LogicalPlan::Scan {
                table_name: name.clone(),
                alias: None,
                filter: stmt.where_clause.clone(),
            }
        }
        TableReference::Join {
            left,
            right,
            on_condition,
        } => {
            let left_plan = build_plan_from_table_ref(left, bpm, tx_id, snapshot, system_catalog)?;
            let right_plan = build_plan_from_table_ref(right, bpm, tx_id, snapshot, system_catalog)?;
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

fn build_plan_from_table_ref(
    table_ref: &TableReference,
    _bpm: &Arc<BufferPoolManager>,
    _tx_id: u32,
    _snapshot: &Snapshot,
    _system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<LogicalPlan, ExecutionError> {
    match table_ref {
        TableReference::Table { name } => {
            Ok(LogicalPlan::Scan {
                table_name: name.clone(),
                alias: None,
                filter: None,
            })
        }
        _ => Err(ExecutionError::PlanningError(
            "Nested joins not supported yet".to_string(),
        )),
    }
}
