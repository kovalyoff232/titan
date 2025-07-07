//! The query planner.
//!
//! This module is responsible for converting the AST from the parser into a logical plan.

use crate::catalog;
use crate::executor;
use crate::parser::{Expression, SelectItem, SelectStatement, TableReference};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        alias: Option<String>,
        filter: Option<Expression>,
        total_rows: usize,
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
) -> Result<LogicalPlan, ()> {
    // For now, we only support a single table or a single JOIN clause.
    let from_plan = match &stmt.from[0] {
        TableReference::Table { name } => {
            let (table_oid, first_page_id) = catalog::find_table(name, bpm, tx_id, snapshot)
                .unwrap()
                .unwrap();
            let schema = catalog::get_table_schema(bpm, table_oid, tx_id, snapshot).unwrap();
            let all_rows = executor::scan_table(
                bpm,
                &Arc::new(Default::default()),
                first_page_id,
                &schema,
                tx_id,
                snapshot,
                false,
            )
            .unwrap();
            LogicalPlan::Scan {
                table_name: name.clone(),
                alias: None,
                filter: stmt.where_clause.clone(),
                total_rows: all_rows.len(),
            }
        }
        TableReference::Join {
            left,
            right,
            on_condition,
        } => {
            let left_plan = build_plan_from_table_ref(left, bpm, tx_id, snapshot)?;
            let right_plan = build_plan_from_table_ref(right, bpm, tx_id, snapshot)?;
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
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<LogicalPlan, ()> {
    match table_ref {
        TableReference::Table { name } => {
            let (table_oid, first_page_id) = catalog::find_table(name, bpm, tx_id, snapshot)
                .unwrap()
                .unwrap();
            let schema = catalog::get_table_schema(bpm, table_oid, tx_id, snapshot).unwrap();
            let all_rows = executor::scan_table(
                bpm,
                &Arc::new(Default::default()),
                first_page_id,
                &schema,
                tx_id,
                snapshot,
                false,
            )
            .unwrap();
            Ok(LogicalPlan::Scan {
                table_name: name.clone(),
                alias: None,
                filter: None,
                total_rows: all_rows.len(),
            })
        }
        _ => Err(()), // Nested joins not supported yet
    }
}
