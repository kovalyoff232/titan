use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::optimizer::{self, PhysicalPlan};
use crate::parser::{Expression, SelectStatement, Statement};
use crate::planner;
use crate::types::{Column, ExecuteResult, ResultSet};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::lock_manager::LockManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::WalManager;
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};

type Row = Vec<String>;

mod ddl;
mod dml;
mod eval;
mod helpers;
mod join;
mod maintenance;
mod pipeline;
mod scan;
use ddl::{execute_create_index, execute_create_table};
use dml::{execute_delete, execute_insert, execute_update};
pub use helpers::parse_tuple;
use join::{HashJoinExecutor, NestedLoopJoinExecutor};
use maintenance::{execute_analyze, execute_vacuum};
use pipeline::{FilterExecutor, ProjectionExecutor, SortExecutor};
use scan::{IndexScanExecutor, TableScanExecutor};

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError>;
    fn schema(&self) -> &Vec<Column>;
}

pub fn execute(
    stmt: &Statement,
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    lm: &Arc<LockManager>,
    wm: &Arc<Mutex<WalManager>>,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
    tx_id: u32,
    snapshot: &Snapshot,
) -> Result<ExecuteResult, ExecutionError> {
    match stmt {
        Statement::Select(select_stmt) => {
            let logical_plan =
                planner::create_logical_plan(select_stmt, bpm, tx_id, snapshot, system_catalog)?;
            let physical_plan =
                optimizer::optimize(logical_plan, bpm, tm, tx_id, snapshot, system_catalog)?;

            println!("[Executor] Physical Plan: {:?}", physical_plan);

            let build_ctx = ExecutorBuildCtx {
                bpm,
                lm,
                tx_id,
                snapshot,
                select_stmt,
                system_catalog,
            };
            let mut executor = create_executor(&build_ctx, &physical_plan)?;
            let schema = executor.schema().clone();
            let mut rows = Vec::new();
            while let Some(row) = executor.next()? {
                rows.push(row);
            }

            Ok(ExecuteResult::ResultSet(ResultSet {
                columns: schema,
                rows,
            }))
        }
        Statement::CreateTable(create_stmt) => {
            execute_create_table(create_stmt, bpm, tm, wm, tx_id).map(|_| ExecuteResult::Ddl)
        }
        Statement::CreateIndex(create_stmt) => execute_create_index(
            create_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(|_| ExecuteResult::Ddl),
        Statement::Insert(insert_stmt) => execute_insert(
            insert_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Insert),
        Statement::Update(update_stmt) => execute_update(
            update_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Update),
        Statement::Delete(delete_stmt) => execute_delete(
            delete_stmt,
            bpm,
            tm,
            lm,
            wm,
            tx_id,
            snapshot,
            system_catalog,
        )
        .map(ExecuteResult::Delete),
        Statement::Vacuum(table_name) => {
            execute_vacuum(table_name, bpm, tm, lm, wm, tx_id, snapshot, system_catalog)
                .map(|_| ExecuteResult::Ddl)
        }
        Statement::Analyze(table_name) => {
            execute_analyze(table_name, bpm, tm, lm, wm, tx_id, snapshot, system_catalog)
                .map(|_| ExecuteResult::Ddl)
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => Ok(ExecuteResult::Ddl),
        _ => Err(ExecutionError::GenericError(
            "Unsupported statement type".to_string(),
        )),
    }
}

struct ExecutorBuildCtx<'a> {
    bpm: &'a Arc<BufferPoolManager>,
    lm: &'a Arc<LockManager>,
    tx_id: u32,
    snapshot: &'a Snapshot,
    select_stmt: &'a SelectStatement,
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
}

fn lock_system_catalog<'a>(
    system_catalog: &'a Arc<Mutex<SystemCatalog>>,
) -> Result<MutexGuard<'a, SystemCatalog>, ExecutionError> {
    system_catalog
        .lock()
        .map_err(|_| ExecutionError::GenericError("system catalog lock poisoned".to_string()))
}

fn create_executor<'a>(
    ctx: &ExecutorBuildCtx<'a>,
    plan: &'a PhysicalPlan,
) -> Result<Box<dyn Executor + 'a>, ExecutionError> {
    match plan {
        PhysicalPlan::TableScan { table_name, filter } => {
            let (table_oid, first_page_id) = lock_system_catalog(ctx.system_catalog)?
                .find_table(table_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = lock_system_catalog(ctx.system_catalog)?.get_table_schema(
                ctx.bpm,
                table_oid,
                ctx.tx_id,
                ctx.snapshot,
            )?;
            let scan_executor = Box::new(TableScanExecutor::new(
                ctx.bpm,
                ctx.lm,
                first_page_id,
                schema,
                ctx.tx_id,
                ctx.snapshot,
                ctx.select_stmt.for_update,
                None,
            ));

            if let Some(predicate) = filter {
                Ok(Box::new(FilterExecutor::new(
                    scan_executor,
                    predicate.clone(),
                )))
            } else {
                Ok(scan_executor)
            }
        }
        PhysicalPlan::IndexScan {
            table_name,
            index_name,
            key,
        } => {
            let (table_oid, _first_page_id) = lock_system_catalog(ctx.system_catalog)?
                .find_table(table_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(table_name.clone()))?;
            let schema = lock_system_catalog(ctx.system_catalog)?.get_table_schema(
                ctx.bpm,
                table_oid,
                ctx.tx_id,
                ctx.snapshot,
            )?;

            let (_index_oid, index_root_page_id) = lock_system_catalog(ctx.system_catalog)?
                .find_table(index_name, ctx.bpm, ctx.tx_id, ctx.snapshot)?
                .ok_or_else(|| ExecutionError::TableNotFound(index_name.clone()))?;

            Ok(Box::new(IndexScanExecutor::new(
                ctx.bpm,
                schema,
                index_root_page_id,
                *key,
                ctx.tx_id,
                ctx.snapshot,
            )))
        }
        PhysicalPlan::Filter { input, predicate } => {
            let input_executor = create_executor(ctx, input)?;
            Ok(Box::new(FilterExecutor::new(
                input_executor,
                predicate.clone(),
            )))
        }
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
        } => {
            let left_executor = create_executor(ctx, left)?;
            let right_executor = create_executor(ctx, right)?;
            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.clone())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.clone())
            } else {
                None
            };
            Ok(Box::new(NestedLoopJoinExecutor::new(
                left_executor,
                right_executor,
                condition.clone(),
                left_table_name,
                right_table_name,
            )))
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            let left_executor = create_executor(ctx, left)?;
            let right_executor = create_executor(ctx, right)?;
            let left_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**left {
                Some(table_name.clone())
            } else {
                None
            };
            let right_table_name = if let PhysicalPlan::TableScan { table_name, .. } = &**right {
                Some(table_name.clone())
            } else {
                None
            };
            Ok(Box::new(HashJoinExecutor::new(
                left_executor,
                right_executor,
                left_key.clone(),
                right_key.clone(),
                left_table_name,
                right_table_name,
            )?))
        }
        PhysicalPlan::Projection { input, expressions } => {
            let input_executor = create_executor(ctx, input)?;
            Ok(Box::new(ProjectionExecutor::new(
                input_executor,
                expressions.clone(),
            )))
        }
        PhysicalPlan::Sort { input, order_by } => {
            let input_executor = create_executor(ctx, input)?;
            Ok(Box::new(SortExecutor::new(
                input_executor,
                order_by.clone(),
            )?))
        }
        PhysicalPlan::MergeJoin { .. } => Err(ExecutionError::GenericError(
            "MergeJoin is not supported by the iterator executor yet".to_string(),
        )),
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            having,
        } => {
            use crate::aggregate_executor::HashAggregateExecutor;
            let input_executor = create_executor(ctx, input)?;
            Ok(Box::new(HashAggregateExecutor::new(
                input_executor,
                group_by.clone(),
                aggregates.clone(),
                having.clone(),
            )))
        }
        PhysicalPlan::StreamAggregate { .. } => Err(ExecutionError::GenericError(
            "StreamAggregate not yet fully implemented".to_string(),
        )),
        PhysicalPlan::Window { .. } => Err(ExecutionError::GenericError(
            "Window functions not yet fully implemented".to_string(),
        )),
        PhysicalPlan::MaterializeCTE { .. } => Err(ExecutionError::GenericError(
            "MaterializeCTE not yet implemented".to_string(),
        )),
        PhysicalPlan::CTEScan { .. } => Err(ExecutionError::GenericError(
            "CTEScan not yet implemented".to_string(),
        )),
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            use crate::limit_executor::LimitExecutor;
            let input_executor = create_executor(ctx, input)?;
            Ok(Box::new(LimitExecutor::new(
                input_executor,
                limit.map(|l| l as usize),
                offset.unwrap_or(0) as usize,
            )))
        }
    }
}
