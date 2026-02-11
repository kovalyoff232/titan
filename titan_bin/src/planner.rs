use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::parser::{
    CommonTableExpression, Expression, SelectItem, SelectStatement, TableReference,
};
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::Snapshot;
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
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<Expression>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expression>,
    },
    Window {
        input: Box<LogicalPlan>,
        window_functions: Vec<WindowFunctionPlan>,
    },
    CteRef {
        name: String,
    },
    WithCte {
        cte_list: Vec<CommonTableExpression>,
        input: Box<LogicalPlan>,
    },
    SetOperation {
        op: SetOperationType,
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<i64>,
        offset: Option<i64>,
    },
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: String,
    pub args: Vec<Expression>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WindowFunctionPlan {
    pub function: String,
    pub args: Vec<Expression>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Expression>,
    pub frame: Option<WindowFramePlan>,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum WindowFramePlan {
    Rows(FrameBoundPlan, FrameBoundPlan),
    Range(FrameBoundPlan, FrameBoundPlan),
}

#[derive(Debug, Clone)]
pub enum FrameBoundPlan {
    UnboundedPreceding,
    CurrentRow,
    UnboundedFollowing,
    Preceding(i64),
    Following(i64),
}

#[derive(Debug, Clone)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

pub fn create_logical_plan(
    stmt: &SelectStatement,
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
) -> Result<LogicalPlan, ExecutionError> {
    let from_plan = if stmt.from.is_empty() {
        LogicalPlan::Scan {
            table_name: "dummy".to_string(),
            alias: None,
            filter: None,
        }
    } else {
        match &stmt.from[0] {
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
                let left_plan =
                    build_plan_from_table_ref(left, bpm, tx_id, snapshot, system_catalog)?;
                let right_plan =
                    build_plan_from_table_ref(right, bpm, tx_id, snapshot, system_catalog)?;
                LogicalPlan::Join {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    condition: on_condition.clone(),
                }
            }
        }
    };

    let mut plan = from_plan;

    if let Some(_where_clause) = &stmt.where_clause {
        if !matches!(plan, LogicalPlan::Scan { .. }) {}
    }

    if let Some(group_by) = &stmt.group_by {
        let aggregates = extract_aggregates(&stmt.select_list)?;
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: group_by.clone(),
            aggregates,
            having: stmt.having.clone(),
        };
    }

    let window_functions = extract_window_functions(&stmt.select_list)?;
    if !window_functions.is_empty() {
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            window_functions,
        };
    }

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

    if let Some(cte_list) = &stmt.with_clause {
        plan = LogicalPlan::WithCte {
            cte_list: cte_list.clone(),
            input: Box::new(plan),
        };
    }

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
        TableReference::Table { name } => Ok(LogicalPlan::Scan {
            table_name: name.clone(),
            alias: None,
            filter: None,
        }),
        _ => Err(ExecutionError::PlanningError(
            "Nested joins not supported yet".to_string(),
        )),
    }
}

fn extract_aggregates(select_list: &[SelectItem]) -> Result<Vec<AggregateExpr>, ExecutionError> {
    let mut aggregates = Vec::new();

    for item in select_list {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if let Expression::Function { name, args } = expr {
                    if is_aggregate_function(name) {
                        aggregates.push(AggregateExpr {
                            function: name.clone(),
                            args: args.clone(),
                            alias: if let SelectItem::ExprWithAlias { alias, .. } = item {
                                Some(alias.clone())
                            } else {
                                None
                            },
                        });
                    }
                }
            }
            _ => {}
        }
    }

    Ok(aggregates)
}

fn extract_window_functions(
    select_list: &[SelectItem],
) -> Result<Vec<WindowFunctionPlan>, ExecutionError> {
    use crate::parser::WindowFunctionType;

    let mut window_functions = Vec::new();

    for item in select_list {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                if let Expression::WindowFunction {
                    function,
                    args,
                    over,
                } = expr
                {
                    let function_name = match function {
                        WindowFunctionType::RowNumber => "ROW_NUMBER",
                        WindowFunctionType::Rank => "RANK",
                        WindowFunctionType::DenseRank => "DENSE_RANK",
                        WindowFunctionType::PercentRank => "PERCENT_RANK",
                        WindowFunctionType::CumeDist => "CUME_DIST",
                        WindowFunctionType::Ntile(_) => "NTILE",
                        WindowFunctionType::Lag { .. } => "LAG",
                        WindowFunctionType::Lead { .. } => "LEAD",
                        WindowFunctionType::FirstValue => "FIRST_VALUE",
                        WindowFunctionType::LastValue => "LAST_VALUE",
                        WindowFunctionType::NthValue(_) => "NTH_VALUE",
                    }
                    .to_string();

                    let frame = over.frame.as_ref().map(convert_window_frame);

                    window_functions.push(WindowFunctionPlan {
                        function: function_name,
                        args: args.clone(),
                        partition_by: over.partition_by.clone(),
                        order_by: over.order_by.iter().map(|o| o.expr.clone()).collect(),
                        frame,
                        alias: if let SelectItem::ExprWithAlias { alias, .. } = item {
                            alias.clone()
                        } else {
                            format!("window_{}", window_functions.len())
                        },
                    });
                }
            }
            _ => {}
        }
    }

    Ok(window_functions)
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "STDDEV"
            | "VARIANCE"
            | "ARRAY_AGG"
            | "STRING_AGG"
    )
}

fn convert_window_frame(frame: &crate::parser::WindowFrame) -> WindowFramePlan {
    use crate::parser::WindowFrame;

    match frame {
        WindowFrame::Rows(start, end) => {
            WindowFramePlan::Rows(convert_frame_bound(start), convert_frame_bound(end))
        }
        WindowFrame::Range(start, end) => {
            WindowFramePlan::Range(convert_frame_bound(start), convert_frame_bound(end))
        }
        WindowFrame::Groups(start, end) => {
            WindowFramePlan::Rows(convert_frame_bound(start), convert_frame_bound(end))
        }
    }
}

fn convert_frame_bound(bound: &crate::parser::FrameBound) -> FrameBoundPlan {
    use crate::parser::FrameBound;

    match bound {
        FrameBound::UnboundedPreceding => FrameBoundPlan::UnboundedPreceding,
        FrameBound::CurrentRow => FrameBoundPlan::CurrentRow,
        FrameBound::UnboundedFollowing => FrameBoundPlan::UnboundedFollowing,
        FrameBound::Preceding(n) => FrameBoundPlan::Preceding(*n),
        FrameBound::Following(n) => FrameBoundPlan::Following(*n),
    }
}
