use crate::catalog::SystemCatalog;
use crate::errors::ExecutionError;
use crate::parser::{
    CommonTableExpression, Expression, OrderByExpr, SelectItem, SelectStatement, TableReference,
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
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression,
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
        order_by: Vec<OrderByExpr>,
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
    let mut from_plan = if stmt.from.is_empty() {
        LogicalPlan::Scan {
            table_name: "dummy".to_string(),
            alias: None,
            filter: None,
        }
    } else {
        let first_table_ref = stmt.from.first().ok_or_else(|| {
            ExecutionError::GenericError("missing FROM table reference".to_string())
        })?;
        build_plan_from_table_ref(first_table_ref, bpm, tx_id, snapshot, system_catalog)?
    };

    if let Some(where_clause) = &stmt.where_clause {
        if let LogicalPlan::Scan { filter, .. } = &mut from_plan {
            *filter = Some(where_clause.clone());
        } else {
            from_plan = LogicalPlan::Filter {
                input: Box::new(from_plan),
                predicate: where_clause.clone(),
            };
        }
    }
    let mut plan = from_plan;

    let aggregates = extract_aggregates(&stmt.select_list)?;
    let requires_aggregate =
        stmt.group_by.is_some() || !aggregates.is_empty() || stmt.having.is_some();
    if requires_aggregate {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: stmt.group_by.clone().unwrap_or_default(),
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
        let resolved_order_by = resolve_order_by_aliases(order_by, &stmt.select_list);
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: resolved_order_by,
        };
    }

    if stmt.limit.is_some() || stmt.offset.is_some() {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: stmt.limit,
            offset: stmt.offset,
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
        TableReference::Join {
            left,
            right,
            on_condition,
        } => {
            let left_plan =
                build_plan_from_table_ref(left, _bpm, _tx_id, _snapshot, _system_catalog)?;
            let right_plan =
                build_plan_from_table_ref(right, _bpm, _tx_id, _snapshot, _system_catalog)?;
            Ok(LogicalPlan::Join {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                condition: on_condition.clone(),
            })
        }
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

fn is_sort_resolvable_expression(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Column(_)
            | Expression::QualifiedColumn(_, _)
            | Expression::Literal(crate::parser::LiteralValue::Number(_))
    )
}

fn resolve_order_by_aliases(
    order_by: &[OrderByExpr],
    select_list: &[SelectItem],
) -> Vec<OrderByExpr> {
    order_by
        .iter()
        .map(|order_expr| {
            let resolved_expr = match &order_expr.expr {
                Expression::Column(alias_name) => select_list
                    .iter()
                    .find_map(|item| match item {
                        SelectItem::ExprWithAlias { expr, alias }
                            if alias == alias_name && is_sort_resolvable_expression(expr) =>
                        {
                            Some(expr.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| order_expr.expr.clone()),
                _ => order_expr.expr.clone(),
            };

            OrderByExpr {
                expr: resolved_expr,
                asc: order_expr.asc,
                nulls_first: order_expr.nulls_first,
            }
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::resolve_order_by_aliases;
    use crate::parser::{Expression, LiteralValue, OrderByExpr, SelectItem};

    #[test]
    fn resolve_order_by_alias_to_underlying_column_expression() {
        let select_list = vec![SelectItem::ExprWithAlias {
            expr: Expression::Column("id".to_string()),
            alias: "item_id".to_string(),
        }];
        let order_by = vec![OrderByExpr {
            expr: Expression::Column("item_id".to_string()),
            asc: false,
            nulls_first: None,
        }];

        let resolved = resolve_order_by_aliases(&order_by, &select_list);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr, Expression::Column("id".to_string()));
        assert!(!resolved[0].asc);
    }

    #[test]
    fn keeps_order_by_alias_when_underlying_expression_is_not_sort_resolvable() {
        let select_list = vec![SelectItem::ExprWithAlias {
            expr: Expression::Binary {
                left: Box::new(Expression::Column("id".to_string())),
                op: crate::parser::BinaryOperator::Plus,
                right: Box::new(Expression::Literal(LiteralValue::Number("1".to_string()))),
            },
            alias: "k".to_string(),
        }];
        let order_by = vec![OrderByExpr {
            expr: Expression::Column("k".to_string()),
            asc: true,
            nulls_first: None,
        }];

        let resolved = resolve_order_by_aliases(&order_by, &select_list);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr, Expression::Column("k".to_string()));
    }
}
