use crate::errors::ExecutionError;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use chrono::prelude::*;
use std::collections::HashMap;

fn parse_i32_literal(value: &str) -> Result<i32, ExecutionError> {
    value.parse::<i32>().map_err(|_| {
        ExecutionError::GenericError(format!(
            "Invalid integer value in expression evaluation: {}",
            value
        ))
    })
}

fn parse_date_literal(value: &str) -> Result<NaiveDate, ExecutionError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
        ExecutionError::GenericError(format!(
            "Invalid date value in expression evaluation: {}",
            value
        ))
    })
}

pub(crate) fn evaluate_expr_for_row(
    expr: &Expression,
    row: &HashMap<String, LiteralValue>,
) -> Result<bool, ExecutionError> {
    match evaluate_expr_for_row_to_val(expr, row)? {
        LiteralValue::Bool(b) => Ok(b),
        _ => Err(ExecutionError::GenericError(
            "Expression did not evaluate to a boolean".to_string(),
        )),
    }
}

pub(crate) fn evaluate_expr_for_row_to_val(
    expr: &Expression,
    row: &HashMap<String, LiteralValue>,
) -> Result<LiteralValue, ExecutionError> {
    match expr {
        Expression::Literal(lit) => Ok(lit.clone()),
        Expression::Column(name) => {
            if let Some(val) = row.get(name) {
                return Ok(val.clone());
            }

            let mut found: Option<LiteralValue> = None;
            let mut ambiguous = false;
            for (key, val) in row {
                if key.ends_with(&format!(".{}", name)) {
                    if found.is_some() {
                        ambiguous = true;
                        break;
                    }
                    found = Some(val.clone());
                }
            }

            if ambiguous {
                return Err(ExecutionError::GenericError(format!(
                    "Column {} is ambiguous",
                    name
                )));
            }

            found.ok_or_else(|| ExecutionError::ColumnNotFound(name.clone()))
        }
        Expression::QualifiedColumn(table, col) => {
            let qname = format!("{}.{}", table, col);
            row.get(&qname)
                .cloned()
                .ok_or(ExecutionError::ColumnNotFound(qname))
        }
        Expression::Binary { left, op, right } => {
            let lval = evaluate_expr_for_row_to_val(left, row)?;
            let rval = evaluate_expr_for_row_to_val(right, row)?;
            match (lval, rval) {
                (LiteralValue::Number(l), LiteralValue::Number(r)) => {
                    let lnum = parse_i32_literal(&l)?;
                    let rnum = parse_i32_literal(&r)?;
                    match op {
                        BinaryOperator::Plus => Ok(LiteralValue::Number((lnum + rnum).to_string())),
                        BinaryOperator::Minus => {
                            Ok(LiteralValue::Number((lnum - rnum).to_string()))
                        }
                        BinaryOperator::Eq => Ok(LiteralValue::Bool(lnum == rnum)),
                        BinaryOperator::NotEq => Ok(LiteralValue::Bool(lnum != rnum)),
                        BinaryOperator::Lt => Ok(LiteralValue::Bool(lnum < rnum)),
                        BinaryOperator::LtEq => Ok(LiteralValue::Bool(lnum <= rnum)),
                        BinaryOperator::Gt => Ok(LiteralValue::Bool(lnum > rnum)),
                        BinaryOperator::GtEq => Ok(LiteralValue::Bool(lnum >= rnum)),
                        BinaryOperator::And => Ok(LiteralValue::Bool(lnum != 0 && rnum != 0)),
                        BinaryOperator::Or => Ok(LiteralValue::Bool(lnum != 0 || rnum != 0)),
                    }
                }
                (LiteralValue::Bool(l), LiteralValue::Bool(r)) => match op {
                    BinaryOperator::Eq => Ok(LiteralValue::Bool(l == r)),
                    BinaryOperator::NotEq => Ok(LiteralValue::Bool(l != r)),
                    BinaryOperator::And => Ok(LiteralValue::Bool(l && r)),
                    BinaryOperator::Or => Ok(LiteralValue::Bool(l || r)),
                    _ => Err(ExecutionError::GenericError(
                        "Unsupported operator for boolean".to_string(),
                    )),
                },
                (LiteralValue::Date(l), LiteralValue::Date(r)) => {
                    let ldate = parse_date_literal(&l)?;
                    let rdate = parse_date_literal(&r)?;
                    match op {
                        BinaryOperator::Eq => Ok(LiteralValue::Bool(ldate == rdate)),
                        BinaryOperator::NotEq => Ok(LiteralValue::Bool(ldate != rdate)),
                        BinaryOperator::Lt => Ok(LiteralValue::Bool(ldate < rdate)),
                        BinaryOperator::LtEq => Ok(LiteralValue::Bool(ldate <= rdate)),
                        BinaryOperator::Gt => Ok(LiteralValue::Bool(ldate > rdate)),
                        BinaryOperator::GtEq => Ok(LiteralValue::Bool(ldate >= rdate)),
                        _ => Err(ExecutionError::GenericError(
                            "Unsupported operator for date".to_string(),
                        )),
                    }
                }
                (LiteralValue::String(l), LiteralValue::String(r)) => match op {
                    BinaryOperator::Eq => Ok(LiteralValue::Bool(l == r)),
                    BinaryOperator::NotEq => Ok(LiteralValue::Bool(l != r)),
                    BinaryOperator::Lt => Ok(LiteralValue::Bool(l < r)),
                    BinaryOperator::LtEq => Ok(LiteralValue::Bool(l <= r)),
                    BinaryOperator::Gt => Ok(LiteralValue::Bool(l > r)),
                    BinaryOperator::GtEq => Ok(LiteralValue::Bool(l >= r)),
                    _ => Err(ExecutionError::GenericError(
                        "Unsupported operator for text".to_string(),
                    )),
                },
                _ => Err(ExecutionError::GenericError(
                    "Type mismatch in binary expression".to_string(),
                )),
            }
        }
        Expression::Unary { op, expr } => {
            let val = evaluate_expr_for_row_to_val(expr, row)?;
            match op {
                crate::parser::UnaryOperator::Not => match val {
                    LiteralValue::Bool(b) => Ok(LiteralValue::Bool(!b)),
                    _ => Err(ExecutionError::GenericError(
                        "NOT operator requires a boolean expression".to_string(),
                    )),
                },
            }
        }
        Expression::WindowFunction { .. } => Err(ExecutionError::GenericError(
            "Window functions cannot be evaluated in WHERE clause".to_string(),
        )),
        Expression::Function { name, args } => match name.to_uppercase().as_str() {
            "COUNT" => Ok(LiteralValue::Number("1".to_string())),
            "SUM" | "AVG" | "MIN" | "MAX" => {
                if let Some(first_arg) = args.first() {
                    evaluate_expr_for_row_to_val(first_arg, row)
                } else {
                    Ok(LiteralValue::Null)
                }
            }
            _ => Err(ExecutionError::GenericError(format!(
                "Unsupported function: {}",
                name
            ))),
        },
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            for (condition, result) in when_clauses {
                let cond_result = if let Some(op) = operand {
                    let op_val = evaluate_expr_for_row_to_val(op, row)?;
                    let cond_val = evaluate_expr_for_row_to_val(condition, row)?;
                    op_val == cond_val
                } else {
                    match evaluate_expr_for_row_to_val(condition, row)? {
                        LiteralValue::Bool(b) => b,
                        _ => false,
                    }
                };

                if cond_result {
                    return evaluate_expr_for_row_to_val(result, row);
                }
            }

            if let Some(else_expr) = else_clause {
                evaluate_expr_for_row_to_val(else_expr, row)
            } else {
                Ok(LiteralValue::Null)
            }
        }
        Expression::Subquery(_) => Err(ExecutionError::GenericError(
            "Subqueries are not yet supported in this context".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::evaluate_expr_for_row_to_val;
    use crate::errors::ExecutionError;
    use crate::parser::{BinaryOperator, Expression, LiteralValue};
    use std::collections::HashMap;

    #[test]
    fn invalid_integer_literal_returns_error_instead_of_panicking() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("1.5".to_string()))),
            op: BinaryOperator::Eq,
            right: Box::new(Expression::Literal(LiteralValue::Number("1".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert!(matches!(
            result,
            Err(ExecutionError::GenericError(msg))
            if msg.contains("Invalid integer value")
        ));
    }

    #[test]
    fn invalid_date_literal_returns_error_instead_of_panicking() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Date(
                "2025-13-01".to_string(),
            ))),
            op: BinaryOperator::Eq,
            right: Box::new(Expression::Literal(LiteralValue::Date(
                "2025-01-01".to_string(),
            ))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert!(matches!(
            result,
            Err(ExecutionError::GenericError(msg))
            if msg.contains("Invalid date value")
        ));
    }

    #[test]
    fn string_equality_comparison_evaluates_to_boolean() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::String("Bob".to_string()))),
            op: BinaryOperator::Eq,
            right: Box::new(Expression::Literal(LiteralValue::String("Bob".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }
}
