use crate::errors::ExecutionError;
use crate::parser::{BinaryOperator, Expression, LiteralValue};
use chrono::prelude::*;
use std::cmp::Ordering;
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

fn like_match(value: &str, pattern: &str) -> bool {
    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let value_len = value_chars.len();
    let pattern_len = pattern_chars.len();
    let mut dp = vec![vec![false; value_len + 1]; pattern_len + 1];
    dp[0][0] = true;

    for i in 1..=pattern_len {
        if pattern_chars[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=pattern_len {
        for j in 1..=value_len {
            dp[i][j] = match pattern_chars[i - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && c == value_chars[j - 1],
            };
        }
    }

    dp[pattern_len][value_len]
}

fn compare_literal_values(
    left: &LiteralValue,
    right: &LiteralValue,
) -> Result<Ordering, ExecutionError> {
    match (left, right) {
        (LiteralValue::Number(l), LiteralValue::Number(r)) => {
            let lnum = parse_i32_literal(l)?;
            let rnum = parse_i32_literal(r)?;
            Ok(lnum.cmp(&rnum))
        }
        (LiteralValue::String(l), LiteralValue::String(r)) => Ok(l.cmp(r)),
        (LiteralValue::Bool(l), LiteralValue::Bool(r)) => Ok(l.cmp(r)),
        (LiteralValue::Date(l), LiteralValue::Date(r)) => {
            let ldate = parse_date_literal(l)?;
            let rdate = parse_date_literal(r)?;
            Ok(ldate.cmp(&rdate))
        }
        _ => Err(ExecutionError::GenericError(
            "Type mismatch in BETWEEN expression".to_string(),
        )),
    }
}

fn trim_string_by_charset(
    value: &str,
    chars: Option<&str>,
    trim_start: bool,
    trim_end: bool,
) -> String {
    let mut result = value;
    if trim_start {
        result = match chars {
            Some(charset) => result.trim_start_matches(|c| charset.contains(c)),
            None => result.trim_start(),
        };
    }
    if trim_end {
        result = match chars {
            Some(charset) => result.trim_end_matches(|c| charset.contains(c)),
            None => result.trim_end(),
        };
    }
    result.to_string()
}

fn literal_to_concat_text(value: LiteralValue) -> Option<String> {
    match value {
        LiteralValue::String(text) | LiteralValue::Number(text) | LiteralValue::Date(text) => {
            Some(text)
        }
        LiteralValue::Bool(flag) => Some(if flag { "t" } else { "f" }.to_string()),
        LiteralValue::Null => None,
    }
}

fn substring_by_char_index(value: &str, start: i32, length: Option<i32>) -> String {
    let total_chars = value.chars().count() as i32;
    let start_index = if start <= 1 { 0 } else { start - 1 };
    if start_index >= total_chars {
        return String::new();
    }
    let max_len = total_chars - start_index;
    let take_len = match length {
        Some(len) if len <= 0 => 0,
        Some(len) => len.min(max_len),
        None => max_len,
    };
    value
        .chars()
        .skip(start_index as usize)
        .take(take_len as usize)
        .collect()
}

fn strpos_by_char_index(value: &str, pattern: &str) -> i32 {
    if pattern.is_empty() {
        return 1;
    }
    if let Some(byte_pos) = value.find(pattern) {
        let char_pos = value
            .char_indices()
            .take_while(|(idx, _)| *idx < byte_pos)
            .count();
        (char_pos + 1) as i32
    } else {
        0
    }
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
            if let Some(value) = row.get(&qname) {
                return Ok(value.clone());
            }
            if !row.keys().any(|key| key.contains('.')) {
                if let Some(value) = row.get(col) {
                    return Ok(value.clone());
                }
            }
            Err(ExecutionError::ColumnNotFound(qname))
        }
        Expression::Binary { left, op, right } => {
            let lval = evaluate_expr_for_row_to_val(left, row)?;
            let rval = evaluate_expr_for_row_to_val(right, row)?;
            match (lval, rval) {
                (LiteralValue::Null, _) | (_, LiteralValue::Null) => match op {
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Like
                    | BinaryOperator::NotLike
                    | BinaryOperator::ILike
                    | BinaryOperator::NotILike => Ok(LiteralValue::Bool(false)),
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => Ok(LiteralValue::Null),
                    _ => Err(ExecutionError::GenericError(
                        "Unsupported operator for NULL".to_string(),
                    )),
                },
                (LiteralValue::Number(l), LiteralValue::Number(r)) => {
                    let lnum = parse_i32_literal(&l)?;
                    let rnum = parse_i32_literal(&r)?;
                    match op {
                        BinaryOperator::Plus => Ok(LiteralValue::Number((lnum + rnum).to_string())),
                        BinaryOperator::Minus => {
                            Ok(LiteralValue::Number((lnum - rnum).to_string()))
                        }
                        BinaryOperator::Multiply => {
                            Ok(LiteralValue::Number((lnum * rnum).to_string()))
                        }
                        BinaryOperator::Divide => {
                            if rnum == 0 {
                                return Err(ExecutionError::GenericError(
                                    "Division by zero".to_string(),
                                ));
                            }
                            Ok(LiteralValue::Number((lnum / rnum).to_string()))
                        }
                        BinaryOperator::Modulo => {
                            if rnum == 0 {
                                return Err(ExecutionError::GenericError(
                                    "Modulo by zero".to_string(),
                                ));
                            }
                            Ok(LiteralValue::Number((lnum % rnum).to_string()))
                        }
                        BinaryOperator::Eq => Ok(LiteralValue::Bool(lnum == rnum)),
                        BinaryOperator::NotEq => Ok(LiteralValue::Bool(lnum != rnum)),
                        BinaryOperator::Lt => Ok(LiteralValue::Bool(lnum < rnum)),
                        BinaryOperator::LtEq => Ok(LiteralValue::Bool(lnum <= rnum)),
                        BinaryOperator::Gt => Ok(LiteralValue::Bool(lnum > rnum)),
                        BinaryOperator::GtEq => Ok(LiteralValue::Bool(lnum >= rnum)),
                        BinaryOperator::And => Ok(LiteralValue::Bool(lnum != 0 && rnum != 0)),
                        BinaryOperator::Or => Ok(LiteralValue::Bool(lnum != 0 || rnum != 0)),
                        BinaryOperator::Like => Err(ExecutionError::GenericError(
                            "Unsupported operator for number".to_string(),
                        )),
                        BinaryOperator::NotLike => Err(ExecutionError::GenericError(
                            "Unsupported operator for number".to_string(),
                        )),
                        BinaryOperator::ILike => Err(ExecutionError::GenericError(
                            "Unsupported operator for number".to_string(),
                        )),
                        BinaryOperator::NotILike => Err(ExecutionError::GenericError(
                            "Unsupported operator for number".to_string(),
                        )),
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
                    BinaryOperator::Like => Ok(LiteralValue::Bool(like_match(&l, &r))),
                    BinaryOperator::NotLike => Ok(LiteralValue::Bool(!like_match(&l, &r))),
                    BinaryOperator::ILike => Ok(LiteralValue::Bool(like_match(
                        &l.to_lowercase(),
                        &r.to_lowercase(),
                    ))),
                    BinaryOperator::NotILike => Ok(LiteralValue::Bool(!like_match(
                        &l.to_lowercase(),
                        &r.to_lowercase(),
                    ))),
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
        Expression::IsNull { expr, negated } => {
            let value = evaluate_expr_for_row_to_val(expr, row)?;
            let is_null = matches!(value, LiteralValue::Null)
                || matches!(value, LiteralValue::String(text) if text.is_empty());
            Ok(LiteralValue::Bool(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        Expression::InList {
            expr,
            list,
            negated,
        } => {
            let value = evaluate_expr_for_row_to_val(expr, row)?;
            let is_match = list
                .iter()
                .map(|item| evaluate_expr_for_row_to_val(item, row))
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .any(|item_value| item_value == &value);
            Ok(LiteralValue::Bool(if *negated {
                !is_match
            } else {
                is_match
            }))
        }
        Expression::Between {
            expr,
            lower,
            upper,
            negated,
        } => {
            let value = evaluate_expr_for_row_to_val(expr, row)?;
            let lower_value = evaluate_expr_for_row_to_val(lower, row)?;
            let upper_value = evaluate_expr_for_row_to_val(upper, row)?;
            let lower_cmp = compare_literal_values(&value, &lower_value)?;
            let upper_cmp = compare_literal_values(&value, &upper_value)?;
            let is_between = (lower_cmp == Ordering::Greater || lower_cmp == Ordering::Equal)
                && (upper_cmp == Ordering::Less || upper_cmp == Ordering::Equal);
            Ok(LiteralValue::Bool(if *negated {
                !is_between
            } else {
                is_between
            }))
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
            "COALESCE" => {
                for arg in args {
                    let value = evaluate_expr_for_row_to_val(arg, row)?;
                    if !matches!(value, LiteralValue::Null)
                        && !matches!(value, LiteralValue::String(ref text) if text.is_empty())
                    {
                        return Ok(value);
                    }
                }
                Ok(LiteralValue::Null)
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(ExecutionError::GenericError(
                        "NULLIF requires exactly 2 arguments".to_string(),
                    ));
                }
                let left = evaluate_expr_for_row_to_val(&args[0], row)?;
                let right = evaluate_expr_for_row_to_val(&args[1], row)?;
                let equals = left == right
                    || matches!(
                        (&left, &right),
                        (LiteralValue::String(text), LiteralValue::Null) if text.is_empty()
                    )
                    || matches!(
                        (&left, &right),
                        (LiteralValue::Null, LiteralValue::String(text)) if text.is_empty()
                    );
                if equals {
                    Ok(LiteralValue::Null)
                } else {
                    Ok(left)
                }
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                if args.len() != 1 {
                    return Err(ExecutionError::GenericError(
                        "LENGTH requires exactly 1 argument".to_string(),
                    ));
                }
                match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => {
                        Ok(LiteralValue::Number(text.chars().count().to_string()))
                    }
                    LiteralValue::Null => Ok(LiteralValue::Null),
                    _ => Err(ExecutionError::GenericError(
                        "LENGTH requires text argument".to_string(),
                    )),
                }
            }
            "CONCAT" => {
                let mut out = String::new();
                for arg in args {
                    if let Some(text) =
                        literal_to_concat_text(evaluate_expr_for_row_to_val(arg, row)?)
                    {
                        out.push_str(&text);
                    }
                }
                Ok(LiteralValue::String(out))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(ExecutionError::GenericError(
                        "REPLACE requires exactly 3 arguments".to_string(),
                    ));
                }
                let source = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "REPLACE source argument must be text".to_string(),
                        ));
                    }
                };
                let from = match evaluate_expr_for_row_to_val(&args[1], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "REPLACE search argument must be text".to_string(),
                        ));
                    }
                };
                let to = match evaluate_expr_for_row_to_val(&args[2], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "REPLACE replacement argument must be text".to_string(),
                        ));
                    }
                };
                Ok(LiteralValue::String(source.replace(&from, &to)))
            }
            "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(ExecutionError::GenericError(
                        "SUBSTRING requires 2 or 3 arguments".to_string(),
                    ));
                }
                let source = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "SUBSTRING source argument must be text".to_string(),
                        ));
                    }
                };
                let start = match evaluate_expr_for_row_to_val(&args[1], row)? {
                    LiteralValue::Number(text) => parse_i32_literal(&text)?,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "SUBSTRING start argument must be integer".to_string(),
                        ));
                    }
                };
                let length = if let Some(length_expr) = args.get(2) {
                    match evaluate_expr_for_row_to_val(length_expr, row)? {
                        LiteralValue::Number(text) => {
                            let parsed = parse_i32_literal(&text)?;
                            if parsed < 0 {
                                return Err(ExecutionError::GenericError(
                                    "SUBSTRING length cannot be negative".to_string(),
                                ));
                            }
                            Some(parsed)
                        }
                        LiteralValue::Null => return Ok(LiteralValue::Null),
                        _ => {
                            return Err(ExecutionError::GenericError(
                                "SUBSTRING length argument must be integer".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };
                Ok(LiteralValue::String(substring_by_char_index(
                    &source, start, length,
                )))
            }
            "STRPOS" | "POSITION" => {
                if args.len() != 2 {
                    return Err(ExecutionError::GenericError(
                        "STRPOS requires exactly 2 arguments".to_string(),
                    ));
                }
                let source = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "STRPOS source argument must be text".to_string(),
                        ));
                    }
                };
                let search = match evaluate_expr_for_row_to_val(&args[1], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "STRPOS search argument must be text".to_string(),
                        ));
                    }
                };
                Ok(LiteralValue::Number(
                    strpos_by_char_index(&source, &search).to_string(),
                ))
            }
            "TRIM" | "BTRIM" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecutionError::GenericError(
                        "TRIM requires 1 or 2 arguments".to_string(),
                    ));
                }
                let value = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "TRIM requires text argument".to_string(),
                        ));
                    }
                };
                let chars = if let Some(chars_expr) = args.get(1) {
                    match evaluate_expr_for_row_to_val(chars_expr, row)? {
                        LiteralValue::String(text) => Some(text),
                        LiteralValue::Null => return Ok(LiteralValue::Null),
                        _ => {
                            return Err(ExecutionError::GenericError(
                                "TRIM characters argument must be text".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };
                Ok(LiteralValue::String(trim_string_by_charset(
                    &value,
                    chars.as_deref(),
                    true,
                    true,
                )))
            }
            "LTRIM" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecutionError::GenericError(
                        "LTRIM requires 1 or 2 arguments".to_string(),
                    ));
                }
                let value = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "LTRIM requires text argument".to_string(),
                        ));
                    }
                };
                let chars = if let Some(chars_expr) = args.get(1) {
                    match evaluate_expr_for_row_to_val(chars_expr, row)? {
                        LiteralValue::String(text) => Some(text),
                        LiteralValue::Null => return Ok(LiteralValue::Null),
                        _ => {
                            return Err(ExecutionError::GenericError(
                                "LTRIM characters argument must be text".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };
                Ok(LiteralValue::String(trim_string_by_charset(
                    &value,
                    chars.as_deref(),
                    true,
                    false,
                )))
            }
            "RTRIM" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(ExecutionError::GenericError(
                        "RTRIM requires 1 or 2 arguments".to_string(),
                    ));
                }
                let value = match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => text,
                    LiteralValue::Null => return Ok(LiteralValue::Null),
                    _ => {
                        return Err(ExecutionError::GenericError(
                            "RTRIM requires text argument".to_string(),
                        ));
                    }
                };
                let chars = if let Some(chars_expr) = args.get(1) {
                    match evaluate_expr_for_row_to_val(chars_expr, row)? {
                        LiteralValue::String(text) => Some(text),
                        LiteralValue::Null => return Ok(LiteralValue::Null),
                        _ => {
                            return Err(ExecutionError::GenericError(
                                "RTRIM characters argument must be text".to_string(),
                            ));
                        }
                    }
                } else {
                    None
                };
                Ok(LiteralValue::String(trim_string_by_charset(
                    &value,
                    chars.as_deref(),
                    false,
                    true,
                )))
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(ExecutionError::GenericError(
                        "LOWER requires exactly 1 argument".to_string(),
                    ));
                }
                match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => Ok(LiteralValue::String(text.to_lowercase())),
                    LiteralValue::Null => Ok(LiteralValue::Null),
                    _ => Err(ExecutionError::GenericError(
                        "LOWER requires text argument".to_string(),
                    )),
                }
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(ExecutionError::GenericError(
                        "UPPER requires exactly 1 argument".to_string(),
                    ));
                }
                match evaluate_expr_for_row_to_val(&args[0], row)? {
                    LiteralValue::String(text) => Ok(LiteralValue::String(text.to_uppercase())),
                    LiteralValue::Null => Ok(LiteralValue::Null),
                    _ => Err(ExecutionError::GenericError(
                        "UPPER requires text argument".to_string(),
                    )),
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

    #[test]
    fn is_null_predicate_returns_true_for_null_values() {
        let expr = Expression::IsNull {
            expr: Box::new(Expression::Column("deleted_at".to_string())),
            negated: false,
        };
        let mut row = HashMap::new();
        row.insert("deleted_at".to_string(), LiteralValue::Null);

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn is_not_null_predicate_returns_false_for_null_values() {
        let expr = Expression::IsNull {
            expr: Box::new(Expression::Column("deleted_at".to_string())),
            negated: true,
        };
        let mut row = HashMap::new();
        row.insert("deleted_at".to_string(), LiteralValue::Null);

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(false));
    }

    #[test]
    fn is_null_predicate_treats_empty_text_as_null_surrogate() {
        let expr = Expression::IsNull {
            expr: Box::new(Expression::Column("payload".to_string())),
            negated: false,
        };
        let mut row = HashMap::new();
        row.insert("payload".to_string(), LiteralValue::String(String::new()));

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn string_like_pattern_comparison_evaluates_to_boolean() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::String(
                "Alice".to_string(),
            ))),
            op: BinaryOperator::Like,
            right: Box::new(Expression::Literal(LiteralValue::String(
                "Al_ce".to_string(),
            ))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn string_not_like_pattern_comparison_evaluates_to_boolean() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::String(
                "Alice".to_string(),
            ))),
            op: BinaryOperator::NotLike,
            right: Box::new(Expression::Literal(LiteralValue::String("Bo%".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn string_ilike_pattern_comparison_evaluates_to_boolean() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::String(
                "ALICE".to_string(),
            ))),
            op: BinaryOperator::ILike,
            right: Box::new(Expression::Literal(LiteralValue::String("al%".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn string_not_ilike_pattern_comparison_evaluates_to_boolean() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::String(
                "ALICE".to_string(),
            ))),
            op: BinaryOperator::NotILike,
            right: Box::new(Expression::Literal(LiteralValue::String("bo%".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn in_list_expression_evaluates_to_boolean() {
        let expr = Expression::InList {
            expr: Box::new(Expression::Literal(LiteralValue::Number("2".to_string()))),
            list: vec![
                Expression::Literal(LiteralValue::Number("1".to_string())),
                Expression::Literal(LiteralValue::Number("2".to_string())),
                Expression::Literal(LiteralValue::Number("3".to_string())),
            ],
            negated: false,
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn not_in_list_expression_evaluates_to_boolean() {
        let expr = Expression::InList {
            expr: Box::new(Expression::Literal(LiteralValue::Number("2".to_string()))),
            list: vec![
                Expression::Literal(LiteralValue::Number("4".to_string())),
                Expression::Literal(LiteralValue::Number("5".to_string())),
                Expression::Literal(LiteralValue::Number("6".to_string())),
            ],
            negated: true,
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn between_expression_evaluates_to_boolean() {
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(LiteralValue::Number("15".to_string()))),
            lower: Box::new(Expression::Literal(LiteralValue::Number("10".to_string()))),
            upper: Box::new(Expression::Literal(LiteralValue::Number("20".to_string()))),
            negated: false,
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn not_between_expression_evaluates_to_boolean() {
        let expr = Expression::Between {
            expr: Box::new(Expression::Literal(LiteralValue::Number("25".to_string()))),
            lower: Box::new(Expression::Literal(LiteralValue::Number("10".to_string()))),
            upper: Box::new(Expression::Literal(LiteralValue::Number("20".to_string()))),
            negated: true,
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(true));
    }

    #[test]
    fn coalesce_function_returns_first_non_null_argument() {
        let expr = Expression::Function {
            name: "COALESCE".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::Null),
                Expression::Literal(LiteralValue::String(String::new())),
                Expression::Literal(LiteralValue::String("fallback".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(
            result.unwrap(),
            LiteralValue::String("fallback".to_string())
        );
    }

    #[test]
    fn coalesce_function_returns_null_when_all_arguments_are_null() {
        let expr = Expression::Function {
            name: "COALESCE".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::Null),
                Expression::Literal(LiteralValue::Null),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Null);
    }

    #[test]
    fn nullif_function_returns_null_when_arguments_are_equal() {
        let expr = Expression::Function {
            name: "NULLIF".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("same".to_string())),
                Expression::Literal(LiteralValue::String("same".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Null);
    }

    #[test]
    fn nullif_function_returns_left_argument_when_values_differ() {
        let expr = Expression::Function {
            name: "NULLIF".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("left".to_string())),
                Expression::Literal(LiteralValue::String("right".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("left".to_string()));
    }

    #[test]
    fn lower_function_converts_text_to_lowercase() {
        let expr = Expression::Function {
            name: "LOWER".to_string(),
            args: vec![Expression::Literal(LiteralValue::String(
                "MiXeD".to_string(),
            ))],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("mixed".to_string()));
    }

    #[test]
    fn upper_function_converts_text_to_uppercase() {
        let expr = Expression::Function {
            name: "UPPER".to_string(),
            args: vec![Expression::Literal(LiteralValue::String(
                "MiXeD".to_string(),
            ))],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("MIXED".to_string()));
    }

    #[test]
    fn length_function_returns_character_count() {
        let expr = Expression::Function {
            name: "LENGTH".to_string(),
            args: vec![Expression::Literal(LiteralValue::String(
                "test".to_string(),
            ))],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Number("4".to_string()));
    }

    #[test]
    fn trim_functions_remove_expected_characters() {
        let trim_expr = Expression::Function {
            name: "TRIM".to_string(),
            args: vec![Expression::Literal(LiteralValue::String(
                "  spaced value  ".to_string(),
            ))],
        };
        let ltrim_expr = Expression::Function {
            name: "LTRIM".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("xxxy".to_string())),
                Expression::Literal(LiteralValue::String("x".to_string())),
            ],
        };
        let rtrim_expr = Expression::Function {
            name: "RTRIM".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("yzzz".to_string())),
                Expression::Literal(LiteralValue::String("z".to_string())),
            ],
        };
        let row = HashMap::new();

        assert_eq!(
            evaluate_expr_for_row_to_val(&trim_expr, &row).unwrap(),
            LiteralValue::String("spaced value".to_string())
        );
        assert_eq!(
            evaluate_expr_for_row_to_val(&ltrim_expr, &row).unwrap(),
            LiteralValue::String("y".to_string())
        );
        assert_eq!(
            evaluate_expr_for_row_to_val(&rtrim_expr, &row).unwrap(),
            LiteralValue::String("y".to_string())
        );
    }

    #[test]
    fn concat_function_skips_null_arguments() {
        let expr = Expression::Function {
            name: "CONCAT".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("user-".to_string())),
                Expression::Literal(LiteralValue::Null),
                Expression::Literal(LiteralValue::Number("42".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("user-42".to_string()));
    }

    #[test]
    fn replace_function_rewrites_matching_substrings() {
        let expr = Expression::Function {
            name: "REPLACE".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("aba".to_string())),
                Expression::Literal(LiteralValue::String("a".to_string())),
                Expression::Literal(LiteralValue::String("x".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("xbx".to_string()));
    }

    #[test]
    fn substring_function_uses_sql_style_start_index() {
        let expr = Expression::Function {
            name: "SUBSTRING".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("abcdef".to_string())),
                Expression::Literal(LiteralValue::Number("2".to_string())),
                Expression::Literal(LiteralValue::Number("3".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::String("bcd".to_string()));
    }

    #[test]
    fn strpos_function_returns_one_based_position() {
        let expr = Expression::Function {
            name: "STRPOS".to_string(),
            args: vec![
                Expression::Literal(LiteralValue::String("alphabet".to_string())),
                Expression::Literal(LiteralValue::String("pha".to_string())),
            ],
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Number("3".to_string()));
    }

    #[test]
    fn numeric_multiply_divide_and_modulo_evaluate_correctly() {
        let row = HashMap::new();

        let multiply_expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("6".to_string()))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(LiteralValue::Number("7".to_string()))),
        };
        let divide_expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("21".to_string()))),
            op: BinaryOperator::Divide,
            right: Box::new(Expression::Literal(LiteralValue::Number("3".to_string()))),
        };
        let modulo_expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("22".to_string()))),
            op: BinaryOperator::Modulo,
            right: Box::new(Expression::Literal(LiteralValue::Number("5".to_string()))),
        };

        assert_eq!(
            evaluate_expr_for_row_to_val(&multiply_expr, &row).unwrap(),
            LiteralValue::Number("42".to_string())
        );
        assert_eq!(
            evaluate_expr_for_row_to_val(&divide_expr, &row).unwrap(),
            LiteralValue::Number("7".to_string())
        );
        assert_eq!(
            evaluate_expr_for_row_to_val(&modulo_expr, &row).unwrap(),
            LiteralValue::Number("2".to_string())
        );
    }

    #[test]
    fn numeric_division_by_zero_returns_error() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Number("21".to_string()))),
            op: BinaryOperator::Divide,
            right: Box::new(Expression::Literal(LiteralValue::Number("0".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert!(matches!(
            result,
            Err(ExecutionError::GenericError(msg)) if msg == "Division by zero"
        ));
    }

    #[test]
    fn null_comparison_returns_false_instead_of_type_error() {
        let expr = Expression::Binary {
            left: Box::new(Expression::Literal(LiteralValue::Null)),
            op: BinaryOperator::Eq,
            right: Box::new(Expression::Literal(LiteralValue::String("x".to_string()))),
        };
        let row = HashMap::new();

        let result = evaluate_expr_for_row_to_val(&expr, &row);
        assert_eq!(result.unwrap(), LiteralValue::Bool(false));
    }
}
