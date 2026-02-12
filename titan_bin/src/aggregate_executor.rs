use crate::errors::ExecutionError;
use crate::executor::{Executor, evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use crate::parser::{Expression, LiteralValue};
use crate::planner::AggregateExpr;
use crate::types::Column;
use std::collections::HashMap;

type Row = Vec<String>;

pub struct HashAggregateExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    group_by: Vec<Expression>,
    aggregates: Vec<AggregateExpr>,
    having: Option<Expression>,
    schema: Vec<Column>,

    groups: HashMap<Vec<String>, AggregateState>,
    result_iterator: Option<std::vec::IntoIter<Row>>,
    materialized: bool,
}

#[derive(Debug, Clone, Default)]
struct AggregateState {
    states: Vec<SingleAggregateState>,
}

#[derive(Debug, Clone)]
struct SingleAggregateState {
    count: i64,
    sum: f64,
    min: Option<LiteralValue>,
    max: Option<LiteralValue>,
    values: Vec<LiteralValue>,
}

impl Default for SingleAggregateState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            values: Vec::new(),
        }
    }
}

impl<'a> HashAggregateExecutor<'a> {
    pub fn new(
        input: Box<dyn Executor + 'a>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expression>,
    ) -> Self {
        let mut schema = Vec::new();

        for expr in &group_by {
            if let Expression::Column(name) = expr {
                schema.push(Column {
                    name: name.clone(),
                    type_id: 25,
                });
            }
        }

        for agg in &aggregates {
            let name = agg.alias.as_ref().unwrap_or(&agg.function);
            let function_name = agg.function.to_uppercase();
            schema.push(Column {
                name: name.clone(),
                type_id: match function_name.as_str() {
                    "COUNT" => 23,
                    "SUM" | "AVG" => 701,
                    _ => 25,
                },
            });
        }

        Self {
            input,
            group_by,
            aggregates,
            having,
            schema,
            groups: HashMap::new(),
            result_iterator: None,
            materialized: false,
        }
    }

    fn materialize(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }

        while let Some(row) = self.input.next()? {
            let row_map = self.row_to_map(&row);

            let mut group_key = Vec::new();
            for expr in &self.group_by {
                let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                group_key.push(val.to_string());
            }

            let state = self
                .groups
                .entry(group_key)
                .or_insert_with(|| AggregateState {
                    states: vec![SingleAggregateState::default(); self.aggregates.len()],
                });

            for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                let agg_state = state.states.get_mut(agg_idx).ok_or_else(|| {
                    ExecutionError::GenericError(
                        "Aggregate state mismatch: missing aggregate state slot".to_string(),
                    )
                })?;
                let function_name = agg.function.to_uppercase();

                if function_name == "COUNT" && agg.args.is_empty() {
                    agg_state.count += 1;
                    continue;
                }

                if let Some(arg) = agg.args.first() {
                    let val = evaluate_expr_for_row_to_val(arg, &row_map)?;

                    if matches!(val, LiteralValue::Null) {
                        continue;
                    }

                    match function_name.as_str() {
                        "COUNT" => {
                            agg_state.count += 1;
                        }
                        "SUM" => {
                            if let LiteralValue::Number(n) = &val {
                                agg_state.sum += n.parse::<f64>().map_err(|_| {
                                    ExecutionError::GenericError(format!(
                                        "Invalid numeric literal for SUM: {}",
                                        n
                                    ))
                                })?;
                            }
                        }
                        "AVG" => {
                            if let LiteralValue::Number(n) = &val {
                                agg_state.sum += n.parse::<f64>().map_err(|_| {
                                    ExecutionError::GenericError(format!(
                                        "Invalid numeric literal for AVG: {}",
                                        n
                                    ))
                                })?;
                                agg_state.count += 1;
                            }
                        }
                        "MIN" => {
                            let should_update = match agg_state.min.as_ref() {
                                None => true,
                                Some(current) => val < *current,
                            };
                            if should_update {
                                agg_state.min = Some(val);
                            }
                        }
                        "MAX" => {
                            let should_update = match agg_state.max.as_ref() {
                                None => true,
                                Some(current) => val > *current,
                            };
                            if should_update {
                                agg_state.max = Some(val);
                            }
                        }
                        _ => {
                            agg_state.values.push(val);
                        }
                    }
                }
            }
        }

        let mut result_rows = Vec::new();
        for (group_key, state) in &self.groups {
            let mut result_row = Vec::new();

            result_row.extend_from_slice(group_key);

            for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                let agg_state = state.states.get(agg_idx).ok_or_else(|| {
                    ExecutionError::GenericError(
                        "Aggregate state mismatch: missing aggregate state slot".to_string(),
                    )
                })?;
                let agg_result = self.compute_aggregate_result(agg_state, agg)?;
                result_row.push(agg_result);
            }

            if let Some(having_expr) = &self.having {
                let row_map = self.result_row_to_map(&result_row);
                if !evaluate_expr_for_row(having_expr, &row_map)? {
                    continue;
                }
            }

            result_rows.push(result_row);
        }

        self.result_iterator = Some(result_rows.into_iter());
        self.materialized = true;

        Ok(())
    }

    fn row_to_map(&self, row: &[String]) -> HashMap<String, LiteralValue> {
        let mut map = HashMap::new();
        for (i, col) in self.input.schema().iter().enumerate() {
            let Some(cell) = row.get(i) else {
                continue;
            };
            let value = match col.type_id {
                23 => LiteralValue::Number(cell.clone()),
                16 => LiteralValue::Bool(cell == "t" || cell == "true"),
                1082 => LiteralValue::Date(cell.clone()),
                _ => LiteralValue::String(cell.clone()),
            };
            map.insert(col.name.clone(), value);
        }
        map
    }

    fn result_row_to_map(&self, row: &[String]) -> HashMap<String, LiteralValue> {
        let mut map = HashMap::new();
        for (i, col) in self.schema.iter().enumerate() {
            let Some(cell) = row.get(i) else {
                continue;
            };
            let value = match col.type_id {
                23 | 701 => LiteralValue::Number(cell.clone()),
                16 => LiteralValue::Bool(cell == "t" || cell == "true"),
                1082 => LiteralValue::Date(cell.clone()),
                _ => LiteralValue::String(cell.clone()),
            };
            map.insert(col.name.clone(), value);
        }
        map
    }

    fn compute_aggregate_result(
        &self,
        state: &SingleAggregateState,
        agg: &AggregateExpr,
    ) -> Result<String, ExecutionError> {
        let result = match agg.function.to_uppercase().as_str() {
            "COUNT" => state.count.to_string(),
            "SUM" => {
                if state.sum.fract() == 0.0 {
                    (state.sum as i64).to_string()
                } else {
                    state.sum.to_string()
                }
            }
            "AVG" => {
                if state.count > 0 {
                    let avg = state.sum / state.count as f64;

                    if avg.fract() == 0.0 {
                        (avg as i64).to_string()
                    } else {
                        avg.to_string()
                    }
                } else {
                    "NULL".to_string()
                }
            }
            "MIN" => state
                .min
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            "MAX" => state
                .max
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            _ => "NULL".to_string(),
        };

        Ok(result)
    }
}

impl<'a> Executor for HashAggregateExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        self.materialize()?;

        if let Some(ref mut iter) = self.result_iterator {
            Ok(iter.next())
        } else {
            Ok(None)
        }
    }
}

#[allow(dead_code)]
pub struct StreamAggregateExecutor<'a> {
    _input: Box<dyn Executor + 'a>,
    _group_by: Vec<Expression>,
    _aggregates: Vec<AggregateExpr>,
    _having: Option<Expression>,
    schema: Vec<Column>,
}

impl<'a> StreamAggregateExecutor<'a> {
    pub fn new(
        input: Box<dyn Executor + 'a>,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expression>,
    ) -> Self {
        let mut schema = Vec::new();

        for expr in &group_by {
            if let Expression::Column(name) = expr {
                schema.push(Column {
                    name: name.clone(),
                    type_id: 25,
                });
            }
        }

        for agg in &aggregates {
            let name = agg.alias.as_ref().unwrap_or(&agg.function);
            let function_name = agg.function.to_uppercase();
            schema.push(Column {
                name: name.clone(),
                type_id: match function_name.as_str() {
                    "COUNT" => 23,
                    "SUM" | "AVG" => 701,
                    _ => 25,
                },
            });
        }

        Self {
            _input: input,
            _group_by: group_by,
            _aggregates: aggregates,
            _having: having,
            schema,
        }
    }
}

impl<'a> Executor for StreamAggregateExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        Err(ExecutionError::GenericError(
            "StreamAggregateExecutor not fully implemented yet".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockExecutor {
        schema: Vec<Column>,
        rows: std::vec::IntoIter<Row>,
    }

    impl MockExecutor {
        fn new(schema: Vec<Column>, rows: Vec<Row>) -> Self {
            Self {
                schema,
                rows: rows.into_iter(),
            }
        }
    }

    impl Executor for MockExecutor {
        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }

        fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
            Ok(self.rows.next())
        }
    }

    #[test]
    fn sum_with_invalid_number_returns_error() {
        let input = MockExecutor::new(
            vec![Column {
                name: "value".to_string(),
                type_id: 23,
            }],
            vec![vec!["not-a-number".to_string()]],
        );
        let mut exec = HashAggregateExecutor::new(
            Box::new(input),
            vec![],
            vec![AggregateExpr {
                function: "SUM".to_string(),
                args: vec![Expression::Column("value".to_string())],
                alias: None,
            }],
            None,
        );

        let result = exec.next();
        assert!(matches!(
            result,
            Err(ExecutionError::GenericError(msg)) if msg.contains("Invalid numeric literal for SUM")
        ));
    }

    #[test]
    fn count_over_null_literal_returns_zero() {
        let input = MockExecutor::new(
            vec![Column {
                name: "id".to_string(),
                type_id: 23,
            }],
            vec![
                vec!["1".to_string()],
                vec!["2".to_string()],
                vec!["3".to_string()],
            ],
        );
        let mut exec = HashAggregateExecutor::new(
            Box::new(input),
            vec![],
            vec![AggregateExpr {
                function: "COUNT".to_string(),
                args: vec![Expression::Literal(LiteralValue::Null)],
                alias: Some("null_cnt".to_string()),
            }],
            None,
        );

        let row = exec
            .next()
            .expect("aggregate execution should succeed")
            .expect("aggregate row must exist");
        assert_eq!(row, vec!["0".to_string()]);
    }
}
