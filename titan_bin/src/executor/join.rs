use super::eval::{evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use super::helpers::row_vec_to_map;
use super::{Executor, Row};
use crate::errors::ExecutionError;
use crate::parser::{Expression, LiteralValue};
use crate::types::Column;
use std::collections::HashMap;

pub(super) struct NestedLoopJoinExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    right: Box<dyn Executor + 'a>,
    condition: Expression,
    joined_schema: Vec<Column>,
    left_row: Option<Row>,
    right_rows: Vec<Row>,
    right_cursor: usize,
    left_table_name: Option<String>,
    right_table_name: Option<String>,
}

impl<'a> NestedLoopJoinExecutor<'a> {
    pub(super) fn new(
        mut left: Box<dyn Executor + 'a>,
        mut right: Box<dyn Executor + 'a>,
        condition: Expression,
        left_table_name: Option<String>,
        right_table_name: Option<String>,
    ) -> Result<Self, ExecutionError> {
        let mut joined_schema = Vec::new();
        let left_schema = left.schema();
        let right_schema = right.schema();
        if let Some(table_name) = &left_table_name {
            for col in left_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(left_schema);
        }
        if let Some(table_name) = &right_table_name {
            for col in right_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(right_schema);
        }

        let mut right_rows = Vec::new();
        while let Some(row) = right.next()? {
            right_rows.push(row);
        }

        let left_row = left.next()?;

        Ok(Self {
            left,
            right,
            condition,
            joined_schema,
            left_row,
            right_rows,
            right_cursor: 0,
            left_table_name,
            right_table_name,
        })
    }
}

impl<'a> Executor for NestedLoopJoinExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.joined_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            if self.left_row.is_none() {
                return Ok(None);
            }

            while self.right_cursor < self.right_rows.len() {
                let Some(right_row) = self.right_rows.get(self.right_cursor) else {
                    return Err(ExecutionError::GenericError(
                        "NestedLoopJoin invariant violation: right row cursor out of bounds"
                            .to_string(),
                    ));
                };
                self.right_cursor += 1;

                let Some(left_row) = self.left_row.as_ref() else {
                    return Err(ExecutionError::GenericError(
                        "NestedLoopJoin invariant violation: missing current left row".to_string(),
                    ));
                };
                let left_map = row_vec_to_map(
                    left_row,
                    self.left.schema(),
                    self.left_table_name.as_deref(),
                );
                let right_map = row_vec_to_map(
                    right_row,
                    self.right.schema(),
                    self.right_table_name.as_deref(),
                );
                let combined_map: HashMap<String, LiteralValue> =
                    left_map.into_iter().chain(right_map).collect();

                if evaluate_expr_for_row(&self.condition, &combined_map)? {
                    let mut joined_row = left_row.clone();
                    joined_row.extend(right_row.clone());
                    return Ok(Some(joined_row));
                }
            }

            self.left_row = self.left.next()?;
            self.right_cursor = 0;
        }
    }
}

pub(super) struct HashJoinExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    left_key: Expression,
    joined_schema: Vec<Column>,
    hash_table: HashMap<LiteralValue, Vec<Row>>,
    current_left_row: Option<Row>,
    current_matches: Vec<Row>,
    left_table_name: Option<String>,
}

impl<'a> HashJoinExecutor<'a> {
    pub(super) fn new(
        left: Box<dyn Executor + 'a>,
        mut right: Box<dyn Executor + 'a>,
        left_key: Expression,
        right_key: Expression,
        left_table_name: Option<String>,
        right_table_name: Option<String>,
    ) -> Result<Self, ExecutionError> {
        let mut joined_schema = Vec::new();
        let left_schema = left.schema();
        let right_schema = right.schema();
        if let Some(table_name) = &left_table_name {
            for col in left_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(left_schema);
        }
        if let Some(table_name) = &right_table_name {
            for col in right_schema {
                joined_schema.push(Column {
                    name: format!("{}.{}", table_name, col.name),
                    type_id: col.type_id,
                });
            }
        } else {
            joined_schema.extend_from_slice(right_schema);
        }

        let mut hash_table: HashMap<LiteralValue, Vec<Row>> = HashMap::new();
        while let Some(row) = right.next()? {
            let row_map = row_vec_to_map(&row, right.schema(), right_table_name.as_deref());
            let key = evaluate_expr_for_row_to_val(&right_key, &row_map)?;
            hash_table.entry(key).or_default().push(row);
        }

        Ok(Self {
            left,
            left_key,
            joined_schema,
            hash_table,
            current_left_row: None,
            current_matches: Vec::new(),
            left_table_name,
        })
    }
}

impl<'a> Executor for HashJoinExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.joined_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            if let Some(match_row) = self.current_matches.pop() {
                let Some(current_left_row) = self.current_left_row.as_ref() else {
                    return Err(ExecutionError::GenericError(
                        "HashJoin invariant violation: missing current left row".to_string(),
                    ));
                };
                let mut joined_row = current_left_row.clone();
                joined_row.extend(match_row);
                return Ok(Some(joined_row));
            }

            let left_row = self.left.next()?;
            if left_row.is_none() {
                return Ok(None);
            }
            self.current_left_row = left_row;
            let Some(left_row_ref) = self.current_left_row.as_ref() else {
                return Err(ExecutionError::GenericError(
                    "HashJoin invariant violation: left row unexpectedly absent".to_string(),
                ));
            };

            let left_map = row_vec_to_map(
                left_row_ref,
                self.left.schema(),
                self.left_table_name.as_deref(),
            );
            let key = evaluate_expr_for_row_to_val(&self.left_key, &left_map)?;

            if let Some(matching_rows) = self.hash_table.get(&key) {
                self.current_matches = matching_rows.clone();
                self.current_matches.reverse();
            } else {
                self.current_matches.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NestedLoopJoinExecutor;
    use crate::errors::ExecutionError;
    use crate::executor::{Executor, Row};
    use crate::parser::{Expression, LiteralValue};
    use crate::types::Column;

    struct EmptyExecutor {
        schema: Vec<Column>,
    }

    impl EmptyExecutor {
        fn new() -> Self {
            Self { schema: Vec::new() }
        }
    }

    impl Executor for EmptyExecutor {
        fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
            Ok(None)
        }

        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }
    }

    struct FailingExecutor {
        schema: Vec<Column>,
    }

    impl FailingExecutor {
        fn new() -> Self {
            Self { schema: Vec::new() }
        }
    }

    impl Executor for FailingExecutor {
        fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
            Err(ExecutionError::GenericError(
                "injected right-side failure".to_string(),
            ))
        }

        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }
    }

    #[test]
    fn nested_loop_join_constructor_propagates_right_input_errors() {
        let left = Box::new(EmptyExecutor::new());
        let right = Box::new(FailingExecutor::new());
        let condition = Expression::Literal(LiteralValue::Bool(true));

        let result = NestedLoopJoinExecutor::new(left, right, condition, None, None);
        assert!(matches!(result, Err(ExecutionError::GenericError(_))));
    }
}
