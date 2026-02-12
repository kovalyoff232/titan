use super::eval::{evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use super::helpers::row_vec_to_map;
use super::{Executor, Row};
use crate::errors::ExecutionError;
use crate::parser::{Expression, LiteralValue, OrderByExpr, SelectItem};
use crate::types::Column;
use chrono::NaiveDate;

pub(super) struct FilterExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    predicate: Expression,
}

impl<'a> FilterExecutor<'a> {
    pub(super) fn new(input: Box<dyn Executor + 'a>, predicate: Expression) -> Self {
        Self { input, predicate }
    }
}

impl<'a> Executor for FilterExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        loop {
            match self.input.next()? {
                Some(row) => {
                    let row_map = row_vec_to_map(&row, self.schema(), None);
                    if evaluate_expr_for_row(&self.predicate, &row_map)? {
                        return Ok(Some(row));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}

pub(super) struct ProjectionExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    expressions: Vec<SelectItem>,
    projected_schema: Vec<Column>,
}

impl<'a> ProjectionExecutor<'a> {
    pub(super) fn new(input: Box<dyn Executor + 'a>, expressions: Vec<SelectItem>) -> Self {
        let mut projected_schema = Vec::new();
        if expressions
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            projected_schema = input.schema().clone();
        } else {
            for (i, item) in expressions.iter().enumerate() {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => alias.clone(),
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expression::Column(name) => name.clone(),
                        Expression::QualifiedColumn(_, col) => col.clone(),
                        _ => format!("?column?_{}", i),
                    },
                    SelectItem::Wildcard => "*".to_string(),
                    SelectItem::QualifiedWildcard(table_name) => format!("{}.*", table_name),
                };
                projected_schema.push(Column { name, type_id: 25 });
            }
        }
        Self {
            input,
            expressions,
            projected_schema,
        }
    }
}

impl<'a> Executor for ProjectionExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.projected_schema
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        let Some(row) = self.input.next()? else {
            return Ok(None);
        };

        if self
            .expressions
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard))
        {
            return Ok(Some(row));
        }

        let row_map = row_vec_to_map(&row, self.input.schema(), None);
        let mut projected_row = Vec::new();
        for item in &self.expressions {
            if let SelectItem::ExprWithAlias { expr, alias } = item {
                if let Some(value) = row_map.get(alias) {
                    projected_row.push(value.to_string());
                    continue;
                }
                if let Expression::Function { name, .. } = expr {
                    if let Some(value) = row_map
                        .get(name)
                        .or_else(|| row_map.get(&name.to_lowercase()))
                        .or_else(|| row_map.get(&name.to_uppercase()))
                    {
                        projected_row.push(value.to_string());
                        continue;
                    }
                }
                let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                projected_row.push(val.to_string());
            } else if let SelectItem::UnnamedExpr(expr) = item {
                if let Expression::Function { name, .. } = expr {
                    if let Some(value) = row_map
                        .get(name)
                        .or_else(|| row_map.get(&name.to_lowercase()))
                        .or_else(|| row_map.get(&name.to_uppercase()))
                    {
                        projected_row.push(value.to_string());
                        continue;
                    }
                }
                let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                projected_row.push(val.to_string());
            }
        }
        Ok(Some(projected_row))
    }
}

pub(super) struct SortExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    sorted_rows: Vec<Row>,
    cursor: usize,
}

impl<'a> SortExecutor<'a> {
    pub(super) fn new(
        mut input: Box<dyn Executor + 'a>,
        order_by: Vec<OrderByExpr>,
    ) -> Result<Self, ExecutionError> {
        let mut rows = Vec::new();
        while let Some(row) = input.next()? {
            rows.push(row);
        }

        let sort_exprs = order_by.as_slice();
        if sort_exprs.is_empty() {
            return Err(ExecutionError::GenericError(
                "missing ORDER BY expression".to_string(),
            ));
        }

        let sort_keys: Vec<(usize, u32, bool)> = sort_exprs
            .iter()
            .map(|sort_expr| match &sort_expr.expr {
                Expression::Column(name) => input
                    .schema()
                    .iter()
                    .position(|c| c.name == *name || c.name.ends_with(&format!(".{}", name)))
                    .and_then(|i| {
                        input
                            .schema()
                            .get(i)
                            .map(|col| (i, col.type_id, sort_expr.asc))
                    })
                    .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone())),
                Expression::QualifiedColumn(table, column) => {
                    let qualified_name = format!("{table}.{column}");
                    input
                        .schema()
                        .iter()
                        .position(|c| {
                            c.name == qualified_name
                                || c.name == *column
                                || c.name.ends_with(&format!(".{}", column))
                        })
                        .and_then(|i| {
                            input
                                .schema()
                                .get(i)
                                .map(|col| (i, col.type_id, sort_expr.asc))
                        })
                        .ok_or_else(|| ExecutionError::ColumnNotFound(column.clone()))
                }
                Expression::Literal(LiteralValue::Number(position_literal)) => {
                    let position = position_literal
                        .parse::<usize>()
                        .ok()
                        .and_then(|v| v.checked_sub(1))
                        .ok_or_else(|| {
                            ExecutionError::GenericError(
                                "ORDER BY position must be a positive integer".to_string(),
                            )
                        })?;
                    input
                        .schema()
                        .get(position)
                        .map(|col| (position, col.type_id, sort_expr.asc))
                        .ok_or_else(|| {
                            ExecutionError::GenericError(format!(
                                "ORDER BY position {} is out of range",
                                position_literal
                            ))
                        })
                }
                _ => Err(ExecutionError::GenericError(
                    "ORDER BY only supports column names or positions".to_string(),
                )),
            })
            .collect::<Result<_, _>>()?;

        if rows
            .iter()
            .any(|row| sort_keys.iter().any(|(idx, _, _)| row.get(*idx).is_none()))
        {
            return Err(ExecutionError::GenericError(
                "ORDER BY column index out of bounds for one or more rows".to_string(),
            ));
        }

        rows.sort_by(|a, b| {
            for (sort_col_idx, sort_col_type, asc) in &sort_keys {
                let Some(val_a) = a.get(*sort_col_idx) else {
                    continue;
                };
                let Some(val_b) = b.get(*sort_col_idx) else {
                    continue;
                };
                let cmp = match sort_col_type {
                    23 => {
                        let num_a = val_a.parse::<i32>().unwrap_or(0);
                        let num_b = val_b.parse::<i32>().unwrap_or(0);
                        num_a.cmp(&num_b)
                    }
                    25 => val_a.cmp(val_b),
                    1082 => {
                        let parsed_a = NaiveDate::parse_from_str(val_a, "%Y-%m-%d");
                        let parsed_b = NaiveDate::parse_from_str(val_b, "%Y-%m-%d");
                        match (parsed_a, parsed_b) {
                            (Ok(date_a), Ok(date_b)) => date_a.cmp(&date_b),
                            _ => val_a.cmp(val_b),
                        }
                    }
                    16 => {
                        let bool_a = val_a == "t";
                        let bool_b = val_b == "t";
                        bool_a.cmp(&bool_b)
                    }
                    _ => std::cmp::Ordering::Equal,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return if *asc { cmp } else { cmp.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(Self {
            input,
            sorted_rows: rows,
            cursor: 0,
        })
    }
}

impl<'a> Executor for SortExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        if self.cursor >= self.sorted_rows.len() {
            return Ok(None);
        }
        let Some(row) = self.sorted_rows.get(self.cursor).cloned() else {
            return Ok(None);
        };
        self.cursor += 1;
        Ok(Some(row))
    }
}

#[cfg(test)]
mod tests {
    use super::{Executor, ProjectionExecutor, Row, SortExecutor};
    use crate::parser::{Expression, LiteralValue, OrderByExpr};
    use crate::types::Column;

    struct StaticRowsExecutor {
        schema: Vec<Column>,
        rows: Vec<Row>,
        cursor: usize,
    }

    impl StaticRowsExecutor {
        fn new(schema: Vec<Column>, rows: Vec<Row>) -> Self {
            Self {
                schema,
                rows,
                cursor: 0,
            }
        }
    }

    impl Executor for StaticRowsExecutor {
        fn next(&mut self) -> Result<Option<Row>, crate::errors::ExecutionError> {
            if self.cursor >= self.rows.len() {
                return Ok(None);
            }
            let Some(row) = self.rows.get(self.cursor).cloned() else {
                return Ok(None);
            };
            self.cursor += 1;
            Ok(Some(row))
        }

        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }
    }

    #[test]
    fn sort_executor_handles_invalid_date_values_without_panicking() {
        let input = StaticRowsExecutor::new(
            vec![Column {
                name: "event_date".to_string(),
                type_id: 1082,
            }],
            vec![
                vec!["2024-01-02".to_string()],
                vec!["not-a-date".to_string()],
                vec!["2023-12-30".to_string()],
            ],
        );

        let mut sort_exec = SortExecutor::new(
            Box::new(input),
            vec![OrderByExpr {
                expr: Expression::Column("event_date".to_string()),
                asc: true,
                nulls_first: None,
            }],
        )
        .expect("sort executor creation should succeed");

        let mut sorted = Vec::new();
        while let Some(row) = sort_exec.next().expect("sorted fetch should succeed") {
            let first = row
                .first()
                .expect("expected sort result row to have first column");
            sorted.push(first.clone());
        }

        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0], "2023-12-30");
        assert!(sorted.contains(&"not-a-date".to_string()));
    }

    #[test]
    fn projection_executor_resolves_qualified_columns_without_hardcoded_table_names() {
        let input = StaticRowsExecutor::new(
            vec![
                Column {
                    name: "alpha.id".to_string(),
                    type_id: 23,
                },
                Column {
                    name: "beta.id".to_string(),
                    type_id: 23,
                },
            ],
            vec![vec!["10".to_string(), "77".to_string()]],
        );

        let mut projection = ProjectionExecutor::new(
            Box::new(input),
            vec![crate::parser::SelectItem::UnnamedExpr(
                Expression::QualifiedColumn("beta".to_string(), "id".to_string()),
            )],
        );

        let row = projection
            .next()
            .expect("projection should succeed")
            .expect("row should be present");
        assert_eq!(row, vec!["77".to_string()]);
    }

    #[test]
    fn sort_executor_supports_multiple_order_columns() {
        let input = StaticRowsExecutor::new(
            vec![
                Column {
                    name: "group_id".to_string(),
                    type_id: 23,
                },
                Column {
                    name: "name".to_string(),
                    type_id: 25,
                },
            ],
            vec![
                vec!["2".to_string(), "b".to_string()],
                vec!["1".to_string(), "z".to_string()],
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "a".to_string()],
            ],
        );

        let mut sort_exec = SortExecutor::new(
            Box::new(input),
            vec![
                OrderByExpr {
                    expr: Expression::Column("group_id".to_string()),
                    asc: true,
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Expression::Column("name".to_string()),
                    asc: true,
                    nulls_first: None,
                },
            ],
        )
        .expect("sort executor creation should succeed");

        let mut sorted = Vec::new();
        while let Some(row) = sort_exec.next().expect("sorted fetch should succeed") {
            sorted.push(row);
        }

        assert_eq!(
            sorted,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["1".to_string(), "z".to_string()],
                vec!["2".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
            ]
        );
    }

    #[test]
    fn sort_executor_supports_desc_order_by_column() {
        let input = StaticRowsExecutor::new(
            vec![Column {
                name: "id".to_string(),
                type_id: 23,
            }],
            vec![
                vec!["3".to_string()],
                vec!["1".to_string()],
                vec!["2".to_string()],
            ],
        );

        let mut sort_exec = SortExecutor::new(
            Box::new(input),
            vec![OrderByExpr {
                expr: Expression::Column("id".to_string()),
                asc: false,
                nulls_first: None,
            }],
        )
        .expect("sort executor creation should succeed");

        let mut sorted = Vec::new();
        while let Some(row) = sort_exec.next().expect("sorted fetch should succeed") {
            sorted.push(row);
        }

        assert_eq!(
            sorted,
            vec![
                vec!["3".to_string()],
                vec!["2".to_string()],
                vec!["1".to_string()],
            ]
        );
    }

    #[test]
    fn sort_executor_supports_order_by_position() {
        let input = StaticRowsExecutor::new(
            vec![
                Column {
                    name: "id".to_string(),
                    type_id: 23,
                },
                Column {
                    name: "name".to_string(),
                    type_id: 25,
                },
            ],
            vec![
                vec!["3".to_string(), "c".to_string()],
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
            ],
        );

        let mut sort_exec = SortExecutor::new(
            Box::new(input),
            vec![OrderByExpr {
                expr: Expression::Literal(LiteralValue::Number("1".to_string())),
                asc: true,
                nulls_first: None,
            }],
        )
        .expect("sort executor creation should succeed");

        let mut sorted = Vec::new();
        while let Some(row) = sort_exec.next().expect("sorted fetch should succeed") {
            sorted.push(row);
        }

        assert_eq!(
            sorted,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
                vec!["3".to_string(), "c".to_string()],
            ]
        );
    }

    #[test]
    fn sort_executor_accepts_qualified_order_by_column() {
        let input = StaticRowsExecutor::new(
            vec![
                Column {
                    name: "users.id".to_string(),
                    type_id: 23,
                },
                Column {
                    name: "users.name".to_string(),
                    type_id: 25,
                },
            ],
            vec![
                vec!["3".to_string(), "c".to_string()],
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
            ],
        );

        let mut sort_exec = SortExecutor::new(
            Box::new(input),
            vec![OrderByExpr {
                expr: Expression::QualifiedColumn("users".to_string(), "id".to_string()),
                asc: true,
                nulls_first: None,
            }],
        )
        .expect("sort executor creation should succeed");

        let mut sorted = Vec::new();
        while let Some(row) = sort_exec.next().expect("sorted fetch should succeed") {
            sorted.push(row);
        }

        assert_eq!(
            sorted,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
                vec!["3".to_string(), "c".to_string()],
            ]
        );
    }
}
