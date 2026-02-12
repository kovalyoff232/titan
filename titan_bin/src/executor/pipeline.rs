use super::eval::{evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use super::helpers::row_vec_to_map;
use super::{Executor, Row};
use crate::errors::ExecutionError;
use crate::parser::{Expression, SelectItem};
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
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
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
        order_by: Vec<Expression>,
    ) -> Result<Self, ExecutionError> {
        let mut rows = Vec::new();
        while let Some(row) = input.next()? {
            rows.push(row);
        }

        if order_by.len() != 1 {
            return Err(ExecutionError::GenericError(
                "ORDER BY only supports a single column".to_string(),
            ));
        }
        let sort_expr = order_by.first().ok_or_else(|| {
            ExecutionError::GenericError("missing ORDER BY expression".to_string())
        })?;

        let (sort_col_idx, sort_col_type) = if let Expression::Column(name) = sort_expr {
            input
                .schema()
                .iter()
                .position(|c| c.name == *name || c.name.ends_with(&format!(".{}", name)))
                .and_then(|i| input.schema().get(i).map(|col| (i, col.type_id)))
                .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone()))?
        } else {
            return Err(ExecutionError::GenericError(
                "ORDER BY only supports column names".to_string(),
            ));
        };

        if rows.iter().any(|row| row.get(sort_col_idx).is_none()) {
            return Err(ExecutionError::GenericError(
                "ORDER BY column index out of bounds for one or more rows".to_string(),
            ));
        }

        rows.sort_by(|a, b| {
            let Some(val_a) = a.get(sort_col_idx) else {
                return std::cmp::Ordering::Equal;
            };
            let Some(val_b) = b.get(sort_col_idx) else {
                return std::cmp::Ordering::Equal;
            };
            match sort_col_type {
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
            }
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
    use crate::parser::Expression;
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
            vec![Expression::Column("event_date".to_string())],
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
}
