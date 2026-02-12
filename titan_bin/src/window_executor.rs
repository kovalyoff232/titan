use crate::errors::ExecutionError;
use crate::executor::Executor;
use crate::parser::{Expression, LiteralValue};
use crate::sql_extensions::{
    CteContext, WindowFunction, WindowSpec,
    window_exec::{WindowExecutor, WindowPartition},
};
use crate::types::Column;
use std::collections::HashMap;
use std::sync::Arc;

type Row = Vec<String>;

pub struct WindowFunctionExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    window_functions: Vec<(WindowFunction, WindowSpec, String)>,
    schema: Vec<Column>,
    executed: bool,
    results: Vec<Row>,
    index: usize,
}

impl<'a> WindowFunctionExecutor<'a> {
    pub fn new(
        input: Box<dyn Executor + 'a>,
        window_functions: Vec<(WindowFunction, WindowSpec, String)>,
    ) -> Self {
        let mut schema = input.schema().clone();
        for (_, _, alias) in &window_functions {
            schema.push(Column {
                name: alias.clone(),
                type_id: 23,
            });
        }

        Self {
            input,
            window_functions,
            schema,
            executed: false,
            results: Vec::new(),
            index: 0,
        }
    }

    fn materialize_input(&mut self) -> Result<Vec<Row>, ExecutionError> {
        let mut rows = Vec::new();
        while let Some(row) = self.input.next()? {
            rows.push(row);
        }
        Ok(rows)
    }

    fn partition_rows(&self, rows: &[Row], partition_by: &[Expression]) -> Vec<WindowPartition> {
        if partition_by.is_empty() {
            return vec![WindowPartition {
                rows: rows.iter().map(|r| self.row_to_literals(r)).collect(),
                partition_key: vec![],
            }];
        }

        let mut partitions: HashMap<Vec<LiteralValue>, Vec<Vec<LiteralValue>>> = HashMap::new();

        for row in rows {
            let partition_key = self.evaluate_partition_key(row, partition_by);
            let row_literals = self.row_to_literals(row);
            partitions
                .entry(partition_key)
                .or_default()
                .push(row_literals);
        }

        partitions
            .into_iter()
            .map(|(key, rows)| WindowPartition {
                rows,
                partition_key: key,
            })
            .collect()
    }

    fn row_to_literals(&self, row: &Row) -> Vec<LiteralValue> {
        row.iter()
            .zip(self.input.schema())
            .map(|(val, col)| match col.type_id {
                16 => LiteralValue::Bool(val == "t"),
                23 => LiteralValue::Number(val.clone()),
                25 => LiteralValue::String(val.clone()),
                1082 => LiteralValue::Date(val.clone()),
                _ => LiteralValue::String(val.clone()),
            })
            .collect()
    }

    fn evaluate_partition_key(&self, row: &Row, partition_by: &[Expression]) -> Vec<LiteralValue> {
        partition_by
            .iter()
            .map(|expr| match expr {
                Expression::Column(name) => {
                    if let Some(idx) = self.input.schema().iter().position(|c| c.name == *name) {
                        if let Some((col, value)) = self.input.schema().get(idx).zip(row.get(idx)) {
                            match col.type_id {
                                16 => LiteralValue::Bool(value == "t"),
                                23 => LiteralValue::Number(value.clone()),
                                25 => LiteralValue::String(value.clone()),
                                1082 => LiteralValue::Date(value.clone()),
                                _ => LiteralValue::String(value.clone()),
                            }
                        } else {
                            LiteralValue::Null
                        }
                    } else {
                        LiteralValue::Null
                    }
                }
                _ => LiteralValue::Null,
            })
            .collect()
    }

    fn sort_partition(&self, partition: &mut WindowPartition, order_by: &[Expression]) {
        if order_by.is_empty() {
            return;
        }

        partition.rows.sort_by(|a, b| match (a.first(), b.first()) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(LiteralValue::Number(n1)), Some(LiteralValue::Number(n2))) => {
                let v1 = n1.parse::<f64>().unwrap_or(0.0);
                let v2 = n2.parse::<f64>().unwrap_or(0.0);
                v1.partial_cmp(&v2).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Some(LiteralValue::String(s1)), Some(LiteralValue::String(s2))) => s1.cmp(s2),
            _ => std::cmp::Ordering::Equal,
        });
    }

    pub fn execute_window_functions(&mut self) -> Result<Vec<Row>, ExecutionError> {
        let input_rows = self.materialize_input()?;
        if input_rows.is_empty() {
            return Ok(vec![]);
        }

        let mut window_results: Vec<Vec<String>> = Vec::new();

        for (func, spec, _alias) in &self.window_functions {
            let mut partitions = self.partition_rows(&input_rows, &spec.partition_by);

            for partition in &mut partitions {
                self.sort_partition(partition, &spec.order_by);
            }

            let executor = WindowExecutor::new(func.clone(), spec.clone());
            let mut all_results = Vec::new();

            for partition in &partitions {
                let partition_results = executor.execute(partition);
                for result in partition_results {
                    all_results.push(result.to_string());
                }
            }

            window_results.push(all_results);
        }

        let mut output_rows = Vec::new();
        for (i, input_row) in input_rows.iter().enumerate() {
            let mut output_row = input_row.clone();
            for window_result in &window_results {
                let value = window_result.get(i).cloned().ok_or_else(|| {
                    ExecutionError::GenericError(
                        "Window function result cardinality mismatch".to_string(),
                    )
                })?;
                output_row.push(value);
            }
            output_rows.push(output_row);
        }

        Ok(output_rows)
    }
}

impl<'a> Executor for WindowFunctionExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        if !self.executed {
            self.results = self.execute_window_functions()?;
            self.executed = true;
            self.index = 0;
        }

        let Some(row) = self.results.get(self.index).cloned() else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(row))
    }

    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}

pub struct CteExecutor<'a> {
    _cte_context: Arc<CteContext>,
    main_query: Box<dyn Executor + 'a>,
    schema: Vec<Column>,
}

impl<'a> CteExecutor<'a> {
    pub fn new(cte_context: Arc<CteContext>, main_query: Box<dyn Executor + 'a>) -> Self {
        let schema = main_query.schema().clone();
        Self {
            _cte_context: cte_context,
            main_query,
            schema,
        }
    }
}

impl<'a> Executor for CteExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        self.main_query.next()
    }

    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}

pub struct CteScanExecutor {
    cte_name: String,
    cte_context: Arc<CteContext>,
    schema: Vec<Column>,
    current_index: usize,
}

impl CteScanExecutor {
    pub fn new(cte_name: String, cte_context: Arc<CteContext>) -> Result<Self, ExecutionError> {
        let schema = cte_context
            .get_cte(&cte_name)
            .ok_or_else(|| ExecutionError::PlanningError(format!("CTE '{}' not found", cte_name)))?
            .schema
            .clone();

        Ok(Self {
            cte_name,
            cte_context,
            schema,
            current_index: 0,
        })
    }
}

impl Executor for CteScanExecutor {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        let cte = self.cte_context.get_cte(&self.cte_name).ok_or_else(|| {
            ExecutionError::PlanningError(format!("CTE '{}' not found", self.cte_name))
        })?;

        let Some(row_values) = cte.rows.get(self.current_index) else {
            return Ok(None);
        };
        let row = row_values.iter().map(|v| v.to_string()).collect();
        self.current_index += 1;
        Ok(Some(row))
    }

    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}

pub struct SetOperationExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    right: Box<dyn Executor + 'a>,
    operation: SetOperationType,
    all: bool,
    schema: Vec<Column>,
    materialized: Option<Vec<Row>>,
    current_index: usize,
}

#[derive(Debug, Clone)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

impl<'a> SetOperationExecutor<'a> {
    pub fn new(
        left: Box<dyn Executor + 'a>,
        right: Box<dyn Executor + 'a>,
        operation: SetOperationType,
        all: bool,
    ) -> Result<Self, ExecutionError> {
        if left.schema().len() != right.schema().len() {
            return Err(ExecutionError::PlanningError(
                "Set operation requires same number of columns".to_string(),
            ));
        }

        let schema = left.schema().clone();

        Ok(Self {
            left,
            right,
            operation,
            all,
            schema,
            materialized: None,
            current_index: 0,
        })
    }

    fn materialize(&mut self) -> Result<(), ExecutionError> {
        if self.materialized.is_some() {
            return Ok(());
        }

        let mut left_rows = Vec::new();
        while let Some(row) = self.left.next()? {
            left_rows.push(row);
        }

        let mut right_rows = Vec::new();
        while let Some(row) = self.right.next()? {
            right_rows.push(row);
        }

        let result = match self.operation {
            SetOperationType::Union => self.compute_union(left_rows, right_rows),
            SetOperationType::Intersect => self.compute_intersect(left_rows, right_rows),
            SetOperationType::Except => self.compute_except(left_rows, right_rows),
        };

        self.materialized = Some(result);
        self.current_index = 0;
        Ok(())
    }

    fn compute_union(&self, mut left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
        if self.all {
            left.extend(right);
            left
        } else {
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            let mut result = Vec::new();

            for row in left.into_iter().chain(right) {
                let key = row.join("|");
                if seen.insert(key) {
                    result.push(row);
                }
            }
            result
        }
    }

    fn compute_intersect(&self, left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
        use std::collections::HashMap;

        let mut right_counts = HashMap::new();
        for row in right {
            let key = row.join("|");
            *right_counts.entry(key).or_insert(0) += 1;
        }

        let mut result = Vec::new();
        let mut seen = HashMap::new();

        for row in left {
            let key = row.join("|");
            let left_count = seen.entry(key.clone()).or_insert(0);
            *left_count += 1;

            if let Some(&right_count) = right_counts.get(&key) {
                if (self.all && *left_count <= right_count) || (!self.all && *left_count == 1) {
                    result.push(row);
                }
            }
        }

        result
    }

    fn compute_except(&self, left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
        use std::collections::HashMap;

        let mut right_counts = HashMap::new();
        for row in right {
            let key = row.join("|");
            *right_counts.entry(key).or_insert(0) += 1;
        }

        let mut result = Vec::new();
        let mut seen = HashMap::new();

        for row in left {
            let key = row.join("|");
            let left_count = seen.entry(key.clone()).or_insert(0);
            *left_count += 1;

            let right_count = right_counts.get(&key).copied().unwrap_or(0);

            if self.all {
                if *left_count > right_count {
                    result.push(row);
                }
            } else if right_count == 0 && *left_count == 1 {
                result.push(row);
            }
        }

        result
    }
}

impl<'a> Executor for SetOperationExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        self.materialize()?;

        if let Some(rows) = &self.materialized {
            let Some(row) = rows.get(self.current_index).cloned() else {
                return Ok(None);
            };
            self.current_index += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}
