//! Window function and CTE executor implementation

use crate::errors::ExecutionError;
use crate::executor::Executor;
use crate::parser::{Expression, LiteralValue};
use crate::sql_extensions::{
    WindowFunction, WindowSpec, WindowFrame, FrameBound,
    AggregateFunction, CteContext, CteTable,
    window_exec::{WindowExecutor, WindowPartition},
};
use crate::types::Column;
use std::collections::HashMap;
use std::sync::Arc;

/// Row type
type Row = Vec<String>;

/// Window function executor that processes window functions over result sets
pub struct WindowFunctionExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    window_functions: Vec<(WindowFunction, WindowSpec, String)>, // (function, spec, alias)
    schema: Vec<Column>,
}

impl<'a> WindowFunctionExecutor<'a> {
    pub fn new(
        input: Box<dyn Executor + 'a>,
        window_functions: Vec<(WindowFunction, WindowSpec, String)>,
    ) -> Self {
        // Build schema with window function columns
        let mut schema = input.schema().clone();
        for (_, _, alias) in &window_functions {
            schema.push(Column {
                name: alias.clone(),
                type_id: 23, // INT for most window functions, simplified
            });
        }
        
        Self {
            input,
            window_functions,
            schema,
        }
    }
    
    /// Materialize all input rows for window function processing
    fn materialize_input(&mut self) -> Result<Vec<Row>, ExecutionError> {
        let mut rows = Vec::new();
        while let Some(row) = self.input.next()? {
            rows.push(row);
        }
        Ok(rows)
    }
    
    /// Partition rows based on PARTITION BY clause
    fn partition_rows(
        &self,
        rows: &[Row],
        partition_by: &[Expression],
    ) -> Vec<WindowPartition> {
        if partition_by.is_empty() {
            // Single partition with all rows
            return vec![WindowPartition {
                rows: rows.iter().map(|r| self.row_to_literals(r)).collect(),
                partition_key: vec![],
            }];
        }
        
        // Group rows by partition key
        let mut partitions: HashMap<Vec<LiteralValue>, Vec<Vec<LiteralValue>>> = HashMap::new();
        
        for row in rows {
            let partition_key = self.evaluate_partition_key(row, partition_by);
            let row_literals = self.row_to_literals(row);
            partitions.entry(partition_key).or_default().push(row_literals);
        }
        
        partitions.into_iter().map(|(key, rows)| {
            WindowPartition {
                rows,
                partition_key: key,
            }
        }).collect()
    }
    
    /// Convert row strings to literal values
    fn row_to_literals(&self, row: &Row) -> Vec<LiteralValue> {
        row.iter().zip(self.input.schema()).map(|(val, col)| {
            match col.type_id {
                16 => LiteralValue::Bool(val == "t"),
                23 => LiteralValue::Number(val.clone()),
                25 => LiteralValue::String(val.clone()),
                1082 => LiteralValue::Date(val.clone()),
                _ => LiteralValue::String(val.clone()),
            }
        }).collect()
    }
    
    /// Evaluate partition key for a row
    fn evaluate_partition_key(&self, row: &Row, partition_by: &[Expression]) -> Vec<LiteralValue> {
        // Simplified: assume partition_by contains column references
        partition_by.iter().map(|expr| {
            match expr {
                Expression::Column(name) => {
                    if let Some(idx) = self.input.schema().iter().position(|c| c.name == *name) {
                        match self.input.schema()[idx].type_id {
                            16 => LiteralValue::Bool(row[idx] == "t"),
                            23 => LiteralValue::Number(row[idx].clone()),
                            25 => LiteralValue::String(row[idx].clone()),
                            1082 => LiteralValue::Date(row[idx].clone()),
                            _ => LiteralValue::String(row[idx].clone()),
                        }
                    } else {
                        LiteralValue::Null
                    }
                }
                _ => LiteralValue::Null,
            }
        }).collect()
    }
    
    /// Sort partition based on ORDER BY clause
    fn sort_partition(&self, partition: &mut WindowPartition, order_by: &[Expression]) {
        if order_by.is_empty() {
            return;
        }
        
        partition.rows.sort_by(|a, b| {
            // Simplified: compare first value
            match (&a[0], &b[0]) {
                (LiteralValue::Number(n1), LiteralValue::Number(n2)) => {
                    let v1 = n1.parse::<f64>().unwrap_or(0.0);
                    let v2 = n2.parse::<f64>().unwrap_or(0.0);
                    v1.partial_cmp(&v2).unwrap_or(std::cmp::Ordering::Equal)
                }
                (LiteralValue::String(s1), LiteralValue::String(s2)) => s1.cmp(s2),
                _ => std::cmp::Ordering::Equal,
            }
        });
    }
    
    /// Execute window functions and produce result rows
    pub fn execute_window_functions(&mut self) -> Result<Vec<Row>, ExecutionError> {
        // Materialize all input rows
        let input_rows = self.materialize_input()?;
        if input_rows.is_empty() {
            return Ok(vec![]);
        }
        
        // Process each window function
        let mut window_results: Vec<Vec<String>> = Vec::new();
        
        for (func, spec, _alias) in &self.window_functions {
            // Partition rows
            let mut partitions = self.partition_rows(&input_rows, &spec.partition_by);
            
            // Sort each partition
            for partition in &mut partitions {
                self.sort_partition(partition, &spec.order_by);
            }
            
            // Execute window function on each partition
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
        
        // Combine input rows with window function results
        let mut output_rows = Vec::new();
        for (i, input_row) in input_rows.iter().enumerate() {
            let mut output_row = input_row.clone();
            for window_result in &window_results {
                output_row.push(window_result[i].clone());
            }
            output_rows.push(output_row);
        }
        
        Ok(output_rows)
    }
}

impl<'a> Executor for WindowFunctionExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        // Window functions require materializing all rows first
        // This is a simplified implementation
        static mut EXECUTED: bool = false;
        static mut RESULTS: Vec<Row> = Vec::new();
        static mut INDEX: usize = 0;
        
        unsafe {
            if !EXECUTED {
                RESULTS = self.execute_window_functions()?;
                EXECUTED = true;
                INDEX = 0;
            }
            
            if INDEX < RESULTS.len() {
                let row = RESULTS[INDEX].clone();
                INDEX += 1;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        }
    }
    
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}

/// CTE executor that handles Common Table Expressions
pub struct CteExecutor<'a> {
    cte_context: Arc<CteContext>,
    main_query: Box<dyn Executor + 'a>,
    schema: Vec<Column>,
}

impl<'a> CteExecutor<'a> {
    pub fn new(
        cte_context: Arc<CteContext>,
        main_query: Box<dyn Executor + 'a>,
    ) -> Self {
        let schema = main_query.schema().clone();
        Self {
            cte_context,
            main_query,
            schema,
        }
    }
}

impl<'a> Executor for CteExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        // CTEs are already materialized in context
        self.main_query.next()
    }
    
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}

/// CTE scan executor that reads from a materialized CTE
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
            .schema.clone();
        
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
        let cte = self.cte_context
            .get_cte(&self.cte_name)
            .ok_or_else(|| ExecutionError::PlanningError(format!("CTE '{}' not found", self.cte_name)))?;
        
        if self.current_index < cte.rows.len() {
            let row = cte.rows[self.current_index].iter()
                .map(|v| v.to_string())
                .collect();
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

/// Set operation executor (UNION, INTERSECT, EXCEPT)
pub struct SetOperationExecutor<'a> {
    left: Box<dyn Executor + 'a>,
    right: Box<dyn Executor + 'a>,
    operation: SetOperationType,
    all: bool, // Keep duplicates if true
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
        // Verify schemas match
        if left.schema().len() != right.schema().len() {
            return Err(ExecutionError::PlanningError(
                "Set operation requires same number of columns".to_string()
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
        
        // Collect all rows from left
        let mut left_rows = Vec::new();
        while let Some(row) = self.left.next()? {
            left_rows.push(row);
        }
        
        // Collect all rows from right
        let mut right_rows = Vec::new();
        while let Some(row) = self.right.next()? {
            right_rows.push(row);
        }
        
        let result = match self.operation {
            SetOperationType::Union => {
                self.compute_union(left_rows, right_rows)
            }
            SetOperationType::Intersect => {
                self.compute_intersect(left_rows, right_rows)
            }
            SetOperationType::Except => {
                self.compute_except(left_rows, right_rows)
            }
        };
        
        self.materialized = Some(result);
        self.current_index = 0;
        Ok(())
    }
    
    fn compute_union(&self, mut left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
        if self.all {
            // UNION ALL - just concatenate
            left.extend(right);
            left
        } else {
            // UNION - remove duplicates
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
        
        // Count occurrences in right
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
                if self.all || *left_count == 1 {
                    // For INTERSECT ALL, include min(left_count, right_count) copies
                    // For INTERSECT, include only once
                    if self.all && *left_count <= right_count {
                        result.push(row);
                    } else if !self.all && *left_count == 1 {
                        result.push(row);
                    }
                }
            }
        }
        
        result
    }
    
    fn compute_except(&self, left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
        use std::collections::HashMap;
        
        // Count occurrences in right
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
                // EXCEPT ALL: include (left_count - right_count) copies if positive
                if *left_count > right_count {
                    result.push(row);
                }
            } else {
                // EXCEPT: include once if not in right
                if right_count == 0 && *left_count == 1 {
                    result.push(row);
                }
            }
        }
        
        result
    }
}

impl<'a> Executor for SetOperationExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        self.materialize()?;
        
        if let Some(rows) = &self.materialized {
            if self.current_index < rows.len() {
                let row = rows[self.current_index].clone();
                self.current_index += 1;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
}
