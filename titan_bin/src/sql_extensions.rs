//! Extended SQL support including Window Functions, CTEs, and advanced SQL features.

use crate::parser::{Expression, LiteralValue};
use crate::types::Column;
use crate::parser::DataType;
use std::collections::HashMap;

/// Window function specification
#[derive(Debug, Clone)]
pub struct WindowSpec {
    /// PARTITION BY expressions
    pub partition_by: Vec<Expression>,
    /// ORDER BY expressions  
    pub order_by: Vec<Expression>,
    /// Frame specification
    pub frame: Option<WindowFrame>,
}

/// Window frame specification
#[derive(Debug, Clone)]
pub enum WindowFrame {
    /// ROWS frame
    Rows(FrameBound, FrameBound),
    /// RANGE frame
    Range(FrameBound, FrameBound),
    /// GROUPS frame (SQL:2011)
    Groups(FrameBound, FrameBound),
}

/// Window frame bound
#[derive(Debug, Clone)]
pub enum FrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// CURRENT ROW
    CurrentRow,
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
    /// N PRECEDING
    Preceding(i64),
    /// N FOLLOWING
    Following(i64),
}

/// Window function types
#[derive(Debug, Clone)]
pub enum WindowFunction {
    /// ROW_NUMBER() - assigns unique sequential integers
    RowNumber,
    /// RANK() - assigns ranks with gaps
    Rank,
    /// DENSE_RANK() - assigns ranks without gaps
    DenseRank,
    /// PERCENT_RANK() - relative rank (0 to 1)
    PercentRank,
    /// CUME_DIST() - cumulative distribution
    CumeDist,
    /// NTILE(n) - divides rows into n buckets
    Ntile(i32),
    /// LAG(expr, offset, default) - access previous row
    Lag {
        expr: Box<Expression>,
        offset: i64,
        default: Option<Box<Expression>>,
    },
    /// LEAD(expr, offset, default) - access following row
    Lead {
        expr: Box<Expression>,
        offset: i64,
        default: Option<Box<Expression>>,
    },
    /// FIRST_VALUE(expr) - first value in window
    FirstValue(Box<Expression>),
    /// LAST_VALUE(expr) - last value in window
    LastValue(Box<Expression>),
    /// NTH_VALUE(expr, n) - nth value in window
    NthValue(Box<Expression>, i64),
    /// Aggregate function as window function
    AggregateWindow(AggregateFunction),
}

/// Aggregate functions that can be used as window functions
#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count(Option<Box<Expression>>),
    Sum(Box<Expression>),
    Avg(Box<Expression>),
    Min(Box<Expression>),
    Max(Box<Expression>),
    StdDev(Box<Expression>),
    Variance(Box<Expression>),
    /// Array aggregation
    ArrayAgg {
        expr: Box<Expression>,
        order_by: Option<Vec<Expression>>,
    },
    /// String aggregation
    StringAgg {
        expr: Box<Expression>,
        separator: String,
        order_by: Option<Vec<Expression>>,
    },
}

/// Common Table Expression (CTE)
#[derive(Debug, Clone)]
pub struct CommonTableExpression {
    /// CTE name
    pub name: String,
    /// Column aliases (optional)
    pub columns: Option<Vec<String>>,
    /// The query for this CTE
    pub query: Box<ExtendedSelectStatement>,
    /// Is this a RECURSIVE CTE?
    pub recursive: bool,
}

/// Extended SELECT statement with CTEs and window functions
#[derive(Debug, Clone)]
pub struct ExtendedSelectStatement {
    /// WITH clause (CTEs)
    pub with_clause: Option<Vec<CommonTableExpression>>,
    /// Main SELECT query
    pub select: SelectCore,
    /// Set operations (UNION, INTERSECT, EXCEPT)
    pub set_operations: Vec<SetOperation>,
    /// ORDER BY clause (for final result)
    pub order_by: Option<Vec<Expression>>,
    /// LIMIT clause
    pub limit: Option<i64>,
    /// OFFSET clause
    pub offset: Option<i64>,
}

/// Core SELECT without set operations
#[derive(Debug, Clone)]
pub struct SelectCore {
    /// DISTINCT modifier
    pub distinct: bool,
    /// SELECT list
    pub select_list: Vec<ExtendedSelectItem>,
    /// FROM clause
    pub from: Option<Vec<TableExpression>>,
    /// WHERE clause
    pub where_clause: Option<Expression>,
    /// GROUP BY clause
    pub group_by: Option<Vec<Expression>>,
    /// HAVING clause
    pub having: Option<Expression>,
    /// WINDOW definitions
    pub windows: HashMap<String, WindowSpec>,
}

/// Extended select item with window functions
#[derive(Debug, Clone)]
pub enum ExtendedSelectItem {
    /// Simple expression
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
    /// Window function call
    WindowFunction {
        function: WindowFunction,
        window: WindowSpec,
        alias: Option<String>,
    },
    /// Wildcard (*)
    Wildcard,
    /// Qualified wildcard (table.*)
    QualifiedWildcard(String),
}

/// Table expression in FROM clause
#[derive(Debug, Clone)]
pub enum TableExpression {
    /// Simple table reference
    Table {
        name: String,
        alias: Option<String>,
    },
    /// Subquery
    Subquery {
        query: Box<ExtendedSelectStatement>,
        alias: String,
    },
    /// Table-valued function
    TableFunction {
        name: String,
        args: Vec<Expression>,
        alias: Option<String>,
    },
    /// JOIN expression
    Join {
        left: Box<TableExpression>,
        right: Box<TableExpression>,
        join_type: JoinType,
        condition: JoinCondition,
    },
    /// VALUES clause
    Values {
        rows: Vec<Vec<Expression>>,
        alias: Option<String>,
    },
}

/// JOIN types
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// JOIN condition
#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expression),
    Using(Vec<String>),
    Natural,
}

/// Set operations
#[derive(Debug, Clone)]
pub struct SetOperation {
    pub op_type: SetOperationType,
    pub all: bool,  // ALL modifier (keeps duplicates)
    pub query: SelectCore,
}

/// Set operation types
#[derive(Debug, Clone)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

/// Extended data types including arrays and JSON
#[derive(Debug, Clone)]
pub enum ExtendedDataType {
    /// Basic SQL types
    Basic(DataType),
    /// Array type
    Array(Box<ExtendedDataType>),
    /// JSON/JSONB
    Json,
    JsonB,
    /// UUID type
    Uuid,
    /// DECIMAL/NUMERIC with precision and scale
    Decimal(Option<u32>, Option<u32>),
    /// TIMESTAMP WITH TIME ZONE
    TimestampTz,
    /// Interval type
    Interval,
    /// Composite/Record type
    Composite(Vec<(String, ExtendedDataType)>),
}

/// Implementation of window function execution
pub mod window_exec {
    use super::*;

    /// Window partition - a group of rows for window function computation
    pub struct WindowPartition {
        pub rows: Vec<Vec<LiteralValue>>,
        pub partition_key: Vec<LiteralValue>,
    }

    /// Window function executor
    pub struct WindowExecutor {
        function: WindowFunction,
        _spec: WindowSpec,
    }

    impl WindowExecutor {
        pub fn new(function: WindowFunction, spec: WindowSpec) -> Self {
            Self { function, _spec: spec }
        }

        /// Execute window function over a partition
        pub fn execute(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            match &self.function {
                WindowFunction::RowNumber => self.compute_row_number(partition),
                WindowFunction::Rank => self.compute_rank(partition),
                WindowFunction::DenseRank => self.compute_dense_rank(partition),
                WindowFunction::PercentRank => self.compute_percent_rank(partition),
                WindowFunction::CumeDist => self.compute_cume_dist(partition),
                WindowFunction::Ntile(n) => self.compute_ntile(partition, *n),
                WindowFunction::Lag { offset, .. } => self.compute_lag(partition, *offset),
                WindowFunction::Lead { offset, .. } => self.compute_lead(partition, *offset),
                WindowFunction::FirstValue(_) => self.compute_first_value(partition),
                WindowFunction::LastValue(_) => self.compute_last_value(partition),
                WindowFunction::NthValue(_, n) => self.compute_nth_value(partition, *n),
                WindowFunction::AggregateWindow(agg) => self.compute_aggregate_window(partition, agg),
            }
        }

        fn compute_row_number(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            (1..=partition.rows.len())
                .map(|i| LiteralValue::Number(i.to_string()))
                .collect()
        }

        fn compute_rank(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            let mut current_rank = 1;
            let mut rows_with_same_value = 0;

            for i in 0..partition.rows.len() {
                if i > 0 && self.order_values_equal(&partition.rows[i-1], &partition.rows[i]) {
                    rows_with_same_value += 1;
                } else {
                    current_rank += rows_with_same_value;
                    rows_with_same_value = 1;
                }
                result.push(LiteralValue::Number(current_rank.to_string()));
            }

            result
        }

        fn compute_dense_rank(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            let mut current_rank = 1;

            for i in 0..partition.rows.len() {
                if i > 0 && !self.order_values_equal(&partition.rows[i-1], &partition.rows[i]) {
                    current_rank += 1;
                }
                result.push(LiteralValue::Number(current_rank.to_string()));
            }

            result
        }

        fn compute_percent_rank(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let ranks = self.compute_rank(partition);
            let n = partition.rows.len() as f64;
            
            ranks.into_iter().map(|rank| {
                if let LiteralValue::Number(r) = rank {
                    let r_val = r.parse::<f64>().unwrap_or(0.0);
                    let percent = if n > 1.0 { (r_val - 1.0) / (n - 1.0) } else { 0.0 };
                    LiteralValue::Number(percent.to_string())
                } else {
                    LiteralValue::Null
                }
            }).collect()
        }

        fn compute_cume_dist(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            let n = partition.rows.len() as f64;

            for i in 0..partition.rows.len() {
                let mut count = i + 1;
                // Count rows with same ORDER BY values
                while count < partition.rows.len() 
                    && self.order_values_equal(&partition.rows[i], &partition.rows[count]) {
                    count += 1;
                }
                let cume_dist = count as f64 / n;
                result.push(LiteralValue::Number(cume_dist.to_string()));
            }

            result
        }

        fn compute_ntile(&self, partition: &WindowPartition, n: i32) -> Vec<LiteralValue> {
            let total = partition.rows.len();
            let base_size = total / n as usize;
            let remainder = total % n as usize;
            
            let mut result = Vec::new();
            let mut current_tile = 1;
            let mut rows_in_current_tile = 0;
            let mut max_for_current = if current_tile <= remainder as i32 {
                base_size + 1
            } else {
                base_size
            };

            for _ in 0..total {
                result.push(LiteralValue::Number(current_tile.to_string()));
                rows_in_current_tile += 1;
                
                if rows_in_current_tile >= max_for_current && current_tile < n {
                    current_tile += 1;
                    rows_in_current_tile = 0;
                    max_for_current = if current_tile <= remainder as i32 {
                        base_size + 1
                    } else {
                        base_size
                    };
                }
            }

            result
        }

        fn compute_lag(&self, partition: &WindowPartition, offset: i64) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            for i in 0..partition.rows.len() {
                if i >= offset as usize {
                    // For simplicity, return the first column value
                    result.push(partition.rows[i - offset as usize][0].clone());
                } else {
                    result.push(LiteralValue::Null);
                }
            }
            result
        }

        fn compute_lead(&self, partition: &WindowPartition, offset: i64) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            for i in 0..partition.rows.len() {
                if i + (offset as usize) < partition.rows.len() {
                    result.push(partition.rows[i + offset as usize][0].clone());
                } else {
                    result.push(LiteralValue::Null);
                }
            }
            result
        }

        fn compute_first_value(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            if partition.rows.is_empty() {
                vec![]
            } else {
                let first = partition.rows[0][0].clone();
                vec![first; partition.rows.len()]
            }
        }

        fn compute_last_value(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            if partition.rows.is_empty() {
                vec![]
            } else {
                let last = partition.rows.last().unwrap()[0].clone();
                vec![last; partition.rows.len()]
            }
        }

        fn compute_nth_value(&self, partition: &WindowPartition, n: i64) -> Vec<LiteralValue> {
            if n <= 0 || n as usize > partition.rows.len() {
                vec![LiteralValue::Null; partition.rows.len()]
            } else {
                let nth = partition.rows[n as usize - 1][0].clone();
                vec![nth; partition.rows.len()]
            }
        }

        fn compute_aggregate_window(&self, partition: &WindowPartition, agg: &AggregateFunction) -> Vec<LiteralValue> {
            // Simplified aggregate computation
            match agg {
                AggregateFunction::Count(_) => {
                    let count = partition.rows.len();
                    vec![LiteralValue::Number(count.to_string()); count]
                }
                AggregateFunction::Sum(_) => {
                    // Compute running sum (simplified)
                    let mut result = Vec::new();
                    let mut sum = 0.0;
                    for row in &partition.rows {
                        if let LiteralValue::Number(n) = &row[0] {
                            sum += n.parse::<f64>().unwrap_or(0.0);
                        }
                        result.push(LiteralValue::Number(sum.to_string()));
                    }
                    result
                }
                _ => vec![LiteralValue::Null; partition.rows.len()],
            }
        }

        fn order_values_equal(&self, row1: &[LiteralValue], row2: &[LiteralValue]) -> bool {
            // Compare based on ORDER BY columns (simplified)
            row1.first() == row2.first()
        }
    }
}

/// CTE execution context
pub struct CteContext {
    /// Materialized CTEs
    pub tables: HashMap<String, CteTable>,
}

/// Materialized CTE
pub struct CteTable {
    pub schema: Vec<Column>,
    pub rows: Vec<Vec<LiteralValue>>,
}

impl CteContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Add a CTE to the context
    pub fn add_cte(&mut self, name: String, table: CteTable) {
        self.tables.insert(name, table);
    }

    /// Get a CTE by name
    pub fn get_cte(&self, name: &str) -> Option<&CteTable> {
        self.tables.get(name)
    }
}
