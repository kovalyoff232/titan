use crate::parser::DataType;
use crate::parser::{Expression, LiteralValue};
use crate::types::Column;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,

    pub order_by: Vec<Expression>,

    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone)]
pub enum WindowFrame {
    Rows(FrameBound, FrameBound),

    Range(FrameBound, FrameBound),

    Groups(FrameBound, FrameBound),
}

#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,

    CurrentRow,

    UnboundedFollowing,

    Preceding(i64),

    Following(i64),
}

#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,

    Rank,

    DenseRank,

    PercentRank,

    CumeDist,

    Ntile(i32),

    Lag {
        expr: Box<Expression>,
        offset: i64,
        default: Option<Box<Expression>>,
    },

    Lead {
        expr: Box<Expression>,
        offset: i64,
        default: Option<Box<Expression>>,
    },

    FirstValue(Box<Expression>),

    LastValue(Box<Expression>),

    NthValue(Box<Expression>, i64),

    AggregateWindow(AggregateFunction),
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count(Option<Box<Expression>>),
    Sum(Box<Expression>),
    Avg(Box<Expression>),
    Min(Box<Expression>),
    Max(Box<Expression>),
    StdDev(Box<Expression>),
    Variance(Box<Expression>),

    ArrayAgg {
        expr: Box<Expression>,
        order_by: Option<Vec<Expression>>,
    },

    StringAgg {
        expr: Box<Expression>,
        separator: String,
        order_by: Option<Vec<Expression>>,
    },
}

#[derive(Debug, Clone)]
pub struct CommonTableExpression {
    pub name: String,

    pub columns: Option<Vec<String>>,

    pub query: Box<ExtendedSelectStatement>,

    pub recursive: bool,
}

#[derive(Debug, Clone)]
pub struct ExtendedSelectStatement {
    pub with_clause: Option<Vec<CommonTableExpression>>,

    pub select: SelectCore,

    pub set_operations: Vec<SetOperation>,

    pub order_by: Option<Vec<Expression>>,

    pub limit: Option<i64>,

    pub offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SelectCore {
    pub distinct: bool,

    pub select_list: Vec<ExtendedSelectItem>,

    pub from: Option<Vec<TableExpression>>,

    pub where_clause: Option<Expression>,

    pub group_by: Option<Vec<Expression>>,

    pub having: Option<Expression>,

    pub windows: HashMap<String, WindowSpec>,
}

#[derive(Debug, Clone)]
pub enum ExtendedSelectItem {
    Expression {
        expr: Expression,
        alias: Option<String>,
    },

    WindowFunction {
        function: WindowFunction,
        window: WindowSpec,
        alias: Option<String>,
    },

    Wildcard,

    QualifiedWildcard(String),
}

#[derive(Debug, Clone)]
pub enum TableExpression {
    Table {
        name: String,
        alias: Option<String>,
    },

    Subquery {
        query: Box<ExtendedSelectStatement>,
        alias: String,
    },

    TableFunction {
        name: String,
        args: Vec<Expression>,
        alias: Option<String>,
    },

    Join {
        left: Box<TableExpression>,
        right: Box<TableExpression>,
        join_type: JoinType,
        condition: JoinCondition,
    },

    Values {
        rows: Vec<Vec<Expression>>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub enum JoinCondition {
    On(Expression),
    Using(Vec<String>),
    Natural,
}

#[derive(Debug, Clone)]
pub struct SetOperation {
    pub op_type: SetOperationType,
    pub all: bool,
    pub query: SelectCore,
}

#[derive(Debug, Clone)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone)]
pub enum ExtendedDataType {
    Basic(DataType),

    Array(Box<ExtendedDataType>),

    Json,
    JsonB,

    Uuid,

    Decimal(Option<u32>, Option<u32>),

    TimestampTz,

    Interval,

    Composite(Vec<(String, ExtendedDataType)>),
}

pub mod window_exec {
    use super::*;

    pub struct WindowPartition {
        pub rows: Vec<Vec<LiteralValue>>,
        pub partition_key: Vec<LiteralValue>,
    }

    pub struct WindowExecutor {
        function: WindowFunction,
        _spec: WindowSpec,
    }

    impl WindowExecutor {
        pub fn new(function: WindowFunction, spec: WindowSpec) -> Self {
            Self {
                function,
                _spec: spec,
            }
        }

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
                WindowFunction::AggregateWindow(agg) => {
                    self.compute_aggregate_window(partition, agg)
                }
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
                if i > 0 && self.order_values_equal(&partition.rows[i - 1], &partition.rows[i]) {
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
                if i > 0 && !self.order_values_equal(&partition.rows[i - 1], &partition.rows[i]) {
                    current_rank += 1;
                }
                result.push(LiteralValue::Number(current_rank.to_string()));
            }

            result
        }

        fn compute_percent_rank(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let ranks = self.compute_rank(partition);
            let n = partition.rows.len() as f64;

            ranks
                .into_iter()
                .map(|rank| {
                    if let LiteralValue::Number(r) = rank {
                        let r_val = r.parse::<f64>().unwrap_or(0.0);
                        let percent = if n > 1.0 {
                            (r_val - 1.0) / (n - 1.0)
                        } else {
                            0.0
                        };
                        LiteralValue::Number(percent.to_string())
                    } else {
                        LiteralValue::Null
                    }
                })
                .collect()
        }

        fn compute_cume_dist(&self, partition: &WindowPartition) -> Vec<LiteralValue> {
            let mut result = Vec::new();
            let n = partition.rows.len() as f64;

            for i in 0..partition.rows.len() {
                let mut count = i + 1;

                while count < partition.rows.len()
                    && self.order_values_equal(&partition.rows[i], &partition.rows[count])
                {
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

        fn compute_aggregate_window(
            &self,
            partition: &WindowPartition,
            agg: &AggregateFunction,
        ) -> Vec<LiteralValue> {
            match agg {
                AggregateFunction::Count(_) => {
                    let count = partition.rows.len();
                    vec![LiteralValue::Number(count.to_string()); count]
                }
                AggregateFunction::Sum(_) => {
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
            row1.first() == row2.first()
        }
    }
}

pub struct CteContext {
    pub tables: HashMap<String, CteTable>,
}

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

    pub fn add_cte(&mut self, name: String, table: CteTable) {
        self.tables.insert(name, table);
    }

    pub fn get_cte(&self, name: &str) -> Option<&CteTable> {
        self.tables.get(name)
    }
}
