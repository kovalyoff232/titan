//! LIMIT/OFFSET executor
//!
//! This module provides an executor that limits the number of rows returned
//! and optionally skips a specified number of rows (OFFSET).

use crate::errors::ExecutionError;
use crate::executor::Executor;
use crate::types::Column;

type Row = Vec<String>;

/// Executor that implements LIMIT and OFFSET functionality
pub struct LimitExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    limit: Option<usize>,
    offset: usize,
    
    // State tracking
    rows_skipped: usize,
    rows_returned: usize,
}

impl<'a> LimitExecutor<'a> {
    /// Creates a new LimitExecutor
    /// 
    /// # Arguments
    /// * `input` - The child executor to limit
    /// * `limit` - Optional limit on number of rows to return (None means no limit)
    /// * `offset` - Number of rows to skip before returning results
    pub fn new(
        input: Box<dyn Executor + 'a>,
        limit: Option<usize>,
        offset: usize,
    ) -> Self {
        Self {
            input,
            limit,
            offset,
            rows_skipped: 0,
            rows_returned: 0,
        }
    }
}

impl<'a> Executor for LimitExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }
    
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        // Check if we've already returned the limit
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                return Ok(None);
            }
        }
        
        // Skip rows for OFFSET
        while self.rows_skipped < self.offset {
            match self.input.next()? {
                Some(_) => {
                    self.rows_skipped += 1;
                }
                None => {
                    // Input exhausted before reaching offset
                    return Ok(None);
                }
            }
        }
        
        // Return the next row from input
        match self.input.next()? {
            Some(row) => {
                self.rows_returned += 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }
}

/// Executor that returns only the top N rows based on an ordering
/// This is a special case of LIMIT that's often optimized differently
pub struct TopNExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    n: usize,
    
    // We materialize all input and sort to get top N
    // In a production system, this could use a heap for better memory efficiency
    materialized: bool,
    sorted_rows: Vec<Row>,
    cursor: usize,
}

impl<'a> TopNExecutor<'a> {
    pub fn new(
        input: Box<dyn Executor + 'a>,
        n: usize,
    ) -> Self {
        Self {
            input,
            n,
            materialized: false,
            sorted_rows: Vec::new(),
            cursor: 0,
        }
    }
    
    fn materialize(&mut self) -> Result<(), ExecutionError> {
        if self.materialized {
            return Ok(());
        }
        
        // Collect all input rows
        let mut rows = Vec::new();
        while let Some(row) = self.input.next()? {
            rows.push(row);
        }
        
        // Keep only the top N rows
        // Note: In a real implementation, we'd need to sort based on ORDER BY columns
        // For now, we just take the first N rows
        self.sorted_rows = rows.into_iter().take(self.n).collect();
        self.materialized = true;
        
        Ok(())
    }
}

impl<'a> Executor for TopNExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        self.input.schema()
    }
    
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        self.materialize()?;
        
        if self.cursor < self.sorted_rows.len() {
            let row = self.sorted_rows[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    
    struct MockExecutor {
        rows: VecDeque<Row>,
        schema: Vec<Column>,
    }
    
    impl MockExecutor {
        fn new(rows: Vec<Row>) -> Self {
            let schema = vec![
                Column { name: "id".to_string(), type_id: 23 },
                Column { name: "value".to_string(), type_id: 25 },
            ];
            Self {
                rows: rows.into_iter().collect(),
                schema,
            }
        }
    }
    
    impl Executor for MockExecutor {
        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }
        
        fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
            Ok(self.rows.pop_front())
        }
    }
    
    #[test]
    fn test_limit_only() {
        let rows = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["4".to_string(), "d".to_string()],
            vec!["5".to_string(), "e".to_string()],
        ];
        
        let mock_exec = Box::new(MockExecutor::new(rows));
        let mut limit_exec = LimitExecutor::new(mock_exec, Some(3), 0);
        
        // Should return exactly 3 rows
        let mut count = 0;
        while let Some(_) = limit_exec.next().unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);
    }
    
    #[test]
    fn test_offset_only() {
        let rows = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["4".to_string(), "d".to_string()],
            vec!["5".to_string(), "e".to_string()],
        ];
        
        let mock_exec = Box::new(MockExecutor::new(rows));
        let mut limit_exec = LimitExecutor::new(mock_exec, None, 2);
        
        // Should skip first 2 rows, return the rest
        let first_row = limit_exec.next().unwrap().unwrap();
        assert_eq!(first_row[0], "3");
        
        let mut count = 1;
        while let Some(_) = limit_exec.next().unwrap() {
            count += 1;
        }
        assert_eq!(count, 3); // Rows 3, 4, 5
    }
    
    #[test]
    fn test_limit_and_offset() {
        let rows = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["4".to_string(), "d".to_string()],
            vec!["5".to_string(), "e".to_string()],
        ];
        
        let mock_exec = Box::new(MockExecutor::new(rows));
        let mut limit_exec = LimitExecutor::new(mock_exec, Some(2), 1);
        
        // Should skip 1 row, then return 2 rows
        let first_row = limit_exec.next().unwrap().unwrap();
        assert_eq!(first_row[0], "2");
        
        let second_row = limit_exec.next().unwrap().unwrap();
        assert_eq!(second_row[0], "3");
        
        // Should return None after limit is reached
        assert!(limit_exec.next().unwrap().is_none());
    }
    
    #[test]
    fn test_offset_exceeds_input() {
        let rows = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
        ];
        
        let mock_exec = Box::new(MockExecutor::new(rows));
        let mut limit_exec = LimitExecutor::new(mock_exec, None, 5);
        
        // Offset is larger than input size, should return no rows
        assert!(limit_exec.next().unwrap().is_none());
    }
}
