//! Tests for aggregate functions

#[cfg(test)]
mod tests {
    use titan_bin::aggregate_executor::{HashAggregateExecutor, StreamAggregateExecutor};
    use titan_bin::executor::Executor;
    use titan_bin::parser::{Expression, LiteralValue};
    use titan_bin::planner::AggregateExpr;
    use titan_bin::types::Column;
    use std::collections::HashMap;

    // Mock executor for testing
    struct MockExecutor {
        rows: Vec<Vec<String>>,
        schema: Vec<Column>,
        cursor: usize,
    }

    impl MockExecutor {
        fn new(rows: Vec<Vec<String>>, schema: Vec<Column>) -> Self {
            Self { rows, schema, cursor: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn schema(&self) -> &Vec<Column> {
            &self.schema
        }

        fn next(&mut self) -> Result<Option<Vec<String>>, titan_bin::errors::ExecutionError> {
            if self.cursor < self.rows.len() {
                let row = self.rows[self.cursor].clone();
                self.cursor += 1;
                Ok(Some(row))
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    fn test_count_aggregate() {
        let schema = vec![
            Column { name: "id".to_string(), type_id: 23 },
            Column { name: "name".to_string(), type_id: 25 },
            Column { name: "age".to_string(), type_id: 23 },
        ];

        let rows = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            vec!["3".to_string(), "Charlie".to_string(), "30".to_string()],
        ];

        let mock_executor = Box::new(MockExecutor::new(rows, schema));
        
        // COUNT(*) without GROUP BY
        let aggregates = vec![
            AggregateExpr {
                function: "COUNT".to_string(),
                args: vec![],
                alias: Some("count".to_string()),
            }
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            mock_executor,
            vec![],
            aggregates,
            None
        );

        let result = agg_executor.next().unwrap();
        assert!(result.is_some());
        let row = result.unwrap();
        assert_eq!(row[0], "3"); // COUNT(*) should be 3
    }

    #[test]
    fn test_group_by_with_count() {
        let schema = vec![
            Column { name: "id".to_string(), type_id: 23 },
            Column { name: "name".to_string(), type_id: 25 },
            Column { name: "age".to_string(), type_id: 23 },
        ];

        let rows = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
            vec!["3".to_string(), "Charlie".to_string(), "30".to_string()],
            vec!["4".to_string(), "David".to_string(), "25".to_string()],
        ];

        let mock_executor = Box::new(MockExecutor::new(rows, schema));
        
        // GROUP BY age, COUNT(*)
        let group_by = vec![Expression::Column("age".to_string())];
        let aggregates = vec![
            AggregateExpr {
                function: "COUNT".to_string(),
                args: vec![],
                alias: Some("count".to_string()),
            }
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            mock_executor,
            group_by,
            aggregates,
            None
        );

        let mut results = Vec::new();
        while let Some(row) = agg_executor.next().unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 2); // Two distinct age groups (25 and 30)
        
        // Check that we have the correct counts
        for row in &results {
            if row[0] == "30" {
                assert_eq!(row[1], "2"); // Two people aged 30
            } else if row[0] == "25" {
                assert_eq!(row[1], "2"); // Two people aged 25
            }
        }
    }

    #[test]
    fn test_sum_and_avg() {
        let schema = vec![
            Column { name: "product".to_string(), type_id: 25 },
            Column { name: "sales".to_string(), type_id: 23 },
        ];

        let rows = vec![
            vec!["A".to_string(), "100".to_string()],
            vec!["A".to_string(), "200".to_string()],
            vec!["B".to_string(), "150".to_string()],
            vec!["B".to_string(), "250".to_string()],
        ];

        let mock_executor = Box::new(MockExecutor::new(rows, schema));
        
        // GROUP BY product, SUM(sales), AVG(sales)
        let group_by = vec![Expression::Column("product".to_string())];
        let aggregates = vec![
            AggregateExpr {
                function: "SUM".to_string(),
                args: vec![Expression::Column("sales".to_string())],
                alias: Some("total_sales".to_string()),
            },
            AggregateExpr {
                function: "AVG".to_string(),
                args: vec![Expression::Column("sales".to_string())],
                alias: Some("avg_sales".to_string()),
            },
        ];

        let mut agg_executor = HashAggregateExecutor::new(
            mock_executor,
            group_by,
            aggregates,
            None
        );

        let mut results = Vec::new();
        while let Some(row) = agg_executor.next().unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 2); // Two products
        
        for row in &results {
            if row[0] == "A" {
                assert_eq!(row[1], "300"); // SUM for A
                assert_eq!(row[2], "150"); // AVG for A
            } else if row[0] == "B" {
                assert_eq!(row[1], "400"); // SUM for B
                assert_eq!(row[2], "200"); // AVG for B
            }
        }
    }
}
