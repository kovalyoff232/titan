//! Aggregate function executors
//!
//! This module provides executors for aggregate functions like COUNT, SUM, AVG, MIN, MAX
//! with support for GROUP BY and HAVING clauses.

use crate::errors::ExecutionError;
use crate::executor::{Executor, evaluate_expr_for_row, evaluate_expr_for_row_to_val};
use crate::parser::{Expression, LiteralValue};
use crate::planner::AggregateExpr;
use crate::types::Column;
use std::collections::HashMap;

type Row = Vec<String>;

/// Hash-based aggregate executor
pub struct HashAggregateExecutor<'a> {
    input: Box<dyn Executor + 'a>,
    group_by: Vec<Expression>,
    aggregates: Vec<AggregateExpr>,
    having: Option<Expression>,
    schema: Vec<Column>,
    
    // State for aggregation
    groups: HashMap<Vec<String>, AggregateState>,
    result_iterator: Option<std::vec::IntoIter<Row>>,
    materialized: bool,
}

#[derive(Debug, Clone)]
struct AggregateState {
    // Map from aggregate function index to its state
    states: Vec<SingleAggregateState>,
}

#[derive(Debug, Clone)]
struct SingleAggregateState {
    count: i64,
    sum: f64,
    min: Option<LiteralValue>,
    max: Option<LiteralValue>,
    values: Vec<LiteralValue>, // For functions that need all values
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

impl Default for AggregateState {
    fn default() -> Self {
        Self {
            states: Vec::new(),
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
        // Build schema for the result
        let mut schema = Vec::new();
        
        // Add GROUP BY columns to schema
        for expr in &group_by {
            if let Expression::Column(name) = expr {
                schema.push(Column {
                    name: name.clone(),
                    type_id: 25, // TEXT type for simplicity
                });
            }
        }
        
        // Add aggregate columns to schema
        for agg in &aggregates {
            let name = agg.alias.as_ref().unwrap_or(&agg.function);
            schema.push(Column {
                name: name.clone(),
                type_id: match agg.function.as_str() {
                    "COUNT" => 23, // INT
                    "SUM" | "AVG" => 701, // NUMERIC
                    _ => 25, // TEXT
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
        
        // Process all input rows
        while let Some(row) = self.input.next()? {
            let row_map = self.row_to_map(&row);
            
            // Compute group key
            let mut group_key = Vec::new();
            for expr in &self.group_by {
                let val = evaluate_expr_for_row_to_val(expr, &row_map)?;
                group_key.push(val.to_string());
            }
            
            // Get or create aggregate state for this group
            let state = self.groups.entry(group_key).or_insert_with(|| {
                AggregateState {
                    states: vec![SingleAggregateState::default(); self.aggregates.len()],
                }
            });
            
            // Update aggregate state inline to avoid borrow issues
            for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                let agg_state = &mut state.states[agg_idx];
                
                // For COUNT(*), we don't need to evaluate arguments
                if agg.function.to_uppercase() == "COUNT" && agg.args.is_empty() {
                    agg_state.count += 1;
                    continue;
                }
                
                // For other aggregates, evaluate the argument
                if let Some(arg) = agg.args.first() {
                    let val = evaluate_expr_for_row_to_val(arg, &row_map)?;
                    
                    // Skip NULL values for most aggregates (except COUNT)
                    if matches!(val, LiteralValue::Null) && agg.function.to_uppercase() != "COUNT" {
                        continue;
                    }
                    
                    match agg.function.to_uppercase().as_str() {
                        "COUNT" => {
                            agg_state.count += 1;
                        }
                        "SUM" => {
                            if let LiteralValue::Number(n) = &val {
                                agg_state.sum += n.parse::<f64>().unwrap_or(0.0);
                            }
                        }
                        "AVG" => {
                            if let LiteralValue::Number(n) = &val {
                                agg_state.sum += n.parse::<f64>().unwrap_or(0.0);
                                agg_state.count += 1;
                            }
                        }
                        "MIN" => {
                            if agg_state.min.is_none() || val < *agg_state.min.as_ref().unwrap() {
                                agg_state.min = Some(val);
                            }
                        }
                        "MAX" => {
                            if agg_state.max.is_none() || val > *agg_state.max.as_ref().unwrap() {
                                agg_state.max = Some(val);
                            }
                        }
                        _ => {
                            // For unsupported aggregates, store all values
                            agg_state.values.push(val);
                        }
                    }
                }
            }
        }
        
        // Build result rows
        let mut result_rows = Vec::new();
        for (group_key, state) in &self.groups {
            let mut result_row = Vec::new();
            
            // Add group by columns
            result_row.extend_from_slice(group_key);
            
            // Add aggregate results
            for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                let agg_result = self.compute_aggregate_result(&state.states[agg_idx], agg)?;
                result_row.push(agg_result);
            }
            
            // Apply HAVING clause if present
            if let Some(having_expr) = &self.having {
                let row_map = self.result_row_to_map(&result_row);
                if !evaluate_expr_for_row(having_expr, &row_map)? {
                    continue; // Skip this group
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
            if i < row.len() {
                let value = match col.type_id {
                    23 => LiteralValue::Number(row[i].clone()), // INT
                    16 => LiteralValue::Bool(row[i] == "t" || row[i] == "true"), // BOOL  
                    1082 => LiteralValue::Date(row[i].clone()), // DATE
                    _ => LiteralValue::String(row[i].clone()), // TEXT and others
                };
                map.insert(col.name.clone(), value);
            }
        }
        map
    }
    
    fn result_row_to_map(&self, row: &[String]) -> HashMap<String, LiteralValue> {
        let mut map = HashMap::new();
        for (i, col) in self.schema.iter().enumerate() {
            if i < row.len() {
                map.insert(col.name.clone(), LiteralValue::String(row[i].clone()));
            }
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
                // Format as integer if it's a whole number, otherwise with decimals
                if state.sum.fract() == 0.0 {
                    (state.sum as i64).to_string()
                } else {
                    state.sum.to_string()
                }
            },
            "AVG" => {
                if state.count > 0 {
                    let avg = state.sum / state.count as f64;
                    // Format as integer if it's a whole number, otherwise with decimals
                    if avg.fract() == 0.0 {
                        (avg as i64).to_string()
                    } else {
                        avg.to_string()
                    }
                } else {
                    "NULL".to_string()
                }
            },
            "MIN" => {
                state.min.as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
            "MAX" => {
                state.max.as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
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
        // Materialize all results if not done yet
        self.materialize()?;
        
        // Return next result row
        if let Some(ref mut iter) = self.result_iterator {
            Ok(iter.next())
        } else {
            Ok(None)
        }
    }
}

/// Stream-based aggregate executor (for pre-sorted input)
/// NOTE: This is a placeholder implementation. The actual implementation
/// would process groups streaming-style as they come from a sorted input.
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
        // Build schema (same as HashAggregateExecutor)
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
            schema.push(Column {
                name: name.clone(),
                type_id: match agg.function.as_str() {
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
    
    // Note: StreamAggregateExecutor would process groups one at a time
    // as they come from a sorted input. This is more memory-efficient
    // but requires the input to be sorted by GROUP BY columns.
    // For now, we'll implement a simplified version.
}

impl<'a> Executor for StreamAggregateExecutor<'a> {
    fn schema(&self) -> &Vec<Column> {
        &self.schema
    }
    
    fn next(&mut self) -> Result<Option<Row>, ExecutionError> {
        // Simplified implementation - delegate to HashAggregateExecutor
        // In a real implementation, we would process groups streaming-style
        Err(ExecutionError::GenericError(
            "StreamAggregateExecutor not fully implemented yet".to_string()
        ))
    }
}
