use super::PhysicalPlan;
use crate::parser::{Expression, LiteralValue};
use crate::planner::LogicalPlan;

pub(super) fn create_simple_physical_plan(plan: LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan {
            table_name, filter, ..
        } => PhysicalPlan::TableScan { table_name, filter },
        LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(create_simple_physical_plan(*input)),
            predicate,
        },
        LogicalPlan::Projection { input, expressions } => PhysicalPlan::Projection {
            input: Box::new(create_simple_physical_plan(*input)),
            expressions,
        },
        LogicalPlan::Join {
            left,
            right,
            condition,
        } => {
            let left_physical = create_simple_physical_plan(*left);
            let right_physical = create_simple_physical_plan(*right);

            if let Expression::Binary {
                left: left_key,
                op: crate::parser::BinaryOperator::Eq,
                right: right_key,
            } = &condition
            {
                return PhysicalPlan::HashJoin {
                    left: Box::new(left_physical),
                    right: Box::new(right_physical),
                    left_key: *left_key.clone(),
                    right_key: *right_key.clone(),
                };
            }

            PhysicalPlan::NestedLoopJoin {
                left: Box::new(left_physical),
                right: Box::new(right_physical),
                condition,
            }
        }
        LogicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
            input: Box::new(create_simple_physical_plan(*input)),
            order_by,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
        } => PhysicalPlan::HashAggregate {
            input: Box::new(create_simple_physical_plan(*input)),
            group_by,
            aggregates,
            having,
        },
        LogicalPlan::Window {
            input,
            window_functions,
        } => PhysicalPlan::Window {
            input: Box::new(create_simple_physical_plan(*input)),
            window_functions,
        },
        LogicalPlan::CteRef { name } => PhysicalPlan::CTEScan { name },
        LogicalPlan::WithCte { cte_list, input } => {
            let mut result = create_simple_physical_plan(*input);
            for cte in cte_list.iter().rev() {
                result = PhysicalPlan::MaterializeCTE {
                    name: cte.name.clone(),
                    plan: Box::new(result),
                };
            }
            result
        }
        LogicalPlan::SetOperation {
            op: _,
            all: _,
            left,
            right,
        } => {
            let left_physical = create_simple_physical_plan(*left);
            let right_physical = create_simple_physical_plan(*right);

            PhysicalPlan::HashJoin {
                left: Box::new(left_physical),
                right: Box::new(right_physical),
                left_key: Expression::Literal(LiteralValue::Number("1".to_string())),
                right_key: Expression::Literal(LiteralValue::Number("1".to_string())),
            }
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => PhysicalPlan::Limit {
            input: Box::new(create_simple_physical_plan(*input)),
            limit,
            offset,
        },
    }
}
