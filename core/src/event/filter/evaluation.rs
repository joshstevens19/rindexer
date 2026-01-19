//! This module evaluates a parsed expression AST against a JSON object.

use super::{
    ast::{
        Accessor, ArithmeticExpr, ArithmeticOperator, ComparisonOperator, Condition, ConditionLeft,
        Expression, LiteralValue, LogicalOperator, VariableSource,
    },
    helpers::{are_same_address, compare_ordered_values, string_to_i256, string_to_u256},
};
use alloy::primitives::U256;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::str::FromStr;
use thiserror::Error;

/// The result of evaluating an arithmetic expression for computed columns.
#[derive(Debug, Clone)]
pub enum ComputedValue {
    /// A U256 numeric result
    U256(U256),
    /// A string result
    String(String),
}

/// Represents errors that can occur during expression evaluation.
#[derive(Debug, Error, PartialEq)]
pub enum EvaluationError {
    /// An error indicating a type mismatch during evaluation.
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    /// An error indicating that an unsupported operator was used.
    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    /// An error indicating that a parse operation failed.
    #[error("Parse error: {0}")]
    ParseError(String),

    /// An error indicating that a variable was not found in the provided data.
    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    /// An error indicating that an index was out of bounds for an array.
    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(String),

    /// An error indicating division by zero.
    #[error("Division by zero")]
    DivisionByZero,

    /// An error indicating arithmetic overflow.
    #[error("Arithmetic overflow: {0}")]
    ArithmeticOverflow(String),
}

const UNSIGNED_INTEGER_KINDS: &[&str] =
    &["uint8", "uint16", "uint32", "uint64", "uint128", "uint256", "number"];
const SIGNED_INTEGER_KINDS: &[&str] = &["int8", "int16", "int32", "int64", "int128", "int256"];
const ARRAY_KINDS: &[&str] = &[
    "array",
    "uint8[]",
    "uint16[]",
    "uint32[]",
    "uint64[]",
    "uint128[]",
    "uint256[]",
    "int8[]",
    "int16[]",
    "int32[]",
    "int64[]",
    "int128[]",
    "int256[]",
    "string[]",
    "address[]",
    "bool[]",
    "fixed[]",
    "ufixed[]",
    "bytes[]",
    "bytes32[]",
    "tuple[]",
];

/// The main entry point for evaluation. Traverses the Expression AST and evaluates it against the given data.
///
/// # Arguments
/// * `expression` - The parsed expression AST to evaluate.
/// * `data` - The JSON data against which the expression is evaluated.
/// # Returns
/// * `Ok(bool)` - The result of the evaluation, true if the expression evaluates to true, false otherwise.
/// * `Err(EvaluationError)` - An error if the evaluation fails due to type mismatches, unsupported operators, parsing errors, or missing variables.
pub fn evaluate<'a>(
    expression: &Expression<'a>,
    data: &JsonValue,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?expression, ?data, "Evaluating expression");

    match expression {
        Expression::Logical { left, operator, right } => {
            let left_val = evaluate(left, data)?;
            match operator {
                LogicalOperator::And => {
                    if !left_val {
                        Ok(false) // Short-circuit
                    } else {
                        evaluate(right, data)
                    }
                }
                LogicalOperator::Or => {
                    if left_val {
                        Ok(true) // Short-circuit
                    } else {
                        evaluate(right, data)
                    }
                }
            }
        }
        Expression::Condition(condition) => evaluate_condition_with_table(condition, data, None),
    }
}

/// Evaluates an expression against event data and optional table data.
/// Use this when your filter expression may contain @variable references to current table state.
///
/// # Arguments
/// * `expression` - The parsed expression AST to evaluate.
/// * `event_data` - The event data (JSON) against which $variables are resolved.
/// * `table_data` - Optional current table row data against which @variables are resolved.
///
/// # Returns
/// * `Ok(bool)` - The result of the evaluation.
/// * `Err(EvaluationError)` - An error if evaluation fails.
pub fn evaluate_with_table_data<'a>(
    expression: &Expression<'a>,
    event_data: &JsonValue,
    table_data: Option<&JsonValue>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?expression, ?event_data, ?table_data, "Evaluating expression with table data");

    match expression {
        Expression::Logical { left, operator, right } => {
            let left_val = evaluate_with_table_data(left, event_data, table_data)?;
            match operator {
                LogicalOperator::And => {
                    if !left_val {
                        Ok(false) // Short-circuit
                    } else {
                        evaluate_with_table_data(right, event_data, table_data)
                    }
                }
                LogicalOperator::Or => {
                    if left_val {
                        Ok(true) // Short-circuit
                    } else {
                        evaluate_with_table_data(right, event_data, table_data)
                    }
                }
            }
        }
        Expression::Condition(condition) => {
            evaluate_condition_with_table(condition, event_data, table_data)
        }
    }
}

/// Represents the result of evaluating an arithmetic expression.
/// Can be either a numeric value or a non-numeric value (for comparisons).
#[derive(Debug, Clone)]
enum ArithmeticValue {
    /// A numeric value (stored as U256 for precision)
    Number(U256),
    /// A non-numeric value with its kind and string representation
    Other { kind: String, value: String },
}

/// Evaluates an arithmetic expression to a numeric value.
/// # Arguments
/// * `expr` - The arithmetic expression to evaluate.
/// * `data` - The JSON data against which variables are resolved.
/// # Returns
/// * `Ok(ArithmeticValue)` - The computed value.
/// * `Err(EvaluationError)` - An error if evaluation fails.
fn evaluate_arithmetic_expr<'a>(
    expr: &ArithmeticExpr<'a>,
    data: &JsonValue,
) -> Result<ArithmeticValue, EvaluationError> {
    match expr {
        ArithmeticExpr::Variable(path) => {
            let resolved = resolve_path(path, data)?;
            let kind = get_kind_from_json_value(resolved);
            let value_str = match resolved {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                JsonValue::Null => "null".to_string(),
                JsonValue::Array(_) | JsonValue::Object(_) => resolved.to_string(),
            };

            // Try to parse as number for arithmetic operations
            if let Ok(num) = string_to_u256(&value_str) {
                Ok(ArithmeticValue::Number(num))
            } else {
                Ok(ArithmeticValue::Other { kind, value: value_str })
            }
        }
        ArithmeticExpr::Literal(lit) => match lit {
            LiteralValue::Number(s) => {
                // Try to parse as U256 first (for integers)
                if let Ok(num) = string_to_u256(s) {
                    Ok(ArithmeticValue::Number(num))
                } else {
                    // For decimals like "123.4", keep as non-numeric value
                    let kind = if s.contains('.') { "fixed" } else { "number" };
                    Ok(ArithmeticValue::Other { kind: kind.to_string(), value: s.to_string() })
                }
            }
            LiteralValue::Bool(b) => {
                Ok(ArithmeticValue::Other { kind: "bool".to_string(), value: b.to_string() })
            }
            LiteralValue::Str(s) => {
                // Try to parse as number first
                if let Ok(num) = string_to_u256(s) {
                    Ok(ArithmeticValue::Number(num))
                } else {
                    let kind = get_kind_from_str(s);
                    Ok(ArithmeticValue::Other { kind, value: s.to_string() })
                }
            }
        },
        ArithmeticExpr::Binary { left, operator, right } => {
            let left_val = evaluate_arithmetic_expr(left, data)?;
            let right_val = evaluate_arithmetic_expr(right, data)?;

            // Both must be numbers for arithmetic
            let (left_num, right_num) = match (left_val, right_val) {
                (ArithmeticValue::Number(l), ArithmeticValue::Number(r)) => (l, r),
                _ => {
                    return Err(EvaluationError::TypeMismatch(
                        "Arithmetic operations require numeric operands".to_string(),
                    ))
                }
            };

            let result = match operator {
                ArithmeticOperator::Add => left_num.checked_add(right_num).ok_or_else(|| {
                    EvaluationError::ArithmeticOverflow("addition overflow".to_string())
                })?,
                ArithmeticOperator::Subtract => {
                    left_num.checked_sub(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("subtraction underflow".to_string())
                    })?
                }
                ArithmeticOperator::Multiply => {
                    left_num.checked_mul(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("multiplication overflow".to_string())
                    })?
                }
                ArithmeticOperator::Divide => {
                    if right_num.is_zero() {
                        return Err(EvaluationError::DivisionByZero);
                    }
                    left_num.checked_div(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("division error".to_string())
                    })?
                }
            };

            Ok(ArithmeticValue::Number(result))
        }
    }
}

/// Evaluates an arithmetic expression with optional table data support.
/// # Arguments
/// * `expr` - The arithmetic expression to evaluate.
/// * `event_data` - The event data against which $variables are resolved.
/// * `table_data` - Optional table data against which @variables are resolved.
/// # Returns
/// * `Ok(ArithmeticValue)` - The computed value.
/// * `Err(EvaluationError)` - An error if evaluation fails.
fn evaluate_arithmetic_expr_with_table<'a>(
    expr: &ArithmeticExpr<'a>,
    event_data: &JsonValue,
    table_data: Option<&JsonValue>,
) -> Result<ArithmeticValue, EvaluationError> {
    match expr {
        ArithmeticExpr::Variable(path) => {
            let resolved = resolve_path_with_table(path, event_data, table_data)?;
            let kind = get_kind_from_json_value(resolved);
            let value_str = match resolved {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                JsonValue::Null => "null".to_string(),
                JsonValue::Array(_) | JsonValue::Object(_) => resolved.to_string(),
            };

            // Try to parse as number for arithmetic operations
            if let Ok(num) = string_to_u256(&value_str) {
                Ok(ArithmeticValue::Number(num))
            } else {
                Ok(ArithmeticValue::Other { kind, value: value_str })
            }
        }
        ArithmeticExpr::Literal(lit) => match lit {
            LiteralValue::Number(s) => {
                if let Ok(num) = string_to_u256(s) {
                    Ok(ArithmeticValue::Number(num))
                } else {
                    let kind = if s.contains('.') { "fixed" } else { "number" };
                    Ok(ArithmeticValue::Other { kind: kind.to_string(), value: s.to_string() })
                }
            }
            LiteralValue::Bool(b) => {
                Ok(ArithmeticValue::Other { kind: "bool".to_string(), value: b.to_string() })
            }
            LiteralValue::Str(s) => {
                if let Ok(num) = string_to_u256(s) {
                    Ok(ArithmeticValue::Number(num))
                } else {
                    let kind = get_kind_from_str(s);
                    Ok(ArithmeticValue::Other { kind, value: s.to_string() })
                }
            }
        },
        ArithmeticExpr::Binary { left, operator, right } => {
            let left_val = evaluate_arithmetic_expr_with_table(left, event_data, table_data)?;
            let right_val = evaluate_arithmetic_expr_with_table(right, event_data, table_data)?;

            let (left_num, right_num) = match (left_val, right_val) {
                (ArithmeticValue::Number(l), ArithmeticValue::Number(r)) => (l, r),
                _ => {
                    return Err(EvaluationError::TypeMismatch(
                        "Arithmetic operations require numeric operands".to_string(),
                    ))
                }
            };

            let result = match operator {
                ArithmeticOperator::Add => left_num.checked_add(right_num).ok_or_else(|| {
                    EvaluationError::ArithmeticOverflow("addition overflow".to_string())
                })?,
                ArithmeticOperator::Subtract => {
                    left_num.checked_sub(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("subtraction underflow".to_string())
                    })?
                }
                ArithmeticOperator::Multiply => {
                    left_num.checked_mul(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("multiplication overflow".to_string())
                    })?
                }
                ArithmeticOperator::Divide => {
                    if right_num.is_zero() {
                        return Err(EvaluationError::DivisionByZero);
                    }
                    left_num.checked_div(right_num).ok_or_else(|| {
                        EvaluationError::ArithmeticOverflow("division error".to_string())
                    })?
                }
            };

            Ok(ArithmeticValue::Number(result))
        }
    }
}

/// Helper to get kind from a string value (for literals)
fn get_kind_from_str(s: &str) -> String {
    // Check if it looks like an address
    if s.starts_with("0x") || s.starts_with("0X") {
        if s.len() == 42 {
            return "address".to_string();
        } else if s.len() == 66 {
            return "bytes32".to_string();
        }
    }
    "string".to_string()
}

/// Evaluates a single condition with optional table data support.
/// # Arguments
/// * `condition` - The condition to evaluate.
/// * `event_data` - The event data against which $variables are resolved.
/// * `table_data` - Optional table data against which @variables are resolved.
/// # Returns
/// * `Ok(bool)` - The result of the condition evaluation.
/// * `Err(EvaluationError)` - An error if the evaluation fails.
fn evaluate_condition_with_table<'a>(
    condition: &Condition<'a>,
    event_data: &JsonValue,
    table_data: Option<&JsonValue>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?condition, ?event_data, ?table_data, "Evaluating condition with table data");

    let left_val = evaluate_arithmetic_expr_with_table(&condition.left, event_data, table_data)?;
    let right_val = evaluate_arithmetic_expr_with_table(&condition.right, event_data, table_data)?;

    // If both are numbers, do numeric comparison
    match (&left_val, &right_val) {
        (ArithmeticValue::Number(l), ArithmeticValue::Number(r)) => {
            let result = match condition.operator {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                ComparisonOperator::Gt => l > r,
                ComparisonOperator::Gte => l >= r,
                ComparisonOperator::Lt => l < r,
                ComparisonOperator::Lte => l <= r,
            };
            Ok(result)
        }
        (ArithmeticValue::Other { kind: lk, value: lv }, ArithmeticValue::Number(r)) => {
            // Left is non-numeric, right is numeric - compare as before
            compare_final_values(lk, lv, &condition.operator, &LiteralValue::Number(&r.to_string()))
        }
        (ArithmeticValue::Number(l), ArithmeticValue::Other { kind: _rk, value: rv }) => {
            // Left is numeric, right is non-numeric
            compare_final_values(
                "uint256",
                &l.to_string(),
                &condition.operator,
                &LiteralValue::Str(rv),
            )
        }
        (
            ArithmeticValue::Other { kind: lk, value: lv },
            ArithmeticValue::Other { value: rv, .. },
        ) => {
            // Both non-numeric - use original comparison logic
            // Determine the right literal type
            let rhs_literal = if rv == "true" || rv == "false" {
                LiteralValue::Bool(rv == "true")
            } else if rv.starts_with("0x") || rv.starts_with("0X") {
                LiteralValue::Str(rv)
            } else {
                LiteralValue::Str(rv)
            };
            compare_final_values(lk, lv, &condition.operator, &rhs_literal)
        }
    }
}

/// Resolves a path from the AST against the JSON data.
/// # Arguments
/// * `path` - The path to resolve, which may include base names and accessors.
/// * `data` - The JSON data against which the path is resolved.
/// # Returns
/// * `Ok(&JsonValue)` - The resolved value from the JSON data.
/// * `Err(EvaluationError)` - An error if the path cannot be resolved due to missing variables, type mismatches, or index out of bounds errors.
fn resolve_path<'a>(
    path: &ConditionLeft<'a>,
    data: &'a JsonValue,
) -> Result<&'a JsonValue, EvaluationError> {
    tracing::debug!(?path, ?data, "Resolving path");

    let base_name = path.base_name();
    let mut current = data
        .get(base_name)
        .ok_or_else(|| EvaluationError::VariableNotFound(base_name.to_string()))?;

    for accessor in path.accessors() {
        current = match (accessor, current) {
            (Accessor::Key(key), JsonValue::Object(map)) => map
                .get(*key)
                .ok_or_else(|| EvaluationError::VariableNotFound((*key).to_string()))?,
            (Accessor::Index(index), JsonValue::Array(arr)) => arr
                .get(*index)
                .ok_or_else(|| EvaluationError::IndexOutOfBounds(index.to_string()))?,
            _ => {
                return Err(EvaluationError::TypeMismatch(format!(
                    "Cannot apply accessor {accessor:?} to value {current:?}"
                )));
            }
        };
    }
    Ok(current)
}

/// Resolves a path from the AST against event data or table data based on VariableSource.
/// # Arguments
/// * `path` - The path to resolve, which may include base names and accessors.
/// * `event_data` - The event data for $variables (VariableSource::Event).
/// * `table_data` - Optional table data for @variables (VariableSource::Table).
/// # Returns
/// * `Ok(&JsonValue)` - The resolved value.
/// * `Err(EvaluationError)` - An error if resolution fails.
fn resolve_path_with_table<'a>(
    path: &ConditionLeft<'a>,
    event_data: &'a JsonValue,
    table_data: Option<&'a JsonValue>,
) -> Result<&'a JsonValue, EvaluationError> {
    tracing::debug!(?path, ?event_data, ?table_data, "Resolving path with table data");

    // Select the data source based on VariableSource
    let data = match path.source() {
        VariableSource::Event => event_data,
        VariableSource::Table => table_data.ok_or_else(|| {
            EvaluationError::VariableNotFound(format!(
                "@{} (table data not available - row may not exist yet)",
                path.base_name()
            ))
        })?,
    };

    let base_name = path.base_name();
    let mut current = data.get(base_name).ok_or_else(|| {
        let prefix = match path.source() {
            VariableSource::Event => "$",
            VariableSource::Table => "@",
        };
        EvaluationError::VariableNotFound(format!("{}{}", prefix, base_name))
    })?;

    for accessor in path.accessors() {
        current = match (accessor, current) {
            (Accessor::Key(key), JsonValue::Object(map)) => map
                .get(*key)
                .ok_or_else(|| EvaluationError::VariableNotFound((*key).to_string()))?,
            (Accessor::Index(index), JsonValue::Array(arr)) => arr
                .get(*index)
                .ok_or_else(|| EvaluationError::IndexOutOfBounds(index.to_string()))?,
            _ => {
                return Err(EvaluationError::TypeMismatch(format!(
                    "Cannot apply accessor {accessor:?} to value {current:?}"
                )));
            }
        };
    }
    Ok(current)
}

/// Routes the comparison to the correct type-specific function.
/// # Arguments
/// * `lhs_kind_str` - The kind of the left-hand side value as a string.
/// * `lhs_value_str` - The string representation of the left-hand side value.
/// * `operator` - The comparison operator to use.
/// * `rhs_literal` - The right-hand side literal value to compare against.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches, unsupported operators, or parsing errors.
fn compare_final_values(
    lhs_kind_str: &str,
    lhs_value_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(
        ?lhs_kind_str,
        ?lhs_value_str,
        ?operator,
        ?rhs_literal,
        "Comparing final values"
    );

    let lhs_kind = lhs_kind_str.to_lowercase();

    if SIGNED_INTEGER_KINDS.contains(&lhs_kind.as_str()) {
        return compare_i256(lhs_value_str, operator, rhs_literal);
    }
    if UNSIGNED_INTEGER_KINDS.contains(&lhs_kind.as_str()) {
        return compare_u256(lhs_value_str, operator, rhs_literal);
    }
    if ARRAY_KINDS.contains(&lhs_kind.as_str()) {
        return compare_array(lhs_value_str, operator, rhs_literal);
    }

    match lhs_kind.as_str() {
        "fixed" | "ufixed" => compare_fixed_point(lhs_value_str, operator, rhs_literal),
        "address" => compare_address(lhs_value_str, operator, rhs_literal),
        "string" | "bytes" | "bytes32" => compare_string(lhs_value_str, operator, rhs_literal),
        "bool" => compare_boolean(lhs_value_str, operator, rhs_literal),
        _ => Err(EvaluationError::TypeMismatch(format!(
            "Unsupported parameter kind for comparison: {lhs_kind_str}",
        ))),
    }
}

/// Compares two JSON array values based on the specified operator.
/// # Arguments
/// * `lhs_json_array_str` - The string representation of the left-hand side JSON array.
/// * `operator` - The comparison operator to use.
/// * `rhs_literal` - The right-hand side literal value to compare against, expected to be a string representation of a JSON array.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches, unsupported operators, or parsing errors.
fn compare_array(
    lhs_json_array_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?lhs_json_array_str, ?operator, ?rhs_literal, "Comparing array values");

    let rhs_target_str = match rhs_literal {
        LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for 'array' comparison, found: {rhs_literal:?}"
            )));
        }
    };

    match operator {
        ComparisonOperator::Eq | ComparisonOperator::Ne => {
            let lhs_json = serde_json::from_str::<JsonValue>(lhs_json_array_str)
                .map_err(|e| EvaluationError::ParseError(e.to_string()))?;
            let rhs_json = serde_json::from_str::<JsonValue>(rhs_target_str)
                .map_err(|e| EvaluationError::ParseError(e.to_string()))?;

            if !lhs_json.is_array() || !rhs_json.is_array() {
                return Err(EvaluationError::TypeMismatch(
                    "Both sides of an array comparison must be valid JSON arrays.".to_string(),
                ));
            }

            let are_equal = lhs_json == rhs_json;
            Ok(if *operator == ComparisonOperator::Eq { are_equal } else { !are_equal })
        }
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Operator {operator:?} not supported for 'array' type. Supported: Eq, Ne.",
        ))),
    }
}

/// Compares two U256 values based on the specified operator.
/// # Arguments
/// * `left_str` - The string representation of the left-hand side U256 value.
/// * `operator` - The comparison operator to use.
/// * `right_literal` - The right-hand side literal value to compare against, expected to be a string or number literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches, or parsing errors.
fn compare_u256(
    left_str: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?left_str, ?operator, ?right_literal, "Comparing U256 values");

    let left = string_to_u256(left_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{left_str}' as U256: {e}"))
    })?;

    let right_str = match right_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for U256 comparison, found: {right_literal:?}"
            )))
        }
    };

    let right = string_to_u256(right_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{right_str}' as U256: {e}"))
    })?;

    Ok(compare_ordered_values(&left, operator, &right))
}

/// Compares two I256 values based on the specified operator.
/// # Arguments
/// * `left_str` - The string representation of the left-hand side I256 value.
/// * `operator` - The comparison operator to use.
/// * `right_literal` - The right-hand side literal value to compare against, expected to be a string or number literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches, or parsing errors.
fn compare_i256(
    left_str: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?left_str, ?operator, ?right_literal, "Comparing I256 values");

    let left = string_to_i256(left_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{left_str}' as I256: {e}"))
    })?;

    let right_str = match right_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for I256 comparison, found: {right_literal:?}"
            )))
        }
    };

    let right = string_to_i256(right_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{right_str}' as I256: {e}"))
    })?;

    Ok(compare_ordered_values(&left, operator, &right))
}

/// Compares two address values based on the specified operator.
/// # Arguments
/// * `left` - The string representation of the left-hand side address value.
/// * `operator` - The comparison operator to use.
/// * `right_literal` - The right-hand side literal value to compare against, expected to be a string literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches or unsupported operators
fn compare_address(
    left: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?left, ?operator, ?right_literal, "Comparing address values");

    let right = match right_literal {
        LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for address comparison, found: {right_literal:?}"
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(are_same_address(left, right)),
        ComparisonOperator::Ne => Ok(!are_same_address(left, right)),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Unsupported operator {operator:?} for address type"
        ))),
    }
}

/// Compares two string values based on the specified operator.
/// # Arguments
/// * `lhs_str` - The string representation of the left-hand side value.
/// * `operator` - The comparison operator to use.
/// * `rhs_literal` - The right-hand side literal value to compare against, expected to be a string literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches or unsupported operators
fn compare_string(
    lhs_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?lhs_str, ?operator, ?rhs_literal, "Comparing string values");

    let left = lhs_str.to_lowercase();
    let right = match rhs_literal {
        LiteralValue::Str(s) => s.to_lowercase(),
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for string comparison, found: {rhs_literal:?}"
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(left == right),
        ComparisonOperator::Ne => Ok(left != right),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Operator {operator:?} not supported for type String",
        ))),
    }
}

/// Compares two fixed point values based on the specified operator.
/// # Arguments
/// * `lhs_str` - The string representation of the left-hand side fixed point value.
/// * `operator` - The comparison operator to use.
/// * `rhs_literal` - The right-hand side literal value to compare against, expected to be a string or number literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches, or parsing errors.
fn compare_fixed_point(
    lhs_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?lhs_str, ?operator, ?rhs_literal, "Comparing fixed point values");

    let left_decimal = Decimal::from_str(lhs_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{lhs_str}' as Decimal: {e}"))
    })?;

    let rhs_str = match rhs_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for Decimal comparison, found: {rhs_literal:?}"
            )))
        }
    };

    let right_decimal = Decimal::from_str(rhs_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{rhs_str}' as Decimal: {e}"))
    })?;

    Ok(compare_ordered_values(&left_decimal, operator, &right_decimal))
}

/// Compares two boolean values based on the specified operator.
/// # Arguments
/// * `lhs_value_str` - The string representation of the left-hand side boolean value.
/// * `operator` - The comparison operator to use.
/// * `rhs_literal` - The right-hand side literal value to compare against, expected to be a boolean literal.
/// # Returns
/// * `Ok(bool)` - The result of the comparison, true if the condition is satisfied, false otherwise.
/// * `Err(EvaluationError)` - An error if the comparison fails due to type mismatches or parsing errors.
fn compare_boolean(
    lhs_value_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?lhs_value_str, ?operator, ?rhs_literal, "Comparing boolean values");

    let lhs = lhs_value_str.parse::<bool>().map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{lhs_value_str}' as bool: {e}"))
    })?;
    let rhs = match rhs_literal {
        LiteralValue::Bool(b) => *b,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected bool literal for Bool comparison, found: {rhs_literal:?}",
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(lhs == rhs),
        ComparisonOperator::Ne => Ok(lhs != rhs),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Unsupported operator {operator:?} for Bool comparison",
        ))),
    }
}

/// Evaluates an arithmetic expression string against the provided data.
/// This is used for computed columns where the value is a formula like "$amount / $total".
///
/// # Arguments
/// * `expr_str` - The arithmetic expression string to evaluate (e.g., "$value * 2", "$amount + $fee").
/// * `data` - The JSON data containing variable values.
///
/// # Returns
/// * `Ok(ComputedValue)` - The computed result (U256 for numeric, String for non-numeric).
/// * `Err(EvaluationError)` - An error if parsing or evaluation fails.
pub fn evaluate_arithmetic(
    expr_str: &str,
    data: &JsonValue,
) -> Result<ComputedValue, EvaluationError> {
    use super::parsing::parse_arithmetic_expression;

    let expr = parse_arithmetic_expression(expr_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse expression '{}': {}", expr_str, e))
    })?;

    let result = evaluate_arithmetic_expr(&expr, data)?;

    match result {
        ArithmeticValue::Number(n) => Ok(ComputedValue::U256(n)),
        ArithmeticValue::Other { value, .. } => Ok(ComputedValue::String(value)),
    }
}

/// Determines the kind of a JSON value based on its content.
/// # Arguments
/// * `value` - The JSON value to analyze.
/// # Returns
/// * `String` - The kind of the value, such as "address", "bytes32", "fixed", "number", etc.
fn get_kind_from_json_value(value: &JsonValue) -> String {
    tracing::debug!(?value, "Determining kind from JSON value");

    match value {
        JsonValue::String(s) => {
            let s_lower = s.to_lowercase();
            if s_lower.starts_with("0x")
                && s.len() == 42
                && s.chars().skip(2).all(|c| c.is_ascii_hexdigit())
            {
                "address".to_string()
            } else if s_lower.starts_with("0x") && s.chars().skip(2).all(|c| c.is_ascii_hexdigit())
            {
                if s.len() == 66 {
                    "bytes32".to_string()
                } else {
                    "bytes".to_string()
                }
            } else if Decimal::from_str(s).is_ok() && s.contains('.') {
                "fixed".to_string()
            } else if string_to_u256(s).is_ok() {
                "number".to_string()
            } else if string_to_i256(s).is_ok() {
                "int256".to_string()
            } else {
                "string".to_string()
            }
        }
        JsonValue::Number(n) => {
            if n.is_f64() || n.to_string().contains('.') {
                "fixed".to_string()
            } else if n.is_i64() && n.as_i64().unwrap_or(0) < 0 {
                "int64".to_string()
            } else {
                "number".to_string()
            }
        }
        JsonValue::Bool(_) => "bool".to_string(),
        JsonValue::Array(_) => "array".to_string(),
        JsonValue::Object(_) => "map".to_string(),
        JsonValue::Null => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::filter::parsing::parse;
    use serde_json::json;

    #[test]
    fn test_evaluate_simple_condition_ok() {
        let data = json!({"age": 30, "name": "Alice"});
        let expr = parse("age > 25").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_evaluate_simple_condition_fail() {
        let data = json!({"age": 20});
        let expr = parse("age > 25").unwrap();
        assert!(!evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_evaluate_logical_and() {
        let data = json!({"age": 30, "city": "Berlin"});
        let expr = parse("age > 25 && city == 'Berlin'").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        let expr_fail = parse("age < 25 && city == 'Berlin'").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_evaluate_logical_or() {
        let data = json!({"age": 20, "city": "Paris"});
        let expr = parse("age > 25 || city == 'Paris'").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        let expr_fail = parse("age > 25 || city == 'Berlin'").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_evaluate_nested_path() {
        let data = json!({
            "user": {
                "details": {
                    "age": 42,
                    "tags": ["a", "b"]
                }
            }
        });
        let expr = parse("user.details.age == 42").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        let expr_index = parse("user.details.tags[0] == 'a'").unwrap();
        assert!(evaluate(&expr_index, &data).unwrap());
    }

    #[test]
    fn test_variable_not_found() {
        let data = json!({"age": 10});
        let expr = parse("name == 'Alice'").unwrap();
        let result = evaluate(&expr, &data);
        assert!(matches!(result, Err(EvaluationError::VariableNotFound(_))));
    }

    #[test]
    fn test_type_mismatch_error() {
        let data = json!({"age": "ten"}); // age is a string
        let expr = parse("age > 5").unwrap(); // comparing string to number
        let result = evaluate(&expr, &data);
        assert!(matches!(result, Err(EvaluationError::TypeMismatch(_))));
    }

    #[test]
    fn test_evaluate_large_unsigned_integer_string() {
        let data = json!({"value": "1996225771303743351"});
        let expr = parse("value > 1000000000000000000").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        let expr_fail = parse("value < 1000000000000000000").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_evaluate_signed_integer_string() {
        let data = json!({"value": "-50"});
        let expr = parse("value < 0").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        let expr_fail = parse("value > 0").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_evaluate_non_numeric_string_is_not_a_number() {
        let data = json!({"value": "hello_world"});
        let expr = parse("value == 'hello_world'").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        // This should fail because "hello_world" cannot be compared to a number
        let expr_fail = parse("value > 100").unwrap();
        assert!(matches!(evaluate(&expr_fail, &data), Err(EvaluationError::TypeMismatch(_))));
    }

    #[test]
    fn test_get_kind_from_json_value_various_types() {
        assert_eq!(
            get_kind_from_json_value(&json!("0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B")),
            "address"
        );
        assert_eq!(
            get_kind_from_json_value(&json!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            "bytes32"
        );
        assert_eq!(get_kind_from_json_value(&json!("0x1234")), "bytes");
        assert_eq!(get_kind_from_json_value(&json!("123.45")), "fixed");
        assert_eq!(get_kind_from_json_value(&json!("12345678901234567890")), "number");
        assert_eq!(get_kind_from_json_value(&json!("-123")), "int256");
        assert_eq!(get_kind_from_json_value(&json!("hello world")), "string");
        assert_eq!(get_kind_from_json_value(&json!(123)), "number");
        assert_eq!(get_kind_from_json_value(&json!(-123)), "int64");
        assert_eq!(get_kind_from_json_value(&json!(123.45)), "fixed");
        assert_eq!(get_kind_from_json_value(&json!(true)), "bool");
        assert_eq!(get_kind_from_json_value(&json!([1, 2])), "array");
        assert_eq!(get_kind_from_json_value(&json!({"a": 1})), "map");
        assert_eq!(get_kind_from_json_value(&json!(null)), "null");
    }

    #[test]
    fn test_resolve_path_errors() {
        let data = json!({ "user": { "tags": ["a", "b"] } });
        // Index out of bounds
        let expr_idx = parse("user.tags[2] == 'c'").unwrap();
        assert!(matches!(evaluate(&expr_idx, &data), Err(EvaluationError::IndexOutOfBounds(_))));

        // Key accessor on array
        let expr_key = parse("user.tags.key == 'a'").unwrap();
        assert!(matches!(evaluate(&expr_key, &data), Err(EvaluationError::TypeMismatch(_))));

        // Index accessor on object
        let expr_obj = parse("user[0] == 'a'").unwrap();
        assert!(matches!(evaluate(&expr_obj, &data), Err(EvaluationError::TypeMismatch(_))));
    }

    #[test]
    fn test_compare_address_case_insensitivity() {
        let data = json!({ "owner": "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B" });
        let expr = parse("owner == '0xab5801a7d398351b8be11c439e05c5b3259aec9b'").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_unsupported_operators() {
        // For string
        let data_str = json!({ "value": "hello" });
        let expr_str = parse("value > 'world'").unwrap();
        assert!(matches!(
            evaluate(&expr_str, &data_str),
            Err(EvaluationError::UnsupportedOperator(_))
        ));

        // For boolean
        let data_bool = json!({ "value": true });
        let expr_bool = parse("value > false").unwrap();
        assert!(matches!(
            evaluate(&expr_bool, &data_bool),
            Err(EvaluationError::UnsupportedOperator(_))
        ));
    }

    #[test]
    fn test_compare_array() {
        let data = json!({ "values": [1, 2, 3] });
        let expr_eq = parse("values == '[1, 2, 3]'").unwrap();
        assert!(evaluate(&expr_eq, &data).unwrap());

        let expr_ne = parse("values != '[1, 2, 4]'").unwrap();
        assert!(evaluate(&expr_ne, &data).unwrap());

        let expr_fail = parse("values == '[1, 2, 4]'").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_compare_fixed_point() {
        let data = json!({ "price": "123.45" });
        let expr_gt = parse("price > 123.4").unwrap();
        assert!(evaluate(&expr_gt, &data).unwrap());

        let expr_lte = parse("price <= 123.45").unwrap();
        assert!(evaluate(&expr_lte, &data).unwrap());

        let expr_ne = parse("price != 123.456").unwrap();
        assert!(evaluate(&expr_ne, &data).unwrap());
    }

    // --- Tests for Error Messages ---

    #[test]
    fn test_variable_not_found_error_message() {
        let data = json!({});
        let expr = parse("non_existent_var == 1").unwrap();
        let err = evaluate(&expr, &data).unwrap_err();
        // Error message includes $ prefix for event variables
        assert_eq!(err.to_string(), "Variable not found: $non_existent_var");
    }

    #[test]
    fn test_index_out_of_bounds_error_message() {
        let data = json!({ "arr": [0, 1] });
        let expr = parse("arr[2] == 0").unwrap();
        let err = evaluate(&expr, &data).unwrap_err();
        assert_eq!(err.to_string(), "Index out of bounds: 2");
    }

    #[test]
    fn test_type_mismatch_on_accessor_error_message() {
        let data = json!({ "arr": [0, 1] });
        let expr = parse("arr.key == 0").unwrap();
        let err = evaluate(&expr, &data).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Type mismatch: Cannot apply accessor Key(\"key\") to value Array [Number(0), Number(1)]"
        );
    }

    #[test]
    fn test_unsupported_operator_error_message() {
        let data = json!({ "value": "hello" });
        let expr = parse("value > 'world'").unwrap();
        let err = evaluate(&expr, &data).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unsupported operator: Operator Gt not supported for type String"
        );
    }

    #[test]
    fn test_parse_error_message() {
        let data = json!({ "value": 123 });
        let expr = parse("value > 'not-a-number'").unwrap();
        let err = evaluate(&expr, &data).unwrap_err();
        assert!(err
            .to_string()
            .contains("Parse error: Failed to parse RHS 'not-a-number' as U256"));
    }

    // --- Tests for Arithmetic Expressions ---

    #[test]
    fn test_arithmetic_addition() {
        let data = json!({"a": 10, "b": 20});
        // 10 + 20 > 25 = 30 > 25 = true
        let expr = parse("a + b > 25").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        // 10 + 20 > 35 = 30 > 35 = false
        let expr_fail = parse("a + b > 35").unwrap();
        assert!(!evaluate(&expr_fail, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_subtraction() {
        let data = json!({"total": 100, "discount": 20});
        // 100 - 20 == 80 = true
        let expr = parse("total - discount == 80").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_multiplication() {
        let data = json!({"price": 10, "quantity": 5});
        // 10 * 5 >= 50 = true
        let expr = parse("price * quantity >= 50").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_division() {
        let data = json!({"total": 100, "count": 4});
        // 100 / 4 == 25 = true
        let expr = parse("total / count == 25").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_precedence() {
        let data = json!({"a": 10, "b": 5, "c": 2});
        // 10 + 5 * 2 = 10 + 10 = 20 (multiplication before addition)
        let expr = parse("a + b * c == 20").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        // (10 + 5) * 2 = 15 * 2 = 30 (parentheses override precedence)
        let expr_parens = parse("(a + b) * c == 30").unwrap();
        assert!(evaluate(&expr_parens, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_with_literals() {
        let data = json!({"value": 100});
        // 100 * 2 > 150 = 200 > 150 = true
        let expr = parse("value * 2 > 150").unwrap();
        assert!(evaluate(&expr, &data).unwrap());

        // 100 + 50 < 200 = 150 < 200 = true
        let expr2 = parse("value + 50 < 200").unwrap();
        assert!(evaluate(&expr2, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_both_sides() {
        let data = json!({"a": 10, "b": 5, "c": 3, "d": 6});
        // a + b > c * d = 15 > 18 = false
        let expr = parse("a + b > c * d").unwrap();
        assert!(!evaluate(&expr, &data).unwrap());

        // a * b == c * d + 32 = 50 == 18 + 32 = 50 == 50 = true
        let expr2 = parse("a * b == c * d + 32").unwrap();
        assert!(evaluate(&expr2, &data).unwrap());
    }

    #[test]
    fn test_arithmetic_with_logical() {
        let data = json!({"value": 100, "threshold": 50, "active": true});
        // (value > threshold * 1) && active == true = (100 > 50) && true = true
        let expr = parse("value > threshold * 1 && active == true").unwrap();
        assert!(evaluate(&expr, &data).unwrap());
    }
}
