//! This module evaluates a parsed expression AST against a JSON object.

use super::{
    ast::{
        Accessor, ComparisonOperator, Condition, ConditionLeft, Expression, LiteralValue,
        LogicalOperator,
    },
    helpers::{are_same_address, compare_ordered_values, string_to_i256, string_to_u256},
};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum EvaluationError {
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Unsupported operator: {0}")]
    UnsupportedOperator(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(String),
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
pub fn evaluate<'a>(
    expression: &Expression<'a>,
    data: &JsonValue,
) -> Result<bool, EvaluationError> {
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
        Expression::Condition(condition) => evaluate_condition(condition, data),
    }
}

/// Evaluates a single condition.
fn evaluate_condition<'a>(
    condition: &Condition<'a>,
    data: &JsonValue,
) -> Result<bool, EvaluationError> {
    let resolved_value = resolve_path(&condition.left, data)?;

    let final_left_kind = get_kind_from_json_value(resolved_value);
    let final_left_value_str = match resolved_value {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "null".to_string(),
        JsonValue::Array(_) | JsonValue::Object(_) => resolved_value.to_string(),
    };

    compare_final_values(
        &final_left_kind,
        &final_left_value_str,
        &condition.operator,
        &condition.right,
    )
}

/// Resolves a path from the AST against the JSON data.
fn resolve_path<'a>(
    path: &ConditionLeft<'a>,
    data: &'a JsonValue,
) -> Result<&'a JsonValue, EvaluationError> {
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
                    "Cannot apply accessor {:?} to value {:?}",
                    accessor, current
                )))
            }
        };
    }
    Ok(current)
}

/// Routes the comparison to the correct type-specific function.
fn compare_final_values(
    lhs_kind_str: &str,
    lhs_value_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
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
            "Unsupported parameter kind for comparison: {}",
            lhs_kind_str
        ))),
    }
}

// ... (The rest of the file remains the same: compare_array, compare_u256, etc.)
fn compare_array(
    lhs_json_array_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let rhs_target_str = match rhs_literal {
        LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for 'array' comparison, found: {:?}",
                rhs_literal
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
            "Operator {:?} not supported for 'array' type. Supported: Eq, Ne.",
            operator
        ))),
    }
}

fn compare_u256(
    left_str: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let left = string_to_u256(left_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{}' as U256: {}", left_str, e))
    })?;

    let right_str = match right_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for U256 comparison, found: {:?}",
                right_literal
            )))
        }
    };

    let right = string_to_u256(right_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{}' as U256: {}", right_str, e))
    })?;

    compare_ordered_values(&left, operator, &right)
}

fn compare_i256(
    left_str: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let left = string_to_i256(left_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{}' as I256: {}", left_str, e))
    })?;

    let right_str = match right_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for I256 comparison, found: {:?}",
                right_literal
            )))
        }
    };

    let right = string_to_i256(right_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{}' as I256: {}", right_str, e))
    })?;

    compare_ordered_values(&left, operator, &right)
}

fn compare_address(
    left: &str,
    operator: &ComparisonOperator,
    right_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let right = match right_literal {
        LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for address comparison, found: {:?}",
                right_literal
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(are_same_address(left, right)),
        ComparisonOperator::Ne => Ok(!are_same_address(left, right)),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Unsupported operator {:?} for address type",
            operator
        ))),
    }
}

fn compare_string(
    lhs_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let left = lhs_str.to_lowercase();
    let right = match rhs_literal {
        LiteralValue::Str(s) => s.to_lowercase(),
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected string literal for string comparison, found: {:?}",
                rhs_literal
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(left == right),
        ComparisonOperator::Ne => Ok(left != right),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Operator {:?} not supported for type String",
            operator
        ))),
    }
}

fn compare_fixed_point(
    lhs_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let left_decimal = Decimal::from_str(lhs_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse LHS '{}' as Decimal: {}", lhs_str, e))
    })?;

    let rhs_str = match rhs_literal {
        LiteralValue::Number(s) | LiteralValue::Str(s) => *s,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected number or string for Decimal comparison, found: {:?}",
                rhs_literal
            )))
        }
    };

    let right_decimal = Decimal::from_str(rhs_str).map_err(|e| {
        EvaluationError::ParseError(format!("Failed to parse RHS '{}' as Decimal: {}", rhs_str, e))
    })?;

    compare_ordered_values(&left_decimal, operator, &right_decimal)
}

fn compare_boolean(
    lhs_value_str: &str,
    operator: &ComparisonOperator,
    rhs_literal: &LiteralValue<'_>,
) -> Result<bool, EvaluationError> {
    let lhs = lhs_value_str.parse::<bool>().map_err(|e| {
        EvaluationError::ParseError(format!(
            "Failed to parse LHS '{}' as bool: {}",
            lhs_value_str, e
        ))
    })?;
    let rhs = match rhs_literal {
        LiteralValue::Bool(b) => *b,
        _ => {
            return Err(EvaluationError::TypeMismatch(format!(
                "Expected bool literal for Bool comparison, found: {:?}",
                rhs_literal
            )))
        }
    };

    match operator {
        ComparisonOperator::Eq => Ok(lhs == rhs),
        ComparisonOperator::Ne => Ok(lhs != rhs),
        _ => Err(EvaluationError::UnsupportedOperator(format!(
            "Unsupported operator {:?} for Bool comparison",
            operator
        ))),
    }
}

fn get_kind_from_json_value(value: &JsonValue) -> String {
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
    use crate::event::conditions::parsing::parse;
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
}
