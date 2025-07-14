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
        Expression::Condition(condition) => evaluate_condition(condition, data),
    }
}

/// Evaluates a single condition.
fn evaluate_condition<'a>(
    condition: &Condition<'a>,
    data: &JsonValue,
) -> Result<bool, EvaluationError> {
    tracing::debug!(?condition, ?data, "Evaluating condition");

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

/// Routes the comparison to the correct type-specific function.
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

// ... (The rest of the file remains the same: compare_array, compare_u256, etc.)
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

    compare_ordered_values(&left, operator, &right)
}

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

    compare_ordered_values(&left, operator, &right)
}

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

    compare_ordered_values(&left_decimal, operator, &right_decimal)
}

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
        assert_eq!(err.to_string(), "Variable not found: non_existent_var");
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
}
