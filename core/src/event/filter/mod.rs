//! This module provides functionality for filtering JSON data based on expressions and conditions.
//! It includes parsing of expressions, evaluation against JSON objects, and utility functions for nested value retrieval.

pub mod ast;
pub mod evaluation;
pub mod helpers;
pub mod parsing;

use self::evaluation::EvaluationError;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Map, Value};
use thiserror::Error;
use winnow::error::{ContextError, ParseError};

/// This regex finds logical operators (&&, ||) possibly surrounded by whitespace.
static LOGICAL_OPERATOR_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s*(&&|\|\|)\s*").unwrap());

/// Error type for the filter module, encapsulating parsing and evaluation errors.
#[derive(Debug, Error)]
pub enum FilterError<'a> {
    /// Error encountered during parsing of the expression.
    #[error("Failed to parse expression: {0}")]
    Parse(ParseError<&'a str, ContextError>),

    /// Error encountered during evaluation of the expression.
    #[error("Failed to evaluate expression: {0}")]
    Evaluation(#[from] EvaluationError),
}

/// Evaluates a filter expression against a given JSON object.
///
/// This is the main public entry point for the new conditions module. It orchestrates
/// the parsing of the expression string and the evaluation of the resulting AST
/// against the provided data.
///
/// # Arguments
/// * `expression_str` - A string representing the filter expression to be evaluated.
/// * `data` - A JSON object (serde_json::Value) against which the expression will be evaluated.
/// # Returns
/// * `Ok(bool)` - If the expression evaluates successfully, returns `true` if the expression matches the data, or `false` otherwise.
/// * `Err(FilterError)` - If there is an error in parsing the expression or evaluating it against the data, returns a `FilterError`.
pub fn filter_by_expression<'a>(
    expression_str: &'a str,
    data: &Value,
) -> Result<bool, FilterError<'a>> {
    let parsed_expr = parsing::parse(expression_str).map_err(FilterError::Parse)?;
    evaluation::evaluate(&parsed_expr, data).map_err(FilterError::from)
}

/// Retrieves a nested value from a JSON object using a dot-separated path.
/// If the path does not exist, it returns `None`.
///
/// # Arguments
/// * `data` - A JSON object (serde_json::Value) from which to retrieve the value.
/// * `path` - A dot-separated string representing the path to the desired value.
/// # Returns
/// * `Some(Value)` - If the value exists at the specified path, returns the value.
/// * `None` - If the path does not exist in the JSON object.
fn get_nested_value(data: &Value, path: &str) -> Option<Value> {
    let keys: Vec<&str> = path.split('.').collect();
    let mut current = data;
    for key in keys {
        match current.get(key) {
            Some(value) => current = value,
            None => return None,
        }
    }
    Some(current.clone())
}
/// This function is a compatibility layer to support the legacy `conditions` format.
/// It translates the legacy format into a modern filter expression to ensure
/// correct operator precedence (e.g., for `> 5 && < 10 || == 0`).
///
/// While it now correctly handles complex expressions, it is still recommended
/// to use the `filter_expression` field directly for new implementations, as it
/// avoids this translation step and is more explicit.
///
/// # Arguments
/// * `value` - The value to evaluate against the condition.
/// * `condition` - A string representing the legacy condition to evaluate (e.g., `> 10 && != 20`).
///
/// # Returns
/// * `bool` - Returns `true` if the value satisfies the condition, `false` otherwise.
fn evaluate_condition(value: &Value, condition: &str) -> bool {
    tracing::debug!(?value, ?condition, "Evaluating legacy condition");

    // Fast path for simple equality checks where no complex operators are present.
    if !condition.contains(['&', '|', '>', '<', '=', '!']) {
        return match value {
            Value::String(s) => s == condition,
            Value::Number(n) => n.to_string() == condition,
            Value::Bool(b) => b.to_string() == condition,
            Value::Null => "null" == condition,
            _ => false,
        };
    }

    // For complex expressions, reconstruct the legacy format into a valid
    // expression for the new evaluation engine.
    //
    // Legacy format: `> 5 && < 10`
    // Target format: `_placeholder_ > 5 && _placeholder_ < 10`
    //
    // The regex finds `&&` or `||` and replaces it with ` [OPERATOR] _placeholder_ `,
    // effectively inserting the subject of the expression for each subsequent clause.
    let reconstructed_after_ops =
        LOGICAL_OPERATOR_RE.replace_all(condition.trim(), " $1 _placeholder_ ");

    // Prepend the placeholder for the very first clause.
    let final_expr = format!("_placeholder_ {reconstructed_after_ops}");
    let context = json!({ "_placeholder_": value });

    tracing::debug!(expression = final_expr, ?context, "Evaluating reconstructed expression");

    // Delegate to the proper engine.
    // If evaluation fails, default to `false`.
    filter_by_expression(&final_expr, &context).unwrap_or(false)
}

/// Filters event data based on a set of conditions.
/// This function checks if the event data matches all conditions specified in the `conditions` vector.
///
/// # Arguments
/// * `event_data` - A JSON object (serde_json::Value) representing the event data to be filtered.
/// * `conditions` - A vector of maps, where each map contains key-value pairs representing the conditions to be checked against the event data.
/// # Returns
/// * `bool` - Returns `true` if the event data matches all conditions, `false` otherwise.
pub fn filter_event_data_by_conditions(
    event_data: &Value,
    conditions: &Vec<Map<String, Value>>,
) -> bool {
    if conditions.is_empty() {
        return true;
    }

    tracing::debug!(?event_data, ?conditions, "Filtering event data using legacy conditions");

    for condition in conditions {
        for (key, value) in condition {
            if let Some(event_value) = get_nested_value(event_data, key) {
                if !evaluate_condition(&event_value, value.as_str().unwrap_or("")) {
                    tracing::debug!(key, "Condition failed, returning false");
                    return false;
                }
            } else {
                tracing::debug!(key, "Key not found in event data, returning false");
                return false;
            }
        }
    }
    tracing::debug!("All conditions passed, returning true");
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_nested_value() {
        let data = json!({
            "a": {
                "b": {
                    "c": "hello"
                }
            },
            "x": "world"
        });

        assert_eq!(get_nested_value(&data, "a.b.c"), Some(json!("hello")));
        assert_eq!(get_nested_value(&data, "x"), Some(json!("world")));
        assert_eq!(get_nested_value(&data, "a.b"), Some(json!({"c": "hello"})));
        assert_eq!(get_nested_value(&data, "a.x"), None);
        assert_eq!(get_nested_value(&data, "d"), None);
    }

    #[test]
    fn test_evaluate_condition_simple_equality() {
        assert!(evaluate_condition(&json!("hello"), "hello"));
        assert!(!evaluate_condition(&json!("hello"), "world"));
        assert!(evaluate_condition(&json!(123), "123"));
        assert!(!evaluate_condition(&json!(123), "456"));
    }

    #[test]
    fn test_evaluate_condition_complex_logical_expressions() {
        // AND conditions
        assert!(evaluate_condition(&json!(10), ">=10&&<=20"));
        assert!(!evaluate_condition(&json!(5), ">=10&&<=20"));

        // OR conditions
        assert!(evaluate_condition(&json!(5), "<10||>20"));
        assert!(evaluate_condition(&json!(25), "<10||>20"));
        assert!(!evaluate_condition(&json!(15), "<10||>20"));

        // Combined AND and OR
        assert!(evaluate_condition(&json!(5), ">=0&&<=10||>=20&&<=30"));
        assert!(evaluate_condition(&json!(25), ">=0&&<=10||>=20&&<=30"));
        assert!(!evaluate_condition(&json!(15), ">=0&&<=10||>=20&&<=30"));
    }

    #[test]
    fn test_evaluate_condition_correct_precedence() {
        // Test with value = 2.
        // `true || (false && false)` -> `true || false` -> `true`
        assert!(evaluate_condition(&json!(2), "== 2 || >= 10 && <= 20"));

        // Test with value = 15.
        // `false || (true && true)` -> `false || true` -> `true`
        assert!(evaluate_condition(&json!(15), "== 2 || >= 10 && <= 20"));

        // Test with value = 30
        // `false || (true && false)` -> `false || false` -> `false`
        assert!(!evaluate_condition(&json!(30), "== 2 || >= 10 && <= 20"));
    }

    #[test]
    fn test_filter_event_data_by_conditions_simple() {
        let event_data = json!({
            "name": "test",
            "value": 100
        });

        let conditions = vec![json!({
            "name": "test"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let conditions = vec![json!({
            "value": "100"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let conditions = vec![json!({
            "name": "wrong"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(!filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_event_data_by_conditions_nested() {
        let event_data = json!({
            "a": {
                "b": {
                    "c": "hello"
                }
            }
        });

        let conditions = vec![json!({
            "a.b.c": "hello"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let conditions = vec![json!({
            "a.b.c": "world"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(!filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_event_data_by_conditions_multiple() {
        let event_data = json!({
            "name": "test",
            "value": 100
        });

        let conditions = vec![
            json!({"name": "test"}).as_object().unwrap().clone(),
            json!({"value": "100"}).as_object().unwrap().clone(),
        ];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let conditions = vec![
            json!({"name": "test"}).as_object().unwrap().clone(),
            json!({"value": "200"}).as_object().unwrap().clone(),
        ];
        assert!(!filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_event_data_by_conditions_complex_expressions() {
        let event_data = json!({
            "value": 15
        });

        let conditions = vec![json!({
            "value": ">=10&&<=20"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let conditions = vec![json!({
            "value": "<10||>20"
        })
        .as_object()
        .unwrap()
        .clone()];
        assert!(!filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_with_null_value() {
        let event_data = json!({ "key": null });
        let conditions = vec![json!({ "key": "null" }).as_object().unwrap().clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_with_boolean_value() {
        let event_data = json!({ "active": true });
        let conditions = vec![json!({ "active": "true" }).as_object().unwrap().clone()];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));

        let event_data_false = json!({ "active": false });
        let conditions_false = vec![json!({ "active": "false" }).as_object().unwrap().clone()];
        assert!(filter_event_data_by_conditions(&event_data_false, &conditions_false));
    }

    #[test]
    fn test_filter_with_missing_key() {
        let event_data = json!({ "actual_key": "some_value" });
        let conditions =
            vec![json!({ "non_existent_key": "some_value" }).as_object().unwrap().clone()];
        assert!(!filter_event_data_by_conditions(&event_data, &conditions));
    }

    #[test]
    fn test_filter_with_empty_conditions() {
        let event_data = json!({ "key": "value" });
        let conditions: Vec<Map<String, Value>> = vec![];
        assert!(filter_event_data_by_conditions(&event_data, &conditions));
    }
}
