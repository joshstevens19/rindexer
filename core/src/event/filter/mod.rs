pub mod ast;
pub mod evaluation;
pub mod helpers;
pub mod parsing;

use self::evaluation::EvaluationError;
use serde_json::{json, Map, Value};
use thiserror::Error;
use winnow::error::{ContextError, ParseError};

#[derive(Debug, Error)]
pub enum FilterError<'a> {
    #[error("Failed to parse expression: {0}")]
    Parse(ParseError<&'a str, ContextError>),

    #[error("Failed to evaluate expression: {0}")]
    Evaluation(#[from] EvaluationError),
}

/// Evaluates a filter expression against a given JSON object.
///
/// This is the main public entry point for the new conditions module. It orchestrates
/// the parsing of the expression string and the evaluation of the resulting AST
/// against the provided data.
pub fn filter_by_expression<'a>(
    expression_str: &'a str,
    data: &Value,
) -> Result<bool, FilterError<'a>> {
    let parsed_expr = parsing::parse(expression_str).map_err(FilterError::Parse)?;
    evaluation::evaluate(&parsed_expr, data).map_err(FilterError::from)
}

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

fn evaluate_condition(value: &Value, condition: &str) -> bool {
    // If the condition is a simple string, do a direct comparison
    if !condition.contains(['&', '|', '>', '<', '=']) {
        return match value {
            Value::String(s) => s == condition,
            Value::Number(n) => n.to_string() == condition,
            _ => false,
        };
    }

    // For complex expressions, use the new evaluation engine
    let parts: Vec<&str> = condition.split("||").collect();
    for part in parts {
        let subparts: Vec<&str> = part.split("&&").collect();
        let mut and_result = true;
        for subpart in subparts {
            // Construct a valid expression for the new engine.
            let expr_str = format!("_placeholder_{}", subpart.trim());
            let context = json!({ "_placeholder_": value });

            // Evaluate the sub-expression
            if !filter_by_expression(&expr_str, &context).unwrap_or(false) {
                and_result = false;
                break;
            }
        }
        if and_result {
            return true;
        }
    }

    false
}

pub fn filter_event_data_by_conditions(
    event_data: &Value,
    conditions: &Vec<Map<String, Value>>,
) -> bool {
    for condition in conditions {
        for (key, value) in condition {
            if let Some(event_value) = get_nested_value(event_data, key) {
                if !evaluate_condition(&event_value, value.as_str().unwrap_or("")) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
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
}
