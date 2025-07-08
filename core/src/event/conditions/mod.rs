pub mod ast;
pub mod evaluation;
pub mod helpers;
pub mod parsing;

use self::evaluation::EvaluationError;
use serde_json::{json, Map, Value};
use thiserror::Error;
use winnow::error::{ContextError, ParseError};

#[derive(Debug, Error)]
pub enum ConditionError<'a> {
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
pub fn evaluate_expression<'a>(
    expression_str: &'a str,
    data: &Value,
) -> Result<bool, ConditionError<'a>> {
    let parsed_expr = parsing::parse(expression_str).map_err(ConditionError::Parse)?;
    evaluation::evaluate(&parsed_expr, data).map_err(ConditionError::from)
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
    if !condition.contains(['&', '|', '>', '<', '=']) {
        return value == &Value::String(condition.to_string());
    }

    // Replicate the old splitting logic,
    // but use the new evaluation engine for each part.
    let parts: Vec<&str> = condition.split("||").collect();
    for part in parts {
        let subparts: Vec<&str> = part.split("&&").collect();
        let mut and_result = true;
        for subpart in subparts {
            // Construct a valid expression for the new engine.
            let expr_str = format!("_placeholder_{}", subpart.trim());
            let context = json!({ "_placeholder_": value });

            // Evaluate the sub-expression
            if !evaluate_expression(&expr_str, &context).unwrap_or(false) {
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
