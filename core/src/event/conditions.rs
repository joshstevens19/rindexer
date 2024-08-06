use ethers::prelude::U64;
use serde_json::{Map, Value};

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

#[allow(clippy::manual_strip)]
fn evaluate_condition(value: &Value, condition: &str) -> bool {
    if condition.contains("||") ||
        condition.contains("&&") ||
        condition.contains('>') ||
        condition.contains('<') ||
        condition.contains('=')
    {
        let parts: Vec<&str> = condition.split("||").collect();
        for part in parts {
            let subparts: Vec<&str> = part.split("&&").collect();
            let mut and_result = true;
            for subpart in subparts {
                let (op, comp) = if subpart.starts_with(">=") {
                    (">=", &subpart[2..])
                } else if subpart.starts_with("<=") {
                    ("<=", &subpart[2..])
                } else if subpart.starts_with(">") {
                    (">", &subpart[1..])
                } else if subpart.starts_with("<") {
                    ("<", &subpart[1..])
                } else if subpart.starts_with("=") {
                    ("=", &subpart[1..])
                } else {
                    ("", subpart)
                };

                and_result &= match op {
                    ">=" => {
                        U64::from_str_radix(value.as_str().unwrap_or("0"), 10).unwrap_or_default() >=
                            U64::from_str_radix(comp, 10).unwrap_or_default()
                    }
                    "<=" => {
                        U64::from_str_radix(value.as_str().unwrap_or("0"), 10).unwrap_or_default() <=
                            U64::from_str_radix(comp, 10).unwrap_or_default()
                    }
                    ">" => {
                        U64::from_str_radix(value.as_str().unwrap_or("0"), 10).unwrap_or_default() >
                            U64::from_str_radix(comp, 10).unwrap_or_default()
                    }
                    "<" => {
                        U64::from_str_radix(value.as_str().unwrap_or("0"), 10).unwrap_or_default() <
                            U64::from_str_radix(comp, 10).unwrap_or_default()
                    }
                    "=" => value == &Value::String(comp.to_string()),
                    "" => value == &Value::String(subpart.to_string()),
                    _ => false,
                };
            }
            if and_result {
                return true;
            }
        }
        false
    } else {
        value == &Value::String(condition.to_string())
    }
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
