use ethers::types::U64;
use regex::Regex;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Template {
    value: String,
}

impl Template {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn parse_template_inline(&self, event_data: &Value) -> String {
        let mut template = self.value.clone();
        let placeholders = self.extract_placeholders(&template);

        for placeholder in placeholders {
            if placeholder.contains('(') {
                if let Some(value) = self.evaluate_function(&placeholder, event_data) {
                    template = template.replace(&format!("{{{{{}}}}}", placeholder), &value);
                }
            } else if let Some(value) = self.get_nested_value(event_data, &placeholder) {
                template = template.replace(&format!("{{{{{}}}}}", placeholder), &value);
            }
        }
        template
    }

    fn extract_placeholders(&self, template: &str) -> Vec<String> {
        let mut placeholders = Vec::new();
        let mut start = 0;
        while let Some(start_index) = template[start..].find("{{") {
            if let Some(end_index) = template[start + start_index + 2..].find("}}") {
                let placeholder =
                    &template[start + start_index + 2..start + start_index + 2 + end_index];
                placeholders.push(placeholder.to_string());
                start += start_index + 2 + end_index + 2;
            } else {
                break;
            }
        }
        placeholders
    }

    fn get_nested_value(&self, data: &Value, path: &str) -> Option<String> {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = data;
        for key in keys {
            if let Some(value) = current.get(key) {
                current = value;
            } else {
                return None;
            }
        }
        Some(current.to_string().replace('"', ""))
    }

    fn evaluate_function(&self, function_call: &str, event_data: &Value) -> Option<String> {
        let re = Regex::new(r"(\w+)\(([^)]+)\)").unwrap();
        if let Some(captures) = re.captures(function_call) {
            let function_name = &captures[1];
            let args: Vec<&str> = captures[2].split(',').map(|s| s.trim()).collect();
            if function_name == "format_value" && args.len() == 2 {
                if let Some(value_str) = self.get_nested_value(event_data, args[0]) {
                    if let Ok(decimals) = args[1].parse::<u32>() {
                        return Some(self.format_value(&value_str, decimals));
                    }
                }
            }
        }
        None
    }

    fn format_value(&self, value: &str, decimals: u32) -> String {
        match U64::from_dec_str(value) {
            Ok(v) => {
                let divisor = U64::from(10).pow(decimals.into());
                let integer_part = v / divisor;
                let fractional_part = v % divisor;
                if fractional_part.is_zero() {
                    return integer_part.to_string();
                }
                format!("{}.{}", integer_part, fractional_part)
            }
            Err(_) => value.to_string(),
        }
    }
}
