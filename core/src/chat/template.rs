use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Template {
    value: String,
}

impl Template {
    pub fn new(value: String) -> Self {
        Self { value }
    }
}

impl Template {
    pub fn parse_template_inline(&self, event_data: &Value) -> String {
        let mut template = self.value.clone();
        let placeholders = self.extract_placeholders(&template);

        for placeholder in placeholders {
            if let Some(value) = self.get_nested_value(event_data, &placeholder) {
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
}
