use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum GenerateOperationsError {
    #[error("File system error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema generation error: {0}")]
    SchemaGeneration(String),
}

fn generate_query(name: &str, fields: &[String]) -> String {
    let base_name = name.trim_start_matches("all");
    let condition_type = format!("{}Condition", &base_name[..base_name.len() - 1]);
    let order_by_type = format!("{base_name}OrderBy");

    // yes it is meant to be formatted like the below to make the graphql query readable
    let args = if name.starts_with("all") {
        format!(
            r#"$after: Cursor,
    $first: Int = 50,
    $condition: {condition_type} = {{}},
    $orderBy: [{order_by_type}!] = BLOCK_NUMBER_DESC"#
        )
    } else {
        "$nodeId: ID!".to_string()
    };

    // yes it is meant to be formatted like the below to make the graphql query readable
    if name.starts_with("all") {
        format!(
            r#"query {}Query(
    {}
) {{
    {}(
        first: $first,
        after: $after,
        condition: $condition,
        orderBy: $orderBy
    ) {{
        nodes {{
            {}
        }}
        pageInfo {{
            endCursor
            hasNextPage
            hasPreviousPage
            startCursor
        }}
    }}
}}"#,
            name,
            args,
            name,
            fields.join("\n            ")
        )
    } else {
        format!(
            r#"query {}Query({}) {{
    {}(nodeId: $nodeId) {{
        {}
    }}
}}"#,
            name,
            args,
            name,
            fields.join("\n        ")
        )
    }
}

fn extract_node_fields(singular_type_name: &str, schema: &Value) -> Vec<String> {
    if let Some(types) = schema["types"].as_array() {
        for type_obj in types {
            if let Some(type_name) = type_obj["name"].as_str() {
                if type_name.eq_ignore_ascii_case(singular_type_name) {
                    if let Some(fields) = type_obj["fields"].as_array() {
                        return fields
                            .iter()
                            .filter_map(|field| field["name"].as_str().map(|s| s.to_string()))
                            .collect();
                    }
                }
            }
        }
    }
    vec![]
}

pub fn generate_operations(
    schema: &Value,
    generate_path: &Path,
) -> Result<(), GenerateOperationsError> {
    let queries_path = generate_path.join("queries");
    fs::create_dir_all(&queries_path)?;

    let types = schema["types"].as_array().ok_or_else(|| {
        GenerateOperationsError::SchemaGeneration("Invalid schema format".to_string())
    })?;

    for type_obj in types {
        if let Some(type_name) = type_obj["name"].as_str() {
            if type_name == "Query" {
                let fields = type_obj["fields"].as_array().ok_or_else(|| {
                    GenerateOperationsError::SchemaGeneration("Invalid fields format".to_string())
                })?;
                for field in fields {
                    if let Some(field_name) = field["name"].as_str() {
                        let is_paged_query = field_name.starts_with("all");
                        let mut singular_type_name = field_name.trim_start_matches("all");
                        if is_paged_query {
                            singular_type_name =
                                &singular_type_name[..singular_type_name.len() - 1];
                        }

                        let node_fields = extract_node_fields(singular_type_name, schema);
                        if node_fields.is_empty() {
                            continue;
                        }

                        let query = generate_query(field_name, &node_fields);

                        let file_path = queries_path.join(format!("{field_name}.graphql"));

                        let mut file = File::create(file_path)?;
                        file.write_all(query.as_bytes())?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_generate_query_single() {
        let query = generate_query("node", &["id".to_string(), "name".to_string()]);
        let expected = r#"query nodeQuery($nodeId: ID!) {
    node(nodeId: $nodeId) {
        id
        name
    }
}"#;
        assert_eq!(query, expected);
    }

    #[test]
    fn test_extract_node_fields() {
        let schema = json!({
            "types": [
                {
                    "name": "Node",
                    "fields": [
                        {"name": "id"},
                        {"name": "name"}
                    ]
                }
            ]
        });

        let fields = extract_node_fields("Node", &schema);
        assert_eq!(fields, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_extract_node_fields_case_insensitive() {
        let schema = json!({
            "types": [
                {
                    "name": "node",
                    "fields": [
                        {"name": "id"},
                        {"name": "name"}
                    ]
                }
            ]
        });

        let fields = extract_node_fields("Node", &schema);
        assert_eq!(fields, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_extract_node_fields_not_found() {
        let schema = json!({
            "types": [
                {
                    "name": "Node",
                    "fields": [
                        {"name": "id"},
                        {"name": "name"}
                    ]
                }
            ]
        });

        let fields = extract_node_fields("NonExistentNode", &schema);
        assert!(fields.is_empty());
    }
}
