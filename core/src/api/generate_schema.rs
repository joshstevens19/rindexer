use std::path::Path;

use reqwest::Client;
use serde_json::Value;

use crate::api::generate_operations::{generate_operations, GenerateOperationsError};

#[derive(thiserror::Error, Debug)]
pub enum GenerateGraphqlQueriesError {
    #[error("Network request failed: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Failed to parse JSON: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("File system error: {0}")]
    Io(#[from] std::io::Error),

    #[error("No data in response")]
    NoData,

    #[error("Failed to generate operations: {0}")]
    GenerateOperationsError(#[from] GenerateOperationsError),
}

pub async fn generate_graphql_queries(
    endpoint: &str,
    generate_path: &Path,
) -> Result<(), GenerateGraphqlQueriesError> {
    let client = Client::new();
    let introspection_query = r#"
    {
      __schema {
        types {
          name
          fields {
            name
            args {
              name
              type {
                name
                kind
                ofType {
                  name
                  kind
                  ofType {
                    name
                    kind
                    ofType {
                      name
                      kind
                    }
                  }
                }
              }
            }
            type {
              name
              kind
              ofType {
                name
                kind
                fields {
                  name
                  type {
                    name
                    kind
                    ofType {
                      name
                      kind
                      ofType {
                        name
                        kind
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    "#;

    let res = client
        .post(endpoint)
        .json(&serde_json::json!({ "query": introspection_query }))
        .send()
        .await?
        .json::<Value>()
        .await?;

    let schema = res["data"]["__schema"].clone();
    if schema.is_null() {
        return Err(GenerateGraphqlQueriesError::NoData);
    }

    generate_operations(&schema, generate_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use mockito::mock;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_generate_graphql_queries_no_data() {
        let _mock = mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"data": {}}"#)
            .create();

        let dir = tempdir().unwrap();
        let generate_path = dir.path();

        let result = generate_graphql_queries(&mockito::server_url(), generate_path).await;
        assert!(matches!(result, Err(GenerateGraphqlQueriesError::NoData)));
    }
}
