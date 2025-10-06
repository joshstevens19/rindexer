use std::path::Path;

use crate::api::generate_operations::{generate_operations, GenerateOperationsError};
use reqwest::Client;
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum GenerateGraphqlQueriesError {
    #[error("Network request failed: {0}")]
    Network(#[from] reqwest::Error),

    #[error("No data in response")]
    NoData,

    #[error("Invalid response. Make sure that {0} can receive GraphQL introspection query.")]
    InvalidData(String),

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
        .await
        .map_err(|_| GenerateGraphqlQueriesError::InvalidData(endpoint.to_string()))?;

    let schema = res["data"]["__schema"].clone();
    if schema.is_null() {
        return Err(GenerateGraphqlQueriesError::NoData);
    }

    generate_operations(&schema, generate_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    use super::*;

    #[test]
    fn test_generate_graphql_queries_no_data() {
        let mut server = mockito::Server::new();

        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"data": {}}"#)
            .create();

        let dir = tempdir().unwrap();
        let generate_path = dir.path();

        let rt = Runtime::new().unwrap();
        let result = rt.block_on(generate_graphql_queries(&server.url(), generate_path));
        assert!(matches!(result, Err(GenerateGraphqlQueriesError::NoData)));
    }
}
