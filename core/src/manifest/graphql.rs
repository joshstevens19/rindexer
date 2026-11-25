use serde::{Deserialize, Serialize};

pub fn default_graphql_port() -> u16 {
    3001
}

/// Browser origins allowed to call the GraphQL API.
///
/// Absent, or present without `allowed_origins`, means every origin is
/// allowed — the same behaviour as before this field existed. An empty
/// `allowed_origins` list allows none.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GraphQLCors {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_origins: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GraphQLSettings {
    #[serde(default = "default_graphql_port")]
    pub port: u16,

    #[serde(default)]
    pub disable_advanced_filters: bool,

    #[serde(default)]
    pub filter_only_on_indexed_columns: bool,

    /// Leave unset to allow every browser origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<GraphQLCors>,
}

impl Default for GraphQLSettings {
    fn default() -> Self {
        Self {
            port: default_graphql_port(),
            disable_advanced_filters: false,
            filter_only_on_indexed_columns: false,
            cors: None,
        }
    }
}

impl GraphQLSettings {
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    /// `None` allows every origin. `Some` is the explicit allow-list, which
    /// may be empty.
    pub fn cors_allowed_origins(&self) -> Option<&[String]> {
        self.cors.as_ref()?.allowed_origins.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_cors_allows_every_origin_and_does_not_round_trip_into_yaml() {
        let settings: GraphQLSettings = serde_yaml::from_str("port: 3001").unwrap();
        assert!(settings.cors_allowed_origins().is_none());

        let yaml = serde_yaml::to_string(&settings).unwrap();
        assert!(!yaml.contains("cors"), "{yaml}");
        assert!(!yaml.contains("allowed_origins"), "{yaml}");
    }

    #[test]
    fn cors_block_without_origins_still_allows_every_origin() {
        let settings: GraphQLSettings = serde_yaml::from_str("cors: {}\n").unwrap();
        assert!(settings.cors.is_some());
        assert!(settings.cors_allowed_origins().is_none());
    }

    #[test]
    fn allowed_origins_parse_and_round_trip() {
        let settings: GraphQLSettings = serde_yaml::from_str(
            r#"
port: 4000
cors:
  allowed_origins:
    - https://app.example
    - http://localhost:3000
"#,
        )
        .unwrap();
        assert_eq!(
            settings.cors_allowed_origins().unwrap(),
            ["https://app.example".to_string(), "http://localhost:3000".to_string()]
        );

        let again: GraphQLSettings =
            serde_yaml::from_str(&serde_yaml::to_string(&settings).unwrap()).unwrap();
        assert_eq!(again.cors_allowed_origins(), settings.cors_allowed_origins());
    }

    #[test]
    fn empty_allowed_origins_allows_none() {
        let settings: GraphQLSettings = serde_yaml::from_str(
            r#"
cors:
  allowed_origins: []
"#,
        )
        .unwrap();
        assert_eq!(settings.cors_allowed_origins(), Some(&[][..]));
    }
}
