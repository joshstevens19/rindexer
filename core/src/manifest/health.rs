use serde::{Deserialize, Serialize};

fn default_port() -> u16 {
    8080
}

/// Health server configuration for rindexer
/// 
/// # Example
/// 
/// ```yaml
/// # In rindexer.yaml
/// health:
///   enabled: true
///   port: 8080
/// ```
/// 
/// Or in Rust code:
/// ```rust
/// use rindexer::{HealthOverrideSettings, StartDetails};
/// use std::path::PathBuf;
/// 
/// let health_details = HealthOverrideSettings {
///     enabled: true,
///     override_port: Some(8080),
/// };
/// 
/// // Example usage - these would be provided by your application
/// let manifest_path = PathBuf::from("rindexer.yaml");
/// let start_details = StartDetails {
///     manifest_path: &manifest_path,
///     indexing_details: None, // or Some(indexing_details)
///     graphql_details: rindexer::GraphqlOverrideSettings { enabled: false, override_port: None },
///     health_details,
/// };
/// ```

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HealthSettings {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default)]
    pub enabled: bool,
}

impl Default for HealthSettings {
    fn default() -> Self {
        Self { 
            port: 8080, 
            enabled: true 
        }
    }
}

impl HealthSettings {
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}
