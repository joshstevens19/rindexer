use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AwsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
}
