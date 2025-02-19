use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AwsConfig {
    pub region: String,
    pub access_key: String,
    pub secret_key: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
}
