use reqwest::Client;
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OpsGenieError {
    #[error("HTTP request error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Could not parse response: {0}")]
    CouldNotParseResponse(#[from] serde_json::Error),

    #[error("OpsGenie API error: {0}")]
    ApiError(String),
}

#[derive(Debug, Clone)]
pub struct OpsGenieBot {
    client: Client,
    api_key: String,
    priority: String,
}

impl OpsGenieBot {
    pub fn new(api_key: String, priority: String) -> Self {
        let client = Client::new();
        Self { client, api_key, priority }
    }

    pub async fn send_message(&self, message: &str) -> Result<(), OpsGenieError> {
        let url = "https://api.opsgenie.com/v2/alerts";
        let response = self
            .client
            .post(url)
            .header("Authorization", format!("GenieKey {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&json!({
                "message": message,
                "priority": self.priority,
                "source": "rindexer"
            }))
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let response_text = response.text().await.unwrap_or_default();
            Err(OpsGenieError::ApiError(format!("HTTP {}: {}", status, response_text)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opsgenie_bot_new() {
        let bot = OpsGenieBot::new("test-api-key".to_string(), "P1".to_string());
        assert_eq!(bot.api_key, "test-api-key");
        assert_eq!(bot.priority, "P1");
    }

    #[tokio::test]
    async fn test_opsgenie_send_message_invalid_key() {
        let bot = OpsGenieBot::new("invalid-key".to_string(), "P1".to_string());
        let result = bot.send_message("test").await;
        assert!(result.is_err());
    }

    /// Smoke test that sends a real OpsGenie alert.
    /// Requires env vars: OPSGENIE_API_KEY
    ///
    /// Run with: cargo test -p rindexer opsgenie_send_alert -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn opsgenie_send_alert() {
        let api_key = std::env::var("OPSGENIE_API_KEY").expect("OPSGENIE_API_KEY must be set");

        let bot = OpsGenieBot::new(api_key, "P5".to_string());
        bot.send_message("Hello from rindexer smoke test!")
            .await
            .expect("Failed to send OpsGenie alert");
    }
}
