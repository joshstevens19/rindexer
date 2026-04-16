use reqwest::Client;
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PagerDutyError {
    #[error("HTTP request error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Could not parse response: {0}")]
    CouldNotParseResponse(#[from] serde_json::Error),

    #[error("PagerDuty API error: {0}")]
    ApiError(String),
}

#[derive(Debug, Clone)]
pub struct PagerDutyBot {
    client: Client,
    routing_key: String,
    severity: String,
}

impl PagerDutyBot {
    pub fn new(routing_key: String, severity: String) -> Self {
        let client = Client::new();
        Self { client, routing_key, severity }
    }

    pub async fn send_message(&self, message: &str) -> Result<(), PagerDutyError> {
        let url = "https://events.pagerduty.com/v2/enqueue";
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&json!({
                "routing_key": self.routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": message,
                    "source": "rindexer",
                    "severity": self.severity
                }
            }))
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let response_text = response.text().await.unwrap_or_default();
            Err(PagerDutyError::ApiError(format!("HTTP {}: {}", status, response_text)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagerduty_bot_new() {
        let bot = PagerDutyBot::new("test-routing-key".to_string(), "critical".to_string());
        assert_eq!(bot.routing_key, "test-routing-key");
        assert_eq!(bot.severity, "critical");
    }

    #[tokio::test]
    async fn test_pagerduty_send_message_invalid_key() {
        let bot = PagerDutyBot::new("invalid-key".to_string(), "critical".to_string());
        let result = bot.send_message("test").await;
        assert!(result.is_err());
    }

    /// Smoke test that sends a real PagerDuty event.
    /// Requires env vars: PAGERDUTY_ROUTING_KEY
    ///
    /// Run with: cargo test -p rindexer pagerduty_send_event -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn pagerduty_send_event() {
        let routing_key =
            std::env::var("PAGERDUTY_ROUTING_KEY").expect("PAGERDUTY_ROUTING_KEY must be set");

        let bot = PagerDutyBot::new(routing_key, "info".to_string());
        bot.send_message("Hello from rindexer smoke test!")
            .await
            .expect("Failed to send PagerDuty event");
    }
}
