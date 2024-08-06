use reqwest::Client;
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SlackError {
    #[error("HTTP request error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Could not parse response: {0}")]
    CouldNotParseResponse(#[from] serde_json::Error),

    #[error("Slack API error: {0}")]
    ApiError(String),
}

#[derive(Debug, Clone)]
pub struct SlackBot {
    client: Client,
    token: String,
}

impl SlackBot {
    pub fn new(token: String) -> Self {
        let client = Client::new();
        Self { client, token }
    }

    pub async fn send_message(&self, channel: &str, message: &str) -> Result<(), SlackError> {
        let url = "https://slack.com/api/chat.postMessage";
        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .json(&json!({
                "channel": channel,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        }
                    }
                ]
            }))
            .send()
            .await?;

        let response_text = response.text().await?;
        let response_json: serde_json::Value = serde_json::from_str(&response_text)?;

        if response_json["ok"].as_bool().unwrap_or(false) {
            Ok(())
        } else {
            Err(SlackError::ApiError(
                response_json["error"].as_str().unwrap_or("Unknown error").to_string(),
            ))
        }
    }
}
