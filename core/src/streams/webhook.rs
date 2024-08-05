use reqwest::Client;
use serde_json::Value;

use crate::streams::STREAM_MESSAGE_ID_KEY;

#[derive(thiserror::Error, Debug)]
pub enum WebhookError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Webhook error: {0}")]
    WebhookError(String),
}

#[derive(Debug, Clone)]
pub struct Webhook {
    client: Client,
}

impl Webhook {
    pub fn new() -> Self {
        Self { client: Client::new() }
    }

    pub async fn publish(
        &self,
        id: &str,
        endpoint: &str,
        shared_secret: &str,
        message: &Value,
    ) -> Result<(), WebhookError> {
        let response = self
            .client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .header("x-rindexer-shared-secret", shared_secret)
            .header(STREAM_MESSAGE_ID_KEY, id)
            .json(message)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(WebhookError::WebhookError(format!(
                "Failed to send webhook: {}",
                response.status()
            )))
        }
    }
}
