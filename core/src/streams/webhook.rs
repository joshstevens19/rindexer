use reqwest::Client;
use serde_json::Value;

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
        message: &Value,
    ) -> Result<(), WebhookError> {
        let response = self
            .client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .header("x-rindexer-id", id)
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
