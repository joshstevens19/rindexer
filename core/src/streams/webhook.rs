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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn publish_sends_correct_headers() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/webhook")
            .match_header("content-type", "application/json")
            .match_header("x-rindexer-shared-secret", "my-secret")
            .match_header(STREAM_MESSAGE_ID_KEY, "msg-001")
            .with_status(200)
            .create_async()
            .await;

        let webhook = Webhook::new();
        let result = webhook
            .publish(
                "msg-001",
                &format!("{}/webhook", server.url()),
                "my-secret",
                &json!({"event": "Transfer"}),
            )
            .await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn publish_returns_error_on_server_failure() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/webhook")
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let webhook = Webhook::new();
        let result = webhook
            .publish(
                "msg-002",
                &format!("{}/webhook", server.url()),
                "secret",
                &json!({"data": 1}),
            )
            .await;

        assert!(matches!(result, Err(WebhookError::WebhookError(_))));
    }
}
