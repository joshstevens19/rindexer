use reqwest::Client;
use serde_json::Value;
use thiserror::Error;

use crate::streams::STREAM_MESSAGE_ID_KEY;

#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum CloudflareQueuesError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Cloudflare API error: {status} - {message}")]
    ApiError { status: u16, message: String },

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

const CLOUDFLARE_API_BASE: &str = "https://api.cloudflare.com";

#[derive(Debug, Clone)]
pub struct CloudflareQueues {
    client: Client,
    api_token: String,
    account_id: String,
    base_url: String,
}

impl CloudflareQueues {
    pub fn new(api_token: String, account_id: String) -> Self {
        Self {
            client: Client::new(),
            api_token,
            account_id,
            base_url: CLOUDFLARE_API_BASE.to_string(),
        }
    }

    pub(crate) fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    pub async fn publish(
        &self,
        id: &str,
        queue_id: &str,
        message: &Value,
    ) -> Result<(), CloudflareQueuesError> {
        let url = format!(
            "{}/client/v4/accounts/{}/queues/{}/messages",
            self.base_url, self.account_id, queue_id
        );

        let mut message_with_metadata = message.clone();
        if let Value::Object(ref mut map) = message_with_metadata {
            map.insert("message_id".to_string(), Value::String(id.to_string()));
        }

        let payload = serde_json::json!({
            "body": message_with_metadata
        });

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .header(STREAM_MESSAGE_ID_KEY, id)
            .json(&payload)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status().as_u16();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            Err(CloudflareQueuesError::ApiError { status, message: error_text })
        }
    }

    #[allow(dead_code)]
    pub async fn publish_batch(
        &self,
        messages: Vec<(String, Value)>,
        queue_id: &str,
    ) -> Result<(), CloudflareQueuesError> {
        if messages.is_empty() {
            return Ok(());
        }

        // Cloudflare Queues supports up to 100 messages per batch
        const MAX_BATCH_SIZE: usize = 100;

        for chunk in messages.chunks(MAX_BATCH_SIZE) {
            let url = format!(
                "{}/client/v4/accounts/{}/queues/{}/messages/batch",
                self.base_url, self.account_id, queue_id
            );

            let batch_messages: Vec<Value> = chunk
                .iter()
                .map(|(id, message)| {
                    let mut message_with_metadata = message.clone();
                    if let Value::Object(ref mut map) = message_with_metadata {
                        map.insert("message_id".to_string(), Value::String(id.to_string()));
                    }
                    serde_json::json!({
                        "body": message_with_metadata
                    })
                })
                .collect();

            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.api_token))
                .header("Content-Type", "application/json")
                .json(&batch_messages)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status().as_u16();
                let error_text =
                    response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                return Err(CloudflareQueuesError::ApiError { status, message: error_text });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_client(base_url: &str) -> CloudflareQueues {
        CloudflareQueues::new("test-token".to_string(), "acc-123".to_string())
            .with_base_url(base_url.to_string())
    }

    #[tokio::test]
    async fn publish_sends_correct_request() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages")
            .match_header("authorization", "Bearer test-token")
            .match_header("content-type", "application/json")
            .with_status(200)
            .create_async()
            .await;

        let client = test_client(&server.url());
        let result = client.publish("msg-001", "q-456", &json!({"event": "data"})).await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn publish_returns_api_error_on_failure() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages")
            .with_status(403)
            .with_body("Forbidden")
            .create_async()
            .await;

        let client = test_client(&server.url());
        let result = client.publish("msg-002", "q-456", &json!({"data": 1})).await;

        match result {
            Err(CloudflareQueuesError::ApiError { status, message }) => {
                assert_eq!(status, 403);
                assert_eq!(message, "Forbidden");
            }
            other => panic!("Expected ApiError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn publish_injects_message_id_into_payload() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages")
            .match_body(mockito::Matcher::Json(
                json!({"body": {"event": "data", "message_id": "msg-003"}}),
            ))
            .with_status(200)
            .create_async()
            .await;

        let client = test_client(&server.url());
        let result = client.publish("msg-003", "q-456", &json!({"event": "data"})).await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn publish_batch_empty_returns_ok() {
        // should not make any HTTP requests
        let client = test_client("http://127.0.0.1:1");
        let result = client.publish_batch(vec![], "q-456").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn publish_batch_sends_to_batch_endpoint() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages/batch")
            .match_header("authorization", "Bearer test-token")
            .with_status(200)
            .create_async()
            .await;

        let client = test_client(&server.url());
        let messages = vec![
            ("msg-1".to_string(), json!({"data": 1})),
            ("msg-2".to_string(), json!({"data": 2})),
        ];
        let result = client.publish_batch(messages, "q-456").await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn publish_batch_propagates_error() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages/batch")
            .with_status(500)
            .with_body("Server Error")
            .create_async()
            .await;

        let client = test_client(&server.url());
        let messages = vec![("msg-1".to_string(), json!({"data": 1}))];
        let result = client.publish_batch(messages, "q-456").await;

        assert!(result.is_err());
    }
}
