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

#[derive(Debug, Clone)]
pub struct CloudflareQueues {
    client: Client,
    api_token: String,
    account_id: String,
}

impl CloudflareQueues {
    pub fn new(api_token: String, account_id: String) -> Self {
        Self { client: Client::new(), api_token, account_id }
    }

    pub async fn publish(
        &self,
        id: &str,
        queue_id: &str,
        message: &Value,
    ) -> Result<(), CloudflareQueuesError> {
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/queues/{}/messages",
            self.account_id, queue_id
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
                "https://api.cloudflare.com/client/v4/accounts/{}/queues/{}/messages/batch",
                self.account_id, queue_id
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
