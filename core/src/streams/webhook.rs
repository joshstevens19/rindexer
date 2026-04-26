use reqwest::Client;
use serde_json::Value;

use crate::streams::{publish_with_retry, STREAM_MESSAGE_ID_KEY};

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
        // Use a bounded host[:port] label instead of the raw URL — the raw
        // URL is unbounded and would explode Prometheus cardinality on
        // `STREAM_PUBLISH_DROPPED_TOTAL` for multi-tenant webhook fanouts.
        let target = webhook_target_label(endpoint);
        publish_with_retry("webhook", &target, || {
            self.publish_once(id, endpoint, shared_secret, message)
        })
        .await
    }

    async fn publish_once(
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

/// Stable low-cardinality label for `STREAM_PUBLISH_DROPPED_TOTAL` on
/// webhooks. The raw endpoint URL is unbounded — multi-tenant deployments
/// with many distinct webhook URLs would blow Prometheus TSDB cardinality.
/// Extracting `host[:port]` bounds the label set to the number of distinct
/// destination hosts (typically a handful) while still letting operators
/// identify which host is dropping events.
fn webhook_target_label(endpoint: &str) -> String {
    const UNKNOWN: &str = "unknown";
    match reqwest::Url::parse(endpoint) {
        Ok(url) => match (url.host_str(), url.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            (None, _) => UNKNOWN.to_string(),
        },
        Err(_) => UNKNOWN.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn webhook_target_label_strips_path_query_and_scheme() {
        // Raw URLs have unbounded path/query combinations; the label must
        // reduce to a stable host[:port] so Prometheus cardinality is
        // bounded by the number of distinct destination hosts.
        assert_eq!(
            webhook_target_label("https://api.example.com/hooks/xyz?token=abc"),
            "api.example.com"
        );
        assert_eq!(webhook_target_label("http://127.0.0.1:8080/hook"), "127.0.0.1:8080");
        // Non-standard default port preserved when explicit.
        assert_eq!(
            webhook_target_label("https://events.example.com:8443/path"),
            "events.example.com:8443"
        );
    }

    #[test]
    fn webhook_target_label_falls_back_on_unparseable() {
        assert_eq!(webhook_target_label("not a url"), "unknown");
        assert_eq!(webhook_target_label(""), "unknown");
    }

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
            .publish("msg-002", &format!("{}/webhook", server.url()), "secret", &json!({"data": 1}))
            .await;

        assert!(matches!(result, Err(WebhookError::WebhookError(_))));
    }
}
