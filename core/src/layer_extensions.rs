use crate::{
    adaptive_concurrency::ADAPTIVE_CONCURRENCY, endpoint_failover::EndpointHealth, rindexer_error,
    rindexer_info,
};
use alloy::{
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::TransportError,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::time::Duration;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct RpcLoggingLayer {
    chain_id: u64,
    /// Credential-redacted url. Safe to write to logs.
    rpc_url: String,
    /// Scrubs this endpoint's secrets out of transport errors before they
    /// are logged. Always present, even for endpoints that do not participate
    /// in failover health tracking.
    redactor: Arc<EndpointHealth>,
    /// Optional per-endpoint health fed with every request outcome. Wired
    /// when the transport participates in a failover pool.
    health: Option<Arc<EndpointHealth>>,
}

impl RpcLoggingLayer {
    pub fn new(chain_id: u64, rpc_url: String) -> Self {
        let redactor = Arc::new(EndpointHealth::new(&rpc_url));
        Self { chain_id, rpc_url: redactor.redacted_url().to_string(), redactor, health: None }
    }

    /// Feed request outcomes (success/failure classification) into the given
    /// endpoint health tracker. The logged url and error redaction follow
    /// that tracker, so a fallback endpoint cannot log a different secret
    /// than the one its health snapshot redacts.
    pub fn with_health(mut self, health: Arc<EndpointHealth>) -> Self {
        self.rpc_url = health.redacted_url().to_string();
        self.redactor = Arc::clone(&health);
        self.health = Some(health);
        self
    }
}

impl<S> Layer<S> for RpcLoggingLayer {
    type Service = RpcLoggingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcLoggingService {
            inner,
            chain_id: self.chain_id,
            rpc_url: self.rpc_url.clone(),
            redactor: Arc::clone(&self.redactor),
            health: self.health.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RpcLoggingService<S> {
    inner: S,
    chain_id: u64,
    rpc_url: String,
    redactor: Arc<EndpointHealth>,
    health: Option<Arc<EndpointHealth>>,
}

impl<S> Service<RequestPacket> for RpcLoggingService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket, Error = TransportError>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let start_time = Instant::now();
        let chain_id = self.chain_id;
        let rpc_url = self.rpc_url.clone();
        let redactor = Arc::clone(&self.redactor);
        let health = self.health.clone();

        let method_name = match &req {
            RequestPacket::Single(r) => r.method().to_string(),
            RequestPacket::Batch(reqs) => {
                if reqs.is_empty() {
                    "empty_batch".to_string()
                } else if reqs.len() == 1 {
                    reqs[0].method().to_string()
                } else {
                    format!("batch_{}_requests", reqs.len())
                }
            }
        };

        let fut = self.inner.call(req);

        Box::pin(async move {
            // Enforce global backoff BEFORE making request (for rate-limited free nodes)
            // Add jitter to prevent thundering herd (all tasks waking at once)
            let backoff_ms = ADAPTIVE_CONCURRENCY.current_backoff_ms();
            if backoff_ms > 0 {
                // Add 0-50% random jitter to spread out requests
                let jitter = (backoff_ms as f64 * rand::random::<f64>() * 0.5) as u64;
                tokio::time::sleep(Duration::from_millis(backoff_ms + jitter)).await;
            }

            match fut.await {
                Ok(response) => {
                    let duration = start_time.elapsed();

                    if let Some(health) = &health {
                        health.record_success();
                    }

                    if duration.as_secs() >= 10 {
                        rindexer_info!(
                            "SLOW RPC call - chain_id: {}, method: {}, duration: {:?}, url: {}",
                            chain_id,
                            method_name,
                            duration,
                            rpc_url
                        );
                    }

                    Ok(response)
                }
                Err(err) => {
                    let duration = start_time.elapsed();
                    // Classify on the raw text (redaction must not hide a
                    // retryable phrase) but only the scrubbed form is logged.
                    // Transport errors embed the full request url, API key
                    // included.
                    let raw_error = err.to_string();
                    let error_str = redactor.redact_error(&raw_error);

                    if let Some(health) = &health {
                        health.record_failure(&raw_error);
                    }

                    let is_known_error = is_known_retryable_error(&raw_error);

                    if !is_known_error {
                        if raw_error.contains("timeout") || raw_error.contains("timed out") {
                            rindexer_error!("RPC TIMEOUT (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, error_str);
                        } else if raw_error.contains("429") || raw_error.contains("rate limit") {
                            // Notify adaptive concurrency to scale down
                            ADAPTIVE_CONCURRENCY.record_rate_limit();
                            rindexer_info!("RPC RATE LIMITED (free public nodes do this a lot consider using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, backoff: {}ms, batch_size: {}, rate_limit_count: {}",
                                          chain_id, method_name, duration, rpc_url,
                                          ADAPTIVE_CONCURRENCY.current_backoff_ms(),
                                          ADAPTIVE_CONCURRENCY.current_batch_size(),
                                          ADAPTIVE_CONCURRENCY.rate_limit_count());
                        } else if raw_error.contains("connection")
                            || raw_error.contains("network")
                            || raw_error.contains("sending request")
                        {
                            rindexer_error!("RPC CONNECTION ERROR (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, error_str);
                        } else {
                            rindexer_error!("RPC ERROR (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, error_str);
                        }
                    }

                    Err(err)
                }
            }
        })
    }
}

fn is_known_retryable_error(error_message: &str) -> bool {
    // mirror handled logic which is in the `retry_with_block_range`
    error_message.contains("this block range should work")
        || error_message.contains("try with this block range")
        || error_message.contains("block range is too wide")
        || error_message.contains("limited to a")
        || error_message.contains("block range too large")
        || error_message.contains("response is too big")
        || error_message.contains("error decoding response body")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint_failover::redact_rpc_url;

    #[test]
    fn logging_layer_redacts_url_and_transport_error() {
        let secret = "https://user:hunter2@rpc.example.com/v2/SECRETKEY?apikey=QUERY";
        let layer = RpcLoggingLayer::new(1, secret.to_string());
        assert_eq!(layer.rpc_url, redact_rpc_url(secret));
        assert!(!layer.rpc_url.contains("SECRETKEY"), "{}", layer.rpc_url);
        assert!(!layer.rpc_url.contains("hunter2"), "{}", layer.rpc_url);
        assert!(!layer.rpc_url.contains("QUERY"), "{}", layer.rpc_url);

        // This is the scrub `call` logs in place of the raw transport error.
        let raw = format!("error sending request for url ({secret}): connection refused");
        let sanitized = layer.redactor.redact_error(&raw);
        assert!(!sanitized.contains("SECRETKEY"), "{sanitized}");
        assert!(!sanitized.contains("hunter2"), "{sanitized}");
        assert!(!sanitized.contains("QUERY"), "{sanitized}");
        assert!(sanitized.contains("rpc.example.com"), "{sanitized}");
    }

    #[test]
    fn with_health_logs_the_shared_endpoint_redaction() {
        let secret = "https://fallback.example/v2/FALLBACKKEY";
        let health = Arc::new(EndpointHealth::new(secret));
        let layer = RpcLoggingLayer::new(1, "https://other.example/v2/OTHERSECRET".to_string())
            .with_health(Arc::clone(&health));
        assert_eq!(layer.rpc_url, health.redacted_url());
        assert!(!layer.rpc_url.contains("FALLBACKKEY"), "{}", layer.rpc_url);
        assert!(!layer.rpc_url.contains("OTHERSECRET"), "{}", layer.rpc_url);

        let raw = format!("connection error for url ({secret})");
        let sanitized = layer.redactor.redact_error(&raw);
        assert!(!sanitized.contains("FALLBACKKEY"), "{sanitized}");
    }
}
