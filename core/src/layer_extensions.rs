use crate::{adaptive_concurrency::ADAPTIVE_CONCURRENCY, rindexer_error, rindexer_info};
use alloy::{
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::TransportError,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tokio::time::Duration;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct RpcLoggingLayer {
    chain_id: u64,
    rpc_url: String,
}

impl RpcLoggingLayer {
    pub fn new(chain_id: u64, rpc_url: String) -> Self {
        Self { chain_id, rpc_url }
    }
}

impl<S> Layer<S> for RpcLoggingLayer {
    type Service = RpcLoggingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcLoggingService { inner, chain_id: self.chain_id, rpc_url: self.rpc_url.clone() }
    }
}

#[derive(Debug, Clone)]
pub struct RpcLoggingService<S> {
    inner: S,
    chain_id: u64,
    rpc_url: String,
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
                    let error_str = err.to_string();

                    let is_known_error = is_known_retryable_error(&error_str);

                    if !is_known_error {
                        if error_str.contains("timeout") || error_str.contains("timed out") {
                            rindexer_error!("RPC TIMEOUT (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, err);
                        } else if error_str.contains("429") || error_str.contains("rate limit") {
                            // Notify adaptive concurrency to scale down
                            ADAPTIVE_CONCURRENCY.record_rate_limit();
                            rindexer_info!("RPC RATE LIMITED (free public nodes do this a lot consider using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, backoff: {}ms, batch_size: {}, rate_limit_count: {}",
                                          chain_id, method_name, duration, rpc_url,
                                          ADAPTIVE_CONCURRENCY.current_backoff_ms(),
                                          ADAPTIVE_CONCURRENCY.current_batch_size(),
                                          ADAPTIVE_CONCURRENCY.rate_limit_count());
                        } else if error_str.contains("connection")
                            || error_str.contains("network")
                            || error_str.contains("sending request")
                        {
                            rindexer_error!("RPC CONNECTION ERROR (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, err);
                        } else {
                            rindexer_error!("RPC ERROR (free public nodes do this a lot consider a using a paid node) - chain_id: {}, method: {}, duration: {:?}, url: {}, error: {:?}",
                                           chain_id, method_name, duration, rpc_url, err);
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
