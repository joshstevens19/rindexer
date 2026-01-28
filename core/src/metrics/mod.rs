//! Prometheus metrics for rindexer observability.
//!
//! This module provides metrics collection and exposition for monitoring
//! rindexer indexing performance, RPC health, and database operations.
//!
//! # Usage
//!
//! ```ignore
//! use rindexer::metrics::{indexing, rpc, database};
//!
//! // Record indexing progress
//! indexing::record_events_indexed("ethereum", "uniswap", "Swap", 100, 12345678, Some(12345700));
//!
//! // Time an RPC request
//! let _timer = rpc::time_rpc_request("ethereum", "eth_getLogs");
//! // ... make RPC call ...
//! // Duration recorded automatically on drop
//!
//! // Record database operation
//! database::record_db_success("insert", 0.005);
//! ```
//!
//! # Metrics Endpoint
//!
//! Add the `/metrics` route to your Axum router:
//!
//! ```ignore
//! use rindexer::metrics::metrics_handler;
//!
//! let app = Router::new()
//!     .route("/metrics", get(metrics_handler));
//! ```

pub mod database;
pub mod definitions;
pub mod indexing;
pub mod rpc;
pub mod streams;
pub mod timer;

// Re-export commonly used items
pub use definitions::init_build_info;
pub use timer::{CallbackTimer, TimerGuard};

use axum::{http::StatusCode, response::IntoResponse};
use prometheus::{Encoder, TextEncoder};

/// Axum handler for the `/metrics` endpoint.
///
/// Returns metrics in Prometheus text exposition format.
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => {
            let body = String::from_utf8(buffer).unwrap_or_default();
            (StatusCode::OK, [("content-type", "text/plain; version=0.0.4; charset=utf-8")], body)
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("Failed to encode metrics: {}", e),
        ),
    }
}

/// Encode all metrics to a string (for testing or custom endpoints).
pub fn encode_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    String::from_utf8(buffer).map_err(|e| prometheus::Error::Msg(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_metrics() {
        // Initialize a metric to ensure something is collected
        indexing::set_active_tasks(5);

        let output = encode_metrics().expect("should encode metrics");
        assert!(output.contains("rindexer_active_indexing_tasks"));
    }

    #[test]
    fn test_indexing_metrics() {
        indexing::record_events_indexed(
            "ethereum",
            "test_contract",
            "Transfer",
            10,
            100,
            Some(110),
        );

        let output = encode_metrics().expect("should encode metrics");
        assert!(output.contains("rindexer_events_processed_total"));
        assert!(output.contains("rindexer_last_synced_block"));
        assert!(output.contains("rindexer_blocks_behind"));
    }
}
