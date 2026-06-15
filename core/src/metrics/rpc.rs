//! RPC-specific metrics helpers.

use super::definitions::{
    RPC_ADAPTIVE_BACKOFF_MS, RPC_ADAPTIVE_BATCH_SIZE, RPC_ADAPTIVE_CONCURRENCY, RPC_ERRORS_TOTAL,
    RPC_RATE_LIMIT_EVENTS_TOTAL, RPC_REQUESTS_IN_FLIGHT, RPC_REQUESTS_TOTAL, RPC_REQUEST_DURATION,
    RPC_SLOW_CALLS_TOTAL,
};
use super::timer::TimerGuard;

/// Error categories for [`record_rpc_error_kind`]. `RATE_LIMITED` covers
/// throttling and temporary-unavailability (HTTP 429/503, `-32001`).
pub mod error_kind {
    pub const RATE_LIMITED: &str = "rate_limited";
    pub const TIMEOUT: &str = "timeout";
    pub const CONNECTION: &str = "connection";
    pub const OTHER: &str = "other";
}

/// Record an RPC error by category.
pub fn record_rpc_error_kind(network: &str, method: &str, kind: &str) {
    RPC_ERRORS_TOTAL.with_label_values(&[network, method, kind]).inc();
}

/// Record an RPC call that completed but exceeded the slow-call threshold (>= 10s).
pub fn record_slow_call(network: &str, method: &str) {
    RPC_SLOW_CALLS_TOTAL.with_label_values(&[network, method]).inc();
}

/// Publish the adaptive-concurrency controller state as gauges (global controller).
pub fn set_adaptive_state(concurrency: usize, batch_size: usize, backoff_ms: u64) {
    RPC_ADAPTIVE_CONCURRENCY.set(concurrency as f64);
    RPC_ADAPTIVE_BATCH_SIZE.set(batch_size as f64);
    RPC_ADAPTIVE_BACKOFF_MS.set(backoff_ms as f64);
}

/// Publish the running total of rate-limit/unavailability events.
pub fn set_rate_limit_events(total: u64) {
    RPC_RATE_LIMIT_EVENTS_TOTAL.set(total as f64);
}

/// Record a completed RPC request.
pub fn record_rpc_request(network: &str, method: &str, success: bool, duration_secs: f64) {
    let status = if success { "success" } else { "error" };

    RPC_REQUESTS_TOTAL.with_label_values(&[network, method, status]).inc();

    RPC_REQUEST_DURATION.with_label_values(&[network, method]).observe(duration_secs);
}

/// Record a successful RPC request.
pub fn record_rpc_success(network: &str, method: &str, duration_secs: f64) {
    record_rpc_request(network, method, true, duration_secs);
}

/// Record a failed RPC request.
pub fn record_rpc_error(network: &str, method: &str, duration_secs: f64) {
    record_rpc_request(network, method, false, duration_secs);
}

/// Create a timer for an RPC request. Records duration on drop.
pub fn time_rpc_request<'a>(network: &str, method: &str) -> TimerGuard<'a> {
    TimerGuard::new(&RPC_REQUEST_DURATION, &[network, method])
}

/// Increment in-flight RPC requests for a network.
pub fn inc_in_flight(network: &str) {
    RPC_REQUESTS_IN_FLIGHT.with_label_values(&[network]).inc();
}

/// Decrement in-flight RPC requests for a network.
pub fn dec_in_flight(network: &str) {
    RPC_REQUESTS_IN_FLIGHT.with_label_values(&[network]).dec();
}

/// RAII guard for tracking in-flight requests.
pub struct InFlightGuard {
    network: String,
}

impl InFlightGuard {
    pub fn new(network: &str) -> Self {
        inc_in_flight(network);
        Self { network: network.to_string() }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        dec_in_flight(&self.network);
    }
}
