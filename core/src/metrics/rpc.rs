//! RPC-specific metrics helpers.

use super::definitions::{RPC_REQUESTS_IN_FLIGHT, RPC_REQUESTS_TOTAL, RPC_REQUEST_DURATION};
use super::timer::TimerGuard;

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
