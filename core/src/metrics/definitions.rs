//! Prometheus metric definitions for rindexer.
//!
//! All metric registrations are centralized here for discoverability.
//! Metrics are lazily initialized on first access.

use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge, register_gauge_vec, register_histogram_vec, CounterVec,
    Gauge, GaugeVec, HistogramVec,
};

// =============================================================================
// Indexing Metrics
// =============================================================================

/// Total number of blocks indexed.
/// Labels: network, contract, event
pub static BLOCKS_INDEXED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_blocks_indexed_total",
        "Total number of blocks indexed",
        &["network", "contract", "event"]
    )
    .expect("failed to register BLOCKS_INDEXED_TOTAL")
});

/// Total number of events processed.
/// Labels: network, contract, event
pub static EVENTS_PROCESSED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_events_processed_total",
        "Total number of events processed",
        &["network", "contract", "event"]
    )
    .expect("failed to register EVENTS_PROCESSED_TOTAL")
});

/// Last synced block number per indexing target.
/// Labels: network, contract, event
pub static LAST_SYNCED_BLOCK: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_last_synced_block",
        "Last synced block number",
        &["network", "contract", "event"]
    )
    .expect("failed to register LAST_SYNCED_BLOCK")
});

/// Latest block number observed on chain.
/// Labels: network
pub static LATEST_CHAIN_BLOCK: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!("rindexer_latest_chain_block", "Latest block number on chain", &["network"])
        .expect("failed to register LATEST_CHAIN_BLOCK")
});

/// Number of blocks behind chain head.
/// Labels: network, contract, event
pub static BLOCKS_BEHIND: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_blocks_behind",
        "Number of blocks behind chain head",
        &["network", "contract", "event"]
    )
    .expect("failed to register BLOCKS_BEHIND")
});

/// Currently active indexing tasks (global gauge).
pub static ACTIVE_INDEXING_TASKS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("rindexer_active_indexing_tasks", "Number of currently active indexing tasks")
        .expect("failed to register ACTIVE_INDEXING_TASKS")
});

// =============================================================================
// RPC Metrics
// =============================================================================

/// Total RPC requests made.
/// Labels: network, method, status (success/error)
pub static RPC_REQUESTS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_rpc_requests_total",
        "Total number of RPC requests made",
        &["network", "method", "status"]
    )
    .expect("failed to register RPC_REQUESTS_TOTAL")
});

/// RPC request duration histogram.
/// Labels: network, method
/// Buckets optimized for typical RPC latencies (10ms to 10s).
pub static RPC_REQUEST_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "rindexer_rpc_request_duration_seconds",
        "RPC request duration in seconds",
        &["network", "method"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    .expect("failed to register RPC_REQUEST_DURATION")
});

/// RPC requests currently in-flight.
/// Labels: network
pub static RPC_REQUESTS_IN_FLIGHT: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_rpc_requests_in_flight",
        "Number of RPC requests currently in-flight",
        &["network"]
    )
    .expect("failed to register RPC_REQUESTS_IN_FLIGHT")
});

// =============================================================================
// Database Metrics
// =============================================================================

/// Total database operations.
/// Labels: operation (insert/update/delete/query), status (success/error)
pub static DB_OPERATIONS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_db_operations_total",
        "Total number of database operations",
        &["operation", "status"]
    )
    .expect("failed to register DB_OPERATIONS_TOTAL")
});

/// Database operation duration histogram.
/// Labels: operation
/// Buckets optimized for typical DB latencies (1ms to 2.5s).
pub static DB_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "rindexer_db_operation_duration_seconds",
        "Database operation duration in seconds",
        &["operation"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
    )
    .expect("failed to register DB_OPERATION_DURATION")
});

/// Database connection pool size.
/// Labels: database (postgres/clickhouse)
pub static DB_POOL_CONNECTIONS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_db_pool_connections",
        "Number of connections in database pool",
        &["database", "state"]
    )
    .expect("failed to register DB_POOL_CONNECTIONS")
});

// =============================================================================
// Chain State Metrics
// =============================================================================

/// Total chain reorganizations detected.
/// Labels: network
pub static REORGS_DETECTED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_reorgs_detected_total",
        "Total number of chain reorganizations detected",
        &["network"]
    )
    .expect("failed to register REORGS_DETECTED_TOTAL")
});

/// Depth of the last detected reorg.
/// Labels: network
pub static REORG_DEPTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_reorg_depth",
        "Depth of the last detected chain reorganization",
        &["network"]
    )
    .expect("failed to register REORG_DEPTH")
});

// =============================================================================
// Stream Metrics
// =============================================================================

/// Total messages sent to streams.
/// Labels: stream_type (sns/kafka/rabbitmq/redis/webhook), status (success/error)
pub static STREAM_MESSAGES_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_stream_messages_total",
        "Total number of messages sent to streams",
        &["stream_type", "status"]
    )
    .expect("failed to register STREAM_MESSAGES_TOTAL")
});

/// Stream message send duration histogram.
/// Labels: stream_type
pub static STREAM_MESSAGE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "rindexer_stream_message_duration_seconds",
        "Stream message send duration in seconds",
        &["stream_type"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .expect("failed to register STREAM_MESSAGE_DURATION")
});

// =============================================================================
// Build Info
// =============================================================================

/// Build information gauge (always 1, labels carry metadata).
/// Labels: version, commit, rust_version
pub static BUILD_INFO: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!("rindexer_build_info", "Build information", &["version"])
        .expect("failed to register BUILD_INFO")
});

/// Initialize build info metric with current version.
pub fn init_build_info(version: &str) {
    BUILD_INFO.with_label_values(&[version]).set(1.0);
}
