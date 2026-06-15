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

/// RPC errors by category (kind: rate_limited/timeout/connection/other), observed
/// at the transport layer so it covers every method. `rate_limited` includes
/// 429/503/`-32001`. Counts each attempt, so it rises with retry volume.
/// Labels: network, method, kind.
pub static RPC_ERRORS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_rpc_errors_total",
        "RPC errors by category, observed at the transport layer",
        &["network", "method", "kind"]
    )
    .expect("failed to register RPC_ERRORS_TOTAL")
});

/// RPC calls that completed but took >= 10s. Labels: network, method.
pub static RPC_SLOW_CALLS_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_rpc_slow_calls_total",
        "RPC calls that completed but took >= 10 seconds",
        &["network", "method"]
    )
    .expect("failed to register RPC_SLOW_CALLS_TOTAL")
});

/// Running total of adaptive-concurrency rate-limit/unavailability events (global).
pub static RPC_RATE_LIMIT_EVENTS_TOTAL: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "rindexer_rpc_rate_limit_events_total",
        "Total adaptive-concurrency rate-limit/unavailability events"
    )
    .expect("failed to register RPC_RATE_LIMIT_EVENTS_TOTAL")
});

/// Adaptive backoff before each RPC request, ms (global). 0 healthy, ramps to
/// 30000 during an incident — the clearest "provider degraded" signal.
pub static RPC_ADAPTIVE_BACKOFF_MS: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "rindexer_rpc_adaptive_backoff_ms",
        "Current adaptive backoff applied before RPC requests, in milliseconds"
    )
    .expect("failed to register RPC_ADAPTIVE_BACKOFF_MS")
});

/// Current adaptive concurrency limit (global). Scales down on rate limits.
pub static RPC_ADAPTIVE_CONCURRENCY: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "rindexer_rpc_adaptive_concurrency",
        "Current adaptive concurrency limit for RPC batches"
    )
    .expect("failed to register RPC_ADAPTIVE_CONCURRENCY")
});

/// Current adaptive RPC batch size (global). Scales down on rate limits.
pub static RPC_ADAPTIVE_BATCH_SIZE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("rindexer_rpc_adaptive_batch_size", "Current adaptive RPC batch size")
        .expect("failed to register RPC_ADAPTIVE_BATCH_SIZE")
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
// Metadata Fetch Metrics
// =============================================================================

/// Total block timestamp fetch failures.
/// Labels: network
/// When this fires, downstream writes will fail with NOT NULL violations.
/// Alert on any non-zero rate.
pub static BLOCK_TIMESTAMP_FETCH_FAILURES_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_block_timestamp_fetch_failures_total",
        "Total block timestamp fetch failures (causes batch retry)",
        &["network"]
    )
    .expect("failed to register BLOCK_TIMESTAMP_FETCH_FAILURES_TOTAL")
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

/// Duration of reorg handling from detection to completion.
/// Labels: network
pub static REORG_HANDLING_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "rindexer_reorg_handling_duration_seconds",
        "Duration of reorg handling from detection to completion",
        &["network"]
    )
    .expect("failed to register rindexer_reorg_handling_duration_seconds")
});

/// Total number of events deleted during reorg rollback.
/// Labels: network
pub static REORG_EVENTS_DELETED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_reorg_events_deleted_total",
        "Total number of events deleted during reorg rollback",
        &["network"]
    )
    .expect("failed to register rindexer_reorg_events_deleted_total")
});

/// Number of reorgs detected by source.
/// Labels: network, source
pub static REORG_DETECTION_SOURCE: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_reorg_detection_source_total",
        "Number of reorgs detected by source",
        &["network", "source"]
    )
    .expect("failed to register rindexer_reorg_detection_source_total")
});

/// Number of cascading reorgs detected immediately after handling a previous reorg.
/// Labels: network
pub static REORG_CASCADE: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_reorg_cascade_total",
        "Number of cascading reorgs detected immediately after handling a previous reorg",
        &["network"]
    )
    .expect("failed to register rindexer_reorg_cascade_total")
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

/// Number of times a per-(stream, network) finalized-delivery buffer grew past
/// the soft cap. Not a hard drop — events still flush when the block finalizes
/// — but sustained growth indicates a flush stall or an under-configured
/// `reorg_safe_distance`.
/// Labels: stream_type, network
pub static STREAM_FINALIZED_BUFFER_OVERFLOW_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_stream_finalized_buffer_overflow_total",
        "Count of finalized-delivery buffer soft-cap overflows per (stream_type, network)",
        &["stream_type", "network"]
    )
    .expect("failed to register STREAM_FINALIZED_BUFFER_OVERFLOW_TOTAL")
});

/// Current number of events sitting in a finalized-delivery buffer for a
/// `(stream_type, network)` pair, summed across all `config_index`es and
/// `event_name`s on that pair. Complements `STREAM_FINALIZED_BUFFER_OVERFLOW_TOTAL`
/// — overflow answers "did the buffer ever breach the soft cap?" while this
/// gauge answers "is the buffer draining?". Alert if this stays non-zero and
/// non-decreasing across consecutive scrapes.
///
/// The gauge updates when an event leaves the in-memory buffer (post-drain),
/// not when its downstream publish completes — events in flight after a
/// flush are not reflected. Pair with `STREAM_PUBLISH_DROPPED_TOTAL` and the
/// `record_finalized_flush_duration` histogram for the publish-side picture.
///
/// Labels: stream_type, network
pub static STREAM_FINALIZED_BUFFER_DEPTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "rindexer_stream_finalized_buffer_depth",
        "Current events buffered for finalized delivery per (stream_type, network)",
        &["stream_type", "network"]
    )
    .expect("failed to register STREAM_FINALIZED_BUFFER_DEPTH")
});

/// End-to-end duration of a `flush_finalized(network, head)` call. Captures
/// lock acquire + drain-decision + publisher fanout. Labels: network.
pub static STREAM_FINALIZED_FLUSH_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "rindexer_stream_finalized_flush_duration_seconds",
        "End-to-end finalized-buffer flush duration in seconds per network",
        &["network"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    )
    .expect("failed to register STREAM_FINALIZED_FLUSH_DURATION")
});

/// Terminal publish failures — a message hit `publish_with_retry`'s final
/// attempt and was not delivered. In Instant delivery mode this is a lost
/// message from the consumer's point of view. Alert on any sustained non-zero
/// rate. Labels: stream_type, target (topic/queue/endpoint id).
pub static STREAM_PUBLISH_DROPPED_TOTAL: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "rindexer_stream_publish_dropped_total",
        "Publishes that exhausted retries and were dropped, per (stream_type, target)",
        &["stream_type", "target"]
    )
    .expect("failed to register STREAM_PUBLISH_DROPPED_TOTAL")
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
