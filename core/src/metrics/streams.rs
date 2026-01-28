//! Stream-specific metrics helpers.

use std::time::Instant;

use super::definitions::{STREAM_MESSAGES_TOTAL, STREAM_MESSAGE_DURATION};

/// Stream type labels for metrics.
pub mod stream_type {
    pub const SNS: &str = "sns";
    pub const WEBHOOK: &str = "webhook";
    pub const RABBITMQ: &str = "rabbitmq";
    pub const KAFKA: &str = "kafka";
    pub const REDIS: &str = "redis";
    pub const CLOUDFLARE_QUEUES: &str = "cloudflare_queues";
}

/// Record a successful stream message send.
pub fn record_stream_success(stream_type: &str, duration_secs: f64, message_count: usize) {
    STREAM_MESSAGES_TOTAL.with_label_values(&[stream_type, "success"]).inc_by(message_count as f64);

    STREAM_MESSAGE_DURATION.with_label_values(&[stream_type]).observe(duration_secs);
}

/// Record a failed stream message send.
pub fn record_stream_error(stream_type: &str, duration_secs: f64) {
    STREAM_MESSAGES_TOTAL.with_label_values(&[stream_type, "error"]).inc();

    STREAM_MESSAGE_DURATION.with_label_values(&[stream_type]).observe(duration_secs);
}

/// Record a stream operation with automatic success/error handling.
pub fn record_stream_operation(
    stream_type: &str,
    success: bool,
    duration_secs: f64,
    message_count: usize,
) {
    if success {
        record_stream_success(stream_type, duration_secs, message_count);
    } else {
        record_stream_error(stream_type, duration_secs);
    }
}

/// RAII guard for timing stream operations.
pub struct StreamTimer {
    stream_type: &'static str,
    start: Instant,
}

impl StreamTimer {
    pub fn new(stream_type: &'static str) -> Self {
        Self { stream_type, start: Instant::now() }
    }

    /// Finish timing and record success with message count.
    pub fn finish_success(self, message_count: usize) {
        let duration = self.start.elapsed().as_secs_f64();
        record_stream_success(self.stream_type, duration, message_count);
    }

    /// Finish timing and record error.
    pub fn finish_error(self) {
        let duration = self.start.elapsed().as_secs_f64();
        record_stream_error(self.stream_type, duration);
    }
}
