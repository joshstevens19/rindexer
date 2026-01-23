//! Adaptive concurrency control for RPC requests.
//!
//! This module provides centralized rate limiting and adaptive scaling for RPC requests.
//! It's used by both the RPC layer (layer_extensions.rs) and the indexer (tables.rs).

use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tracing::{debug, info, warn};

/// Adaptive concurrency controller that scales based on success/failure rates.
/// Scales up when requests succeed, scales down when rate limits are hit.
pub struct AdaptiveConcurrency {
    current: AtomicUsize,
    min: usize,
    max: usize,
    /// Count of consecutive successes - used to decide when to scale up
    consecutive_successes: AtomicUsize,
    /// Threshold of consecutive successes before scaling up
    scale_up_threshold: usize,
    /// Current backoff delay in milliseconds (for rate-limited free nodes)
    backoff_ms: AtomicU64,
    /// Maximum backoff delay in milliseconds (30 seconds)
    max_backoff_ms: u64,
    /// Current batch size (number of calls per RPC batch)
    batch_size: AtomicUsize,
    /// Minimum batch size
    min_batch_size: usize,
    /// Maximum batch size
    max_batch_size: usize,
    /// Total rate limit count (for diagnostics)
    rate_limit_count: AtomicU64,
}

impl AdaptiveConcurrency {
    pub fn new(initial: usize, min: usize, max: usize) -> Self {
        Self {
            current: AtomicUsize::new(initial),
            min,
            max,
            consecutive_successes: AtomicUsize::new(0),
            scale_up_threshold: 10, // Scale up after 10 consecutive successes
            backoff_ms: AtomicU64::new(0),
            max_backoff_ms: 30_000,           // Max 30 second backoff
            batch_size: AtomicUsize::new(50), // Start with 50 calls per batch
            min_batch_size: 5,                // Minimum 5 calls per batch
            max_batch_size: 100,              // Maximum 100 calls per batch
            rate_limit_count: AtomicU64::new(0),
        }
    }

    /// Get current concurrency level
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Get current batch size (calls per RPC batch)
    pub fn current_batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed)
    }

    /// Get current backoff delay in milliseconds
    pub fn current_backoff_ms(&self) -> u64 {
        self.backoff_ms.load(Ordering::Relaxed)
    }

    /// Get total rate limit count
    pub fn rate_limit_count(&self) -> u64 {
        self.rate_limit_count.load(Ordering::Relaxed)
    }

    /// Wait for the backoff delay if one is active.
    /// Call this before making RPC requests to respect rate limits.
    pub async fn wait_for_backoff(&self) {
        let delay_ms = self.backoff_ms.load(Ordering::Relaxed);
        if delay_ms > 0 {
            debug!("Rate limit backoff: waiting {}ms before next RPC request", delay_ms);
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
        }
    }

    /// Record a successful request - may scale up and reduce backoff
    pub fn record_success(&self) {
        // Reduce backoff on success (by 25%, minimum 0)
        let current_backoff = self.backoff_ms.load(Ordering::Relaxed);
        if current_backoff > 0 {
            let new_backoff = current_backoff * 3 / 4; // Reduce by 25%
            if self
                .backoff_ms
                .compare_exchange(
                    current_backoff,
                    new_backoff,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
                && new_backoff == 0
            {
                info!("Adaptive concurrency: backoff cleared after successful requests");
            }
        }

        let successes = self.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;
        if successes >= self.scale_up_threshold {
            self.consecutive_successes.store(0, Ordering::Relaxed);

            // Scale up concurrency
            let current = self.current.load(Ordering::Relaxed);
            if current < self.max {
                // Scale up by 20% or at least 1
                let increase = std::cmp::max(1, current / 5);
                let new_val = std::cmp::min(self.max, current + increase);
                if self
                    .current
                    .compare_exchange(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    info!(
                        "Adaptive concurrency: scaling UP from {} to {} (consecutive successes)",
                        current, new_val
                    );
                }
            }

            // Scale up batch size (by 20% or at least 5)
            let current_batch = self.batch_size.load(Ordering::Relaxed);
            if current_batch < self.max_batch_size {
                let increase = std::cmp::max(5, current_batch / 5);
                let new_batch = std::cmp::min(self.max_batch_size, current_batch + increase);
                if self
                    .batch_size
                    .compare_exchange(
                        current_batch,
                        new_batch,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    info!(
                        "Adaptive batch size: scaling UP from {} to {} (consecutive successes)",
                        current_batch, new_batch
                    );
                }
            }
        }
    }

    /// Record a rate limit error - scale down aggressively and increase backoff
    pub fn record_rate_limit(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let count = self.rate_limit_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Increase backoff (double it, starting from 500ms, max 30s)
        let current_backoff = self.backoff_ms.load(Ordering::Relaxed);
        let new_backoff = if current_backoff == 0 {
            500 // Start with 500ms
        } else {
            std::cmp::min(self.max_backoff_ms, current_backoff * 2)
        };
        let _ = self.backoff_ms.compare_exchange(
            current_backoff,
            new_backoff,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );

        // Scale down batch size by 50%
        let current_batch = self.batch_size.load(Ordering::Relaxed);
        let new_batch = std::cmp::max(self.min_batch_size, current_batch / 2);
        let batch_changed = self
            .batch_size
            .compare_exchange(current_batch, new_batch, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();

        // Scale down concurrency by 50%
        let current = self.current.load(Ordering::Relaxed);
        if current > self.min {
            let new_val = std::cmp::max(self.min, current / 2);
            if self
                .current
                .compare_exchange(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                warn!(
                    "Adaptive: concurrency {} -> {}, batch {} -> {}, backoff: {}ms, total_rate_limits: {} (rate limit)",
                    current, new_val, current_batch, new_batch, new_backoff, count
                );
            }
        } else if batch_changed {
            warn!(
                "Adaptive: at min concurrency {}, batch {} -> {}, backoff: {}ms, total_rate_limits: {} (rate limit)",
                self.min, current_batch, new_batch, new_backoff, count
            );
        } else {
            warn!(
                "Adaptive: at minimum (concurrency: {}, batch: {}), backoff: {}ms, total_rate_limits: {}",
                self.min, self.min_batch_size, new_backoff, count
            );
        }
    }

    /// Record a general error - scale down slightly
    pub fn record_error(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        let current = self.current.load(Ordering::Relaxed);
        if current > self.min {
            // Scale down by 10% on general error
            let decrease = std::cmp::max(1, current / 10);
            let new_val = std::cmp::max(self.min, current.saturating_sub(decrease));
            if self
                .current
                .compare_exchange(current, new_val, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                debug!(
                    "Adaptive concurrency: scaling down from {} to {} (error)",
                    current, new_val
                );
            }
        }
    }
}

/// Global adaptive concurrency controller for RPC batches.
/// Used by both the RPC layer and indexer code.
pub static ADAPTIVE_CONCURRENCY: Lazy<AdaptiveConcurrency> = Lazy::new(|| {
    AdaptiveConcurrency::new(
        20,  // Start with 20 concurrent batches
        2,   // Minimum 2
        200, // Maximum 200
    )
});
