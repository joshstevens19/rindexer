//! Timer utilities for automatic duration recording.
//!
//! Provides RAII guards that record elapsed time to Prometheus histograms on drop.

use prometheus::HistogramVec;
use std::time::Instant;

/// RAII guard for timing operations.
///
/// Records the elapsed duration to a histogram when dropped.
/// Use `stop()` to record early and get the elapsed time.
pub struct TimerGuard<'a> {
    histogram: &'a HistogramVec,
    labels: Vec<String>,
    start: Instant,
    stopped: bool,
}

impl<'a> TimerGuard<'a> {
    /// Create a new timer guard.
    pub fn new(histogram: &'a HistogramVec, labels: &[&str]) -> Self {
        Self {
            histogram,
            labels: labels.iter().map(|s| s.to_string()).collect(),
            start: Instant::now(),
            stopped: false,
        }
    }

    /// Get elapsed time without stopping the timer.
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    /// Stop the timer and record the duration. Returns elapsed seconds.
    pub fn stop(mut self) -> f64 {
        self.stopped = true;
        let elapsed = self.start.elapsed().as_secs_f64();
        let label_refs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        self.histogram.with_label_values(&label_refs).observe(elapsed);
        elapsed
    }
}

impl Drop for TimerGuard<'_> {
    fn drop(&mut self) {
        if !self.stopped {
            let elapsed = self.start.elapsed().as_secs_f64();
            let label_refs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
            self.histogram.with_label_values(&label_refs).observe(elapsed);
        }
    }
}

/// Simpler callback-based timer for custom recording logic.
pub struct CallbackTimer<F: FnOnce(f64)> {
    start: Instant,
    callback: Option<F>,
}

impl<F: FnOnce(f64)> CallbackTimer<F> {
    pub fn new(callback: F) -> Self {
        Self {
            start: Instant::now(),
            callback: Some(callback),
        }
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    pub fn stop(mut self) -> f64 {
        let elapsed = self.start.elapsed().as_secs_f64();
        if let Some(cb) = self.callback.take() {
            cb(elapsed);
        }
        elapsed
    }

    pub fn cancel(mut self) {
        self.callback = None;
    }
}

impl<F: FnOnce(f64)> Drop for CallbackTimer<F> {
    fn drop(&mut self) {
        if let Some(cb) = self.callback.take() {
            cb(self.start.elapsed().as_secs_f64());
        }
    }
}
