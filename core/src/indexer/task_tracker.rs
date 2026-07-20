use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::sync::Lazy;
use tracing::warn;

use crate::metrics::indexing as metrics;

static INDEXING_TASKS: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

pub fn indexing_event_processing() {
    INDEXING_TASKS.fetch_add(1, Ordering::SeqCst);
    metrics::inc_active_tasks();
}

pub fn indexing_event_processed() {
    // Skip the decrement at 0 so the unsigned counter can't underflow;
    if INDEXING_TASKS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_err()
    {
        warn!("indexing_event_processed called with no active task, counter imbalance");
        return;
    }

    metrics::dec_active_tasks();
}

pub fn active_indexing_count() -> usize {
    INDEXING_TASKS.load(Ordering::SeqCst)
}
