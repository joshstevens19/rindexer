use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::sync::Lazy;
use tracing::debug;

static INDEXING_TASKS: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

pub fn indexing_event_processing() {
    let current = INDEXING_TASKS.fetch_add(1, Ordering::SeqCst);
    let new_count = current + 1;
    debug!("Task started - active tasks: {}", new_count);
}

pub fn indexing_event_processed() {
    let current = INDEXING_TASKS.fetch_sub(1, Ordering::SeqCst);
    let new_count = current - 1;
    debug!("Task completed - active tasks: {}", new_count);
}

pub fn active_indexing_count() -> usize {
    INDEXING_TASKS.load(Ordering::SeqCst)
}
