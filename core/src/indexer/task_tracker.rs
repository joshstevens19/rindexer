use std::sync::atomic::{AtomicUsize, Ordering};

use once_cell::sync::Lazy;

static INDEXING_TASKS: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

pub fn indexing_event_processing() {
    INDEXING_TASKS.fetch_add(1, Ordering::SeqCst);
}

pub fn indexing_event_processed() {
    INDEXING_TASKS.fetch_sub(1, Ordering::SeqCst);
}

pub fn active_indexing_count() -> usize {
    INDEXING_TASKS.load(Ordering::SeqCst)
}
