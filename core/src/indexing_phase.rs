//! Whether the task currently handling an event batch is the live-indexing pass.
//!
//! Historical backfill does not set this. Code outside the indexer reads `false`.

use std::future::Future;

tokio::task_local! {
    static LIVE_INDEXING: bool;
}

/// `true` while this task is inside the live-indexing pass.
///
/// Historical batches, and any call outside the indexer, return `false`.
/// Rust handlers can use this to skip work that should only run on new blocks.
/// It does not change handler signatures.
pub fn in_live_indexing() -> bool {
    LIVE_INDEXING.try_with(|live| *live).unwrap_or(false)
}

pub(crate) async fn with_indexing_phase<F: Future>(live: bool, fut: F) -> F::Output {
    LIVE_INDEXING.scope(live, fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outside_the_indexer_is_not_live() {
        assert!(!in_live_indexing());
    }

    #[tokio::test]
    async fn scope_sets_the_phase_for_the_task_only() {
        assert!(!in_live_indexing());
        let seen = with_indexing_phase(true, async { in_live_indexing() }).await;
        assert!(seen);
        assert!(!in_live_indexing());
    }
}
