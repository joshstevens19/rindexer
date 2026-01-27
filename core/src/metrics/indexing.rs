//! Indexing-specific metrics helpers.

use super::definitions::{
    ACTIVE_INDEXING_TASKS, BLOCKS_BEHIND, BLOCKS_INDEXED_TOTAL, EVENTS_PROCESSED_TOTAL,
    LAST_SYNCED_BLOCK, LATEST_CHAIN_BLOCK, REORGS_DETECTED_TOTAL, REORG_DEPTH,
};

/// Record events being indexed for a contract/event pair.
pub fn record_events_indexed(
    network: &str,
    contract: &str,
    event: &str,
    event_count: u64,
    last_block: u64,
    latest_chain_block: Option<u64>,
) {
    let labels = [network, contract, event];

    if event_count > 0 {
        EVENTS_PROCESSED_TOTAL.with_label_values(&labels).inc_by(event_count as f64);
    }

    LAST_SYNCED_BLOCK.with_label_values(&labels).set(last_block as f64);

    if let Some(chain_block) = latest_chain_block {
        LATEST_CHAIN_BLOCK.with_label_values(&[network]).set(chain_block as f64);

        let behind = chain_block.saturating_sub(last_block);
        BLOCKS_BEHIND.with_label_values(&labels).set(behind as f64);
    }
}

/// Record a range of blocks being indexed.
pub fn record_blocks_indexed(network: &str, contract: &str, event: &str, block_count: u64) {
    if block_count > 0 {
        BLOCKS_INDEXED_TOTAL
            .with_label_values(&[network, contract, event])
            .inc_by(block_count as f64);
    }
}

/// Update the last synced block gauge.
pub fn set_last_synced_block(network: &str, contract: &str, event: &str, block: u64) {
    LAST_SYNCED_BLOCK.with_label_values(&[network, contract, event]).set(block as f64);
}

/// Update the latest chain block gauge.
pub fn set_latest_chain_block(network: &str, block: u64) {
    LATEST_CHAIN_BLOCK.with_label_values(&[network]).set(block as f64);
}

/// Update the blocks behind gauge.
pub fn set_blocks_behind(
    network: &str,
    contract: &str,
    event: &str,
    last_synced: u64,
    latest: u64,
) {
    let behind = latest.saturating_sub(last_synced);
    BLOCKS_BEHIND.with_label_values(&[network, contract, event]).set(behind as f64);
}

/// Update active indexing task count.
pub fn set_active_tasks(count: usize) {
    ACTIVE_INDEXING_TASKS.set(count as f64);
}

/// Increment active indexing tasks.
pub fn inc_active_tasks() {
    ACTIVE_INDEXING_TASKS.inc();
}

/// Decrement active indexing tasks.
pub fn dec_active_tasks() {
    ACTIVE_INDEXING_TASKS.dec();
}

/// Record a chain reorganization event.
pub fn record_reorg(network: &str, depth: u64) {
    REORGS_DETECTED_TOTAL.with_label_values(&[network]).inc();
    REORG_DEPTH.with_label_values(&[network]).set(depth as f64);
}
