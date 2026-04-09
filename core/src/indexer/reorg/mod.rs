pub mod coordinator;
pub mod persistence;
pub mod task;
pub mod window;

pub use coordinator::ReorgCoordinator;
pub use persistence::LatestBlocksPersistence;
pub use task::EventTableInfo;
pub use window::BlockChainWindow;

use std::sync::Arc;

use alloy::primitives::{B256, U64};
use tracing::{debug, error, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;

/// Returns true if a reorg was detected and handled (caller should `continue`).
pub async fn detect_and_handle_reorg(
    coordinator: &mut ReorgCoordinator,
    block_number: u64,
    block_hash: B256,
    parent_hash: B256,
    log_prefix: &str,
    postgres: Option<&PostgresClient>,
    clickhouse: Option<&Arc<ClickhouseClient>>,
) -> bool {
    match coordinator.on_new_block(block_number, block_hash, parent_hash).await {
        Ok(Some(reorg_task)) => {
            warn!(
                "{} - REORG DETECTED by coordinator on block {} (fork_point: {}, depth: {}). Executing rollback.",
                log_prefix,
                block_number,
                reorg_task.fork_point,
                reorg_task.detection_point - reorg_task.fork_point + 1,
            );

            if let Err(e) = coordinator.handle_reorg(reorg_task, postgres, clickhouse).await {
                error!(
                    "{} - Failed to execute reorg rollback: {}",
                    log_prefix, e
                );
            }

            true
        }
        Ok(None) => false,
        Err(e) => {
            error!("{} - Reorg coordinator error: {}", log_prefix, e);
            false
        }
    }
}

/// Handles chain state notifications (reorgs, reverts, commits)
pub fn handle_chain_notification(
    notification: ChainStateNotification,
    info_log_name: &str,
    network: &str,
) {
    match notification {
        ChainStateNotification::Reorged {
            revert_from_block,
            revert_to_block,
            new_from_block,
            new_to_block,
            new_tip_hash,
        } => {
            let depth = revert_from_block.saturating_sub(revert_to_block);
            metrics::record_reorg(network, depth);

            warn!(
                "{} - REORG DETECTED! Need to revert blocks {} to {} and re-index {} to {} (new tip: {})",
                info_log_name,
                revert_from_block, revert_to_block,
                new_from_block, new_to_block,
                new_tip_hash
            );
            // TODO: In future PR, actually handle the reorg by reverting and re-indexing
        }
        ChainStateNotification::Reverted { from_block, to_block } => {
            let depth = from_block.saturating_sub(to_block);
            metrics::record_reorg(network, depth);

            warn!(
                "{} - CHAIN REVERTED! Blocks {} to {} have been reverted",
                info_log_name, from_block, to_block
            );
            // TODO: In future PR, mark affected logs as removed in the database
        }
        ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
            debug!(
                "{} - Chain committed: blocks {} to {} (tip: {})",
                info_log_name, from_block, to_block, tip_hash
            );
        }
    }
}

pub fn reorg_safe_distance_for_chain(chain_id: u64) -> U64 {
    if chain_id == 1 {
        U64::from(12)
    } else {
        U64::from(64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = 1;
        assert_eq!(reorg_safe_distance_for_chain(mainnet_chain_id), U64::from(12));

        let testnet_chain_id = 3;
        assert_eq!(reorg_safe_distance_for_chain(testnet_chain_id), U64::from(64));

        let other_chain_id = 42;
        assert_eq!(reorg_safe_distance_for_chain(other_chain_id), U64::from(64));
    }
}
