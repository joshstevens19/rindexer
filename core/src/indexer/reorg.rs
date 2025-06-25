use alloy::primitives::{U256, U64};
use tracing::{debug, warn};

use crate::provider::ChainStateNotification;

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
            warn!(
                "{}::{} - REORG DETECTED! Need to revert blocks {} to {} and re-index {} to {} (new tip: {})",
                info_log_name,
                network,
                revert_from_block, revert_to_block,
                new_from_block, new_to_block,
                new_tip_hash
            );
            // TODO: In future PR, actually handle the reorg by reverting and re-indexing
        }
        ChainStateNotification::Reverted { from_block, to_block } => {
            warn!(
                "{}::{} - CHAIN REVERTED! Blocks {} to {} have been reverted",
                info_log_name, network, from_block, to_block
            );
            // TODO: In future PR, mark affected logs as removed in the database
        }
        ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
            debug!(
                "{}::{} - Chain committed: blocks {} to {} (tip: {})",
                info_log_name, network, from_block, to_block, tip_hash
            );
        }
    }
}

pub fn reorg_safe_distance_for_chain(chain_id: &U256) -> U64 {
    if chain_id == &U256::from(1) {
        U64::from(12)
    } else {
        U64::from(64)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::*;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = U256::from(1);
        assert_eq!(reorg_safe_distance_for_chain(&mainnet_chain_id), U64::from(12));

        let testnet_chain_id = U256::from(3);
        assert_eq!(reorg_safe_distance_for_chain(&testnet_chain_id), U64::from(64));

        let other_chain_id = U256::from(42);
        assert_eq!(reorg_safe_distance_for_chain(&other_chain_id), U64::from(64));
    }
}
