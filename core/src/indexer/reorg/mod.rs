pub mod coordinator;
pub mod persistence;
pub mod task;
pub mod window;

use alloy::primitives::{B256, U64};
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::event::callback_registry::EventCallbackRegistry;
use crate::indexer::fetch_logs::ReorgInfo;
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;
use crate::streams::StreamsClients;

pub use coordinator::ReorgCoordinator;
pub use persistence::LatestBlocksPersistence;
pub use task::{DerivedTableInfo, EventTableInfo};
pub use window::BlockChainWindow;
// Re-export ReorgContext so callers can use `reorg::ReorgContext`
// (it's defined just below)

/// Bundles the optional DB clients, callback registry, and streams clients
/// that reorg recovery needs, avoiding parameter sprawl.
pub struct ReorgContext<'a> {
    pub postgres: Option<&'a PostgresClient>,
    pub clickhouse: Option<&'a Arc<ClickhouseClient>>,
    pub registry: Option<&'a EventCallbackRegistry>,
    pub streams_clients: Option<&'a StreamsClients>,
}

/// Returns `Some(fork_point)` if a reorg was detected and handled (caller should rewind and `continue`),
/// or `None` if no reorg was detected.
pub async fn detect_and_handle_reorg(
    coordinator: &mut ReorgCoordinator,
    block_number: u64,
    block_hash: B256,
    parent_hash: B256,
    log_prefix: &str,
    ctx: &ReorgContext<'_>,
) -> Option<u64> {
    match coordinator.on_new_block(block_number, block_hash, parent_hash).await {
        Ok(Some(reorg_task)) => {
            let fork_point = reorg_task.fork_point;
            warn!(
                "{} - REORG DETECTED by coordinator on block {} (fork_point: {}, depth: {}). Executing rollback.",
                log_prefix,
                block_number,
                fork_point,
                reorg_task.detection_point - fork_point + 1,
            );

            if let Err(e) =
                coordinator.handle_reorg(reorg_task, ctx).await
            {
                error!(
                    "{} - Failed to execute reorg rollback: {}",
                    log_prefix, e
                );
            }

            Some(fork_point)
        }
        Ok(None) => None,
        Err(e) => {
            error!("{} - Reorg coordinator error: {}", log_prefix, e);
            None
        }
    }
}

/// Broadcast event sent when a reorg is detected and recovery is complete.
/// Available in code-gen mode via `EventContext::reorg_receiver()`.
#[derive(Debug, Clone, Serialize)]
pub struct ReorgEvent {
    pub network: String,
    pub fork_block: u64,
    pub depth: u64,
    pub affected_tx_hashes: Vec<B256>,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
}

/// Handles chain state notifications (reorgs, reverts, commits).
/// Used by reth feature-gated providers that emit chain state events.
/// Returns `Some(ReorgInfo)` when a reorg/revert is detected, so the caller
/// can forward it to the main indexing loop for recovery.
pub fn handle_chain_notification(
    notification: ChainStateNotification,
    info_log_name: &str,
    network: &str,
) -> Option<ReorgInfo> {
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
                "{} - REORG (reth): revert blocks {} to {}, re-index {} to {} (new tip: {})",
                info_log_name,
                revert_from_block,
                revert_to_block,
                new_from_block,
                new_to_block,
                new_tip_hash
            );

            Some(ReorgInfo {
                fork_block: U64::from(revert_to_block),
                depth,
                affected_tx_hashes: vec![],
            })
        }
        ChainStateNotification::Reverted { from_block, to_block } => {
            let depth = from_block.saturating_sub(to_block);
            metrics::record_reorg(network, depth);

            warn!(
                "{} - CHAIN REVERTED (reth): blocks {} to {} have been reverted",
                info_log_name, from_block, to_block
            );

            Some(ReorgInfo { fork_block: U64::from(to_block), depth, affected_tx_hashes: vec![] })
        }
        ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
            debug!(
                "{} - Chain committed: blocks {} to {} (tip: {})",
                info_log_name, from_block, to_block, tip_hash
            );
            None
        }
    }
}

/// Returns the default safe reorg distance (in blocks) for a given chain.
/// Used when `reorg_safe_distance: true` in YAML (no custom override).
pub fn reorg_safe_distance_for_chain(chain_id: u64) -> u64 {
    match chain_id {
        // Ethereum mainnet — Casper FFG finality (~13 min, 2 epochs)
        1 => 20,
        // Polygon PoS — historically deep reorgs (157-block in Feb 2023)
        137 => 200,
        // Arbitrum One — sequencer-ordered, no observed reorgs
        42161 => 24,
        // Optimism — sequencer-ordered, no observed reorgs
        10 => 24,
        // Base — sequencer-ordered (Coinbase), no observed reorgs
        8453 => 24,
        // BNB Smart Chain — 3s blocks, DPoS
        56 => 24,
        // Avalanche C-Chain — sub-second finality with Snowman consensus
        43114 => 12,
        // Gnosis Chain (xDai) — POSDAO/AuRa consensus
        100 => 24,
        // All other chains — conservative default
        _ => 64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::manifest::contract::ReorgSafeDistance;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        assert_eq!(reorg_safe_distance_for_chain(1), 20); // Ethereum
        assert_eq!(reorg_safe_distance_for_chain(137), 200); // Polygon
        assert_eq!(reorg_safe_distance_for_chain(42161), 24); // Arbitrum
        assert_eq!(reorg_safe_distance_for_chain(10), 24); // Optimism
        assert_eq!(reorg_safe_distance_for_chain(8453), 24); // Base
        assert_eq!(reorg_safe_distance_for_chain(56), 24); // BNB
        assert_eq!(reorg_safe_distance_for_chain(43114), 12); // Avalanche
        assert_eq!(reorg_safe_distance_for_chain(100), 24); // Gnosis
        assert_eq!(reorg_safe_distance_for_chain(999), 64); // Unknown chain
    }

    // ======================================================================
    // ReorgSafeDistance serde (untagged enum: bool | u64)
    // ======================================================================

    #[test]
    fn test_reorg_safe_distance_serde_true() {
        let yaml = "true";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Enabled(true)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_false() {
        let yaml = "false";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Enabled(false)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_custom_u64() {
        let yaml = "200";
        let val: ReorgSafeDistance = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(val, ReorgSafeDistance::Custom(200)));
    }

    #[test]
    fn test_reorg_safe_distance_serde_roundtrip() {
        let variants = vec![
            ReorgSafeDistance::Enabled(true),
            ReorgSafeDistance::Enabled(false),
            ReorgSafeDistance::Custom(42),
        ];
        for original in variants {
            let yaml = serde_yaml::to_string(&original).unwrap();
            let parsed: ReorgSafeDistance = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(original.resolve(1), parsed.resolve(1));
        }
    }

    // ======================================================================
    // ReorgSafeDistance::resolve()
    // ======================================================================

    #[test]
    fn test_resolve_enabled_true_uses_chain_default() {
        let rsd = ReorgSafeDistance::Enabled(true);
        assert_eq!(rsd.resolve(137), Some(200)); // Polygon default
        assert_eq!(rsd.resolve(1), Some(20)); // Ethereum default
        assert_eq!(rsd.resolve(999), Some(64)); // Unknown chain fallback
    }

    #[test]
    fn test_resolve_enabled_false_returns_none() {
        let rsd = ReorgSafeDistance::Enabled(false);
        assert_eq!(rsd.resolve(137), None);
        assert_eq!(rsd.resolve(1), None);
    }

    #[test]
    fn test_resolve_custom_overrides_chain_default() {
        let rsd = ReorgSafeDistance::Custom(500);
        assert_eq!(rsd.resolve(137), Some(500));
        assert_eq!(rsd.resolve(1), Some(500));
        assert_eq!(rsd.resolve(999), Some(500));
    }

    // ======================================================================
    // handle_chain_notification()
    // ======================================================================

    #[test]
    fn test_handle_chain_notification_reorged() {
        let notification = ChainStateNotification::Reorged {
            revert_from_block: 110,
            revert_to_block: 100,
            new_from_block: 100,
            new_to_block: 112,
            new_tip_hash: B256::from([0xab; 32]),
        };
        let result = handle_chain_notification(notification, "test", "polygon");
        assert!(result.is_some());
        let reorg = result.unwrap();
        assert_eq!(reorg.fork_block, U64::from(100));
        assert_eq!(reorg.depth, 10); // 110 - 100
        assert!(reorg.affected_tx_hashes.is_empty());
    }

    #[test]
    fn test_handle_chain_notification_reverted() {
        let notification = ChainStateNotification::Reverted { from_block: 200, to_block: 195 };
        let result = handle_chain_notification(notification, "test", "ethereum");
        assert!(result.is_some());
        let reorg = result.unwrap();
        assert_eq!(reorg.fork_block, U64::from(195));
        assert_eq!(reorg.depth, 5); // 200 - 195
    }

    #[test]
    fn test_handle_chain_notification_committed_returns_none() {
        let notification = ChainStateNotification::Committed {
            from_block: 100,
            to_block: 200,
            tip_hash: B256::ZERO,
        };
        let result = handle_chain_notification(notification, "test", "polygon");
        assert!(result.is_none());
    }

    // ======================================================================
    // ReorgEvent serialization
    // ======================================================================

    #[test]
    fn test_reorg_event_serialization() {
        let tx_hash = B256::from([0xde; 32]);
        let event = ReorgEvent {
            network: "polygon".to_string(),
            fork_block: 1000,
            depth: 3,
            affected_tx_hashes: vec![tx_hash],
            indexer_name: "TestIndexer".to_string(),
            contract_name: "USDC".to_string(),
            event_name: "Transfer".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["network"], "polygon");
        assert_eq!(json["fork_block"], 1000);
        assert_eq!(json["depth"], 3);
        assert_eq!(json["indexer_name"], "TestIndexer");
        assert_eq!(json["contract_name"], "USDC");
        assert_eq!(json["event_name"], "Transfer");

        let hashes = json["affected_tx_hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        let hash_str = hashes[0].as_str().unwrap();
        assert!(hash_str.starts_with("0x"));
        assert_eq!(hash_str.len(), 66); // "0x" + 64 hex chars
    }

    #[test]
    fn test_reorg_event_empty_tx_hashes() {
        let event = ReorgEvent {
            network: "ethereum".to_string(),
            fork_block: 5000,
            depth: 1,
            affected_tx_hashes: vec![],
            indexer_name: "Idx".to_string(),
            contract_name: "DAI".to_string(),
            event_name: "Approval".to_string(),
        };

        let json = serde_json::to_value(&event).unwrap();
        assert!(json["affected_tx_hashes"].as_array().unwrap().is_empty());
    }
}
