use alloy::primitives::{B256, U64};
use lru::LruCache;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::{
    generate_internal_event_table_name, generate_internal_event_table_name_no_shorten,
};
use crate::event::config::EventProcessingConfig;
use crate::helpers::camel_to_snake;
use crate::indexer::fetch_logs::{BlockMeta, ReorgInfo};
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::JsonRpcCachedProvider;
use crate::PostgresClient;

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

/// Walk backwards from the reorged block to find the fork point.
///
/// Compares cached block hashes with current canonical chain hashes from the RPC.
/// Returns the first block number that diverged (i.e., the fork point).
pub async fn find_fork_point(
    block_cache: &LruCache<u64, BlockMeta>,
    provider: &Arc<JsonRpcCachedProvider>,
    reorged_block: u64,
) -> u64 {
    // Collect cached block numbers walking backwards from just before the reorg.
    // Cap scan at cache size to avoid iterating millions of empty slots.
    let mut blocks_to_check: Vec<U64> = Vec::new();
    let max_scan = block_cache.len() + 64; // allow gaps between cached blocks
    let scan_start = reorged_block.saturating_sub(1);
    let scan_end = scan_start.saturating_sub(max_scan as u64);
    for block_num in (scan_end..=scan_start).rev() {
        if block_cache.peek(&block_num).is_some() {
            blocks_to_check.push(U64::from(block_num));
        }
        if blocks_to_check.len() >= 64 {
            break;
        }
    }

    if blocks_to_check.is_empty() {
        warn!("No cached blocks to compare for fork point discovery, using reorged_block");
        return reorged_block;
    }

    match provider.get_block_by_number_batch(&blocks_to_check, false).await {
        Ok(canonical_blocks) => {
            // Check each canonical block against our cache (newest first)
            for block in canonical_blocks {
                let block_num = block.header.number;
                let canonical_hash = block.header.hash;

                if let Some(cached) = block_cache.peek(&block_num) {
                    if cached.hash == canonical_hash {
                        info!(
                            "Fork point found: block {} matches canonical chain, fork at {}",
                            block_num,
                            block_num + 1
                        );
                        return block_num + 1;
                    }
                }
            }

            let oldest = blocks_to_check.last().map(|b| b.to::<u64>()).unwrap_or(reorged_block);
            warn!(
                "Could not find matching block in cache (checked {} blocks), using oldest: {}",
                blocks_to_check.len(),
                oldest
            );
            oldest
        }
        Err(e) => {
            error!("Failed to fetch blocks for fork point discovery: {:?}", e);
            reorged_block.saturating_sub(1)
        }
    }
}

/// Handles reorg recovery: collects affected tx hashes, deletes orphaned events, and rewinds
/// the checkpoint. Returns the union of tx hashes from the reorg signal and from storage.
pub async fn handle_reorg_recovery(
    config: &Arc<EventProcessingConfig>,
    reorg: &ReorgInfo,
) -> Vec<B256> {
    let fork_block = reorg.fork_block.to::<u64>();
    let network = &config.network_contract().network;
    let indexer_name = config.indexer_name();
    let contract_name = config.contract_name();
    let event_name = config.event_name();
    let schema = generate_indexer_contract_schema_name(&indexer_name, &contract_name);
    let event_table_name = camel_to_snake(&event_name);
    let rewind_block = fork_block.saturating_sub(1);

    info!(
        "Reorg recovery: deleting events from block >= {} for {}.{} on {} (depth={})",
        fork_block, schema, event_table_name, network, reorg.depth
    );

    // Collect tx hashes from storage before deletion
    let mut all_tx_hashes: std::collections::HashSet<B256> =
        reorg.affected_tx_hashes.iter().copied().collect();

    if let Some(postgres) = &config.postgres() {
        let db_hashes = collect_affected_tx_hashes_postgres(
            postgres,
            &schema,
            &event_table_name,
            fork_block,
            network,
        )
        .await;
        all_tx_hashes.extend(db_hashes);
        delete_events_postgres(postgres, &schema, &event_table_name, fork_block, network).await;
        rewind_checkpoint_postgres(postgres, &schema, &event_name, rewind_block, network).await;
    }

    if let Some(clickhouse) = &config.clickhouse() {
        delete_events_clickhouse(clickhouse, &schema, &event_table_name, fork_block).await;
        rewind_checkpoint_clickhouse(clickhouse, &schema, &event_name, rewind_block, network).await;
    }

    // Delete derived/custom table rows affected by the reorg
    let tables = config.tables();
    if !tables.is_empty() {
        delete_derived_table_rows(
            &tables,
            &config.postgres(),
            &config.clickhouse(),
            fork_block,
            network,
        )
        .await;
    }

    let result: Vec<B256> = all_tx_hashes.into_iter().collect();

    // Broadcast reorg event (for code-gen mode subscribers)
    if let Some(sender) = config.reorg_sender() {
        let _ = sender.send(ReorgEvent {
            network: network.to_string(),
            fork_block,
            depth: reorg.depth,
            affected_tx_hashes: result.clone(),
            indexer_name: indexer_name.clone(),
            contract_name: contract_name.clone(),
            event_name: event_name.clone(),
        });
    }

    // Stream retraction event (for no-code mode streams: webhooks, Kafka, etc.)
    if let Some(streams) = config.streams_clients().as_ref() {
        if let Err(e) = streams.stream_reorg(network, fork_block, reorg.depth, &result).await {
            error!("Failed to stream reorg retraction: {:?}", e);
        }
    }

    info!(
        "Reorg recovery complete: checkpoint rewound to block {} for {}.{} ({} affected txs)",
        rewind_block,
        schema,
        event_table_name,
        result.len()
    );

    result
}

/// Queries PostgreSQL for distinct tx hashes in blocks >= fork_block.
async fn collect_affected_tx_hashes_postgres(
    postgres: &Arc<PostgresClient>,
    schema: &str,
    event_table: &str,
    fork_block: u64,
    network: &str,
) -> Vec<B256> {
    let full_table = format!("{}.{}", schema, event_table);
    let query = format!(
        "SELECT DISTINCT tx_hash FROM {} WHERE block_number >= {} AND network = '{}'",
        full_table, fork_block, network
    );

    match postgres.query(&query, &[]).await {
        Ok(rows) => {
            let hashes: Vec<B256> = rows
                .iter()
                .filter_map(|row| {
                    let hex_str: String = row.get(0);
                    hex_str.parse::<B256>().ok()
                })
                .collect();
            debug!("PostgreSQL: found {} affected tx hashes in {}", hashes.len(), full_table);
            hashes
        }
        Err(e) => {
            warn!("PostgreSQL: failed to collect affected tx hashes: {:?}", e);
            vec![]
        }
    }
}

async fn delete_events_postgres(
    postgres: &Arc<PostgresClient>,
    schema: &str,
    event_table: &str,
    fork_block: u64,
    network: &str,
) {
    let full_table = format!("{}.{}", schema, event_table);
    let query = format!(
        "DELETE FROM {} WHERE block_number >= {} AND network = '{}'",
        full_table, fork_block, network
    );

    match postgres.batch_execute(&query).await {
        Ok(_) => info!("PostgreSQL: deleted events from block >= {} in {}", fork_block, full_table),
        Err(e) => error!("PostgreSQL: failed to delete reorged events: {:?}", e),
    }
}

async fn delete_events_clickhouse(
    clickhouse: &Arc<ClickhouseClient>,
    schema: &str,
    event_table: &str,
    fork_block: u64,
) {
    let full_table = format!("{}.{}", schema, event_table);
    // mutations_sync = 1 makes the DELETE synchronous — waits for completion before returning.
    // Without this, rindexer can re-index and insert new events before the old ones are deleted.
    let query = format!(
        "ALTER TABLE {} DELETE WHERE block_number >= {} SETTINGS mutations_sync = 1",
        full_table, fork_block
    );

    match clickhouse.execute(&query).await {
        Ok(_) => {
            info!("ClickHouse: deleted events from block >= {} in {}", fork_block, full_table)
        }
        Err(e) => error!("ClickHouse: failed to delete reorged events: {:?}", e),
    }
}

async fn rewind_checkpoint_postgres(
    postgres: &Arc<PostgresClient>,
    schema: &str,
    event_name: &str,
    rewind_block: u64,
    network: &str,
) {
    let internal_table = generate_internal_event_table_name(schema, event_name);
    let query = format!(
        "UPDATE rindexer_internal.{} SET last_synced_block = {} WHERE network = '{}'",
        internal_table, rewind_block, network
    );

    match postgres.batch_execute(&query).await {
        Ok(_) => info!(
            "PostgreSQL: checkpoint rewound to block {} in rindexer_internal.{}",
            rewind_block, internal_table
        ),
        Err(e) => error!("PostgreSQL: failed to rewind checkpoint: {:?}", e),
    }
}

async fn rewind_checkpoint_clickhouse(
    clickhouse: &Arc<ClickhouseClient>,
    schema: &str,
    event_name: &str,
    rewind_block: u64,
    network: &str,
) {
    let internal_table = generate_internal_event_table_name_no_shorten(schema, event_name);
    let query = format!(
        "INSERT INTO rindexer_internal.{} (network, last_synced_block) VALUES ('{}', {})",
        internal_table, network, rewind_block
    );

    match clickhouse.execute(&query).await {
        Ok(_) => info!(
            "ClickHouse: checkpoint rewound to block {} in rindexer_internal.{}",
            rewind_block, internal_table
        ),
        Err(e) => error!("ClickHouse: failed to rewind checkpoint: {:?}", e),
    }
}

/// Deletes rows from derived/custom tables where `rindexer_block_number >= fork_block`.
/// For `cross_chain` tables, no network filter is applied.
pub async fn delete_derived_table_rows(
    tables: &[super::tables::TableRuntime],
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
    fork_block: u64,
    network: &str,
) {
    for table_rt in tables {
        let full_table = &table_rt.full_table_name;
        let is_cross_chain = table_rt.table.cross_chain;

        if let Some(pg) = postgres {
            let query = if is_cross_chain {
                format!("DELETE FROM {} WHERE rindexer_block_number >= {}", full_table, fork_block)
            } else {
                format!(
                    "DELETE FROM {} WHERE rindexer_block_number >= {} AND network = '{}'",
                    full_table, fork_block, network
                )
            };

            match pg.batch_execute(&query).await {
                Ok(_) => info!(
                    "PostgreSQL: deleted derived table rows from block >= {} in {}",
                    fork_block, full_table
                ),
                Err(e) => error!(
                    "PostgreSQL: failed to delete derived table rows in {}: {:?}",
                    full_table, e
                ),
            }
        }

        if let Some(ch) = clickhouse {
            let query = if is_cross_chain {
                format!(
                    "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} SETTINGS mutations_sync = 1",
                    full_table, fork_block
                )
            } else {
                format!(
                    "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} AND network = '{}' SETTINGS mutations_sync = 1",
                    full_table, fork_block, network
                )
            };

            match ch.execute(&query).await {
                Ok(_) => info!(
                    "ClickHouse: deleted derived table rows from block >= {} in {}",
                    fork_block, full_table
                ),
                Err(e) => error!(
                    "ClickHouse: failed to delete derived table rows in {}: {:?}",
                    full_table, e
                ),
            }
        }
    }
}

/// Handles reorg recovery for native transfer indexing (PostgreSQL only, no ClickHouse for traces).
pub async fn handle_native_transfer_reorg_recovery(
    postgres: &Option<Arc<PostgresClient>>,
    indexer_name: &str,
    network: &str,
    fork_block: u64,
    depth: u64,
    streams_clients: &Option<Arc<Option<crate::streams::StreamsClients>>>,
) {
    let schema = generate_indexer_contract_schema_name(indexer_name, "EvmTraces");
    let event_table_name = "native_transfer";
    let rewind_block = fork_block.saturating_sub(1);

    info!(
        "Native transfer reorg recovery: deleting from block >= {} for {}.{} on {}",
        fork_block, schema, event_table_name, network
    );

    let mut affected_tx_hashes = Vec::new();

    if let Some(pg) = postgres {
        affected_tx_hashes =
            collect_affected_tx_hashes_postgres(pg, &schema, event_table_name, fork_block, network)
                .await;
        delete_events_postgres(pg, &schema, event_table_name, fork_block, network).await;
        rewind_checkpoint_postgres(pg, &schema, "native_transfer", rewind_block, network).await;
        info!(
            "Native transfer reorg recovery complete: checkpoint rewound to block {} for {}.{}",
            rewind_block, schema, event_table_name
        );
    }

    // Stream retraction for native transfer reorgs
    if let Some(sc) = streams_clients {
        if let Some(streams) = sc.as_ref() {
            if let Err(e) =
                streams.stream_reorg(network, fork_block, depth, &affected_tx_hashes).await
            {
                error!("Failed to stream native transfer reorg retraction: {:?}", e);
            }
        }
    }
}

/// Shadow cache entry: just the block hash, kept separately from the main LRU cache.
/// The verifier reads from this after blocks have been confirmed.
pub type ShadowCache = Arc<std::sync::Mutex<std::collections::HashMap<u64, B256>>>;

/// Creates a new empty shadow cache for post-confirmation verification.
pub fn new_shadow_cache() -> ShadowCache {
    Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Spawns a background task that periodically verifies block hashes after N confirmations.
///
/// Compares cached hashes against the canonical chain from RPC. If a mismatch is found,
/// sends a `ReorgInfo` through the provided channel for the main loop to handle.
///
/// The task runs until the `cancel_token` is cancelled.
pub fn spawn_post_confirmation_verifier(
    shadow_cache: ShadowCache,
    provider: Arc<JsonRpcCachedProvider>,
    confirmations: u64,
    reorg_signal_tx: tokio::sync::mpsc::UnboundedSender<ReorgInfo>,
    cancel_token: tokio_util::sync::CancellationToken,
    network: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = std::time::Duration::from_secs(30);
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("Post-confirmation verifier stopped for {}", network);
                    return;
                }
                _ = tokio::time::sleep(check_interval) => {}
            }

            // Get current chain tip
            let latest_block = match provider.get_block_number().await {
                Ok(block) => block.to::<u64>(),
                Err(e) => {
                    warn!("Verifier: failed to get latest block for {}: {:?}", network, e);
                    continue;
                }
            };

            // Only verify blocks that have enough confirmations
            let verify_up_to = latest_block.saturating_sub(confirmations);

            // Collect blocks to verify from shadow cache
            let blocks_to_verify: Vec<(u64, B256)> = {
                let cache = match shadow_cache.try_lock() {
                    Ok(c) => c,
                    Err(_) => continue, // Skip if locked
                };
                cache
                    .iter()
                    .filter(|(block_num, _)| **block_num <= verify_up_to)
                    .map(|(k, v)| (*k, *v))
                    .collect()
            };

            if blocks_to_verify.is_empty() {
                continue;
            }

            // Batch-fetch canonical hashes from RPC
            let block_numbers: Vec<U64> =
                blocks_to_verify.iter().map(|(num, _)| U64::from(*num)).collect();

            let canonical_blocks =
                match provider.get_block_by_number_batch(&block_numbers, false).await {
                    Ok(blocks) => blocks,
                    Err(e) => {
                        warn!("Verifier: failed to fetch blocks for {}: {:?}", network, e);
                        continue;
                    }
                };

            // Compare hashes
            let mut mismatch_block: Option<u64> = None;
            for block in &canonical_blocks {
                let block_num = block.header.number;
                if let Some((_, cached_hash)) =
                    blocks_to_verify.iter().find(|(num, _)| *num == block_num)
                {
                    if block.header.hash != *cached_hash {
                        warn!(
                            "Verifier: hash mismatch at block {} on {} (cached: {}, canonical: {})",
                            block_num, network, cached_hash, block.header.hash
                        );
                        mismatch_block = Some(match mismatch_block {
                            Some(existing) => existing.min(block_num),
                            None => block_num,
                        });
                    }
                }
            }

            // Remove verified blocks from shadow cache
            {
                if let Ok(mut cache) = shadow_cache.try_lock() {
                    for (block_num, _) in &blocks_to_verify {
                        cache.remove(block_num);
                    }
                }
            }

            // Signal reorg if mismatch detected
            if let Some(fork_block) = mismatch_block {
                let depth = latest_block.saturating_sub(fork_block);
                warn!(
                    "Verifier: post-confirmation reorg detected on {} at block {} (depth: {})",
                    network, fork_block, depth
                );
                metrics::record_reorg(&network, depth);
                let _ = reorg_signal_tx.send(ReorgInfo {
                    fork_block: U64::from(fork_block),
                    depth,
                    affected_tx_hashes: vec![],
                });
            }
        }
    })
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
        // Test that serialization + deserialization roundtrips correctly
        let variants = vec![
            ReorgSafeDistance::Enabled(true),
            ReorgSafeDistance::Enabled(false),
            ReorgSafeDistance::Custom(42),
        ];
        for original in variants {
            let yaml = serde_yaml::to_string(&original).unwrap();
            let parsed: ReorgSafeDistance = serde_yaml::from_str(&yaml).unwrap();
            // Compare resolved values on Ethereum (chain_id=1)
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
        // Custom value should override regardless of chain
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

        // tx hashes should serialize as hex strings
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

    // ======================================================================
    // ShadowCache
    // ======================================================================

    #[test]
    fn test_shadow_cache_basic_operations() {
        let cache = new_shadow_cache();

        // Insert
        {
            let mut c = cache.lock().unwrap();
            c.insert(100, B256::from([1u8; 32]));
            c.insert(101, B256::from([2u8; 32]));
        }

        // Read
        {
            let c = cache.lock().unwrap();
            assert_eq!(c.len(), 2);
            assert_eq!(c.get(&100), Some(&B256::from([1u8; 32])));
            assert_eq!(c.get(&101), Some(&B256::from([2u8; 32])));
            assert_eq!(c.get(&102), None);
        }

        // Remove
        {
            let mut c = cache.lock().unwrap();
            c.remove(&100);
            assert_eq!(c.len(), 1);
        }
    }
}
