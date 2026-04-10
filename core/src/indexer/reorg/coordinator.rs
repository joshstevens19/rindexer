use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::{B256, U64};
use tracing::{info, warn};

use crate::event::callback_registry::ReorgNotification;
use crate::metrics::indexing as metrics;
use crate::provider::JsonRpcCachedProvider;

use super::persistence::LatestBlocksPersistence;
use super::task::{DerivedTableInfo, EventTableInfo, ReorgTask};
use super::window::{BlockChainWindow, ParentValidation};
use super::ReorgContext;

const FLUSH_INTERVAL: u64 = 50;

pub struct ReorgCoordinator {
    network: String,
    window: BlockChainWindow,
    persistence: Arc<LatestBlocksPersistence>,
    provider: Option<Arc<JsonRpcCachedProvider>>,
    event_tables: Vec<EventTableInfo>,
    derived_tables: Vec<DerivedTableInfo>,
    blocks_since_flush: u64,
}

impl ReorgCoordinator {
    pub fn new(
        network: String,
        window: BlockChainWindow,
        persistence: Arc<LatestBlocksPersistence>,
        provider: Arc<JsonRpcCachedProvider>,
        event_tables: Vec<EventTableInfo>,
    ) -> Self {
        Self {
            network,
            window,
            persistence,
            provider: Some(provider),
            event_tables,
            derived_tables: vec![],
            blocks_since_flush: 0,
        }
    }

    pub fn set_derived_tables(&mut self, derived_tables: Vec<DerivedTableInfo>) {
        self.derived_tables = derived_tables;
    }

    /// Called on each new block during live indexing.
    /// Returns `Some(ReorgTask)` if a reorg is detected, `None` otherwise.
    pub async fn on_new_block(
        &mut self,
        block_number: u64,
        block_hash: B256,
        parent_hash: B256,
    ) -> Result<Option<ReorgTask>, String> {
        match self.window.validate_parent(block_number, parent_hash) {
            ParentValidation::Valid | ParentValidation::NoPreviousBlock => {
                self.window.insert(block_number, block_hash, parent_hash);
                self.persist_and_maybe_prune(block_number, block_hash, parent_hash)?;
                Ok(None)
            }
            ParentValidation::Mismatch { expected, got } => {
                warn!(
                    network = %self.network,
                    block_number,
                    %expected,
                    %got,
                    "Parent hash mismatch — reorg detected"
                );
                metrics::record_reorg_detection_source(&self.network, "rpc");

                let (fork_point, canonical_blocks) = self.find_fork_point().await?;
                let depth = block_number.saturating_sub(fork_point) + 1;
                metrics::record_reorg(&self.network, depth);

                Ok(Some(ReorgTask {
                    network: self.network.clone(),
                    fork_point,
                    detection_point: block_number,
                    event_tables: self.event_tables.clone(),
                    derived_tables: self.derived_tables.clone(),
                    canonical_blocks,
                }))
            }
        }
    }

    /// Called once on restart before indexing resumes.
    /// Checks whether any blocks in the window were reorged while offline.
    pub async fn validate_on_startup(&self) -> Result<Option<ReorgTask>, String> {
        let block_numbers = self.window.block_numbers();
        if block_numbers.is_empty() {
            return Ok(None);
        }

        let provider = self
            .provider
            .as_ref()
            .ok_or_else(|| "No provider configured for startup validation".to_string())?;

        let block_numbers_u64: Vec<U64> = block_numbers.iter().map(|&n| U64::from(n)).collect();
        let blocks = provider
            .get_block_by_number_batch(&block_numbers_u64, false)
            .await
            .map_err(|e| format!("Failed to fetch blocks for startup validation: {}", e))?;

        let canonical: Vec<(u64, B256)> =
            blocks.iter().map(|b| (b.header.number, b.header.hash)).collect();

        match self.window.find_fork_point(&canonical) {
            Some(last_match) => {
                // Check if the last match is the latest block — all good, no reorg
                let latest = self.window.latest_block().unwrap_or(0);
                if last_match >= latest {
                    info!(
                        network = %self.network,
                        "Startup validation: all blocks match canonical chain"
                    );
                    return Ok(None);
                }

                // Partial match: fork found after last_match
                let fork_point = last_match + 1;
                let detection_point = latest;
                let depth = detection_point.saturating_sub(fork_point) + 1;
                warn!(
                    network = %self.network,
                    fork_point,
                    detection_point,
                    depth,
                    "Startup validation: offline reorg detected"
                );
                metrics::record_reorg_detection_source(&self.network, "startup");
                metrics::record_reorg(&self.network, depth);

                Ok(Some(ReorgTask {
                    network: self.network.clone(),
                    fork_point,
                    detection_point,
                    event_tables: self.event_tables.clone(),
                    derived_tables: self.derived_tables.clone(),
                    canonical_blocks: vec![],
                }))
            }
            None => {
                // Entire window reorged
                let oldest = self.window.oldest_block().unwrap_or(0);
                let latest = self.window.latest_block().unwrap_or(0);
                let depth = latest.saturating_sub(oldest) + 1;
                warn!(
                    network = %self.network,
                    oldest,
                    latest,
                    depth,
                    "Startup validation: entire window reorged"
                );
                metrics::record_reorg_detection_source(&self.network, "startup");
                metrics::record_reorg(&self.network, depth);

                Ok(Some(ReorgTask {
                    network: self.network.clone(),
                    fork_point: oldest,
                    detection_point: latest,
                    event_tables: self.event_tables.clone(),
                    derived_tables: self.derived_tables.clone(),
                    canonical_blocks: vec![],
                }))
            }
        }
    }

    /// Create a ReorgTask for a known block range (e.g. from removed-logs detection).
    /// The caller is responsible for executing the task via `handle_reorg`.
    pub fn create_reorg_task_for_block_range(
        &self,
        fork_point: u64,
        detection_point: u64,
    ) -> ReorgTask {
        metrics::record_reorg_detection_source(&self.network, "removed_logs");
        ReorgTask {
            network: self.network.clone(),
            fork_point,
            detection_point,
            event_tables: self.event_tables.clone(),
            derived_tables: self.derived_tables.clone(),
            canonical_blocks: vec![],
        }
    }

    /// Handle reth ExEx notification — fork point provided directly.
    /// `revert_from_block` is the higher block (detection point),
    /// `revert_to_block` is the lower block (fork point).
    pub fn on_exex_reorg(&self, revert_from_block: u64, revert_to_block: u64) -> ReorgTask {
        metrics::record_reorg_detection_source(&self.network, "exex");
        metrics::record_reorg(&self.network, revert_from_block.saturating_sub(revert_to_block) + 1);
        ReorgTask {
            network: self.network.clone(),
            fork_point: revert_to_block,
            detection_point: revert_from_block,
            event_tables: self.event_tables.clone(),
            derived_tables: self.derived_tables.clone(),
            canonical_blocks: vec![],
        }
    }

    /// Find the fork point by comparing window entries against canonical chain from RPC.
    /// Returns `(fork_point, canonical_blocks)` where canonical_blocks are the
    /// `(block_number, block_hash, parent_hash)` tuples fetched from the RPC,
    /// so callers can reuse them without a second fetch.
    async fn find_fork_point(&self) -> Result<(u64, Vec<(u64, B256, B256)>), String> {
        let block_numbers = self.window.block_numbers();
        if block_numbers.is_empty() {
            return Err("Cannot find fork point: window is empty".to_string());
        }

        let provider = self
            .provider
            .as_ref()
            .ok_or_else(|| "No provider configured for fork point detection".to_string())?;

        let block_numbers_u64: Vec<U64> = block_numbers.iter().map(|&n| U64::from(n)).collect();
        let blocks = provider
            .get_block_by_number_batch(&block_numbers_u64, false)
            .await
            .map_err(|e| format!("Failed to fetch blocks for fork point detection: {}", e))?;

        let canonical: Vec<(u64, B256)> =
            blocks.iter().map(|b| (b.header.number, b.header.hash)).collect();

        let canonical_blocks: Vec<(u64, B256, B256)> =
            blocks.iter().map(|b| (b.header.number, b.header.hash, b.header.parent_hash)).collect();

        let fork_point = match self.window.find_fork_point(&canonical) {
            Some(last_match) => last_match + 1,
            None => block_numbers[0],
        };

        Ok((fork_point, canonical_blocks))
    }

    /// Execute a reorg task through the coordinator, keeping internals encapsulated.
    pub async fn handle_reorg(
        &mut self,
        reorg_task: ReorgTask,
        ctx: &ReorgContext<'_>,
    ) -> Result<(), String> {
        let result = reorg_task
            .execute(
                &mut self.window,
                &self.persistence,
                ctx.postgres,
                ctx.clickhouse,
                self.provider.as_ref(),
            )
            .await?;

        let affected_tx_hashes: Vec<B256> =
            result.affected_tx_hashes.iter().filter_map(|h| B256::from_str(h).ok()).collect();

        if let Some(registry) = ctx.registry {
            let notification = ReorgNotification {
                network: reorg_task.network.clone(),
                fork_block: reorg_task.fork_point,
                detection_block: reorg_task.detection_point,
                invalidated_tx_hashes: affected_tx_hashes.clone(),
            };
            registry.fire_on_reorg(notification).await;
        }

        // Publish reorg retraction through instant-mode streams (fire-and-forget)
        if let Some(clients) = ctx.streams_clients {
            let network = reorg_task.network.clone();
            let fork_point = reorg_task.fork_point;
            let depth = reorg_task.detection_point.saturating_sub(reorg_task.fork_point) + 1;
            let tx_hashes = affected_tx_hashes.clone();

            // stream_reorg requires &self so we need a pointer; StreamsClients is not
            // Clone, but the callers always have it behind Arc<Option<StreamsClients>>.
            // Since we only have a reference here, we cannot move it into a spawn.
            // Keep the await inline (the method is fast — it just publishes to queues).
            if let Err(e) = clients.stream_reorg(&network, fork_point, depth, &tx_hashes).await {
                tracing::error!(
                    network = %network,
                    fork_point,
                    "Failed to publish reorg notification to streams: {}",
                    e
                );
            }
        }

        // TODO: Part B — FinalizedBuffer integration.

        Ok(())
    }

    /// Persist the new block to DB (fire-and-forget) and periodically prune old entries.
    fn persist_and_maybe_prune(
        &mut self,
        block_number: u64,
        block_hash: B256,
        parent_hash: B256,
    ) -> Result<(), String> {
        self.blocks_since_flush += 1;

        // Fire-and-forget: spawn the DB write so the live loop is not blocked.
        let persistence = Arc::clone(&self.persistence);
        let network = self.network.clone();
        let needs_prune = self.blocks_since_flush >= FLUSH_INTERVAL;
        if needs_prune {
            self.blocks_since_flush = 0;
        }
        let oldest = self.window.oldest_block();

        tokio::spawn(async move {
            if let Err(e) = persistence
                .insert_block(
                    &network,
                    block_number,
                    &format!("{:#x}", block_hash),
                    &format!("{:#x}", parent_hash),
                )
                .await
            {
                tracing::error!("Background DB insert failed for block {}: {}", block_number, e);
            }

            if needs_prune {
                if let Some(oldest) = oldest {
                    if let Err(e) = persistence.prune(&network, oldest).await {
                        tracing::error!("Background DB prune failed: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        B256::from(bytes)
    }

    fn make_window_with_blocks(blocks: &[(u64, u8, u8)]) -> BlockChainWindow {
        let mut window = BlockChainWindow::new(100);
        for &(num, h, p) in blocks {
            window.insert(num, hash(h), hash(p));
        }
        window
    }

    fn make_coordinator(window: BlockChainWindow) -> ReorgCoordinator {
        let persistence = Arc::new(LatestBlocksPersistence::new(None, None));
        ReorgCoordinator {
            network: "test".to_string(),
            window,
            persistence,
            provider: None,
            event_tables: vec![],
            derived_tables: vec![],
            blocks_since_flush: 0,
        }
    }

    #[tokio::test]
    async fn test_on_new_block_valid_chain() {
        // Build a window with blocks 10, 11, 12
        let window = make_window_with_blocks(&[(10, 10, 9), (11, 11, 10), (12, 12, 11)]);
        let mut coordinator = make_coordinator(window);

        // Block 13 with parent_hash matching block 12's hash
        let result = coordinator.on_new_block(13, hash(13), hash(12)).await.unwrap();

        assert!(result.is_none(), "Expected no reorg for valid chain continuation");
        assert!(coordinator.window.get(13).is_some(), "Block 13 should be in window");
    }

    #[tokio::test]
    async fn test_on_new_block_no_previous_block() {
        // Empty window — NoPreviousBlock path
        let window = BlockChainWindow::new(100);
        let mut coordinator = make_coordinator(window);

        let result = coordinator.on_new_block(100, hash(1), hash(0)).await.unwrap();

        assert!(result.is_none(), "Expected None for NoPreviousBlock");
        assert!(coordinator.window.get(100).is_some(), "Block should be inserted");
    }

    #[tokio::test]
    async fn test_on_new_block_reorg_detected() {
        // Build a window with blocks 10, 11, 12
        let window = make_window_with_blocks(&[(10, 10, 9), (11, 11, 10), (12, 12, 11)]);
        let mut coordinator = make_coordinator(window);

        // Block 13 with a parent_hash that does NOT match block 12's hash → mismatch
        let result = coordinator.on_new_block(13, hash(13), hash(99)).await;

        // Without a provider, find_fork_point will return an error,
        // confirming we entered the Mismatch branch (not Valid or NoPreviousBlock)
        match result {
            Err(err) => assert!(
                err.contains("No provider configured"),
                "Expected provider-related error, got: {}",
                err
            ),
            Ok(_) => panic!("Expected error from mismatch branch (no provider for fork point)"),
        }
    }

    #[test]
    fn test_on_exex_reorg() {
        let window = BlockChainWindow::new(100);
        let persistence = Arc::new(LatestBlocksPersistence::new(None, None));
        let coordinator = ReorgCoordinator {
            network: "test".to_string(),
            window,
            persistence,
            provider: None,
            event_tables: vec![EventTableInfo::new(
                "schema".to_string(),
                "table".to_string(),
                "schema_table".to_string(),
            )],
            derived_tables: vec![],
            blocks_since_flush: 0,
        };

        let task = coordinator.on_exex_reorg(110, 100);
        assert_eq!(task.network, "test");
        assert_eq!(task.fork_point, 100);
        assert_eq!(task.detection_point, 110);
        assert_eq!(task.event_tables.len(), 1);
    }
}
