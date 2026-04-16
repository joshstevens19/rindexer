use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::{B256, U64};
use anyhow::Context;
use tracing::{info, warn};

use crate::event::callback_registry::ReorgNotification;
use crate::metrics::indexing as metrics;
use crate::provider::{ChainProvider, JsonRpcCachedProvider};

use super::persistence::ReorgBlockHashPersistence;
use super::task::{DerivedTableInfo, EventTableInfo, ReorgTask};
use super::window::{BlockChainWindow, ParentValidation};
use super::ReorgContext;

/// Number of blocks between periodic flushes of the in-memory block window to the database.
/// Balances write frequency against data-loss risk on crash.
const FLUSH_INTERVAL: u64 = 50;

pub struct ReorgCoordinator {
    network: String,
    window: BlockChainWindow,
    persistence: Arc<ReorgBlockHashPersistence>,
    provider: Option<Arc<JsonRpcCachedProvider>>,
    event_tables: Vec<EventTableInfo>,
    derived_tables: Vec<DerivedTableInfo>,
    blocks_since_flush: u64,
}

impl ReorgCoordinator {
    pub fn new(
        network: String,
        window: BlockChainWindow,
        persistence: Arc<ReorgBlockHashPersistence>,
        provider: Arc<dyn ChainProvider>,
        event_tables: Vec<EventTableInfo>,
        derived_tables: Vec<DerivedTableInfo>,
    ) -> anyhow::Result<Self> {
        super::validate_sql_value(&network, "network name")?;
        Ok(Self {
            network,
            window,
            persistence,
            provider: Some(provider),
            event_tables,
            derived_tables,
            blocks_since_flush: 0,
        })
    }

    /// Called on each new block during live indexing.
    /// Returns `Some(ReorgTask)` if a reorg is detected, `None` otherwise.
    pub async fn on_new_block(
        &mut self,
        block_number: u64,
        block_hash: B256,
        parent_hash: B256,
    ) -> anyhow::Result<Option<ReorgTask>> {
        match self.window.validate_parent(block_number, parent_hash) {
            ParentValidation::Valid => {
                self.window.insert(block_number, block_hash, parent_hash);
                self.persist_and_maybe_prune(block_number, block_hash, parent_hash).await?;
                Ok(None)
            }
            ParentValidation::NoPreviousBlock => {
                if self.window.is_empty() || block_number == 0 {
                    self.window.insert(block_number, block_hash, parent_hash);
                    self.persist_and_maybe_prune(block_number, block_hash, parent_hash).await?;
                    return Ok(None);
                }

                // Gap detected: the poller skipped one or more blocks. Fetch the
                // missing blocks from the canonical chain, then validate each one
                // against the window to detect reorgs that happened during the gap.
                let window_latest = self.window.latest_block().unwrap(); // safe: window non-empty
                let gap_start = window_latest + 1;
                let gap_end = block_number; // exclusive — we don't fetch block_number itself
                if gap_start < gap_end {
                    info!(
                        network = %self.network,
                        block_number,
                        window_latest,
                        gap_size = gap_end - gap_start,
                        "Filling block gap in reorg window"
                    );
                    if let Some(provider) = &self.provider {
                        let missing: Vec<U64> = (gap_start..gap_end).map(U64::from).collect();
                        if let Ok(blocks) =
                            provider.get_block_by_number_batch(&missing, false).await
                        {
                            // Validate each fetched block against the window in order.
                            // The first fetched block's parent_hash must match the
                            // window's latest block hash — if it doesn't, a reorg
                            // happened during the gap.
                            for b in &blocks {
                                match self
                                    .window
                                    .validate_parent(b.header.number, b.header.parent_hash)
                                {
                                    ParentValidation::Mismatch { expected, got } => {
                                        return self
                                            .handle_mismatch(block_number, expected, got)
                                            .await;
                                    }
                                    _ => {
                                        self.window.insert(
                                            b.header.number,
                                            b.header.hash,
                                            b.header.parent_hash,
                                        );
                                        self.persist_and_maybe_prune(
                                            b.header.number,
                                            b.header.hash,
                                            b.header.parent_hash,
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                    }
                }

                // Re-validate the incoming block now that the gap is filled.
                self.insert_or_detect_reorg(block_number, block_hash, parent_hash).await
            }
            ParentValidation::Mismatch { expected, got } => {
                self.handle_mismatch(block_number, expected, got).await
            }
        }
    }

    /// Insert a block after validation, or trigger reorg detection on mismatch.
    async fn insert_or_detect_reorg(
        &mut self,
        block_number: u64,
        block_hash: B256,
        parent_hash: B256,
    ) -> anyhow::Result<Option<ReorgTask>> {
        match self.window.validate_parent(block_number, parent_hash) {
            ParentValidation::Valid | ParentValidation::NoPreviousBlock => {
                self.window.insert(block_number, block_hash, parent_hash);
                self.persist_and_maybe_prune(block_number, block_hash, parent_hash).await?;
                Ok(None)
            }
            ParentValidation::Mismatch { expected, got } => {
                self.handle_mismatch(block_number, expected, got).await
            }
        }
    }

    /// Common path for parent-hash mismatch: log, find fork point, return ReorgTask.
    async fn handle_mismatch(
        &mut self,
        block_number: u64,
        expected: B256,
        got: B256,
    ) -> anyhow::Result<Option<ReorgTask>> {
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

    /// Called once on restart before indexing resumes.
    /// Checks whether any blocks in the window were reorged while offline.
    pub async fn validate_on_startup(&self) -> anyhow::Result<Option<ReorgTask>> {
        let block_numbers = self.window.block_numbers();
        if block_numbers.is_empty() {
            return Ok(None);
        }

        let provider = self
            .provider
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No provider configured for startup validation"))?;

        let block_numbers_u64: Vec<U64> = block_numbers.iter().map(|&n| U64::from(n)).collect();
        let blocks = provider
            .get_block_by_number_batch(&block_numbers_u64, false)
            .await
            .context("Failed to fetch blocks for startup validation")?;

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

                // last_match is the highest block whose hash still matches the canonical chain.
                // The fork therefore starts at last_match + 1 (the first divergent block).
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
    pub fn try_create_reorg_task_for_block_range(
        &self,
        fork_point: u64,
        detection_point: u64,
    ) -> anyhow::Result<ReorgTask> {
        anyhow::ensure!(
            fork_point <= detection_point,
            "fork_point ({}) must be <= detection_point ({})",
            fork_point,
            detection_point
        );
        metrics::record_reorg_detection_source(&self.network, "removed_logs");
        Ok(ReorgTask {
            network: self.network.clone(),
            fork_point,
            detection_point,
            event_tables: self.event_tables.clone(),
            derived_tables: self.derived_tables.clone(),
            canonical_blocks: vec![],
        })
    }

    /// Handle reth ExEx notification — fork point provided directly.
    /// `revert_from_block` is the higher block (detection point),
    /// `revert_to_block` is the lower block (fork point).
    pub fn on_exex_reorg(
        &self,
        revert_from_block: u64,
        revert_to_block: u64,
    ) -> anyhow::Result<ReorgTask> {
        anyhow::ensure!(
            revert_to_block <= revert_from_block,
            "revert_to_block ({}) must be <= revert_from_block ({})",
            revert_to_block,
            revert_from_block
        );
        metrics::record_reorg_detection_source(&self.network, "exex");
        metrics::record_reorg(&self.network, revert_from_block - revert_to_block + 1);
        Ok(ReorgTask {
            network: self.network.clone(),
            fork_point: revert_to_block,
            detection_point: revert_from_block,
            event_tables: self.event_tables.clone(),
            derived_tables: self.derived_tables.clone(),
            canonical_blocks: vec![],
        })
    }

    /// Find the fork point by comparing window entries against canonical chain from RPC.
    /// Returns `(fork_point, canonical_blocks)` where canonical_blocks are the
    /// `(block_number, block_hash, parent_hash)` tuples fetched from the RPC,
    /// so callers can reuse them without a second fetch.
    async fn find_fork_point(&self) -> anyhow::Result<(u64, Vec<(u64, B256, B256)>)> {
        let block_numbers = self.window.block_numbers();
        anyhow::ensure!(!block_numbers.is_empty(), "Cannot find fork point: window is empty");

        let provider = self
            .provider
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No provider configured for fork point detection"))?;

        let block_numbers_u64: Vec<U64> = block_numbers.iter().map(|&n| U64::from(n)).collect();
        let blocks = provider
            .get_block_by_number_batch(&block_numbers_u64, false)
            .await
            .context("Failed to fetch blocks for fork point detection")?;

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
    ) -> anyhow::Result<()> {
        let result = reorg_task
            .execute(&mut self.window, ctx.postgres, ctx.clickhouse, self.provider.as_ref())
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

        Ok(())
    }

    /// Persist the new block to DB and periodically prune old entries.
    /// The insert is awaited directly to ensure block hashes are persisted before
    /// any reorg detection that depends on them. Pruning is fire-and-forget since
    /// a missed prune is harmless.
    async fn persist_and_maybe_prune(
        &mut self,
        block_number: u64,
        block_hash: B256,
        parent_hash: B256,
    ) -> anyhow::Result<()> {
        self.blocks_since_flush += 1;

        self.persistence
            .insert_block(
                &self.network,
                block_number,
                &format!("{:#x}", block_hash),
                &format!("{:#x}", parent_hash),
            )
            .await
            .with_context(|| format!("Failed to persist block {} hash to DB", block_number))?;

        if self.blocks_since_flush >= FLUSH_INTERVAL {
            self.blocks_since_flush = 0;
            if let Some(oldest) = self.window.oldest_block() {
                let persistence = Arc::clone(&self.persistence);
                let network = self.network.clone();
                tokio::spawn(async move {
                    if let Err(e) = persistence.prune(&network, oldest).await {
                        tracing::error!("Background DB prune failed: {}", e);
                    }
                });
            }
        }

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
        let mut window = BlockChainWindow::try_new(100).unwrap();
        for &(num, h, p) in blocks {
            window.insert(num, hash(h), hash(p));
        }
        window
    }

    fn make_coordinator(window: BlockChainWindow) -> ReorgCoordinator {
        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
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
        let window = BlockChainWindow::try_new(100).unwrap();
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
                err.to_string().contains("No provider configured"),
                "Expected provider-related error, got: {}",
                err
            ),
            Ok(_) => panic!("Expected error from mismatch branch (no provider for fork point)"),
        }
    }

    #[tokio::test]
    async fn test_handle_reorg_fires_on_reorg_callback() {
        use crate::event::callback_registry::{EventCallbackRegistry, ReorgNotification};
        use std::sync::Mutex;

        let window = make_window_with_blocks(&[(10, 10, 9), (11, 11, 10), (12, 12, 11)]);
        let mut coordinator = make_coordinator(window);

        let task = ReorgTask {
            network: "test".to_string(),
            fork_point: 11,
            detection_point: 12,
            event_tables: vec![],
            derived_tables: vec![],
            canonical_blocks: vec![],
        };

        // Set up a callback that captures the notification
        let captured: Arc<Mutex<Option<ReorgNotification>>> = Arc::new(Mutex::new(None));
        let captured_clone = Arc::clone(&captured);
        let mut registry = EventCallbackRegistry::new();
        registry.register_on_reorg(Arc::new(move |notification| {
            let captured = Arc::clone(&captured_clone);
            Box::pin(async move {
                *captured.lock().unwrap() = Some(notification);
            })
        }));

        let ctx = ReorgContext {
            postgres: None,
            clickhouse: None,
            registry: Some(&registry),
            streams_clients: None,
        };

        coordinator.handle_reorg(task, &ctx).await.unwrap();

        let notification =
            captured.lock().unwrap().take().expect("on_reorg callback was not fired");
        assert_eq!(notification.network, "test");
        assert_eq!(notification.fork_block, 11);
        assert_eq!(notification.detection_block, 12);
        assert!(notification.invalidated_tx_hashes.is_empty(), "no PG → no affected hashes");
    }

    #[test]
    fn test_derived_tables_propagate_to_tasks() {
        let window = make_window_with_blocks(&[(10, 10, 9), (11, 11, 10), (12, 12, 11)]);
        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
        let coordinator = ReorgCoordinator {
            network: "test".to_string(),
            window,
            persistence,
            provider: None,
            event_tables: vec![],
            derived_tables: vec![
                DerivedTableInfo {
                    full_table_name: "schema.balances".to_string(),
                    cross_chain: false,
                    rollback_ops: vec![],
                    journal_columns: vec![],
                },
                DerivedTableInfo {
                    full_table_name: "schema.global_stats".to_string(),
                    cross_chain: true,
                    rollback_ops: vec![],
                    journal_columns: vec![],
                },
            ],
            blocks_since_flush: 0,
        };

        // on_exex_reorg creates a task — verify derived_tables are included
        let task = coordinator.on_exex_reorg(12, 10).unwrap();
        assert_eq!(task.derived_tables.len(), 2);
        assert_eq!(task.derived_tables[0].full_table_name, "schema.balances");
        assert!(!task.derived_tables[0].cross_chain);
        assert_eq!(task.derived_tables[1].full_table_name, "schema.global_stats");
        assert!(task.derived_tables[1].cross_chain);
    }

    #[test]
    fn test_derived_tables_propagate_to_removed_logs_task() {
        let window = make_window_with_blocks(&[(10, 10, 9), (11, 11, 10)]);
        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
        let coordinator = ReorgCoordinator {
            network: "test".to_string(),
            window,
            persistence,
            provider: None,
            event_tables: vec![],
            derived_tables: vec![DerivedTableInfo {
                full_table_name: "schema.totals".to_string(),
                cross_chain: false,
                rollback_ops: vec![],
                journal_columns: vec![],
            }],
            blocks_since_flush: 0,
        };

        let task = coordinator.try_create_reorg_task_for_block_range(10, 11).unwrap();
        assert_eq!(task.derived_tables.len(), 1);
        assert_eq!(task.derived_tables[0].full_table_name, "schema.totals");
    }

    #[test]
    fn test_on_exex_reorg() {
        let window = BlockChainWindow::try_new(100).unwrap();
        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
        let coordinator = ReorgCoordinator {
            network: "test".to_string(),
            window,
            persistence,
            provider: None,
            event_tables: vec![EventTableInfo::try_new(
                "schema".to_string(),
                "table".to_string(),
                "schema_table".to_string(),
            )
            .unwrap()],
            derived_tables: vec![],
            blocks_since_flush: 0,
        };

        let task = coordinator.on_exex_reorg(110, 100).unwrap();
        assert_eq!(task.network, "test");
        assert_eq!(task.fork_point, 100);
        assert_eq!(task.detection_point, 110);
        assert_eq!(task.event_tables.len(), 1);
    }
}
