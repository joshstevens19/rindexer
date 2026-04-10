use std::sync::Arc;

use alloy::primitives::{B256, U64};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::metrics::indexing as metrics;
use crate::provider::JsonRpcCachedProvider;

use super::persistence::LatestBlocksPersistence;
use super::window::BlockChainWindow;

#[derive(Clone)]
pub struct EventTableInfo {
    pub schema: String,
    pub table_name: String,
    /// Full table path: schema.table_name
    pub full_name: String,
    /// Checkpoint table name in rindexer_internal (without schema prefix)
    pub checkpoint_table: String,
}

impl EventTableInfo {
    pub fn new(schema: String, table_name: String, checkpoint_table: String) -> Self {
        let full_name = format!("{}.{}", schema, table_name);
        Self { schema, table_name, full_name, checkpoint_table }
    }
}

/// Metadata for a derived/custom table needed during reorg rollback.
#[derive(Clone)]
pub struct DerivedTableInfo {
    pub full_table_name: String,
    pub cross_chain: bool,
}

pub struct ReorgTask {
    pub network: String,
    pub fork_point: u64,
    pub detection_point: u64,
    pub event_tables: Vec<EventTableInfo>,
    pub derived_tables: Vec<DerivedTableInfo>,
}

pub struct ReorgTaskResult {
    pub events_deleted: u64,
    pub events_reindexed: u64,
    pub duration_secs: f64,
    pub affected_tx_hashes: Vec<String>,
}

impl ReorgTask {
    pub async fn execute(
        &self,
        window: &mut BlockChainWindow,
        _persistence: &LatestBlocksPersistence,
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
        provider: Option<&Arc<JsonRpcCachedProvider>>,
    ) -> Result<ReorgTaskResult, String> {
        let start = std::time::Instant::now();

        tracing::info!(
            network = %self.network,
            fork_point = self.fork_point,
            detection_point = self.detection_point,
            depth = self.detection_point - self.fork_point + 1,
            "Starting reorg task"
        );

        let mut corrected_blocks_owned: Vec<(u64, String, String)> = Vec::new();
        if let Some(provider) = provider {
            let block_numbers: Vec<U64> = (self.fork_point..=self.detection_point)
                .map(|n| U64::from(n))
                .collect();
            match provider.get_block_by_number_batch(&block_numbers, false).await {
                Ok(blocks) => {
                    corrected_blocks_owned = blocks
                        .iter()
                        .map(|b| {
                            (
                                b.header.number,
                                format!("{:#x}", b.header.hash),
                                format!("{:#x}", b.header.parent_hash),
                            )
                        })
                        .collect();

                    let window_updates: Vec<(u64, B256, B256)> = blocks
                        .iter()
                        .map(|b| (b.header.number, b.header.hash, b.header.parent_hash))
                        .collect();
                    window.update_range(&window_updates);
                }
                Err(e) => {
                    tracing::error!("Failed to fetch corrected blocks for reorg range: {}", e)
                }
            }
        }

        let corrected_blocks: Vec<(u64, &str, &str)> = corrected_blocks_owned
            .iter()
            .map(|(n, h, p)| (*n, h.as_str(), p.as_str()))
            .collect();

        // Collect affected tx hashes before deletion (for on_reorg callback)
        let mut affected_tx_hashes: Vec<String> = Vec::new();
        if let Some(pg) = postgres {
            for table in &self.event_tables {
                match pg
                    .collect_affected_tx_hashes(
                        &table.full_name,
                        &self.network,
                        self.fork_point,
                        self.detection_point,
                    )
                    .await
                {
                    Ok(hashes) => affected_tx_hashes.extend(hashes),
                    Err(e) => tracing::warn!(
                        "Failed to collect tx hashes from {}: {:?}",
                        table.full_name,
                        e
                    ),
                }
            }
        }
        affected_tx_hashes.sort();
        affected_tx_hashes.dedup();

        // Step 2: Delete stale events + insert new events + update latest_blocks
        let mut total_deleted = 0u64;

        if let Some(pg) = postgres {
            let table_names: Vec<&str> =
                self.event_tables.iter().map(|t| t.full_name.as_str()).collect();
            let checkpoint_tables: Vec<&str> =
                self.event_tables.iter().map(|t| t.checkpoint_table.as_str()).collect();

            total_deleted = pg
                .reorg_rollback_transaction(
                    &table_names,
                    &self.network,
                    self.fork_point,
                    self.detection_point,
                    &corrected_blocks,
                    &checkpoint_tables,
                )
                .await
                .map_err(|e| e.to_string())?;
        }

        if let Some(ch) = clickhouse {
            let tables: Vec<(String, String)> = self
                .event_tables
                .iter()
                .map(|t| (t.schema.clone(), t.table_name.clone()))
                .collect();

            ch.reorg_rollback(&tables, &self.network, self.fork_point, self.detection_point)
                .await
                .map_err(|e| e.to_string())?;

            // Rewind ClickHouse checkpoint tables via DELETE + INSERT
            // (ReplacingMergeTree discards lower values on merge, so we must delete first)
            let rewind_block = self.fork_point.saturating_sub(1);
            for table in &self.event_tables {
                let delete_sql = format!(
                    "ALTER TABLE rindexer_internal.{} DELETE WHERE network = '{}' SETTINGS mutations_sync = 1",
                    table.checkpoint_table, self.network
                );
                ch.execute(&delete_sql).await.map_err(|e| e.to_string())?;

                let insert_sql = format!(
                    "INSERT INTO rindexer_internal.{} (network, last_synced_block) VALUES ('{}', {})",
                    table.checkpoint_table, self.network, rewind_block
                );
                ch.execute(&insert_sql).await.map_err(|e| e.to_string())?;
            }
        }

        // Step 2b: Delete rows from derived/custom tables
        for dt in &self.derived_tables {
            if let Some(pg) = postgres {
                let query = if dt.cross_chain {
                    format!(
                        "DELETE FROM {} WHERE rindexer_block_number >= {}",
                        dt.full_table_name, self.fork_point
                    )
                } else {
                    format!(
                        "DELETE FROM {} WHERE rindexer_block_number >= {} AND network = '{}'",
                        dt.full_table_name, self.fork_point, self.network
                    )
                };

                match pg.batch_execute(&query).await {
                    Ok(_) => tracing::info!(
                        "PostgreSQL: deleted derived table rows from block >= {} in {}",
                        self.fork_point, dt.full_table_name
                    ),
                    Err(e) => tracing::error!(
                        "PostgreSQL: failed to delete derived table rows in {}: {:?}",
                        dt.full_table_name, e
                    ),
                }
            }

            if let Some(ch) = clickhouse {
                let query = if dt.cross_chain {
                    format!(
                        "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} SETTINGS mutations_sync = 1",
                        dt.full_table_name, self.fork_point
                    )
                } else {
                    format!(
                        "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} AND network = '{}' SETTINGS mutations_sync = 1",
                        dt.full_table_name, self.fork_point, self.network
                    )
                };

                match ch.execute(&query).await {
                    Ok(_) => tracing::info!(
                        "ClickHouse: deleted derived table rows from block >= {} in {}",
                        self.fork_point, dt.full_table_name
                    ),
                    Err(e) => tracing::error!(
                        "ClickHouse: failed to delete derived table rows in {}: {:?}",
                        dt.full_table_name, e
                    ),
                }
            }
        }

        // Step 4: Trigger callbacks for re-indexed events
        // TODO: Wire up callback triggering (Task 12 integration)

        // Step 5: Fire on_reorg notification
        // TODO: Wire up on_reorg callback (Task 12 integration)

        // Step 6: Record metrics
        let duration = start.elapsed().as_secs_f64();
        metrics::record_reorg_handling_duration(&self.network, duration);
        metrics::record_reorg_events_deleted(&self.network, total_deleted);

        tracing::info!(
            network = %self.network,
            events_deleted = total_deleted,
            duration_secs = duration,
            "Reorg task completed"
        );

        Ok(ReorgTaskResult {
            events_deleted: total_deleted,
            events_reindexed: 0,
            duration_secs: duration,
            affected_tx_hashes,
        })
    }
}
