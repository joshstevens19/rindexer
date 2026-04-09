use std::sync::Arc;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::metrics::indexing as metrics;

use super::persistence::LatestBlocksPersistence;
use super::window::BlockChainWindow;

#[derive(Clone)]
pub struct EventTableInfo {
    pub schema: String,
    pub table_name: String,
    /// Full table path: schema.table_name
    pub full_name: String,
}

impl EventTableInfo {
    pub fn new(schema: String, table_name: String) -> Self {
        let full_name = format!("{}.{}", schema, table_name);
        Self { schema, table_name, full_name }
    }
}

pub struct ReorgTask {
    pub network: String,
    pub fork_point: u64,
    pub detection_point: u64,
    pub event_tables: Vec<EventTableInfo>,
}

pub struct ReorgTaskResult {
    pub events_deleted: u64,
    pub events_reindexed: u64,
    pub duration_secs: f64,
}

impl ReorgTask {
    pub async fn execute(
        &self,
        _window: &mut BlockChainWindow,
        _persistence: &LatestBlocksPersistence,
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
    ) -> Result<ReorgTaskResult, String> {
        let start = std::time::Instant::now();

        tracing::info!(
            network = %self.network,
            fork_point = self.fork_point,
            detection_point = self.detection_point,
            depth = self.detection_point - self.fork_point + 1,
            "Starting reorg task"
        );

        // Step 1: Re-fetch logs for [fork_point, detection_point]
        // TODO: Wire up log re-fetching during integration (Task 12)
        // This will use the existing fetch_logs pipeline

        // Step 2: Delete stale events + insert new events + update latest_blocks
        let mut total_deleted = 0u64;

        if let Some(pg) = postgres {
            let table_names: Vec<&str> =
                self.event_tables.iter().map(|t| t.full_name.as_str()).collect();

            // corrected_blocks will be populated during integration (Task 12)
            // when re-fetched blocks provide the correct hashes
            let corrected_blocks: Vec<(u64, &str, &str)> = vec![];

            total_deleted = pg
                .reorg_rollback_transaction(
                    &table_names,
                    &self.network,
                    self.fork_point,
                    self.detection_point,
                    &corrected_blocks,
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
        }

        // Step 3: Update in-memory window
        // TODO: Update with corrected hashes from re-fetched blocks (Task 12 integration)

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
            events_reindexed: 0, // populated during integration (Task 12)
            duration_secs: duration,
        })
    }
}
