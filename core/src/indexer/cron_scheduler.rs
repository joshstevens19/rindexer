//! Cron scheduler for table operations.
//!
//! This module provides scheduled (cron) triggers for custom tables,
//! allowing operations to run on a time-based schedule instead of
//! (or in addition to) event triggers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::is_running;
use crate::manifest::contract::{parse_interval, ColumnType, Table, TableCronMapping};
use crate::manifest::core::Manifest;
use crate::provider::JsonRpcCachedProvider;

use super::tables::{TableRowData, TxMetadata};

/// Enum representing the type of schedule for cron tasks.
#[derive(Debug, Clone)]
pub enum CronSchedule {
    /// Fixed interval (e.g., every 5 minutes)
    Interval(Duration),
    /// Cron expression (e.g., "*/5 * * * *")
    Cron(Box<croner::Cron>),
}

/// A single cron task configuration
#[derive(Debug, Clone)]
pub struct CronTask {
    pub contract_name: String,
    pub table: Table,
    pub full_table_name: String,
    pub cron_entry: TableCronMapping,
    pub network: String,
    pub contract_address: Address,
    pub schedule: CronSchedule,
    pub indexer_name: String,
}

/// The cron scheduler manages and executes scheduled table operations.
pub struct CronScheduler {
    pub tasks: Vec<CronTask>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    pub providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
}

impl CronScheduler {
    /// Create a new cron scheduler from the manifest.
    ///
    /// This scans all contracts and tables for cron configurations and
    /// builds a list of tasks to execute.
    pub fn new(
        manifest: &Manifest,
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
        providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
    ) -> Self {
        let mut tasks = Vec::new();

        for contract in &manifest.contracts {
            let Some(tables) = &contract.tables else {
                continue;
            };

            for table in tables {
                let Some(cron_entries) = &table.cron else {
                    continue;
                };

                for cron_entry in cron_entries {
                    // Determine which networks to run on
                    let networks: Vec<String> = if let Some(network) = &cron_entry.network {
                        vec![network.clone()]
                    } else {
                        contract.details.iter().map(|d| d.network.clone()).collect()
                    };

                    // Parse the schedule
                    let schedule = if let Some(interval_str) = &cron_entry.interval {
                        match parse_interval(interval_str) {
                            Ok(duration) => CronSchedule::Interval(duration),
                            Err(e) => {
                                error!(
                                    "Invalid interval '{}' for table '{}': {}",
                                    interval_str, table.name, e
                                );
                                continue;
                            }
                        }
                    } else if let Some(cron_expr) = &cron_entry.schedule {
                        match croner::Cron::new(cron_expr).parse() {
                            Ok(cron) => CronSchedule::Cron(Box::new(cron)),
                            Err(e) => {
                                error!(
                                    "Invalid cron expression '{}' for table '{}': {}",
                                    cron_expr, table.name, e
                                );
                                continue;
                            }
                        }
                    } else {
                        error!(
                            "Cron entry for table '{}' has neither interval nor schedule",
                            table.name
                        );
                        continue;
                    };

                    for network in networks {
                        // Get contract address for this network
                        let contract_address = contract
                            .details
                            .iter()
                            .find(|d| d.network == network)
                            .and_then(|d| d.address.as_ref())
                            .and_then(|addr| match addr {
                                alloy::rpc::types::ValueOrArray::Value(a) => Some(*a),
                                alloy::rpc::types::ValueOrArray::Array(arr) => arr.first().copied(),
                            })
                            .unwrap_or_default();

                        let full_table_name = crate::database::generate::generate_table_full_name(
                            &manifest.name,
                            &contract.name,
                            &table.name,
                        );

                        tasks.push(CronTask {
                            contract_name: contract.name.clone(),
                            table: table.clone(),
                            full_table_name,
                            cron_entry: cron_entry.clone(),
                            network,
                            contract_address,
                            schedule: schedule.clone(),
                            indexer_name: manifest.name.clone(),
                        });
                    }
                }
            }
        }

        info!("Cron scheduler initialized with {} tasks", tasks.len());

        Self { tasks, postgres, clickhouse, providers }
    }

    /// Check if there are any cron tasks to run.
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Start the cron scheduler - runs until shutdown.
    ///
    /// This spawns a task for each cron configuration and waits for all to complete.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.tasks.is_empty() {
            info!("No cron tasks configured, scheduler exiting");
            return Ok(());
        }

        let postgres = self.postgres;
        let clickhouse = self.clickhouse;
        let providers = self.providers;

        let mut handles = Vec::new();

        for task in self.tasks {
            let postgres = postgres.clone();
            let clickhouse = clickhouse.clone();
            let providers = providers.clone();

            let handle = tokio::spawn(async move {
                run_cron_task(task, postgres, clickhouse, providers).await;
            });

            handles.push(handle);
        }

        // Wait for all tasks (they run until shutdown)
        futures::future::join_all(handles).await;

        info!("Cron scheduler stopped");
        Ok(())
    }
}

/// Maximum number of retry attempts for failed cron operations.
const MAX_RETRIES: u32 = 3;

/// Initial backoff delay for retries (doubles each attempt).
const INITIAL_BACKOFF_SECS: u64 = 2;

/// Run a single cron task until shutdown.
async fn run_cron_task(
    task: CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) {
    info!(
        "Starting cron task for table '{}' on network '{}' with schedule {:?}",
        task.table.name,
        task.network,
        task.cron_entry.interval.as_ref().or(task.cron_entry.schedule.as_ref())
    );

    loop {
        if !is_running() {
            debug!("Cron task for table '{}' stopping due to shutdown", task.table.name);
            break;
        }

        // Wait for next trigger
        let wait_duration = match &task.schedule {
            CronSchedule::Interval(duration) => *duration,
            CronSchedule::Cron(cron) => {
                // Calculate next occurrence from now
                let now = chrono::Utc::now();
                match cron.find_next_occurrence(&now, false) {
                    Ok(next) => {
                        let duration = next - now;
                        duration.to_std().unwrap_or(Duration::from_secs(60))
                    }
                    Err(e) => {
                        warn!(
                            "Could not find next cron occurrence for table '{}': {}, using 60s default",
                            task.table.name, e
                        );
                        Duration::from_secs(60)
                    }
                }
            }
        };

        debug!(
            "Cron task for table '{}' waiting {:?} until next execution",
            task.table.name, wait_duration
        );

        // Sleep in chunks to check for shutdown
        let sleep_chunk = Duration::from_secs(1);
        let mut remaining = wait_duration;

        while remaining > Duration::ZERO {
            if !is_running() {
                debug!("Cron task for table '{}' stopping due to shutdown", task.table.name);
                return;
            }

            let sleep_time = remaining.min(sleep_chunk);
            tokio::time::sleep(sleep_time).await;
            remaining = remaining.saturating_sub(sleep_time);
        }

        if !is_running() {
            break;
        }

        // Execute the cron operations with retry and exponential backoff
        let mut retry_count = 0;
        let mut last_error: Option<String> = None;

        while retry_count <= MAX_RETRIES {
            if !is_running() {
                break;
            }

            match execute_cron_operations(
                &task,
                postgres.clone(),
                clickhouse.clone(),
                providers.clone(),
            )
            .await
            {
                Ok(()) => {
                    // Success - clear any previous error state
                    if retry_count > 0 {
                        info!(
                            "Cron task for table '{}' succeeded after {} retry attempt(s)",
                            task.table.name, retry_count
                        );
                    }
                    break;
                }
                Err(e) => {
                    last_error = Some(e.clone());
                    retry_count += 1;

                    if retry_count <= MAX_RETRIES {
                        // Calculate exponential backoff: 2s, 4s, 8s
                        let backoff_secs = INITIAL_BACKOFF_SECS * (1 << (retry_count - 1));
                        let backoff = Duration::from_secs(backoff_secs);

                        warn!(
                            "Cron task failed for table '{}' on network '{}': {}. Retrying in {:?} (attempt {}/{})",
                            task.table.name, task.network, e, backoff, retry_count, MAX_RETRIES
                        );

                        // Sleep with shutdown check
                        let mut backoff_remaining = backoff;
                        while backoff_remaining > Duration::ZERO {
                            if !is_running() {
                                return;
                            }
                            let sleep_time = backoff_remaining.min(Duration::from_secs(1));
                            tokio::time::sleep(sleep_time).await;
                            backoff_remaining = backoff_remaining.saturating_sub(sleep_time);
                        }
                    }
                }
            }
        }

        // If we exhausted all retries, log the final error
        if retry_count > MAX_RETRIES {
            if let Some(e) = last_error {
                error!(
                    "Cron task failed for table '{}' on network '{}' after {} retries: {}",
                    task.table.name, task.network, MAX_RETRIES, e
                );
            }
        }
    }
}

/// Execute the cron operations for a task.
async fn execute_cron_operations(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) -> Result<(), String> {
    let provider = providers.get(&task.network);

    // Get latest block number for this network
    let latest_block = if let Some(p) = provider {
        p.get_block_number().await.ok().map(|b| b.to::<u64>()).unwrap_or(0)
    } else {
        warn!(
            "No provider found for network '{}' in cron task for table '{}'",
            task.network, task.table.name
        );
        0
    };

    let now = chrono::Utc::now();

    // Create synthetic metadata (no event data)
    let tx_metadata = TxMetadata {
        block_number: latest_block,
        block_timestamp: Some(U256::from(now.timestamp() as u64)),
        tx_hash: B256::ZERO,
        block_hash: B256::ZERO,
        contract_address: task.contract_address,
        log_index: U256::ZERO,
        tx_index: 0,
    };

    debug!(
        "Executing cron operations for table '{}' on network '{}' at block {}",
        task.table.name, task.network, latest_block
    );

    // Process each operation
    for operation in &task.cron_entry.operations {
        let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

        // Get provider for view calls
        let provider_ref = provider.map(|p| p.as_ref());

        // Add where clause columns
        for (column_name, value_ref) in &operation.where_clause {
            let column_def = task.table.columns.iter().find(|c| &c.name == column_name);

            if let Some(column_def) = column_def {
                if let Some(value) = extract_cron_value(
                    value_ref,
                    &tx_metadata,
                    &task.contract_address,
                    column_def.resolved_type(),
                    provider_ref,
                    &task.network,
                )
                .await
                {
                    columns.insert(column_name.clone(), value);
                }
            }
        }

        // Add set columns with their values
        for set_col in &operation.set {
            let column_def = task.table.columns.iter().find(|c| c.name == set_col.column);

            if let Some(column_def) = column_def {
                if let Some(value) = extract_cron_value(
                    set_col.effective_value(),
                    &tx_metadata,
                    &task.contract_address,
                    column_def.resolved_type(),
                    provider_ref,
                    &task.network,
                )
                .await
                {
                    columns.insert(set_col.column.clone(), value);
                }
            }
        }

        if columns.is_empty() {
            debug!(
                "No columns extracted for cron operation on table '{}', skipping",
                task.table.name
            );
            continue;
        }

        // Add auto-injected metadata columns
        // For cron, we use a synthetic sequence_id based on timestamp to ensure uniqueness
        let sequence_id = crate::manifest::contract::compute_sequence_id(
            latest_block,
            0, // no tx_index for cron
            0, // no log_index for cron
        );

        columns.insert(
            crate::manifest::contract::injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
            EthereumSqlTypeWrapper::U128(sequence_id),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::BLOCK_NUMBER.to_string(),
            EthereumSqlTypeWrapper::U64BigInt(latest_block),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::BLOCK_TIMESTAMP.to_string(),
            EthereumSqlTypeWrapper::DateTime(now),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::TX_HASH.to_string(),
            EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::BLOCK_HASH.to_string(),
            EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::CONTRACT_ADDRESS.to_string(),
            EthereumSqlTypeWrapper::Address(task.contract_address),
        );

        let rows = vec![TableRowData { columns, network: task.network.clone() }];

        // Execute the operation
        if let Some(postgres) = &postgres {
            super::tables::execute_postgres_operation_internal(
                postgres,
                &task.full_table_name,
                &task.table,
                operation,
                &rows,
                None, // No SQL condition for cron
            )
            .await?;
        }

        if let Some(clickhouse) = &clickhouse {
            super::tables::execute_clickhouse_operation_internal(
                clickhouse,
                &task.full_table_name,
                &task.table,
                operation,
                &rows,
            )
            .await?;
        }
    }

    info!(
        "Cron operations completed for table '{}' on network '{}' at block {}",
        task.table.name, task.network, latest_block
    );

    Ok(())
}

/// Extract value for cron operations (no event data available).
///
/// This is similar to `extract_value_from_event_async` but only supports:
/// - $call(...) - view function calls
/// - $contract - contract address
/// - $rindexer_* - built-in metadata
/// - Literals
async fn extract_cron_value(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
    provider: Option<&JsonRpcCachedProvider>,
    network: &str,
) -> Option<EthereumSqlTypeWrapper> {
    // Handle $call(...) - view function calls
    if value_ref.starts_with("$call(") {
        let provider = provider?;
        // Use existing view call infrastructure from tables module
        return super::tables::execute_view_call_for_cron(
            value_ref,
            tx_metadata,
            contract_address,
            column_type,
            provider,
            network,
        )
        .await;
    }

    // Handle $contract - contract address
    if value_ref == "$contract" {
        return Some(EthereumSqlTypeWrapper::Address(*contract_address));
    }

    // Handle $rindexer_* built-ins
    if let Some(field_name) = value_ref.strip_prefix('$') {
        match field_name {
            "rindexer_block_number" => {
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number));
            }
            "rindexer_block_timestamp" | "rindexer_timestamp" => {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        return Some(EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)));
                    }
                }
                return None;
            }
            "rindexer_tx_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.tx_hash
                )));
            }
            "rindexer_block_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.block_hash
                )));
            }
            "rindexer_contract_address" => {
                return Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address));
            }
            _ => {
                // Unknown $ reference - this shouldn't happen after validation
                warn!("Unknown cron value reference: {}", value_ref);
                return None;
            }
        }
    }

    // Handle literal values
    super::tables::parse_literal_value_for_column(value_ref, column_type)
}

/// Check if any tables in the manifest have cron triggers.
pub fn manifest_has_cron_tables(manifest: &Manifest) -> bool {
    manifest.contracts.iter().filter_map(|c| c.tables.as_ref()).flatten().any(|t| t.has_cron())
}
