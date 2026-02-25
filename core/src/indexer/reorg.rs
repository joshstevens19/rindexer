use alloy::{
    dyn_abi::DynSolValue,
    json_abi::Param,
    primitives::{Address, B256, I256, U256, U64},
};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::abi::{ABIInput, ABIItem};
use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::{
    generate_column_names_only_with_base_properties,
    generate_internal_event_table_name, generate_internal_event_table_name_no_shorten,
};
use crate::event::config::EventProcessingConfig;
use crate::helpers::{camel_to_snake, get_full_path, parse_solidity_integer_type};
use crate::indexer::fetch_logs::{BlockMeta, ReorgInfo};
use crate::indexer::tables::{process_table_operations, TableRuntime, TxMetadata};
use crate::manifest::contract::{OperationType, SetAction};
use crate::metrics::indexing as metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::JsonRpcCachedProvider;
use crate::types::core::LogParam;
use crate::types::single_or_array::StringOrArray;
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

        if let Err(e) = recompute_derived_tables(config, &tables, fork_block, network).await {
            error!("Reorg: failed to recompute derived tables for {}: {}", event_name, e);
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayStrategy {
    DeletionOnly,
    IncrementalReplay { from_block: u64 },
    FullReplay,
}

fn determine_replay_strategy(
    table_runtime: &TableRuntime,
    event_name: &str,
    fork_block: u64,
) -> ReplayStrategy {
    let Some(mapping) = table_runtime.table.events.iter().find(|e| e.event == event_name) else {
        return ReplayStrategy::DeletionOnly;
    };

    let has_upsert = mapping.operations.iter().any(|op| op.operation_type == OperationType::Upsert);
    if !has_upsert {
        return ReplayStrategy::DeletionOnly;
    }

    let has_accumulative_upsert = mapping.operations.iter().any(|op| {
        op.operation_type == OperationType::Upsert
            && op.set.iter().any(|set| matches!(set.action, SetAction::Add | SetAction::Subtract))
    });

    if has_accumulative_upsert {
        ReplayStrategy::FullReplay
    } else {
        ReplayStrategy::IncrementalReplay { from_block: fork_block }
    }
}

pub async fn recompute_derived_tables(
    config: &Arc<EventProcessingConfig>,
    tables: &[TableRuntime],
    fork_block: u64,
    network: &str,
) -> Result<(), String> {
    let event_name = config.event_name();
    let mut replay_tables: Vec<(TableRuntime, ReplayStrategy)> = vec![];

    for table in tables {
        let strategy = determine_replay_strategy(table, &event_name, fork_block);
        if strategy != ReplayStrategy::DeletionOnly {
            replay_tables.push((table.clone(), strategy));
        }
    }

    if replay_tables.is_empty() {
        return Ok(());
    }

    let abi_inputs = load_event_abi_inputs(config)?;
    if abi_inputs.is_empty() {
        return Err(format!(
            "could not find ABI inputs for {}::{}",
            config.contract_name(),
            event_name
        ));
    }

    for (table_runtime, strategy) in replay_tables {
        if matches!(strategy, ReplayStrategy::FullReplay) {
            clear_table_for_replay(
                &table_runtime,
                &config.postgres(),
                &config.clickhouse(),
                if table_runtime.table.cross_chain { None } else { Some(network) },
            )
            .await;
        }

        let from_block = match strategy {
            ReplayStrategy::IncrementalReplay { from_block } => from_block,
            ReplayStrategy::FullReplay => 0,
            ReplayStrategy::DeletionOnly => continue,
        };

        replay_table_from_storage(
            config,
            &table_runtime,
            &abi_inputs,
            if table_runtime.table.cross_chain { None } else { Some(network) },
            from_block,
            &event_name,
        )
        .await?;
    }

    Ok(())
}

async fn replay_table_from_storage(
    config: &Arc<EventProcessingConfig>,
    table_runtime: &TableRuntime,
    abi_inputs: &[ABIInput],
    network_filter: Option<&str>,
    from_block: u64,
    event_name: &str,
) -> Result<(), String> {
    if config.postgres().is_some() {
        return replay_table_from_postgres(
            config,
            table_runtime,
            abi_inputs,
            network_filter,
            from_block,
            event_name,
        )
        .await;
    }

    if config.clickhouse().is_some() {
        return replay_table_from_clickhouse(
            config,
            table_runtime,
            abi_inputs,
            network_filter,
            from_block,
            event_name,
        )
        .await;
    }

    Err("replay requires either PostgreSQL or ClickHouse raw event storage".to_string())
}

fn load_event_abi_inputs(config: &EventProcessingConfig) -> Result<Vec<ABIInput>, String> {
    let Some(contract_abi) = config.contract_abi() else {
        return Err("missing contract ABI in event processing config".to_string());
    };

    let project_path = config.project_path();
    let merged_abi_json = match contract_abi {
        StringOrArray::Single(abi_path) => {
            let full_path = get_full_path(&project_path, &abi_path)
                .map_err(|e| format!("failed to resolve ABI path {}: {}", abi_path, e))?;
            std::fs::read_to_string(full_path)
                .map_err(|e| format!("failed to read ABI {}: {}", abi_path, e))?
        }
        StringOrArray::Multiple(abi_paths) => {
            let mut unique_entries = std::collections::HashSet::new();
            let mut merged_entries = Vec::new();

            for abi_path in abi_paths {
                let full_path = get_full_path(&project_path, &abi_path)
                    .map_err(|e| format!("failed to resolve ABI path {}: {}", abi_path, e))?;
                let abi_str = std::fs::read_to_string(full_path)
                    .map_err(|e| format!("failed to read ABI {}: {}", abi_path, e))?;
                let abi_value: serde_json::Value = serde_json::from_str(&abi_str)
                    .map_err(|e| format!("invalid ABI JSON in {}: {}", abi_path, e))?;

                let serde_json::Value::Array(entries) = abi_value else {
                    return Err(format!("ABI file {} must be a JSON array", abi_path));
                };

                for entry in entries {
                    let key = serde_json::to_string(&entry)
                        .map_err(|e| format!("failed to serialize ABI entry: {}", e))?;
                    if unique_entries.insert(key) {
                        merged_entries.push(entry);
                    }
                }
            }

            serde_json::to_string(&merged_entries)
                .map_err(|e| format!("failed to merge ABI entries: {}", e))?
        }
    };

    let abi_items: Vec<ABIItem> = serde_json::from_str(&merged_abi_json)
        .map_err(|e| format!("failed to parse ABI items: {}", e))?;
    let event_name = config.event_name();
    let event = abi_items
        .into_iter()
        .find(|item| item.type_ == "event" && item.name == event_name)
        .ok_or_else(|| format!("event {} not found in ABI", event_name))?;

    Ok(event.inputs)
}

async fn clear_table_for_replay(
    table_runtime: &TableRuntime,
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
    network: Option<&str>,
) {
    let full_table = &table_runtime.full_table_name;

    if let Some(pg) = postgres {
        let query = if let Some(network) = network {
            format!("DELETE FROM {} WHERE network = '{}'", full_table, network)
        } else {
            format!("DELETE FROM {}", full_table)
        };
        if let Err(e) = pg.batch_execute(&query).await {
            error!(
                "Reorg replay: failed clearing table {}{}: {:?}",
                full_table,
                network.map(|n| format!(" for network {}", n)).unwrap_or_default(),
                e
            );
        }
    }

    if let Some(ch) = clickhouse {
        let query = if let Some(network) = network {
            format!(
                "ALTER TABLE {} DELETE WHERE network = '{}' SETTINGS mutations_sync = 1",
                full_table, network
            )
        } else {
            format!(
                "ALTER TABLE {} DELETE WHERE 1 = 1 SETTINGS mutations_sync = 1",
                full_table
            )
        };
        if let Err(e) = ch.execute(&query).await {
            error!(
                "Reorg replay: failed clearing ClickHouse table {}{}: {:?}",
                full_table,
                network.map(|n| format!(" for network {}", n)).unwrap_or_default(),
                e
            );
        }
    }
}

async fn replay_table_from_postgres(
    config: &Arc<EventProcessingConfig>,
    table_runtime: &TableRuntime,
    abi_inputs: &[ABIInput],
    network_filter: Option<&str>,
    from_block: u64,
    event_name: &str,
) -> Result<(), String> {
    let Some(postgres) = config.postgres() else {
        return Err("replay requires PostgreSQL raw event storage".to_string());
    };

    let schema = generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
    let event_table = camel_to_snake(event_name);
    let full_event_table = format!("{}.{}", schema, event_table);
    let batch_size = 2_000_u64;
    let mut last_cursor: Option<(u64, u64, u64)> = None;

    let providers = config.providers();
    let constants = config.constants();
    let multicall_addresses = config.multicall_addresses();

    loop {
        let cursor_condition = if let Some((last_block, last_tx_index, last_log_index)) = last_cursor
        {
            format!(
                " AND (block_number > {} OR (block_number = {} AND tx_index > {}) OR (block_number = {} AND tx_index = {} AND CAST(log_index AS NUMERIC) > {}))",
                last_block,
                last_block,
                last_tx_index,
                last_block,
                last_tx_index,
                last_log_index
            )
        } else {
            String::new()
        };

        let network_predicate = network_filter
            .map(|network| format!("network = '{}' AND ", network))
            .unwrap_or_default();

        let query = format!(
            "SELECT row_to_json(t) AS row_data FROM (\
             SELECT * FROM {} \
             WHERE {}block_number >= {}{} \
             ORDER BY block_number ASC, tx_index ASC, CAST(log_index AS NUMERIC) ASC \
             LIMIT {}\
             ) t",
            full_event_table, network_predicate, from_block, cursor_condition, batch_size
        );

        let rows = postgres
            .query(&query, &[])
            .await
            .map_err(|e| format!("failed replay query for {}: {}", full_event_table, e))?;

        if rows.is_empty() {
            break;
        }

        let mut events_data: Vec<(Vec<LogParam>, String, TxMetadata)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let row_json: serde_json::Value = row.get("row_data");
            let log_params = build_log_params_from_row(abi_inputs, &row_json)?;
            let tx_metadata = build_tx_metadata_from_row(&row_json)?;
            let event_network = parse_network_from_row(&row_json)?;
            events_data.push((log_params, event_network, tx_metadata));
        }

        process_table_operations(
            std::slice::from_ref(table_runtime),
            event_name,
            &events_data,
            config.postgres(),
            config.clickhouse(),
            providers.clone(),
            constants.as_ref(),
            multicall_addresses.as_ref(),
            None,
        )
        .await?;

        if let Some((_, _, tx_metadata)) = events_data.last() {
            let last_log_index = tx_metadata.log_index.to::<u64>();
            last_cursor = Some((tx_metadata.block_number, tx_metadata.tx_index, last_log_index));
        }

        if rows.len() < batch_size as usize {
            break;
        }
    }

    info!(
        "Reorg replay complete for table {} from block {}{}",
        table_runtime.full_table_name,
        from_block,
        network_filter.map(|n| format!(" on {}", n)).unwrap_or(" across all networks".to_string())
    );

    Ok(())
}

#[derive(Debug, clickhouse::Row, Deserialize)]
struct ClickhouseReplayRow {
    row_data: String,
}

async fn replay_table_from_clickhouse(
    config: &Arc<EventProcessingConfig>,
    table_runtime: &TableRuntime,
    abi_inputs: &[ABIInput],
    network_filter: Option<&str>,
    from_block: u64,
    event_name: &str,
) -> Result<(), String> {
    let Some(clickhouse) = config.clickhouse() else {
        return Err("replay requires ClickHouse raw event storage".to_string());
    };

    let schema = generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
    let event_table = camel_to_snake(event_name);
    let full_event_table = format!("{}.{}", schema, event_table);
    let batch_size = 2_000_u64;
    let mut last_cursor: Option<(u64, u64, u64)> = None;

    let providers = config.providers();
    let constants = config.constants();
    let multicall_addresses = config.multicall_addresses();

    let row_columns = generate_column_names_only_with_base_properties(abi_inputs);
    let select_columns = row_columns
        .iter()
        .map(|column| format!("`{}`", column))
        .collect::<Vec<_>>()
        .join(", ");

    loop {
        let cursor_condition = if let Some((last_block, last_tx_index, last_log_index)) = last_cursor
        {
            format!(
                " AND (block_number > {} OR (block_number = {} AND tx_index > {}) OR (block_number = {} AND tx_index = {} AND log_index > {}))",
                last_block, last_block, last_tx_index, last_block, last_tx_index, last_log_index
            )
        } else {
            String::new()
        };

        let network_predicate = network_filter
            .map(|network| format!("network = '{}' AND ", network))
            .unwrap_or_default();

        let query = format!(
            "SELECT toJSONString(({})) AS row_data \
             FROM {} \
             WHERE {}block_number >= {}{} \
             ORDER BY block_number ASC, tx_index ASC, log_index ASC \
             LIMIT {}",
            select_columns, full_event_table, network_predicate, from_block, cursor_condition, batch_size
        );

        let rows: Vec<ClickhouseReplayRow> = clickhouse
            .query_all(&query)
            .await
            .map_err(|e| format!("failed replay query for {}: {}", full_event_table, e))?;

        if rows.is_empty() {
            break;
        }

        let mut events_data: Vec<(Vec<LogParam>, String, TxMetadata)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let row_json = row_json_from_clickhouse_tuple(&row.row_data, &row_columns)?;
            let log_params = build_log_params_from_row(abi_inputs, &row_json)?;
            let tx_metadata = build_tx_metadata_from_row(&row_json)?;
            let event_network = parse_network_from_row(&row_json)?;
            events_data.push((log_params, event_network, tx_metadata));
        }

        process_table_operations(
            std::slice::from_ref(table_runtime),
            event_name,
            &events_data,
            config.postgres(),
            config.clickhouse(),
            providers.clone(),
            constants.as_ref(),
            multicall_addresses.as_ref(),
            None,
        )
        .await?;

        if let Some((_, _, tx_metadata)) = events_data.last() {
            let last_log_index = tx_metadata.log_index.to::<u64>();
            last_cursor = Some((tx_metadata.block_number, tx_metadata.tx_index, last_log_index));
        }

        if rows.len() < batch_size as usize {
            break;
        }
    }

    info!(
        "Reorg replay complete (ClickHouse source) for table {} from block {}{}",
        table_runtime.full_table_name,
        from_block,
        network_filter.map(|n| format!(" on {}", n)).unwrap_or(" across all networks".to_string())
    );

    Ok(())
}

fn row_json_from_clickhouse_tuple(
    tuple_json: &str,
    row_columns: &[String],
) -> Result<serde_json::Value, String> {
    let tuple_value: serde_json::Value =
        serde_json::from_str(tuple_json).map_err(|e| format!("invalid ClickHouse tuple JSON: {}", e))?;

    let values = tuple_value
        .as_array()
        .ok_or_else(|| format!("expected tuple JSON array, got {}", tuple_value))?;

    if values.len() != row_columns.len() {
        return Err(format!(
            "column/value length mismatch for ClickHouse replay row: {} columns vs {} values",
            row_columns.len(),
            values.len()
        ));
    }

    let mut obj = serde_json::Map::with_capacity(row_columns.len());
    for (column, value) in row_columns.iter().zip(values.iter()) {
        obj.insert(column.clone(), value.clone());
    }

    Ok(serde_json::Value::Object(obj))
}

fn build_tx_metadata_from_row(row: &serde_json::Value) -> Result<TxMetadata, String> {
    let contract_address = parse_address(value_as_str(&row["contract_address"])?)?;
    let tx_hash = parse_b256(value_as_str(&row["tx_hash"])?)?;
    let block_hash = parse_b256(value_as_str(&row["block_hash"])?)?;
    let block_number = parse_u64_json(&row["block_number"])?;
    let tx_index = parse_u64_json(&row["tx_index"])?;
    let log_index = parse_u256_json(&row["log_index"])?;
    let block_timestamp = parse_block_timestamp_to_u256(&row["block_timestamp"])?;

    Ok(TxMetadata {
        block_number,
        block_timestamp,
        tx_hash,
        block_hash,
        contract_address,
        log_index,
        tx_index,
    })
}

fn parse_network_from_row(row: &serde_json::Value) -> Result<String, String> {
    value_as_str(&row["network"]).map(|s| s.to_string())
}

fn build_log_params_from_row(
    abi_inputs: &[ABIInput],
    row: &serde_json::Value,
) -> Result<Vec<LogParam>, String> {
    let mut params = Vec::with_capacity(abi_inputs.len());
    for input in abi_inputs {
        let value = parse_input_from_row(input, row, None)?;
        let components = input
            .components
            .as_ref()
            .map(|c| abi_inputs_to_params(c))
            .unwrap_or_default();
        params.push(LogParam { name: input.name.clone(), value, components });
    }
    Ok(params)
}

fn abi_inputs_to_params(inputs: &[ABIInput]) -> Vec<Param> {
    inputs
        .iter()
        .map(|input| Param {
            name: input.name.clone(),
            ty: input.type_.clone(),
            internal_type: None,
            components: input
                .components
                .as_ref()
                .map(|components| abi_inputs_to_params(components))
                .unwrap_or_default(),
        })
        .collect()
}

fn parse_input_from_row(
    input: &ABIInput,
    row: &serde_json::Value,
    prefix: Option<&str>,
) -> Result<DynSolValue, String> {
    let field_name = field_name(prefix, &input.name);
    let base_type = input.type_.split('[').next().unwrap_or(&input.type_);
    let is_array = input.type_.contains('[');

    if base_type == "tuple" && !is_array {
        let Some(components) = input.components.as_ref() else {
            return Err(format!("tuple input {} missing components", input.name));
        };

        let mut values = Vec::with_capacity(components.len());
        for component in components {
            values.push(parse_input_from_row(component, row, Some(&field_name))?);
        }
        return Ok(DynSolValue::Tuple(values));
    }

    let value = row
        .get(&field_name)
        .ok_or_else(|| format!("missing field {} in replay row", field_name))?;
    parse_sol_value(base_type, &input.type_, value, input.components.as_deref())
}

fn parse_sol_value(
    base_type: &str,
    full_type: &str,
    value: &serde_json::Value,
    components: Option<&[ABIInput]>,
) -> Result<DynSolValue, String> {
    if let serde_json::Value::String(json_encoded) = value {
        if full_type.contains('[') || base_type == "tuple" {
            if let Ok(decoded_json) = serde_json::from_str::<serde_json::Value>(json_encoded) {
                return parse_sol_value(base_type, full_type, &decoded_json, components);
            }
        }

        if base_type == "bool" {
            match json_encoded.as_str() {
                "true" => return Ok(DynSolValue::Bool(true)),
                "false" => return Ok(DynSolValue::Bool(false)),
                _ => {}
            }
        }
    }

    let is_array = full_type.contains('[');
    let fixed_array_len = extract_fixed_array_len(full_type);

    if is_array {
        let values = value
            .as_array()
            .ok_or_else(|| format!("expected array for type {}", full_type))?;

        let parsed_values: Result<Vec<DynSolValue>, String> = values
            .iter()
            .map(|entry| {
                if base_type == "tuple" {
                    let Some(tuple_components) = components else {
                        return Err("tuple[] missing components".to_string());
                    };
                    parse_tuple_json(tuple_components, entry)
                } else {
                    parse_sol_value(base_type, base_type, entry, components)
                }
            })
            .collect();

        let parsed_values = parsed_values?;
        return Ok(if fixed_array_len.is_some() {
            DynSolValue::FixedArray(parsed_values)
        } else {
            DynSolValue::Array(parsed_values)
        });
    }

    if base_type == "tuple" {
        let Some(tuple_components) = components else {
            return Err("tuple missing components".to_string());
        };
        return parse_tuple_json(tuple_components, value);
    }

    match base_type {
        "address" => Ok(DynSolValue::Address(parse_address(value_as_str(value)?)?)),
        "bool" => {
            let b = value
                .as_bool()
                .ok_or_else(|| format!("expected bool, got {}", value))?;
            Ok(DynSolValue::Bool(b))
        }
        "string" => Ok(DynSolValue::String(value_as_str(value)?.to_string())),
        t if t.starts_with("bytes") => {
            let parsed = parse_hex_bytes(value_as_str(value)?)?;
            if t == "bytes" {
                Ok(DynSolValue::Bytes(parsed))
            } else {
                let size = t.trim_start_matches("bytes").parse::<usize>().unwrap_or(32);
                let fixed = alloy::primitives::FixedBytes::<32>::left_padding_from(&parsed);
                Ok(DynSolValue::FixedBytes(fixed, size))
            }
        }
        t if t.starts_with("uint") => {
            let (_, bits) = parse_solidity_integer_type(t);
            let parsed = U256::from_str(value_as_str(value)?)
                .map_err(|e| format!("invalid uint value for {}: {}", t, e))?;
            Ok(DynSolValue::Uint(parsed, bits))
        }
        t if t.starts_with("int") => {
            let (_, bits) = parse_solidity_integer_type(t);
            let parsed = I256::from_str(value_as_str(value)?)
                .map_err(|e| format!("invalid int value for {}: {}", t, e))?;
            Ok(DynSolValue::Int(parsed, bits))
        }
        _ => Err(format!("unsupported ABI type in replay: {}", base_type)),
    }
}

fn parse_tuple_json(components: &[ABIInput], value: &serde_json::Value) -> Result<DynSolValue, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("expected object for tuple value, got {}", value))?;
    let mut tuple_values = Vec::with_capacity(components.len());
    for component in components {
        let component_value = object
            .get(&component.name)
            .ok_or_else(|| format!("missing tuple field {}", component.name))?;
        let base_type = component.type_.split('[').next().unwrap_or(&component.type_);
        tuple_values.push(parse_sol_value(
            base_type,
            &component.type_,
            component_value,
            component.components.as_deref(),
        )?);
    }
    Ok(DynSolValue::Tuple(tuple_values))
}

fn field_name(prefix: Option<&str>, name: &str) -> String {
    let name = camel_to_snake(name);
    match prefix {
        Some(prefix) => format!("{}_{}", prefix, name),
        None => name,
    }
}

fn extract_fixed_array_len(sol_type: &str) -> Option<usize> {
    let start = sol_type.rfind('[')?;
    let end = sol_type.rfind(']')?;
    if end <= start + 1 {
        return None;
    }

    let len_str = &sol_type[start + 1..end];
    len_str.parse::<usize>().ok()
}

fn value_as_str(value: &serde_json::Value) -> Result<&str, String> {
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => {
            Err(format!("expected string, got numeric value {}", n))
        }
        serde_json::Value::Null => Err("unexpected null value".to_string()),
        _ => Err(format!("unsupported JSON value: {}", value)),
    }
}

fn parse_u64_json(value: &serde_json::Value) -> Result<u64, String> {
    match value {
        serde_json::Value::Number(num) => num
            .as_u64()
            .ok_or_else(|| format!("value {} does not fit u64", num)),
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|e| format!("invalid u64 {}: {}", s, e)),
        _ => Err(format!("unsupported u64 JSON value: {}", value)),
    }
}

fn parse_u256_json(value: &serde_json::Value) -> Result<U256, String> {
    match value {
        serde_json::Value::Number(num) => {
            let as_u64 = num
                .as_u64()
                .ok_or_else(|| format!("value {} does not fit u64", num))?;
            Ok(U256::from(as_u64))
        }
        serde_json::Value::String(s) => U256::from_str(s).map_err(|e| format!("invalid U256 {}: {}", s, e)),
        _ => Err(format!("unsupported U256 JSON value: {}", value)),
    }
}

fn parse_block_timestamp_to_u256(value: &serde_json::Value) -> Result<Option<U256>, String> {
    if value.is_null() {
        return Ok(None);
    }

    match value {
        serde_json::Value::String(ts) => {
            let dt = chrono::DateTime::parse_from_rfc3339(ts)
                .map_err(|e| format!("invalid block timestamp {}: {}", ts, e))?;
            Ok(Some(U256::from(dt.timestamp() as u64)))
        }
        serde_json::Value::Number(num) => {
            let ts = num
                .as_u64()
                .ok_or_else(|| format!("invalid numeric block timestamp {}", num))?;
            Ok(Some(U256::from(ts)))
        }
        _ => Err(format!("unsupported block_timestamp value: {}", value)),
    }
}

fn parse_address(value: &str) -> Result<Address, String> {
    Address::from_str(value).map_err(|e| format!("invalid address {}: {}", value, e))
}

fn parse_b256(value: &str) -> Result<B256, String> {
    B256::from_str(value).map_err(|e| format!("invalid B256 {}: {}", value, e))
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, String> {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    hex::decode(raw).map_err(|e| format!("invalid hex {}: {}", value, e))
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
    use std::collections::HashMap;

    use alloy::primitives::Address;
    use crate::manifest::contract::ReorgSafeDistance;
    use crate::manifest::contract::{
        OperationType, SetAction, SetColumn, Table, TableEventMapping, TableOperation,
    };

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
    // Replay strategy + row reconstruction helpers
    // ======================================================================

    fn build_table_runtime_with_operations(
        operation_type: OperationType,
        set_actions: Vec<SetAction>,
        cross_chain: bool,
    ) -> TableRuntime {
        let operations = vec![TableOperation {
            operation_type,
            where_clause: HashMap::new(),
            if_condition: None,
            filter: None,
            set: set_actions
                .into_iter()
                .map(|action| SetColumn {
                    column: "value".to_string(),
                    action,
                    value: Some("$value".to_string()),
                })
                .collect(),
        }];

        let table = Table {
            name: "balances".to_string(),
            global: false,
            cross_chain,
            columns: vec![],
            events: vec![TableEventMapping {
                event: "Transfer".to_string(),
                iterate: vec![],
                operations,
            }],
            cron: None,
            timestamp: false,
        };

        TableRuntime::new(table, "idx", "erc20")
    }

    #[test]
    fn test_determine_replay_strategy_insert_only_is_deletion_only() {
        let table = build_table_runtime_with_operations(OperationType::Insert, vec![], false);
        let strategy = determine_replay_strategy(&table, "Transfer", 1234);
        assert!(matches!(strategy, ReplayStrategy::DeletionOnly));
    }

    #[test]
    fn test_determine_replay_strategy_set_only_upsert_is_incremental() {
        let table = build_table_runtime_with_operations(
            OperationType::Upsert,
            vec![SetAction::Set],
            false,
        );
        let strategy = determine_replay_strategy(&table, "Transfer", 4567);
        assert!(matches!(
            strategy,
            ReplayStrategy::IncrementalReplay { from_block: 4567 }
        ));
    }

    #[test]
    fn test_determine_replay_strategy_accumulative_upsert_is_full_replay() {
        let table = build_table_runtime_with_operations(
            OperationType::Upsert,
            vec![SetAction::Add],
            true,
        );
        let strategy = determine_replay_strategy(&table, "Transfer", 7890);
        assert!(matches!(strategy, ReplayStrategy::FullReplay));
    }

    #[test]
    fn test_build_tx_metadata_from_row_parses_expected_fields() {
        let row = serde_json::json!({
            "contract_address": "0x1111111111111111111111111111111111111111",
            "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "block_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "block_number": 100,
            "tx_index": 2,
            "log_index": "3",
            "block_timestamp": "2026-02-25T10:00:00Z"
        });

        let metadata = build_tx_metadata_from_row(&row).unwrap();
        assert_eq!(metadata.block_number, 100);
        assert_eq!(metadata.tx_index, 2);
        assert_eq!(metadata.log_index, U256::from(3_u64));
        assert_eq!(
            metadata.contract_address,
            Address::from_str("0x1111111111111111111111111111111111111111").unwrap()
        );
        assert!(metadata.block_timestamp.is_some());
    }

    #[test]
    fn test_build_log_params_from_row_parses_flat_and_tuple_inputs() {
        let abi_inputs = vec![
            ABIInput {
                indexed: Some(true),
                name: "from".to_string(),
                type_: "address".to_string(),
                components: None,
            },
            ABIInput {
                indexed: Some(false),
                name: "amount".to_string(),
                type_: "uint256".to_string(),
                components: None,
            },
            ABIInput {
                indexed: Some(false),
                name: "meta".to_string(),
                type_: "tuple".to_string(),
                components: Some(vec![
                    ABIInput {
                        indexed: None,
                        name: "id".to_string(),
                        type_: "uint256".to_string(),
                        components: None,
                    },
                    ABIInput {
                        indexed: None,
                        name: "tag".to_string(),
                        type_: "string".to_string(),
                        components: None,
                    },
                ]),
            },
        ];

        let row = serde_json::json!({
            "from": "0x2222222222222222222222222222222222222222",
            "amount": "42",
            "meta_id": "7",
            "meta_tag": "vip"
        });

        let params = build_log_params_from_row(&abi_inputs, &row).unwrap();
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].name, "from");
        assert_eq!(params[1].name, "amount");
        assert_eq!(params[2].name, "meta");
        assert!(matches!(params[0].value, DynSolValue::Address(_)));
        assert!(matches!(params[1].value, DynSolValue::Uint(_, 256)));
        assert!(matches!(params[2].value, DynSolValue::Tuple(_)));
    }

    #[test]
    fn test_parse_network_from_row() {
        let row = serde_json::json!({"network": "polygon"});
        assert_eq!(parse_network_from_row(&row).unwrap(), "polygon");
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
