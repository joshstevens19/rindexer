use crate::generator::event_callback_registry::{
    EventCallbackRegistry, EventResult, IndexingContractSetup, NetworkContract,
};
use crate::helpers::{camel_to_snake, get_full_path};
use crate::indexer::progress::{IndexingEventProgressStatus, IndexingEventsProgressState};
use crate::manifest::yaml::{CsvDetails, DependencyEventTree};
use crate::provider::JsonRpcCachedProvider;
use crate::{EthereumSqlTypeWrapper, PostgresClient};
use ethers::middleware::MiddlewareError;
use ethers::prelude::{Block, Filter, JsonRpcError, Log, ProviderError};
use ethers::types::{Address, BlockNumber, Bloom, FilteredParams, ValueOrArray, H256, U64};
use futures::future::join_all;
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

pub struct EventProcessingConfig {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub info_log_name: String,
    pub topic_id: String,
    pub event_name: String,
    pub network_contract: Arc<NetworkContract>,
    pub start_block: U64,
    pub end_block: U64,
    pub semaphore: Arc<Semaphore>,
    pub registry: Arc<EventCallbackRegistry>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}

#[derive(Debug, Clone)]
pub struct EventsDependencyTree {
    pub events_name: Vec<String>,
    pub then: Box<Option<Arc<EventsDependencyTree>>>,
}

impl EventsDependencyTree {
    pub fn from_dependency_event_tree(event_tree: DependencyEventTree) -> Self {
        Self {
            events_name: event_tree.events,
            then: match event_tree.then {
                Some(children) if !children.is_empty() => Box::new(Some(Arc::new(
                    EventsDependencyTree::from_dependency_event_tree(children[0].clone()),
                ))),
                _ => Box::new(None),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventDependencies {
    pub tree: Arc<EventsDependencyTree>,
    pub dependency_event_names: Vec<String>,
}

impl EventDependencies {
    pub fn has_dependency(&self, event_name: &str) -> bool {
        self.dependency_event_names
            .contains(&event_name.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ContractEventDependencies {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
}

pub struct ContractEventsConfig {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
    pub events_config: Vec<EventProcessingConfig>,
}

#[derive(Debug)]
struct RetryWithBlockRangeResult {
    from: BlockNumber,
    to: BlockNumber,
    // This is only populated if you are using an RPC provider
    // who doesn't give block ranges, this tends to be providers
    // which are a lot slower than others, expect these providers
    // to be slow
    max_block_range: Option<U64>,
}

/// Attempts to retry with a new block range based on the error message.
fn retry_with_block_range(
    error: &JsonRpcError,
    from_block: U64,
    to_block: U64,
) -> Option<RetryWithBlockRangeResult> {
    let error_message = &error.message;
    // some providers put the data in the data field
    let error_data = match &error.data {
        Some(data) => &data.to_string(),
        None => &String::from(""),
    };

    fn compile_regex(pattern: &str) -> Result<Regex, regex::Error> {
        Regex::new(pattern)
    }

    // Thanks Ponder for the regex patterns - https://github.com/ponder-sh/ponder/blob/889096a3ef5f54a0c5a06df82b0da9cf9a113996/packages/utils/src/getLogsRetryHelper.ts#L34

    // Alchemy
    if let Ok(re) =
        compile_regex(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]")
    {
        if let Some(captures) = re
            .captures(error_message)
            .or_else(|| re.captures(error_data))
        {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = start_block.as_str();
                let end_block_str = end_block.as_str();
                if let (Ok(from), Ok(to)) = (
                    BlockNumber::from_str(start_block_str),
                    BlockNumber::from_str(end_block_str),
                ) {
                    return Some(RetryWithBlockRangeResult {
                        from,
                        to,
                        max_block_range: None,
                    });
                }
            }
        }
    }

    // Infura, Thirdweb, zkSync, Tenderly
    if let Ok(re) =
        compile_regex(r"Try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]")
    {
        if let Some(captures) = re
            .captures(error_message)
            .or_else(|| re.captures(error_data))
        {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = format!("0x{}", start_block.as_str());
                let end_block_str = format!("0x{}", end_block.as_str());
                if let (Ok(from), Ok(to)) = (
                    BlockNumber::from_str(&start_block_str),
                    BlockNumber::from_str(&end_block_str),
                ) {
                    return Some(RetryWithBlockRangeResult {
                        from,
                        to,
                        max_block_range: None,
                    });
                }
            }
        }
    }

    // Ankr
    if error_message.contains("block range is too wide") && error.code == -32600 {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 3000),
            max_block_range: Some(3000.into()),
        });
    }

    // QuickNode, 1RPC, zkEVM, Blast, BlockPI
    if let Ok(re) = compile_regex(r"limited to a ([\d,.]+)") {
        if let Some(captures) = re
            .captures(error_message)
            .or_else(|| re.captures(error_data))
        {
            if let Some(range_str_match) = captures.get(1) {
                let range_str = range_str_match.as_str().replace(&['.', ','][..], "");
                if let Ok(range) = U64::from_dec_str(&range_str) {
                    return Some(RetryWithBlockRangeResult {
                        from: BlockNumber::from(from_block),
                        to: BlockNumber::from(from_block + range),
                        max_block_range: Some(range),
                    });
                }
            }
        }
    }

    // Base
    if error_message.contains("block range too large") {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 2000),
            max_block_range: Some(2000.into()),
        });
    }

    // Fallback range
    if to_block > from_block {
        let fallback_range = (to_block - from_block) / 2;
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + fallback_range),
            max_block_range: Some(fallback_range),
        });
    }

    None
}

#[derive(thiserror::Error, Debug)]
pub enum FetchLogsError {
    #[error("Provider error: {0}")]
    Provider(#[from] ProviderError),
}

pub struct FetchLogsResult {
    pub logs: Vec<Log>,
    #[allow(dead_code)]
    pub from_block: U64,
    pub to_block: U64,
}

pub struct LiveIndexingDetails {
    pub indexing_distance_from_head: U64,
}

fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let address_filter =
        FilteredParams::address_filter(&Some(ValueOrArray::Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![ValueOrArray::Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

pub fn fetch_logs_stream(
    cached_provider: Arc<JsonRpcCachedProvider>,
    topic_id: H256,
    initial_filter: Filter,
    info_log_name: String,
    live_indexing_details: Option<LiveIndexingDetails>,
    semaphore: Arc<Semaphore>,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsResult, Box<dyn std::error::Error + Send>>>
       + Send
       + Unpin {
    let (tx, rx) = mpsc::unbounded_channel();
    let live_indexing = live_indexing_details.is_some();
    let contract_address = initial_filter.address.clone();
    let reorg_safe_distance =
        live_indexing_details.map_or(U64::from(0), |details| details.indexing_distance_from_head);

    tokio::spawn(async move {
        let snapshot_to_block = initial_filter.get_to_block().unwrap();
        let mut current_filter = initial_filter.clone();

        // Process historical logs first
        let mut max_block_range_limitation = None;
        while current_filter.get_from_block().unwrap() <= snapshot_to_block {
            let semaphore_client = Arc::clone(&semaphore);
            let permit = semaphore_client.acquire_owned().await;

            match permit {
                Ok(permit) => {
                    let result = process_historic_logs_stream(
                        &cached_provider,
                        &tx,
                        &topic_id,
                        current_filter.clone(),
                        max_block_range_limitation,
                        snapshot_to_block,
                        &info_log_name,
                    )
                    .await;

                    drop(permit);

                    // slow indexing warn user
                    if let Some(range) = max_block_range_limitation {
                        info!(
                            "{} - RPC PROVIDER IS SLOW - Slow indexing mode enabled, max block range limitation: {} blocks - we advise using a faster provider who can predict the next block ranges.",
                            &info_log_name,
                            range
                        );
                    }

                    if let Some(result) = result {
                        current_filter = result.next;
                        max_block_range_limitation = result.max_block_range_limitation;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        "{} - {} - Semaphore error: {}",
                        &info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        e
                    );
                    continue;
                }
            }
        }

        info!(
            "{} - {} - Finished indexing historic events",
            &info_log_name,
            IndexingEventProgressStatus::Completed.log()
        );

        // Live indexing mode
        if live_indexing {
            live_indexing_stream(
                &cached_provider,
                &tx,
                &contract_address,
                &topic_id,
                reorg_safe_distance,
                current_filter,
                &info_log_name,
                semaphore,
            )
            .await;
        }
    });

    UnboundedReceiverStream::new(rx)
}

fn calculate_process_historic_log_to_block(
    new_from_block: &U64,
    snapshot_to_block: &U64,
    max_block_range_limitation: &Option<U64>,
) -> U64 {
    if let Some(max_block_range_limitation) = max_block_range_limitation {
        let to_block = new_from_block + max_block_range_limitation;
        if to_block > *snapshot_to_block {
            *snapshot_to_block
        } else {
            to_block
        }
    } else {
        *snapshot_to_block
    }
}

struct ProcessHistoricLogsStreamResult {
    pub next: Filter,
    pub max_block_range_limitation: Option<U64>,
}

async fn process_historic_logs_stream(
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn std::error::Error + Send>>>,
    topic_id: &H256,
    current_filter: Filter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
) -> Option<ProcessHistoricLogsStreamResult> {
    let from_block = current_filter.get_from_block().unwrap();
    let to_block = current_filter.get_to_block().unwrap_or(snapshot_to_block);
    debug!(
        "{} - {} - Process historic events - blocks: {} - {}",
        info_log_name,
        IndexingEventProgressStatus::Syncing.log(),
        from_block,
        to_block
    );

    if from_block > to_block {
        debug!(
            "{} - {} - from_block {:?} > to_block {:?}",
            info_log_name,
            IndexingEventProgressStatus::Syncing.log(),
            from_block,
            to_block
        );
        return Some(ProcessHistoricLogsStreamResult {
            next: current_filter.from_block(to_block),
            max_block_range_limitation: None,
        });
    }

    debug!(
        "{} - {} - Processing filter: {:?}",
        info_log_name,
        IndexingEventProgressStatus::Syncing.log(),
        current_filter
    );

    match cached_provider.get_logs(&current_filter).await {
        Ok(logs) => {
            debug!(
                "{} - {} - topic_id {}, Logs: {} from {} to {}",
                info_log_name,
                IndexingEventProgressStatus::Syncing.log(),
                topic_id,
                logs.len(),
                from_block,
                to_block
            );

            debug!(
                "{} - {} - Fetched {} event logs - blocks: {} - {}",
                info_log_name,
                IndexingEventProgressStatus::Syncing.log(),
                logs.len(),
                from_block,
                to_block
            );

            if tx
                .send(Ok(FetchLogsResult {
                    logs: logs.clone(),
                    from_block,
                    to_block,
                }))
                .is_err()
            {
                error!(
                    "{} - {} - Failed to send logs to stream consumer!",
                    IndexingEventProgressStatus::Syncing.log(),
                    info_log_name
                );
                return None;
            }

            if logs.is_empty() {
                let next_from_block = to_block + 1;
                return if next_from_block > snapshot_to_block {
                    None
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );

                    debug!(
                        "{} - {} - new_from_block {:?} new_to_block {:?}",
                        info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        next_from_block,
                        new_to_block
                    );

                    Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .from_block(next_from_block)
                            .to_block(new_to_block),
                        max_block_range_limitation,
                    })
                };
            }

            if let Some(last_log) = logs.last() {
                let next_from_block = last_log.block_number.unwrap() + U64::from(1);
                debug!(
                    "{} - {} - next_block {:?}",
                    info_log_name,
                    IndexingEventProgressStatus::Syncing.log(),
                    next_from_block
                );
                return if next_from_block > snapshot_to_block {
                    // to avoid the thread closing before the stream is consumed
                    // lets just sit here for 1 seconds to avoid the race
                    // and info logs are not in the wrong order
                    // probably a better way to handle this but hey
                    // TODO handle this nicer
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    None
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );

                    debug!(
                        "{} - {} - new_from_block {:?} new_to_block {:?}",
                        info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        next_from_block,
                        new_to_block
                    );

                    Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .from_block(next_from_block)
                            .to_block(new_to_block),
                        max_block_range_limitation,
                    })
                };
            }
        }
        Err(err) => {
            if let Some(json_rpc_error) = err.as_error_response() {
                if let Some(retry_result) =
                    retry_with_block_range(json_rpc_error, from_block, to_block)
                {
                    debug!(
                        "{} - {} - Retrying with block range: {:?}",
                        info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        retry_result
                    );
                    return Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .from_block(retry_result.from)
                            .to_block(retry_result.to),
                        max_block_range_limitation: retry_result.max_block_range,
                    });
                }
            }

            error!(
                "{} - {} - Error fetching logs: {}",
                info_log_name,
                IndexingEventProgressStatus::Syncing.log(),
                err
            );

            let _ = tx.send(Err(Box::new(err)));
            return None;
        }
    }

    None
}

/// Handles live indexing mode, continuously checking for new blocks, ensuring they are
/// within a safe range, updating the filter, and sending the logs to the provided channel.
#[allow(clippy::too_many_arguments)]
async fn live_indexing_stream(
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn std::error::Error + Send>>>,
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    reorg_safe_distance: U64,
    mut current_filter: Filter,
    info_log_name: &str,
    semaphore: Arc<Semaphore>,
) {
    let mut last_seen_block_number = U64::from(0);
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        if let Some(latest_block) = cached_provider.get_latest_block().await.unwrap() {
            let latest_block_number = latest_block.number.unwrap();
            if last_seen_block_number == latest_block_number {
                debug!(
                    "{} - {} - No new blocks to process...",
                    info_log_name,
                    IndexingEventProgressStatus::Live.log()
                );
                continue;
            }
            info!(
                "{} - {} - New block seen {} - Last seen block {}",
                info_log_name,
                IndexingEventProgressStatus::Live.log(),
                latest_block_number,
                last_seen_block_number
            );
            let safe_block_number = latest_block_number - reorg_safe_distance;

            let from_block = current_filter.get_from_block().unwrap();
            // check reorg distance and skip if not safe
            if from_block > safe_block_number {
                info!(
                    "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                    info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    from_block,
                    safe_block_number
                );
                continue;
            }

            let to_block = safe_block_number;

            if from_block == to_block
                && !is_relevant_block(contract_address, topic_id, &latest_block)
            {
                debug!(
                    "{} - {} - Skipping block {} as it's not relevant",
                    info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );
                info!(
                    "{} - {} - LogsBloom check - No events found in the block {}",
                    info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );
                current_filter = current_filter.from_block(to_block + 1);
                last_seen_block_number = to_block;
                continue;
            }

            current_filter = current_filter.to_block(to_block);

            debug!(
                "{} - {} - Processing live filter: {:?}",
                info_log_name,
                IndexingEventProgressStatus::Live.log(),
                current_filter
            );

            let semaphore_client = Arc::clone(&semaphore);
            let permit = semaphore_client.acquire_owned().await;

            if let Ok(permit) = permit {
                match cached_provider.get_logs(&current_filter).await {
                    Ok(logs) => {
                        debug!(
                            "{} - {} - Live topic_id {}, Logs: {} from {} to {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            topic_id,
                            logs.len(),
                            from_block,
                            to_block
                        );

                        debug!(
                            "{} - {} - Fetched {} event logs - blocks: {} - {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            logs.len(),
                            from_block,
                            to_block
                        );

                        last_seen_block_number = to_block;

                        if tx
                            .send(Ok(FetchLogsResult {
                                logs: logs.clone(),
                                from_block,
                                to_block,
                            }))
                            .is_err()
                        {
                            error!(
                                "{} - {} - Failed to send logs to stream consumer!",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log()
                            );
                            drop(permit);
                            break;
                        }

                        if logs.is_empty() {
                            current_filter = current_filter.from_block(to_block + 1);
                            info!(
                                "{} - {} - No events found between blocks {} - {}",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                from_block,
                                to_block
                            );
                        } else if let Some(last_log) = logs.last() {
                            current_filter = current_filter
                                .from_block(last_log.block_number.unwrap() + U64::from(1));
                        }

                        drop(permit);
                    }
                    Err(err) => {
                        error!(
                            "{} - {} - Error fetching logs: {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            err
                        );
                        drop(permit);
                    }
                }
            }
        }
    }
}

/// Checks if a given block is relevant by examining the bloom filter for the contract
/// address and topic ID. Returns true if the block contains relevant logs, false otherwise.
fn is_relevant_block(
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    latest_block: &Block<H256>,
) -> bool {
    match latest_block.logs_bloom {
        None => false,
        Some(logs_bloom) => {
            if let Some(contract_address) = contract_address {
                match contract_address {
                    ValueOrArray::Value(address) => {
                        if !contract_in_bloom(*address, logs_bloom) {
                            return false;
                        }
                    }
                    ValueOrArray::Array(addresses) => {
                        if addresses
                            .iter()
                            .all(|addr| !contract_in_bloom(*addr, logs_bloom))
                        {
                            return false;
                        }
                    }
                }
            }

            if !topic_in_bloom(*topic_id, logs_bloom) {
                return false;
            }

            true
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractEventsWithDependenciesError {
    #[error("{0}")]
    ProcessEventsWithDependenciesError(ProcessEventsWithDependenciesError),

    #[error("{0}")]
    JoinError(JoinError),
}

pub async fn process_contract_events_with_dependencies(
    contract_events_config: Vec<ContractEventsConfig>,
) -> Result<(), ProcessContractEventsWithDependenciesError> {
    let mut handles: Vec<JoinHandle<Result<(), ProcessEventsWithDependenciesError>>> = Vec::new();

    for contract_events in contract_events_config {
        let handle = tokio::spawn(async move {
            process_events_with_dependencies(
                contract_events.event_dependencies,
                contract_events.events_config,
            )
            .await
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(inner_result) => inner_result.map_err(
                ProcessContractEventsWithDependenciesError::ProcessEventsWithDependenciesError,
            )?,
            Err(join_error) => {
                return Err(ProcessContractEventsWithDependenciesError::JoinError(
                    join_error,
                ))
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventsWithDependenciesError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(BuildFilterError),

    #[error("Event config not found")]
    EventConfigNotFound,

    #[error("Could not run all the logs processes {0}")]
    JoinError(JoinError),
}

async fn process_events_with_dependencies(
    dependencies: EventDependencies,
    events_processing_config: Vec<EventProcessingConfig>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    process_events_dependency_tree(dependencies.tree, Arc::new(events_processing_config)).await
}

#[derive(Debug, Clone)]
struct OrderedLiveIndexingDetails {
    pub filter: Filter,
    pub last_seen_block_number: U64,
}

async fn process_events_dependency_tree(
    tree: Arc<EventsDependencyTree>,
    events_processing_config: Arc<Vec<EventProcessingConfig>>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    let mut stack = vec![tree];

    let live_indexing_events = Arc::new(Mutex::new(Vec::<(ProcessLogsParams, H256)>::new()));

    while let Some(current_tree) = stack.pop() {
        let mut tasks = vec![];

        for dependency in &current_tree.events_name {
            let event_processing_config = Arc::clone(&events_processing_config);
            let dependency = dependency.clone();
            let live_indexing_events = Arc::clone(&live_indexing_events);

            let task = tokio::spawn(async move {
                let event_processing_config = event_processing_config
                    .iter()
                    .find(|e| e.event_name == dependency)
                    .ok_or(ProcessEventsWithDependenciesError::EventConfigNotFound)?;

                let filter = build_filter(
                    &event_processing_config.topic_id,
                    &event_processing_config
                        .network_contract
                        .indexing_contract_setup,
                    event_processing_config.start_block,
                    event_processing_config.end_block,
                )
                .map_err(ProcessEventsWithDependenciesError::BuildFilterError)?;

                let logs_params = ProcessLogsParams {
                    project_path: event_processing_config.project_path.clone(),
                    indexer_name: event_processing_config.indexer_name.clone(),
                    contract_name: event_processing_config.contract_name.clone(),
                    info_log_name: event_processing_config.info_log_name.clone(),
                    topic_id: event_processing_config.topic_id.clone(),
                    event_name: event_processing_config.event_name.clone(),
                    network_contract: event_processing_config.network_contract.clone(),
                    filter,
                    registry: event_processing_config.registry.clone(),
                    progress: event_processing_config.progress.clone(),
                    database: event_processing_config.database.clone(),
                    csv_details: event_processing_config.csv_details.clone(),
                    execute_events_logs_in_order: event_processing_config.index_event_in_order,
                    // sync the historic ones first and live indexing is added to the stack to process after
                    live_indexing: false,
                    indexing_distance_from_head: event_processing_config
                        .indexing_distance_from_head,
                    semaphore: Arc::clone(&event_processing_config.semaphore),
                };

                process_logs(logs_params.clone())
                    .await
                    .map_err(ProcessEventsWithDependenciesError::ProcessLogs)?;

                if event_processing_config.live_indexing {
                    let topic_id = event_processing_config.topic_id.parse::<H256>().unwrap();
                    live_indexing_events
                        .lock()
                        .await
                        .push((logs_params.clone(), topic_id));
                }

                Ok::<(), ProcessEventsWithDependenciesError>(())
            });

            tasks.push(task);
        }

        let results = join_all(tasks).await;
        for result in results {
            if let Err(e) = result {
                error!("Error processing logs: {:?}", e);
                return Err(ProcessEventsWithDependenciesError::JoinError(e));
            }
        }

        // If there are more dependencies to process, push the next level onto the stack
        if let Some(next_tree) = &*current_tree.then {
            stack.push(next_tree.clone());
        }
    }

    let mut ordering_live_indexing_details_map: HashMap<
        H256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::new();
    let live_indexing_events = live_indexing_events.lock().await;
    for (log, topic) in live_indexing_events.iter() {
        let mut filter = log.filter.clone();
        let last_seen_block_number = filter.get_to_block().unwrap();
        let next_block_number = last_seen_block_number + 1;
        filter = filter
            .from_block(next_block_number)
            .to_block(next_block_number);
        ordering_live_indexing_details_map.insert(
            *topic,
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
            })),
        );
    }

    if live_indexing_events.is_empty() {
        return Ok(());
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        for (logs_params, parsed_topic_id) in live_indexing_events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(parsed_topic_id)
                .unwrap()
                .lock()
                .await
                .clone();

            let reorg_safe_distance = logs_params.indexing_distance_from_head;
            if let Some(latest_block) = logs_params
                .network_contract
                .cached_provider
                .get_latest_block()
                .await
                .unwrap()
            {
                let latest_block_number = latest_block.number.unwrap();
                if ordering_live_indexing_details.last_seen_block_number == latest_block_number {
                    debug!(
                        "{} - {} - No new blocks to process...",
                        logs_params.info_log_name,
                        IndexingEventProgressStatus::Live.log()
                    );
                    continue;
                }
                info!(
                    "{} - {} - New block seen {} - Last seen block {}",
                    logs_params.info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    latest_block_number,
                    ordering_live_indexing_details.last_seen_block_number
                );
                let safe_block_number = latest_block_number - reorg_safe_distance;

                let from_block = ordering_live_indexing_details
                    .filter
                    .get_from_block()
                    .unwrap();
                // check reorg distance and skip if not safe
                if from_block > safe_block_number {
                    info!(
                        "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                        logs_params.info_log_name,
                        IndexingEventProgressStatus::Live.log(),
                        from_block,
                        safe_block_number
                    );
                    continue;
                }

                let to_block = safe_block_number;

                if from_block == to_block
                    && !is_relevant_block(
                        &ordering_live_indexing_details.filter.address,
                        parsed_topic_id,
                        &latest_block,
                    )
                {
                    debug!(
                        "{} - {} - Skipping block {} as it's not relevant",
                        logs_params.info_log_name,
                        IndexingEventProgressStatus::Live.log(),
                        from_block
                    );
                    info!(
                        "{} - {} - LogsBloom check - No events found in the block {}",
                        logs_params.info_log_name,
                        IndexingEventProgressStatus::Live.log(),
                        from_block
                    );
                    ordering_live_indexing_details.filter = ordering_live_indexing_details
                        .filter
                        .from_block(to_block + 1);
                    ordering_live_indexing_details.last_seen_block_number = to_block;
                    *ordering_live_indexing_details_map
                        .get(parsed_topic_id)
                        .unwrap()
                        .lock()
                        .await = ordering_live_indexing_details;
                    continue;
                }

                ordering_live_indexing_details.filter =
                    ordering_live_indexing_details.filter.to_block(to_block);

                debug!(
                    "{} - {} - Processing live filter: {:?}",
                    logs_params.info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    ordering_live_indexing_details.filter
                );

                let semaphore_client = Arc::clone(&logs_params.semaphore);
                let permit = semaphore_client.acquire_owned().await;

                if let Ok(permit) = permit {
                    match logs_params
                        .network_contract
                        .cached_provider
                        .get_logs(&ordering_live_indexing_details.filter)
                        .await
                    {
                        Ok(logs) => {
                            debug!(
                                "{} - {} - Live topic_id {}, Logs: {} from {} to {}",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                logs_params.topic_id,
                                logs.len(),
                                from_block,
                                to_block
                            );

                            debug!(
                                "{} - {} - Fetched {} event logs - blocks: {} - {}",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                logs.len(),
                                from_block,
                                to_block
                            );

                            let fetched_logs = Ok(FetchLogsResult {
                                logs: logs.clone(),
                                from_block,
                                to_block,
                            });

                            let result = handle_logs_result(
                                logs_params.project_path.clone(),
                                logs_params.indexer_name.clone(),
                                logs_params.contract_name.clone(),
                                logs_params.event_name.clone(),
                                logs_params.topic_id.clone(),
                                logs_params.execute_events_logs_in_order,
                                logs_params.progress.clone(),
                                logs_params.network_contract.clone(),
                                logs_params.database.clone(),
                                logs_params.csv_details.clone(),
                                logs_params.registry.clone(),
                                fetched_logs,
                            )
                            .await;

                            match result {
                                Ok(_) => {
                                    ordering_live_indexing_details.last_seen_block_number =
                                        to_block;
                                    if logs.is_empty() {
                                        ordering_live_indexing_details.filter =
                                            ordering_live_indexing_details
                                                .filter
                                                .from_block(to_block + 1);
                                        info!(
                                            "{} - {} - No events found between blocks {} - {}",
                                            logs_params.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            from_block,
                                            to_block
                                        );
                                    } else if let Some(last_log) = logs.last() {
                                        ordering_live_indexing_details.filter =
                                            ordering_live_indexing_details.filter.from_block(
                                                last_log.block_number.unwrap() + U64::from(1),
                                            );
                                    }

                                    *ordering_live_indexing_details_map
                                        .get(parsed_topic_id)
                                        .unwrap()
                                        .lock()
                                        .await = ordering_live_indexing_details;

                                    drop(permit);
                                }
                                Err(err) => {
                                    error!(
                                "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                err
                            );
                                    drop(permit);
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            error!(
                                "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                err
                            );
                            drop(permit);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(BuildFilterError),
}

pub async fn process_event(
    event_processing_config: EventProcessingConfig,
) -> Result<(), ProcessEventError> {
    debug!(
        "{} - Processing events",
        event_processing_config.info_log_name
    );

    let filter = build_filter(
        &event_processing_config.topic_id,
        &event_processing_config
            .network_contract
            .indexing_contract_setup,
        event_processing_config.start_block,
        event_processing_config.end_block,
    )
    .map_err(ProcessEventError::BuildFilterError)?;

    process_logs(ProcessLogsParams {
        project_path: event_processing_config.project_path,
        indexer_name: event_processing_config.indexer_name,
        contract_name: event_processing_config.contract_name,
        info_log_name: event_processing_config.info_log_name,
        topic_id: event_processing_config.topic_id,
        event_name: event_processing_config.event_name,
        network_contract: event_processing_config.network_contract,
        filter,
        registry: event_processing_config.registry,
        progress: event_processing_config.progress,
        database: event_processing_config.database,
        csv_details: event_processing_config.csv_details,
        execute_events_logs_in_order: event_processing_config.index_event_in_order,
        live_indexing: event_processing_config.live_indexing,
        indexing_distance_from_head: event_processing_config.indexing_distance_from_head,
        semaphore: event_processing_config.semaphore,
    })
    .await
    .map_err(ProcessEventError::ProcessLogs)?;

    Ok(())
}

/// Parameters for processing logs.
#[derive(Clone)]
pub struct ProcessLogsParams {
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    info_log_name: String,
    topic_id: String,
    event_name: String,
    network_contract: Arc<NetworkContract>,
    filter: Filter,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    execute_events_logs_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
    semaphore: Arc<Semaphore>,
}

async fn process_logs(params: ProcessLogsParams) -> Result<(), Box<ProviderError>> {
    let provider = Arc::clone(&params.network_contract.cached_provider);
    let mut logs_stream = fetch_logs_stream(
        provider,
        params
            .topic_id
            .parse::<H256>()
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?,
        params.filter,
        params.info_log_name,
        if params.live_indexing {
            Some(LiveIndexingDetails {
                indexing_distance_from_head: params.indexing_distance_from_head,
            })
        } else {
            None
        },
        params.semaphore,
    );

    while let Some(result) = logs_stream.next().await {
        handle_logs_result(
            params.project_path.clone(),
            params.indexer_name.clone(),
            params.contract_name.clone(),
            params.event_name.clone(),
            params.topic_id.clone(),
            params.execute_events_logs_in_order,
            params.progress.clone(),
            params.network_contract.clone(),
            params.database.clone(),
            params.csv_details.clone(),
            params.registry.clone(),
            result,
        )
        .await
        .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_logs_result(
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    event_name: String,
    topic_id: String,
    execute_events_logs_in_order: bool,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    registry: Arc<EventCallbackRegistry>,
    result: Result<FetchLogsResult, Box<dyn std::error::Error + Send>>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    match result {
        Ok(result) => {
            let fn_data = result
                .logs
                .iter()
                .map(|log| EventResult::new(network_contract.clone(), log))
                .collect::<Vec<_>>();

            debug!(
                "Processing logs {} - length {}",
                event_name,
                result.logs.len()
            );

            if !fn_data.is_empty() {
                if execute_events_logs_in_order {
                    registry.trigger_event(&topic_id, fn_data).await;
                    update_progress_and_last_synced(
                        project_path.clone(),
                        indexer_name.clone(),
                        contract_name,
                        event_name.clone(),
                        progress,
                        network_contract,
                        database,
                        csv_details,
                        result.to_block,
                    );
                } else {
                    tokio::spawn(async move {
                        registry.trigger_event(&topic_id, fn_data).await;
                        update_progress_and_last_synced(
                            project_path,
                            indexer_name.clone(),
                            contract_name,
                            event_name.clone(),
                            progress,
                            network_contract,
                            database,
                            csv_details,
                            result.to_block,
                        );
                    });
                }
            }

            Ok(())
        }
        Err(e) => {
            error!("Error fetching logs: {:?}", e);
            Err(e)
        }
    }
}

fn build_last_synced_block_number_for_csv(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> String {
    format!(
        "{}/{}/last-synced-blocks/{}-{}-{}.txt",
        get_full_path(project_path, &csv_details.path).display(),
        contract_name,
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase()
    )
}

async fn get_last_synced_block_number_for_csv(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> Result<Option<U64>, CsvError> {
    let file_path = build_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    );
    let path = Path::new(&file_path);

    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    if reader.read_line(&mut line).await? > 0 {
        let value = line.trim();
        let parse = U64::from_dec_str(value);
        return match parse {
            Ok(value) => Ok(Some(value)),
            Err(e) => Err(CsvError::ParseError(value.to_string(), e.to_string())),
        };
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_last_synced_block_number(
    project_path: &PathBuf,
    database: Option<Arc<PostgresClient>>,
    csv_details: &Option<CsvDetails>,
    contract_csv_enabled: bool,
    indexer_name: &str,
    contract_name: &str,
    event_name: &str,
    network: &str,
) -> Option<U64> {
    // check CSV file for last seen block
    if database.is_none() && contract_csv_enabled {
        if let Some(csv_details) = csv_details {
            let result = get_last_synced_block_number_for_csv(
                project_path,
                csv_details,
                contract_name,
                network,
                event_name,
            )
            .await;

            match result {
                Ok(result) => return result,
                Err(e) => {
                    error!("Error fetching last synced block from CSV: {:?}", e);
                }
            }

            return None;
        }
    }

    match database {
        Some(database) => {
            let query = format!(
                "SELECT last_synced_block FROM rindexer_internal.{}_{}_{} WHERE network = $1",
                camel_to_snake(indexer_name),
                camel_to_snake(contract_name),
                camel_to_snake(event_name)
            );

            let row = database.query_one(&query, &[&network]).await;
            match row {
                Ok(row) => {
                    // TODO - UNCOMMENT
                    // let result: Decimal = row.get("last_synced_block");
                    // Some(U64::from_dec_str(&result.to_string()).unwrap())
                    None
                }
                Err(e) => {
                    error!("Error fetching last synced block: {:?}", e);
                    None
                }
            }
        }
        None => None,
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CsvError {
    #[error("File IO error: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("Failed to parse block number: {0} err: {0}")]
    ParseError(String, String),
}

async fn update_last_synced_block_number_for_csv_to_file(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
    to_block: U64,
) -> Result<(), CsvError> {
    let file_path = build_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    );

    let last_block = get_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    )
    .await?;

    if last_block.is_none() || to_block > last_block.unwrap() {
        let temp_file_path = format!("{}.tmp", file_path);

        let mut file = fs::File::create(&temp_file_path).await?;
        file.write_all(to_block.to_string().as_bytes()).await?;
        file.sync_all().await?;

        fs::rename(temp_file_path, file_path).await?;
    }

    Ok(())
}

/// Updates the progress and the last synced block number
#[allow(clippy::too_many_arguments)]
fn update_progress_and_last_synced(
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    event_name: String,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    to_block: U64,
) {
    tokio::spawn(async move {
        progress
            .lock()
            .await
            .update_last_synced_block(&network_contract.id, to_block);

        if let Some(database) = database {
            let result = database
                .execute(
                    &format!(
                        "UPDATE rindexer_internal.{}_{}_{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                        camel_to_snake(&indexer_name),
                        camel_to_snake(&contract_name),
                        camel_to_snake(&event_name)
                    ),
                    &[
                        &EthereumSqlTypeWrapper::U64(to_block),
                        &network_contract.network,
                    ],
                )
                .await;

            if let Err(e) = result {
                error!("Error updating last synced block: {:?}", e);
            }
        } else if let Some(csv_details) = csv_details {
            if let Err(e) = update_last_synced_block_number_for_csv_to_file(
                &project_path,
                &csv_details,
                &contract_name,
                &network_contract.network,
                &event_name,
                to_block,
            )
            .await
            {
                error!("Error updating last synced block to CSV: {:?}", e);
            }
        }
    });
}

#[derive(thiserror::Error, Debug)]
pub enum BuildFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,

    #[error("Topic0 is valid format")]
    Topic0InvalidFormat,
}

fn build_filter(
    topic_id: &str,
    indexing_contract_setup: &IndexingContractSetup,
    current_block: U64,
    next_block: U64,
) -> Result<Filter, BuildFilterError> {
    match indexing_contract_setup {
        IndexingContractSetup::Address(address) => {
            let address = address
                .parse::<Address>()
                .map_err(|_| BuildFilterError::AddressInvalidFormat)?;
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(Filter::new()
                .address(address)
                .topic0(topic0)
                .from_block(current_block)
                .to_block(next_block))
        }
        IndexingContractSetup::Filter(filter) => {
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(filter.extend_filter_indexed(
                Filter::new()
                    .topic0(topic0)
                    .from_block(current_block)
                    .to_block(next_block),
            ))
        }
        IndexingContractSetup::Factory(factory) => {
            let address = factory
                .address
                .parse::<Address>()
                .map_err(|_| BuildFilterError::AddressInvalidFormat)?;
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(Filter::new()
                .address(address)
                .topic0(topic0)
                .from_block(current_block)
                .to_block(next_block))
        }
    }
}
