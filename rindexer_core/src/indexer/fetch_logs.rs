use crate::indexer::progress::IndexingEventProgressStatus;
use crate::indexer::start::ProcessEventError;
use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Block, Filter, JsonRpcError, Log};
use ethers::types::{Address, BlockNumber, Bloom, FilteredParams, ValueOrArray, H256, U64};
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, AcquireError, OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info};

/// Struct to hold the result of retrying with a new block range.
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
/// This function parses the error message to extract a suggested block range for retrying the request.
///
/// # Arguments
///
/// * `error` - The JSON-RPC error received.
/// * `from_block` - The starting block number of the current request.
/// * `to_block` - The ending block number of the current request.
///
/// # Returns
///
/// An `Option<RetryWithBlockRangeResult>` with the new block range to retry, or `None` if no suggestion is found.
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

/// Struct to hold the logs fetched in a block range.
pub struct FetchLogsStream {
    pub logs: Vec<Log>,
    pub from_block: U64,
    pub to_block: U64,
}

/// Struct to hold details for live indexing.
pub struct LiveIndexingDetails {
    pub indexing_distance_from_head: U64,
}

/// Checks if a contract address is present in the logs bloom filter.
///
/// # Arguments
///
/// * `contract_address` - The contract address to check.
/// * `logs_bloom` - The logs bloom filter to match against.
///
/// # Returns
///
/// `true` if the contract address is in the bloom filter, otherwise `false`.
fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let address_filter =
        FilteredParams::address_filter(&Some(ValueOrArray::Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

/// Checks if a topic ID is present in the logs bloom filter.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to check.
/// * `logs_bloom` - The logs bloom filter to match against.
///
/// # Returns
///
/// `true` if the topic ID is in the bloom filter, otherwise `false`.
fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![ValueOrArray::Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

pub fn fetch_logs_stream<M: Middleware + Clone + Send + 'static>(
    provider: Arc<M>,
    topic_id: H256,
    initial_filter: Filter,
    info_log_name: String,
    live_indexing_details: Option<LiveIndexingDetails>,
    semaphore: Arc<Semaphore>,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsStream, Box<<M as Middleware>::Error>>>
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
            let semaphore_client = semaphore.clone();
            let permit = semaphore_client.acquire_owned().await;

            match permit {
                Ok(permit) => {
                    let result = process_historic_logs_stream(
                        &provider,
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
                &provider,
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

async fn process_historic_logs_stream<M: Middleware + Clone + Send + 'static>(
    provider: &Arc<M>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsStream, Box<<M as Middleware>::Error>>>,
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

    match provider.get_logs(&current_filter).await {
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
                .send(Ok(FetchLogsStream {
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
async fn live_indexing_stream<M: Middleware + Clone + Send + 'static>(
    provider: &Arc<M>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsStream, Box<<M as Middleware>::Error>>>,
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

        // TODO cache get latest block on provider to avoid multiple calls with many contracts
        if let Some(latest_block) = provider.get_block(BlockNumber::Latest).await.unwrap() {
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

            let semaphore_client = semaphore.clone();
            let permit = semaphore_client.acquire_owned().await;

            if let Ok(permit) = permit {
                match provider.get_logs(&current_filter).await {
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
                            .send(Ok(FetchLogsStream {
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
///
/// # Arguments
///
/// * `contract_address` - The contract address to check.
/// * `topic_id` - The topic ID to check.
/// * `latest_block` - The latest block containing the logs bloom filter.
///
pub fn is_relevant_block(
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
