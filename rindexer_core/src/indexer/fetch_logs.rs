use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Block, Filter, JsonRpcError, Log};
use ethers::types::{Address, BlockNumber, Bloom, FilteredParams, ValueOrArray, H256, U64};
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Struct to hold the result of retrying with a new block range.
#[derive(Debug)]
struct RetryWithBlockRangeResult {
    from: BlockNumber,
    to: BlockNumber,
}

/// Attempts to retry with a new block range based on the error message.
///
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

    // Alchemy
    let re = Regex::new(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]")
        .unwrap();
    if let Some(captures) = re.captures(error_message) {
        let start_block = captures.get(1).unwrap().as_str();
        let end_block = captures.get(2).unwrap().as_str();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from_str(start_block).unwrap(),
            to: BlockNumber::from_str(end_block).unwrap(),
        });
    }

    // Infura, Thirdweb, zkSync
    let re =
        Regex::new(r"Try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let start_block = captures.get(1).unwrap().as_str();
        let end_block = captures.get(2).unwrap().as_str();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from_str(start_block).unwrap(),
            to: BlockNumber::from_str(end_block).unwrap(),
        });
    }

    // Ankr
    if error_message.contains("block range is too wide") && error.code == -32600 {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 3000),
        });
    }

    // QuickNode, 1RPC, zkEVM, Blast
    let re = Regex::new(r"limited to a ([\d,.]+)").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let range_str = captures[1].replace(&['.', ','][..], "");
        let range = U64::from_dec_str(&range_str).unwrap();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + range),
        });
    }

    // BlockPI
    let re = Regex::new(r"limited to ([\d,.]+) block").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let range_str = captures[1].replace(&['.', ','][..], "");
        let range = U64::from_dec_str(&range_str).unwrap();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + range),
        });
    }

    // Base
    if error_message.contains("block range too large") {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 2000),
        });
    }

    // Fallback range
    if to_block > from_block {
        let fallback_range = (to_block - from_block) / 2;
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + fallback_range),
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

/// Fetches logs from the Ethereum blockchain, starting with historical logs and transitioning to live indexing mode if enabled.
///
/// # Arguments
///
/// * `provider` - The provider to interact with the Ethereum blockchain.
/// * `topic_id` - The topic ID to filter logs by.
/// * `initial_filter` - The initial filter specifying the block range and other parameters.
/// * `live_indexing_details` - Optional details for live indexing.
///
/// # Returns
///
/// A stream of fetched logs, wrapped in a `Result` to handle any errors.
pub fn fetch_logs_stream<M: Middleware + Clone + Send + 'static>(
    provider: Arc<M>,
    topic_id: H256,
    initial_filter: Filter,
    live_indexing_details: Option<LiveIndexingDetails>,
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
        while current_filter.get_from_block().unwrap() <= snapshot_to_block {
            let result = process_historic_logs(
                &provider,
                &tx,
                &topic_id,
                current_filter.clone(),
                snapshot_to_block,
            )
            .await;

            if let Some(new_filter) = result {
                current_filter = new_filter;
            } else {
                break;
            }
        }

        // Live indexing mode
        if live_indexing {
            live_indexing_mode(
                &provider,
                &tx,
                &contract_address,
                &topic_id,
                reorg_safe_distance,
                current_filter,
            )
            .await;
        }
    });

    UnboundedReceiverStream::new(rx)
}

/// Processes logs within the specified historical range, fetches logs using the provider,
/// and sends them to the provided channel. Updates the filter to move to the next block range.
///
/// # Arguments
///
/// * `provider` - The provider to interact with the Ethereum blockchain.
/// * `tx` - The channel to send fetched logs to.
/// * `topic_id` - The topic ID to filter logs by.
/// * `current_filter` - The current filter specifying the block range and other parameters.
/// * `snapshot_to_block` - The upper block limit for processing historical logs.
///
/// # Returns
///
/// An updated filter for further processing, or `None` if processing should stop.
async fn process_historic_logs<M: Middleware + Clone + Send + 'static>(
    provider: &Arc<M>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsStream, Box<<M as Middleware>::Error>>>,
    topic_id: &H256,
    current_filter: Filter,
    snapshot_to_block: U64,
) -> Option<Filter> {
    let from_block = current_filter.get_from_block().unwrap();
    let to_block = current_filter.get_to_block().unwrap_or(snapshot_to_block);
    println!("from_block {:?} to_block {:?}", from_block, to_block);

    if from_block > to_block {
        println!("from_block {:?} > to_block {:?}", from_block, to_block);
        return Some(current_filter.from_block(to_block));
    }

    println!("Processing filter: {:?}", current_filter);

    match provider.get_logs(&current_filter).await {
        Ok(logs) => {
            println!(
                "topic_id {}, Logs: {} from {} to {}",
                topic_id,
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
                println!("Failed to send logs to stream consumer!");
                return None;
            }

            if logs.is_empty() {
                let new_from_block = to_block + 1;
                let new_filter = current_filter
                    .from_block(new_from_block)
                    .to_block(snapshot_to_block);
                println!(
                    "new_from_block {:?} snapshot_to_block {:?}",
                    new_from_block, snapshot_to_block
                );
                return if new_from_block > snapshot_to_block {
                    None
                } else {
                    Some(new_filter)
                };
            }

            if let Some(last_log) = logs.last() {
                let next_block = last_log.block_number.unwrap() + U64::from(1);
                println!("next_block {:?}", next_block);
                return Some(
                    current_filter
                        .from_block(next_block)
                        .to_block(snapshot_to_block),
                );
            }
        }
        Err(err) => {
            println!("Error fetching logs: {}", err);

            if let Some(json_rpc_error) = err.as_error_response() {
                if let Some(retry_result) =
                    retry_with_block_range(json_rpc_error, from_block, to_block)
                {
                    println!("Retrying with block range: {:?}", retry_result);
                    return Some(
                        current_filter
                            .from_block(retry_result.from)
                            .to_block(retry_result.to),
                    );
                }
            }

            let _ = tx.send(Err(Box::new(err)));
            return None;
        }
    }

    None
}

/// Handles live indexing mode, continuously checking for new blocks, ensuring they are
/// within a safe range, updating the filter, and sending the logs to the provided channel.
///
/// # Arguments
///
/// * `provider` - The provider to interact with the Ethereum blockchain.
/// * `tx` - The channel to send fetched logs to.
/// * `contract_address` - The contract address to filter logs by.
/// * `topic_id` - The topic ID to filter logs by.
/// * `reorg_safe_distance` - The safe distance from the latest block to avoid reorgs.
/// * `current_filter` - The current filter specifying the block range and other parameters.
async fn live_indexing_mode<M: Middleware + Clone + Send + 'static>(
    provider: &Arc<M>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsStream, Box<<M as Middleware>::Error>>>,
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    reorg_safe_distance: U64,
    mut current_filter: Filter,
) {
    let mut last_seen_block = U64::from(0);
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        if let Some(latest_block) = provider.get_block(BlockNumber::Latest).await.unwrap() {
            if last_seen_block == latest_block.number.unwrap() {
                println!("No new blocks to process...");
                continue;
            }
            println!("Latest block: {:?}", latest_block.number.unwrap());
            println!("Last seen block: {:?}", last_seen_block);
            let safe_block_number = latest_block.number.unwrap() - reorg_safe_distance;

            let from_block = current_filter.get_from_block().unwrap();
            // check reorg distance and skip if not safe
            if from_block > safe_block_number {
                println!(
                    "safe_block_number is not safe range yet {:?} > {:?}",
                    from_block, safe_block_number
                );
                continue;
            }

            let to_block = safe_block_number;

            if from_block == to_block
                && !is_relevant_block(contract_address, topic_id, &latest_block)
            {
                println!("Skipping block {} as it's not relevant", from_block);
                current_filter = current_filter.from_block(to_block + 1);
                last_seen_block = to_block;
                continue;
            }

            current_filter = current_filter.to_block(to_block);

            println!("Processing live filter: {:?}", current_filter);

            match provider.get_logs(&current_filter).await {
                Ok(logs) => {
                    println!(
                        "Live topic_id {}, Logs: {} from {} to {}",
                        topic_id,
                        logs.len(),
                        from_block,
                        to_block
                    );

                    last_seen_block = to_block;

                    if tx
                        .send(Ok(FetchLogsStream {
                            logs: logs.clone(),
                            from_block,
                            to_block,
                        }))
                        .is_err()
                    {
                        println!("Failed to send logs to stream consumer!");
                        break;
                    }

                    if logs.is_empty() {
                        current_filter = current_filter.from_block(to_block + 1);
                    } else if let Some(last_log) = logs.last() {
                        current_filter = current_filter
                            .from_block(last_log.block_number.unwrap() + U64::from(1));
                    }

                    println!(
                        "current_filter from_block {:?}",
                        current_filter.get_from_block().unwrap()
                    );
                }
                Err(err) => {
                    println!("Error fetching logs: {}", err);
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
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
/// # Returns
///
/// `true` if the block contains relevant logs, otherwise `false`.
fn is_relevant_block(
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    latest_block: &Block<H256>,
) -> bool {
    if let Some(contract_address) = contract_address {
        match contract_address {
            ValueOrArray::Value(address) => {
                if !contract_in_bloom(*address, latest_block.logs_bloom.unwrap()) {
                    return false;
                }
            }
            ValueOrArray::Array(addresses) => {
                if addresses
                    .iter()
                    .all(|addr| !contract_in_bloom(*addr, latest_block.logs_bloom.unwrap()))
                {
                    return false;
                }
            }
        }
    }

    if !topic_in_bloom(*topic_id, latest_block.logs_bloom.unwrap()) {
        return false;
    }

    true
}
