use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Filter, JsonRpcError, Log};
use ethers::types::{Address, BlockNumber, Bloom, FilteredParams, ValueOrArray, H256, U64};
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use ValueOrArray::Value;

/// Struct to hold the result of retrying with a new block range.
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
    let address_filter = FilteredParams::address_filter(&Some(Value(contract_address)));
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
    let topic_filter = FilteredParams::topics_filter(&Some(vec![Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

/// Fetches logs in a stream, retrying with smaller ranges if necessary.
///
/// # Arguments
///
/// * `provider` - The middleware provider.
/// * `topic_id` - The topic ID to filter logs.
/// * `initial_filter` - The initial filter to fetch logs.
/// * `live_indexing_details` - Optional details for live indexing.
///
/// # Returns
///
/// A stream of fetched logs.
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
    let reorg_safe_distance = live_indexing_details
        .unwrap_or(LiveIndexingDetails {
            indexing_distance_from_head: U64::from(0),
        })
        .indexing_distance_from_head;

    tokio::spawn(async move {
        let snapshot_to_block = initial_filter.clone().get_to_block().unwrap();
        let mut current_filter = initial_filter;

        loop {
            let from_block = current_filter.get_from_block().unwrap();
            let to_block = current_filter.get_to_block().unwrap();
            if from_block > to_block {
                current_filter = current_filter.from_block(to_block);
            }

            match provider.get_logs(&current_filter).await {
                Ok(logs) => {
                    if logs.is_empty() {
                        if tx
                            .send(Ok(FetchLogsStream {
                                logs: vec![],
                                from_block,
                                to_block,
                            }))
                            .is_err()
                        {
                            println!("Failed to send logs to stream consumer!");
                            break;
                        }
                        if live_indexing {
                            current_filter = current_filter.from_block(to_block + 1);
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                                let latest_block =
                                    provider.get_block(BlockNumber::Latest).await.unwrap();
                                match latest_block {
                                    Some(latest_block) => {
                                        let safe_block_number =
                                            latest_block.number.unwrap() - reorg_safe_distance;

                                        if safe_block_number == to_block {
                                            if let Some(Value(contract_address)) = &contract_address
                                            {
                                                if !contract_in_bloom(
                                                    *contract_address,
                                                    latest_block.logs_bloom.unwrap(),
                                                ) {
                                                    continue;
                                                }
                                            }
                                            if !topic_in_bloom(
                                                topic_id,
                                                latest_block.logs_bloom.unwrap(),
                                            ) {
                                                continue;
                                            }
                                            break;
                                        }

                                        if safe_block_number >= to_block {
                                            current_filter =
                                                current_filter.to_block(safe_block_number);
                                            break;
                                        }
                                    }
                                    None => continue,
                                }
                            }
                            continue;
                        } else {
                            break;
                        }
                    }

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

                    if let Some(last_log) = logs.last() {
                        let next_block = last_log.block_number.unwrap() + U64::from(1);
                        current_filter = current_filter
                            .from_block(next_block)
                            .to_block(snapshot_to_block);
                    }
                }
                Err(err) => {
                    if let Some(json_rpc_error) = err.as_error_response() {
                        if let Some(retry_result) =
                            retry_with_block_range(json_rpc_error, from_block, to_block)
                        {
                            current_filter = current_filter
                                .from_block(retry_result.from)
                                .to_block(retry_result.to);
                            continue;
                        }
                    }

                    if live_indexing {
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        continue;
                    }
                    let _ = tx.send(Err(Box::new(err)));
                    break;
                }
            }
        }
    });

    UnboundedReceiverStream::new(rx)
}
