use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Filter, JsonRpcError, Log};
use ethers::types::{BlockNumber, Bloom, FilteredParams, ValueOrArray, H256, U64, Address};
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use ValueOrArray::Value;

struct RetryWithBlockRangeResult {
    from: BlockNumber,
    to: BlockNumber,
}

// credit to https://github.com/ponder-sh/ponder/blob/main/packages/utils/src/getLogsRetryHelper.ts for
// the investigation on the error message
fn retry_with_block_range(
    error: &JsonRpcError,
    from_block: U64,
    to_block: U64,
) -> Option<RetryWithBlockRangeResult> {
    let error_message = &error.message;

    // alchemy
    let re = Regex::new(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]")
        .unwrap();
    if let Some(captures) = re.captures(error_message) {
        let start_block = captures.get(1).unwrap().as_str();
        // println!("start_block: {:?}", start_block);

        let end_block = captures.get(2).unwrap().as_str();
        // println!("end_block: {:?}", end_block);

        // let range = end_block.as_number().unwrap() - start_block.as_number().unwrap();
        // println!("range: {:?}", range);

        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from_str(start_block).unwrap(),
            to: BlockNumber::from_str(end_block).unwrap(),
        });
    }

    // infura, thirdweb, zksync
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

    // ankr
    let re = Regex::new("block range is too wide").unwrap();
    if re.is_match(error_message) && error.code == -32600 {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 3000),
        });
    }

    // quicknode, 1rpc, zkevm, blast
    let re = Regex::new(r"limited to a ([\d,.]+)").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let range_str = captures[1].replace(&['.', ','][..], "");
        let range = U64::from_dec_str(&range_str).unwrap();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + range),
        });
    }

    // blockpi
    let re = Regex::new(r"limited to ([\d,.]+) block").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let range_str = captures[1].replace(&['.', ','][..], "");
        let range = U64::from_dec_str(&range_str).unwrap();
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + range),
        });
    }

    // base
    let re = Regex::new("block range too large").unwrap();
    if re.is_match(error_message) {
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + 2000),
        });
    }

    if to_block > from_block {
        let fallback_range = (to_block - from_block) / 2;
        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from(from_block),
            to: BlockNumber::from(from_block + fallback_range),
        });
    }

    // TODO! work out here if we should panic ??
    None
}

pub struct FetchLogsStream {
    pub logs: Vec<Log>,
    pub from_block: U64,
    pub to_block: U64,
}

pub struct LiveIndexingDetails {
    pub indexing_distance_from_head: U64,
}

/// Checks if a contract address is present in the logs bloom filter.
///
/// This function takes a contract address and a logs bloom filter and checks if the contract
/// address is present in the logs bloom filter. It uses the `FilteredParams::address_filter`
/// method to create an address filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `contract_address` - The contract address to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    // TODO create a issue on reth about this
    let address_filter =
        FilteredParams::address_filter(&Some(Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

/// Checks if a topic ID is present in the logs bloom filter.
///
/// This function takes a topic ID and a logs bloom filter and checks if the topic ID is present
/// in the logs bloom filter. It uses the `FilteredParams::topics_filter` method to create a
/// topics filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

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
            // when hits head lets make sure no overlap
            let from_block = current_filter.get_from_block().unwrap();
            let to_block = current_filter.get_to_block().unwrap();
            if from_block > to_block {
                current_filter = current_filter.from_block(to_block);
            }
            // println!("Fetching logs for filter: {:?}", current_filter);
            match provider.get_logs(&current_filter).await {
                Ok(logs) => {
                    // println!(
                    //     "Fetched logs: {} - filter: {:?}",
                    //     logs.len(),
                    //     current_filter
                    // );
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
                            // println!("Waiting for more logs..");
                            // always +1 onto the next one
                            current_filter = current_filter.from_block(to_block + 1);
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                                let latest_block =
                                    provider.get_block(BlockNumber::Latest).await.unwrap();
                                match latest_block {
                                    Some(latest_block) => {
                                        let safe_block_number =
                                            latest_block.number.unwrap() - reorg_safe_distance;

                                        // check blooms
                                        if safe_block_number == to_block {
                                            match &contract_address {
                                                Some(Value(contract_address)) if !contract_in_bloom(*contract_address, latest_block.logs_bloom.unwrap()) => {
                                                    continue;
                                                }
                                                _ => {}
                                            }
                                            if !topic_in_bloom(topic_id, latest_block.logs_bloom.unwrap()) {
                                                continue;
                                            }
                                            break;
                                        }

                                        // println!("Current block: {:?}", current_block);
                                        // println!("Safe block number: {:?}", safe_block_number);
                                        // println!("To block: {:?}", to_block);
                                        if safe_block_number >= to_block {
                                            current_filter =
                                                current_filter.to_block(safe_block_number);
                                            break;
                                        }
                                    }
                                    None => {
                                        continue;
                                    }
                                }
                                // println!("Waiting for block number to reach a safe distance. Current safe block: {:?}", safe_block_number);
                            }
                            continue;
                        } else {
                            break;
                        }
                        // println!("All logs fetched!");
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
                        // TODO! we should not skip a block as we might miss logs in the same block
                        let next_block = last_log.block_number.unwrap() + U64::from(1);
                        current_filter = current_filter
                            .from_block(next_block)
                            .to_block(snapshot_to_block);
                        // println!("Updated filter: {:?}", current_filter);
                    }
                }
                Err(err) => {
                    // println!("Failed to fetch logs: {:?}", err);
                    let json_rpc_error = err.as_error_response();
                    if let Some(json_rpc_error) = json_rpc_error {
                        let retry_result =
                            retry_with_block_range(json_rpc_error, from_block, to_block);
                        if let Some(retry_result) = retry_result {
                            current_filter = current_filter
                                .from_block(retry_result.from)
                                .to_block(retry_result.to);
                            // println!("Retrying with block range: {:?}", current_filter);
                            continue;
                        }
                    }

                    if live_indexing {
                        // println!("Error fetching logs: retry in 500ms {:?}", err);
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        continue;
                    }
                    // eprintln!("Error fetching logs: exiting... {:?}", err);
                    let _ = tx.send(Err(Box::new(err)));
                    break;
                }
            }
        }
    });

    UnboundedReceiverStream::new(rx)
}
