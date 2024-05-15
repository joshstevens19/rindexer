use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Filter, JsonRpcError, Log};
use ethers::types::{BlockNumber, U64};
use regex::Regex;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

struct RetryWithBlockRangeResult {
    from: BlockNumber,
    to: BlockNumber,
    range: u64,
}

fn retry_with_block_range(error: &JsonRpcError) -> Option<RetryWithBlockRangeResult> {
    let error_message = &error.message;

    // alchemy - https://github.com/ponder-sh/ponder/blob/main/packages/utils/src/getLogsRetryHelper.ts
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
            range: 10u64,
        });
    }

    None
}

// TODO! can be removed if no use for it as we stream info across
pub fn fetch_logs<M: Middleware + Clone + Send + 'static>(
    provider: Arc<M>,
    filter: Filter,
) -> Pin<Box<dyn Future<Output = Result<Vec<Log>, Box<dyn Error>>> + Send>> {
    async fn inner_fetch_logs<M: Middleware + Clone + Send + 'static>(
        provider: Arc<M>,
        filter: Filter,
    ) -> Result<Vec<Log>, Box<dyn Error>> {
        //println!("Fetching logs for filter: {:?}", filter);
        let logs_result = provider.get_logs(&filter).await;
        match logs_result {
            Ok(logs) => {
                //println!("Fetched logs: {:?}", logs.len());
                Ok(logs)
            }
            Err(err) => {
                println!("Failed to fetch logs: {:?}", err);
                let json_rpc_error = err.as_error_response();
                if let Some(json_rpc_error) = json_rpc_error {
                    let retry_result = retry_with_block_range(json_rpc_error);
                    if let Some(retry_result) = retry_result {
                        let filter = filter
                            .from_block(retry_result.from)
                            .to_block(retry_result.to);
                        println!("Retrying with block range: {}", retry_result.range);
                        let future = Box::pin(inner_fetch_logs(provider.clone(), filter));
                        future.await
                    } else {
                        Err(Box::new(err))
                    }
                } else {
                    Err(Box::new(err))
                }
            }
        }
    }

    Box::pin(inner_fetch_logs(provider, filter))
}

pub struct FetchLogsStream {
    pub logs: Vec<Log>,
    pub from_block: U64,
    pub to_block: U64,
}

pub struct LiveIndexingDetails {
    pub indexing_distance_from_head: U64,
}

pub fn fetch_logs_stream<M: Middleware + Clone + Send + 'static>(
    provider: Arc<M>,
    initial_filter: Filter,
    live_indexing_details: Option<LiveIndexingDetails>,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsStream, Box<<M as Middleware>::Error>>>
       + Send
       + Unpin {
    let (tx, rx) = mpsc::unbounded_channel();
    let live_indexing = live_indexing_details.is_some();
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
                            // switch these around!
                            current_filter = current_filter.from_block(to_block);
                            loop {
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                                let latest_block = provider.get_block_number().await.unwrap();
                                let safe_block_number = latest_block - reorg_safe_distance;

                                // println!("Current block: {:?}", current_block);
                                // println!("Safe block number: {:?}", safe_block_number);
                                // println!("To block: {:?}", to_block);
                                if safe_block_number > to_block {
                                    current_filter = current_filter.to_block(safe_block_number);
                                    break;
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
                        let retry_result = retry_with_block_range(json_rpc_error);
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
