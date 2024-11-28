use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

use ethers::{
    addressbook::Address,
    middleware::MiddlewareError,
    prelude::{BlockNumber, JsonRpcError, ValueOrArray, H256, U64},
};
use regex::Regex;
use tokio::{
    sync::{mpsc, Semaphore},
    time::Instant,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{log_helpers::is_relevant_block, IndexingEventProgressStatus},
    provider::{JsonRpcCachedProvider, WrappedLog},
};

pub struct FetchLogsResult {
    pub logs: Vec<WrappedLog>,
    pub from_block: U64,
    pub to_block: U64,
}

pub fn fetch_logs_stream(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsResult, Box<dyn Error + Send>>> + Send + Unpin
{
    let (tx, rx) = mpsc::unbounded_channel();

    let initial_filter = config.to_event_filter().unwrap();
    let contract_address = initial_filter.contract_address();

    tokio::spawn(async move {
        let snapshot_to_block = initial_filter.get_to_block();
        let from_block = initial_filter.get_from_block();
        let mut current_filter = initial_filter;

        // add any max block range limitation before we start processing
        let mut max_block_range_limitation =
            config.network_contract.cached_provider.max_block_range;
        if max_block_range_limitation.is_some() {
            current_filter = current_filter.set_to_block(calculate_process_historic_log_to_block(
                &from_block,
                &snapshot_to_block,
                &max_block_range_limitation,
            ));
            warn!(
                "{} - {} - max block range limitation of {} blocks applied - block range indexing will be slower then RPC providers supplying the optimal ranges - https://rindexer.xyz/docs/references/rpc-node-providers#rpc-node-providers",
                config.info_log_name,
                IndexingEventProgressStatus::Syncing.log(),
                max_block_range_limitation.unwrap()
            );
        }
        while current_filter.get_from_block() <= snapshot_to_block {
            let semaphore_client = Arc::clone(&config.semaphore);
            let permit = semaphore_client.acquire_owned().await;

            match permit {
                Ok(permit) => {
                    let result = fetch_historic_logs_stream(
                        &config.network_contract.cached_provider,
                        &tx,
                        &config.topic_id,
                        current_filter.clone(),
                        max_block_range_limitation,
                        snapshot_to_block,
                        &config.info_log_name,
                    )
                    .await;

                    drop(permit);

                    // slow indexing warn user
                    if let Some(range) = max_block_range_limitation {
                        warn!(
                            "{} - RPC PROVIDER IS SLOW - Slow indexing mode enabled, max block range limitation: {} blocks - we advise using a faster provider who can predict the next block ranges.",
                            &config.info_log_name,
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
                        &config.info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        e
                    );
                    continue;
                }
            }
        }

        info!(
            "{} - {} - Finished indexing historic events",
            &config.info_log_name,
            IndexingEventProgressStatus::Completed.log()
        );

        // Live indexing mode
        if config.live_indexing && !force_no_live_indexing {
            live_indexing_stream(
                &config.network_contract.cached_provider,
                &tx,
                &contract_address,
                &config.topic_id,
                &config.indexing_distance_from_head,
                current_filter,
                &config.info_log_name,
                &config.semaphore,
                config.network_contract.disable_logs_bloom_checks,
            )
            .await;
        }
    });

    UnboundedReceiverStream::new(rx)
}

struct ProcessHistoricLogsStreamResult {
    pub next: RindexerEventFilter,
    pub max_block_range_limitation: Option<U64>,
}

async fn fetch_historic_logs_stream(
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    topic_id: &H256,
    current_filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
) -> Option<ProcessHistoricLogsStreamResult> {
    let from_block = current_filter.get_from_block();
    let to_block = current_filter.get_to_block();
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
            next: current_filter.set_from_block(to_block),
            max_block_range_limitation,
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

            let logs_empty = logs.is_empty();
            // clone here over the full logs way less overhead
            let last_log = logs.last().cloned();

            if tx.send(Ok(FetchLogsResult { logs, from_block, to_block })).is_err() {
                error!(
                    "{} - {} - Failed to send logs to stream consumer!",
                    IndexingEventProgressStatus::Syncing.log(),
                    info_log_name
                );
                return None;
            }

            if logs_empty {
                info!(
                    "{} - No events found between blocks {} - {}",
                    info_log_name, from_block, to_block
                );
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
                            .set_from_block(next_from_block)
                            .set_to_block(new_to_block),
                        max_block_range_limitation,
                    })
                };
            }

            if let Some(last_log) = last_log {
                let next_from_block = last_log
                    .inner
                    .block_number
                    .expect("block number should always be present in a log") +
                    U64::from(1);
                debug!(
                    "{} - {} - next_block {:?}",
                    info_log_name,
                    IndexingEventProgressStatus::Syncing.log(),
                    next_from_block
                );
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
                            .set_from_block(next_from_block)
                            .set_to_block(new_to_block),
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
                            .set_from_block(retry_result.from)
                            .set_to_block(retry_result.to),
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
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    reorg_safe_distance: &U64,
    mut current_filter: RindexerEventFilter,
    info_log_name: &str,
    semaphore: &Arc<Semaphore>,
    disable_logs_bloom_checks: bool,
) {
    let mut last_seen_block_number = U64::from(0);

    // this is used for less busy chains to make sure they know rindexer is still alive
    let mut last_no_new_block_log_time = Instant::now();
    let log_no_new_block_interval = Duration::from_secs(300);

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let latest_block = cached_provider.get_latest_block().await;
        match latest_block {
            Ok(latest_block) => {
                if let Some(latest_block) = latest_block {
                    if let Some(latest_block_number) = latest_block.number {
                        if last_seen_block_number == latest_block_number {
                            debug!(
                                "{} - {} - No new blocks to process...",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log()
                            );
                            if last_no_new_block_log_time.elapsed() >= log_no_new_block_interval {
                                info!(
                                    "{} - {} - No new blocks published in the last 5 minutes - latest block number {}",
                                    info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    last_seen_block_number,
                                );
                                last_no_new_block_log_time = Instant::now();
                            }
                            continue;
                        }
                        debug!(
                            "{} - {} - New block seen {} - Last seen block {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            last_seen_block_number
                        );

                        let safe_block_number = latest_block_number - reorg_safe_distance;
                        let from_block = current_filter.get_from_block();
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
                        if from_block == to_block &&
                            !disable_logs_bloom_checks &&
                            !is_relevant_block(contract_address, topic_id, &latest_block)
                        {
                            debug!(
                                "{} - {} - Skipping block {} as it's not relevant",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                from_block
                            );
                            debug!(
                                "{} - {} - Did not need to hit RPC as no events in {} block - LogsBloom for block checked",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                from_block
                            );
                            current_filter = current_filter.set_from_block(to_block + 1);
                            last_seen_block_number = to_block;
                            continue;
                        }

                        current_filter = current_filter.set_to_block(to_block);

                        debug!(
                            "{} - {} - Processing live filter: {:?}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            current_filter
                        );

                        let semaphore_client = Arc::clone(semaphore);
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

                                    let logs_empty = logs.is_empty();
                                    // clone here over the full logs way less overhead
                                    let last_log = logs.last().cloned();

                                    if tx
                                        .send(Ok(FetchLogsResult { logs, from_block, to_block }))
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

                                    if logs_empty {
                                        current_filter =
                                            current_filter.set_from_block(to_block + 1);
                                        info!(
                                            "{} - {} - No events found between blocks {} - {}",
                                            info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            from_block,
                                            to_block
                                        );
                                    } else if let Some(last_log) = last_log {
                                        if let Some(last_log_block_number) =
                                            last_log.inner.block_number
                                        {
                                            current_filter = current_filter.set_from_block(
                                                last_log_block_number + U64::from(1),
                                            );
                                        } else {
                                            error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                        }
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
                    } else {
                        info!("WARNING - empty latest block returned from provider, will try again in 200ms");
                    }
                } else {
                    info!("WARNING - empty latest block returned from provider, will try again in 200ms");
                }
            }
            Err(e) => {
                error!(
                    "Error getting latest block, will try again in 1 seconds - err: {}",
                    e.to_string()
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
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
    let error_data_binding = error.data.as_ref().map(|data| data.to_string());
    let empty_string = String::from("");
    let error_data = match &error_data_binding {
        Some(data) => data,
        None => &empty_string,
    };

    fn compile_regex(pattern: &str) -> Result<Regex, regex::Error> {
        Regex::new(pattern)
    }

    // Thanks Ponder for the regex patterns - https://github.com/ponder-sh/ponder/blob/889096a3ef5f54a0c5a06df82b0da9cf9a113996/packages/utils/src/getLogsRetryHelper.ts#L34

    // Alchemy
    if let Ok(re) =
        compile_regex(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]")
    {
        if let Some(captures) = re.captures(error_message).or_else(|| re.captures(error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = start_block.as_str();
                let end_block_str = end_block.as_str();
                if let (Ok(from), Ok(to)) =
                    (BlockNumber::from_str(start_block_str), BlockNumber::from_str(end_block_str))
                {
                    return Some(RetryWithBlockRangeResult { from, to, max_block_range: None });
                }
            }
        }
    }

    // Infura, Thirdweb, zkSync, Tenderly
    if let Ok(re) =
        compile_regex(r"Try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]")
    {
        if let Some(captures) = re.captures(error_message).or_else(|| {
            let blah = re.captures(error_data);
            blah
        }) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = format!("0x{}", start_block.as_str());
                let end_block_str = format!("0x{}", end_block.as_str());
                if let (Ok(from), Ok(to)) =
                    (BlockNumber::from_str(&start_block_str), BlockNumber::from_str(&end_block_str))
                {
                    return Some(RetryWithBlockRangeResult { from, to, max_block_range: None });
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
        if let Some(captures) = re.captures(error_message).or_else(|| re.captures(error_data)) {
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
