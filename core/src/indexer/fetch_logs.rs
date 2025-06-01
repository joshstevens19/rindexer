use std::{error::Error, str::FromStr, sync::Arc, time::Duration};

use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{
        log_helpers::{halved_block_number, is_relevant_block},
        IndexingEventProgressStatus,
    },
    provider::{JsonRpcCachedProvider, ProviderError},
};
use alloy::{
    primitives::{Address, BlockNumber, B256, U64},
    rpc::types::{Log, ValueOrArray},
};
use rand::random_ratio;
use regex::Regex;
use tokio::{
    sync::{mpsc, Semaphore},
    time::Instant,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{
        IndexingEventProgressStatus,
    },
    provider::{JsonRpcCachedProvider, ProviderError},
};
use crate::helpers::{halved_block_number, is_relevant_block};

pub struct FetchLogsResult {
    pub logs: Vec<Log>,
    pub from_block: U64,
    pub to_block: U64,
}

pub fn fetch_logs_stream(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsResult, Box<dyn Error + Send>>> + Send + Unpin
{
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut current_filter = config.to_event_filter().unwrap();

        let snapshot_to_block = current_filter.get_to_block();
        let from_block = current_filter.get_from_block();

        // add any max block range limitation before we start processing
        let mut max_block_range_limitation =
            config.network_contract().cached_provider.max_block_range;
        if max_block_range_limitation.is_some() {
            current_filter = current_filter.set_to_block(calculate_process_historic_log_to_block(
                &from_block,
                &snapshot_to_block,
                &max_block_range_limitation,
            ));
            warn!(
                "{}::{} - {} - max block range limitation of {} blocks applied - block range indexing will be slower then RPC providers supplying the optimal ranges - https://rindexer.xyz/docs/references/rpc-node-providers#rpc-node-providers",
                config.info_log_name(),
                config.network_contract.network,
                IndexingEventProgressStatus::Syncing.log(),
                max_block_range_limitation.unwrap()
            );
        }
        while current_filter.get_from_block() <= snapshot_to_block {
            let semaphore_client = Arc::clone(&config.semaphore());
            let permit = semaphore_client.acquire_owned().await;

            match permit {
                Ok(permit) => {
                    let result = fetch_historic_logs_stream(
                        &config.network_contract().cached_provider,
                        &tx,
                        &config.topic_id(),
                        current_filter.clone(),
                        max_block_range_limitation,
                        snapshot_to_block,
                        &config.info_log_name(),
                        &config.network_contract().network,
                    )
                    .await;

                    drop(permit);

                    // This check can be very noisy. We want to only sample this warning to notify
                    // the user, rather than warn on every log fetch.
                    if let Some(range) = max_block_range_limitation {
                        if random_ratio(1, 50) {
                            warn!(
                                "{}::{} - RPC PROVIDER IS SLOW - Slow indexing mode enabled, max block range limitation: {} blocks - we advise using a faster provider who can predict the next block ranges.",
                                &config.info_log_name(),
                                &config.network_contract.network,
                                range
                            );
                        }
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
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Syncing.log(),
                        e
                    );
                    continue;
                }
            }
        }

        info!(
            "{} - {} - Finished indexing historic events",
            &config.info_log_name(),
            IndexingEventProgressStatus::Completed.log()
        );

        // Live indexing mode
        if config.live_indexing() && !force_no_live_indexing {
            live_indexing_stream(
                &config.network_contract().cached_provider,
                &tx,
                snapshot_to_block,
                &config.topic_id(),
                &config.indexing_distance_from_head(),
                current_filter,
                &config.info_log_name(),
                &config.semaphore(),
                config.network_contract().disable_logs_bloom_checks,
                &config.network_contract().network,
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

#[allow(clippy::too_many_arguments)]
async fn fetch_historic_logs_stream(
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    topic_id: &B256,
    current_filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
    network: &str,
) -> Option<ProcessHistoricLogsStreamResult> {
    let from_block = current_filter.get_from_block();
    let to_block = current_filter.get_to_block();

    debug!(
        "{}::{} - {} - Process historic events - blocks: {} - {}",
        info_log_name,
        network,
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
            next: current_filter.set_from_block(to_block).set_to_block(to_block + U64::from(1)),
            max_block_range_limitation,
        });
    }

    // debug!(
    //     "{} - {} - Processing filter: {:?}",
    //     info_log_name,
    //     IndexingEventProgressStatus::Syncing.log(),
    //     current_filter
    // );

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
                    "{} - {} - {} - Failed to send logs to stream consumer!",
                    IndexingEventProgressStatus::Syncing.log(),
                    network,
                    info_log_name
                );
                return None;
            }

            if logs_empty {
                info!(
                    "{}::{} - No events found between blocks {} - {}",
                    info_log_name, network, from_block, to_block,
                );
                let next_from_block = to_block + U64::from(1);
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
                let next_from_block = U64::from(
                    last_log.block_number.expect("block number should always be present in a log")
                        + 1,
                );
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
            if let Some(retry_result) = retry_with_block_range(&err, from_block, to_block) {
                warn!(
                    "{}::{} - {} - Overfetched from {} to {} - shrinking to block range: {:?}",
                    info_log_name,
                    network,
                    IndexingEventProgressStatus::Syncing.log(),
                    from_block,
                    to_block,
                    retry_result
                );

                return Some(ProcessHistoricLogsStreamResult {
                    next: current_filter
                        .set_from_block(U64::from(retry_result.from))
                        .set_to_block(U64::from(retry_result.to)),
                    max_block_range_limitation: retry_result.max_block_range,
                });
            }

            let halved_to_block = halved_block_number(to_block, from_block);

            // Handle deserialization, networking, and other non-rpc related errors.
            error!(
                "[{}] - {} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                network,
                info_log_name,
                IndexingEventProgressStatus::Syncing.log(),
                from_block,
                to_block,
                from_block,
                halved_to_block,
                err
            );

            return Some(ProcessHistoricLogsStreamResult {
                next: current_filter.set_from_block(from_block).set_to_block(halved_to_block),
                max_block_range_limitation,
            });
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
    last_seen_block_number: U64,
    topic_id: &B256,
    reorg_safe_distance: &U64,
    mut current_filter: RindexerEventFilter,
    info_log_name: &str,
    semaphore: &Arc<Semaphore>,
    disable_logs_bloom_checks: bool,
    network: &str,
) {
    let mut last_seen_block_number = last_seen_block_number;
    let mut log_response_to_large_to_block: Option<U64> = None;
    let mut last_no_new_block_log_time = Instant::now();
    let log_no_new_block_interval = Duration::from_secs(300);
    let target_iteration_duration = Duration::from_millis(200);

    loop {
        let iteration_start = Instant::now();

        let latest_block = cached_provider.get_latest_block().await;
        match latest_block {
            Ok(latest_block) => {
                if let Some(latest_block) = latest_block {
                    let to_block_number = log_response_to_large_to_block
                        .unwrap_or(U64::from(latest_block.header.number));

                    if last_seen_block_number == to_block_number {
                        debug!(
                            "{} - {} - No new blocks to process...",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log()
                        );
                        if last_no_new_block_log_time.elapsed() >= log_no_new_block_interval {
                            info!(
                                    "{}::{} - {} - No new blocks published in the last 5 minutes - latest block number {}",
                                    info_log_name,
                                    network,
                                    IndexingEventProgressStatus::Live.log(),
                                    last_seen_block_number,
                                );
                            last_no_new_block_log_time = Instant::now();
                        }
                    } else {
                        debug!(
                            "{} - {} - New block seen {} - Last seen block {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            to_block_number,
                            last_seen_block_number
                        );

                        let safe_block_number = to_block_number - reorg_safe_distance;
                        let from_block = current_filter.get_from_block();
                        if from_block > safe_block_number {
                            info!(
                                "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                                info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                from_block,
                                safe_block_number
                            );
                        } else {
                            let contract_address = current_filter.contract_address().await;

                            let to_block = safe_block_number;
                            if from_block == to_block
                                && !disable_logs_bloom_checks
                                && !is_relevant_block(&contract_address, topic_id, &latest_block)
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
                                current_filter =
                                    current_filter.set_from_block(to_block + U64::from(1));
                                last_seen_block_number = to_block;
                            } else {
                                current_filter = current_filter.set_to_block(to_block);

                                // debug!(
                                //     "{} - {} - Processing live filter: {:?}",
                                //     info_log_name,
                                //     IndexingEventProgressStatus::Live.log(),
                                //     current_filter
                                // );

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
                                            let last_log = logs.last().cloned();

                                            if let Err(e) = tx.send(Ok(FetchLogsResult {
                                                logs,
                                                from_block,
                                                to_block,
                                            })) {
                                                error!(
                                                        "{}::{} - {} - Failed to send logs to stream consumer! Err: {}",
                                                        info_log_name,
                                                        network,
                                                        IndexingEventProgressStatus::Live.log(),
                                                        e
                                                    );
                                                drop(permit);
                                                break;
                                            }

                                            if logs_empty {
                                                current_filter = current_filter
                                                    .set_from_block(to_block + U64::from(1));
                                                info!(
                                                    "{}::{} - {} - No events found between blocks {} - {}",
                                                    info_log_name,
                                                    network,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    from_block,
                                                    to_block,
                                                );
                                            } else if let Some(last_log) = last_log {
                                                if let Some(last_log_block_number) =
                                                    last_log.block_number
                                                {
                                                    current_filter = current_filter.set_from_block(
                                                        U64::from(last_log_block_number + 1),
                                                    );
                                                } else {
                                                    error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                                }
                                            }

                                            log_response_to_large_to_block = None;

                                            drop(permit);
                                        }
                                        Err(err) => {
                                            if let Some(retry_result) =
                                                retry_with_block_range(&err, from_block, to_block)
                                            {
                                                warn!(
                                                    "{}::{} - {} - Overfetched from {} to {} - shrinking to block range: from {} to {}",
                                                    info_log_name,
                                                    network,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    retry_result.to
                                                    );

                                                log_response_to_large_to_block =
                                                    Some(retry_result.to);
                                            } else {
                                                let halved_to_block =
                                                    halved_block_number(to_block, from_block);

                                                error!(
                                                    "{}::{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                                                    info_log_name,
                                                    network,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    halved_to_block,
                                                    err
                                                );

                                                log_response_to_large_to_block =
                                                    Some(halved_to_block);
                                            }

                                            drop(permit);
                                        }
                                    }
                                }
                            }
                        }
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
                continue;
            }
        }

        let elapsed = iteration_start.elapsed();
        if elapsed < target_iteration_duration {
            tokio::time::sleep(target_iteration_duration - elapsed).await;
        }
    }
}

#[derive(Debug)]
struct RetryWithBlockRangeResult {
    from: U64,
    to: U64,
    // This is only populated if you are using an RPC provider
    // who doesn't give block ranges, this tends to be providers
    // which are a lot slower than others, expect these providers
    // to be slow
    max_block_range: Option<U64>,
}

/// Attempts to retry with a new block range based on the error message.
fn retry_with_block_range(
    error: &ProviderError,
    from_block: U64,
    to_block: U64,
) -> Option<RetryWithBlockRangeResult> {
    let error_struct = match error {
        ProviderError::RequestFailed(json_rpc_err) => json_rpc_err.as_error_resp(),
        _ => None,
    };

    let (error_message, error_data) = if let Some(error) = error_struct {
        let error_message = error.message.to_string();
        let error_data_binding = error.data.as_ref().map(|data| data.to_string());
        let empty_string = String::from("");
        let error_data = error_data_binding.unwrap_or(empty_string);

        (error_message, error_data)
    } else {
        let str_err = error.to_string();
        debug!("Failed to parse structured error, trying with raw string: {}", &str_err);
        (str_err, "".to_string())
    };

    fn compile_regex(pattern: &str) -> Result<Regex, regex::Error> {
        Regex::new(pattern)
    }

    // Thanks Ponder for the regex patterns - https://github.com/ponder-sh/ponder/blob/889096a3ef5f54a0c5a06df82b0da9cf9a113996/packages/utils/src/getLogsRetryHelper.ts#L34

    // Alchemy
    if let Ok(re) =
        compile_regex(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]")
    {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = start_block.as_str();
                let end_block_str = end_block.as_str();

                if let (Ok(from), Ok(to)) =
                    (BlockNumber::from_str(start_block_str), BlockNumber::from_str(end_block_str))
                {
                    if from > to {
                        error!(
                            "Alchemy returned a negative block range. Overriding to single block fetch."
                        );

                        return Some(RetryWithBlockRangeResult {
                            from: from_block,
                            to: from_block + U64::from(1),
                            max_block_range: None,
                        });
                    }

                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
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
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = format!("0x{}", start_block.as_str());
                let end_block_str = format!("0x{}", end_block.as_str());
                if let (Ok(from), Ok(to)) =
                    (BlockNumber::from_str(&start_block_str), BlockNumber::from_str(&end_block_str))
                {
                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
                        max_block_range: None,
                    });
                }
            }
        }
    }

    // Ankr
    if error_message.contains("block range is too wide") {
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + U64::from(3000),
            max_block_range: Some(U64::from(3000)),
        });
    }

    // QuickNode, 1RPC, zkEVM, Blast, BlockPI
    if let Ok(re) = compile_regex(r"limited to a ([\d,.]+)") {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let Some(range_str_match) = captures.get(1) {
                let range_str = range_str_match.as_str().replace(&['.', ','][..], "");
                if let Ok(range) = U64::from_str(&range_str) {
                    return Some(RetryWithBlockRangeResult {
                        from: from_block,
                        to: from_block + range,
                        max_block_range: Some(range),
                    });
                }
            }
        }
    }

    // Base
    if error_message.contains("block range too large") {
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + U64::from(2000),
            max_block_range: Some(U64::from(2000)),
        });
    }

    // Fallback range
    if to_block > from_block {
        let diff = to_block - from_block;

        let mut block_range = FallbackBlockRange::from_diff(diff);
        let mut next_to_block = from_block + block_range.value();

        if next_to_block == to_block {
            block_range = block_range.lower();
            next_to_block = from_block + block_range.value();
        }

        if next_to_block < from_block {
            error!("Computed a negative fallback block range. Overriding to single block fetch.");

            return Some(RetryWithBlockRangeResult {
                from: from_block,
                to: from_block + U64::from(1),
                max_block_range: None,
            });
        }

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: next_to_block,
            max_block_range: Some(U64::from(block_range.value())),
        });
    }

    None
}

#[derive(Debug, PartialEq)]
enum FallbackBlockRange {
    Range5000,
    Range500,
    Range75,
    Range50,
    Range45,
    Range40,
    Range35,
    Range30,
    Range25,
    Range20,
    Range15,
    Range10,
    Range5,
    Range1,
}

impl FallbackBlockRange {
    fn value(&self) -> U64 {
        match self {
            FallbackBlockRange::Range5000 => U64::from(5000),
            FallbackBlockRange::Range500 => U64::from(500),
            FallbackBlockRange::Range75 => U64::from(75),
            FallbackBlockRange::Range50 => U64::from(50),
            FallbackBlockRange::Range45 => U64::from(45),
            FallbackBlockRange::Range40 => U64::from(40),
            FallbackBlockRange::Range35 => U64::from(35),
            FallbackBlockRange::Range30 => U64::from(30),
            FallbackBlockRange::Range25 => U64::from(25),
            FallbackBlockRange::Range20 => U64::from(20),
            FallbackBlockRange::Range15 => U64::from(15),
            FallbackBlockRange::Range10 => U64::from(10),
            FallbackBlockRange::Range5 => U64::from(5),
            FallbackBlockRange::Range1 => U64::from(1),
        }
    }

    fn lower(&self) -> FallbackBlockRange {
        match self {
            FallbackBlockRange::Range5000 => FallbackBlockRange::Range500,
            FallbackBlockRange::Range500 => FallbackBlockRange::Range75,
            FallbackBlockRange::Range75 => FallbackBlockRange::Range50,
            FallbackBlockRange::Range50 => FallbackBlockRange::Range45,
            FallbackBlockRange::Range45 => FallbackBlockRange::Range40,
            FallbackBlockRange::Range40 => FallbackBlockRange::Range35,
            FallbackBlockRange::Range35 => FallbackBlockRange::Range30,
            FallbackBlockRange::Range30 => FallbackBlockRange::Range25,
            FallbackBlockRange::Range25 => FallbackBlockRange::Range20,
            FallbackBlockRange::Range20 => FallbackBlockRange::Range15,
            FallbackBlockRange::Range15 => FallbackBlockRange::Range10,
            FallbackBlockRange::Range10 => FallbackBlockRange::Range5,
            FallbackBlockRange::Range5 => FallbackBlockRange::Range1,
            FallbackBlockRange::Range1 => FallbackBlockRange::Range1,
        }
    }

    fn from_diff(diff: U64) -> FallbackBlockRange {
        let diff = diff.as_limbs()[0];
        if diff >= 5000 {
            FallbackBlockRange::Range5000
        } else if diff >= 500 {
            FallbackBlockRange::Range500
        } else if diff >= 75 {
            FallbackBlockRange::Range75
        } else if diff >= 50 {
            FallbackBlockRange::Range50
        } else if diff >= 45 {
            FallbackBlockRange::Range45
        } else if diff >= 40 {
            FallbackBlockRange::Range40
        } else if diff >= 35 {
            FallbackBlockRange::Range35
        } else if diff >= 30 {
            FallbackBlockRange::Range30
        } else if diff >= 25 {
            FallbackBlockRange::Range25
        } else if diff >= 20 {
            FallbackBlockRange::Range20
        } else if diff >= 15 {
            FallbackBlockRange::Range15
        } else if diff >= 10 {
            FallbackBlockRange::Range10
        } else if diff >= 5 {
            FallbackBlockRange::Range5
        } else {
            FallbackBlockRange::Range1
        }
    }
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
