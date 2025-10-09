use crate::blockclock::BlockClock;
use crate::helpers::{halved_block_number, is_relevant_block};
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{reorg::handle_chain_notification, IndexingEventProgressStatus},
    is_running,
    provider::{JsonRpcCachedProvider, ProviderError},
};
use alloy::{
    primitives::{B256, U64},
    rpc::types::Log,
};
use lru::LruCache;
use rand::{random_bool, random_ratio};
use regex::Regex;
use std::num::NonZeroUsize;
use std::{error::Error, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};

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
    // If the sink is slower than the producer it can lead to unbounded memory growth and
    // a system OOM kill.
    //
    // To prevent this, we maintain a memory bound to give the system time to catch up and
    // backpressure the producer. Many RPC responses are large, so this is important.
    //
    // This is per network contract-event, so it should be relatively small.
    let channel_size = config.config().buffer.unwrap_or(4);

    debug!("{} Configured with {} event buffer", config.info_log_name(), channel_size);

    let (tx, rx) = mpsc::channel(channel_size);

    tokio::spawn(async move {
        let mut current_filter = config.to_event_filter().unwrap();

        let snapshot_to_block = current_filter.to_block();
        let from_block = current_filter.from_block();

        // add any max block range limitation before we start processing
        let original_max_limit = config.network_contract().cached_provider.max_block_range;
        let mut max_block_range_limitation =
            config.network_contract().cached_provider.max_block_range;
        if max_block_range_limitation.is_some() {
            current_filter = current_filter.set_to_block(calculate_process_historic_log_to_block(
                &from_block,
                &snapshot_to_block,
                &max_block_range_limitation,
            ));
            if random_ratio(1, 20) {
                warn!(
                    "{} - {} - max block range of {} applied - indexing will be slower than providers supplying the optimal ranges - https://rindexer.xyz/docs/references/rpc-node-providers#rpc-node-providers",
                    config.info_log_name(),
                    IndexingEventProgressStatus::Syncing.log(),
                    max_block_range_limitation.unwrap()
                );
            }
        }

        while current_filter.from_block() <= snapshot_to_block {
            if !is_running() {
                break;
            }

            let result = fetch_historic_logs_stream(
                config.timestamps(),
                config.network_contract().block_clock.clone(),
                &config.network_contract().cached_provider,
                &tx,
                &config.topic_id(),
                current_filter.clone(),
                max_block_range_limitation,
                snapshot_to_block,
                &config.info_log_name(),
            )
            .await;

            // This check can be very noisy. We want to only sample this warning to notify
            // the user, rather than warn on every log fetch.
            if let Some(range) = max_block_range_limitation {
                if range.to::<u64>() < 5000 && random_ratio(1, 20) {
                    warn!(
                        "{} - RPC PROVIDER IS SLOW - Slow indexing mode enabled, max block range limitation: {} blocks - we advise using a faster provider who can predict the next block ranges.",
                        &config.info_log_name(),
                        range
                    );
                }
            }

            if let Some(result) = result {
                // Useful for occasionally breaking out of temporary limitations or parsing errors
                // that lock down to a `1` block limitation. Returns back to the original
                let new_max_block_range_limitation = if random_bool(0.10) {
                    original_max_limit
                } else {
                    result.max_block_range_limitation
                };

                current_filter = result.next;
                max_block_range_limitation = new_max_block_range_limitation;
            } else {
                break;
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
                config.timestamps(),
                config.network_contract().block_clock.clone(),
                &config.network_contract().cached_provider,
                &tx,
                snapshot_to_block,
                &config.topic_id(),
                &config.indexing_distance_from_head(),
                current_filter,
                &config.info_log_name(),
                config.network_contract().disable_logs_bloom_checks,
                original_max_limit,
            )
            .await;
        }
    });

    ReceiverStream::new(rx)
}

struct ProcessHistoricLogsStreamResult {
    pub next: RindexerEventFilter,
    pub max_block_range_limitation: Option<U64>,
}

#[allow(clippy::too_many_arguments)]
async fn fetch_historic_logs_stream(
    timestamps: bool,
    block_clock: BlockClock,
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::Sender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    topic_id: &B256,
    current_filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
) -> Option<ProcessHistoricLogsStreamResult> {
    let from_block = current_filter.from_block();
    let to_block = current_filter.to_block();

    debug!(
        "{} - {} - Process historic events - blocks: {} - {}",
        info_log_name,
        IndexingEventProgressStatus::Syncing.log(),
        from_block,
        to_block
    );

    if from_block > to_block {
        warn!(
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

    debug!(
        "{} - {} - Processing filter: {:?}",
        info_log_name,
        IndexingEventProgressStatus::Syncing.log(),
        current_filter
    );

    let sender = tx.reserve().await.ok()?;

    if tx.capacity() == 0 {
        debug!(
            "{} - {} - Log channel full, waiting for events to be processed.",
            info_log_name,
            IndexingEventProgressStatus::Syncing.log(),
        );
    }

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

            let logs_empty = logs.is_empty();
            // clone here over the full logs way less overhead
            let last_log = logs.last().cloned();

            if !logs_empty {
                info!(
                    "{} - {} - Fetched {} logs between: {} - {}",
                    info_log_name,
                    IndexingEventProgressStatus::Syncing.log(),
                    logs.len(),
                    from_block,
                    to_block
                );
            }

            if timestamps {
                if let Ok(logs) = block_clock.attach_log_timestamps(logs).await {
                    sender.send(Ok(FetchLogsResult { logs, from_block, to_block }));
                } else {
                    return Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .set_from_block(from_block)
                            .set_to_block(halved_block_number(to_block, from_block)),
                        max_block_range_limitation,
                    });
                }
            } else {
                sender.send(Ok(FetchLogsResult { logs, from_block, to_block }));
            }

            if logs_empty {
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
                        "{} - No events between {} - {}. Searching next {} blocks.",
                        info_log_name,
                        from_block,
                        to_block,
                        new_to_block - next_from_block
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
            // This is fundamental to the rindexer flow. We intentionally fetch a large block range
            // to get information on what the ideal block range should be.
            if let Some(retry_result) = retry_with_block_range(
                info_log_name,
                &err,
                from_block,
                to_block,
                max_block_range_limitation,
            )
            .await
            {
                // Log if we "overshrink"
                if retry_result.to - retry_result.from < U64::from(1000) {
                    debug!(
                        "{} - {} - Over-fetched {} to {}. Shrunk ({}): {} to {}{}",
                        info_log_name,
                        IndexingEventProgressStatus::Syncing.log(),
                        from_block,
                        to_block,
                        retry_result.to - retry_result.from,
                        retry_result.from,
                        retry_result.to,
                        retry_result
                            .max_block_range
                            .map(|m| format!(" (max {m})"))
                            .unwrap_or("".to_owned()),
                    );
                }

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
                "{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
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
    timestamps: bool,
    block_clock: BlockClock,
    cached_provider: &Arc<JsonRpcCachedProvider>,
    tx: &mpsc::Sender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    last_seen_block_number: U64,
    topic_id: &B256,
    reorg_safe_distance: &U64,
    mut current_filter: RindexerEventFilter,
    info_log_name: &str,
    disable_logs_bloom_checks: bool,
    original_max_limit: Option<U64>,
) {
    let mut last_seen_block_number = last_seen_block_number;
    let mut log_response_to_large_to_block: Option<U64> = None;
    let mut last_no_new_block_log_time = Instant::now();
    let log_no_new_block_interval = Duration::from_secs(300);
    let target_iteration_duration = Duration::from_millis(200);

    let chain_state_notification = cached_provider.get_chain_state_notification();

    // Spawn a separate task to handle notifications
    if let Some(notifications) = chain_state_notification {
        let info_log_name = info_log_name.to_string();
        tokio::spawn(async move {
            let mut notifications_clone = notifications.subscribe();
            while let Ok(notification) = notifications_clone.recv().await {
                handle_chain_notification(notification, &info_log_name);
            }
        });
    }

    // This is a local cache of the last blocks we've crawled and their timestamps.
    //
    // It allows us to cheaply persist and fetch timestamps for blocks in any log range
    // fetch for a recent period. It is about 16-bytes per entry.
    //
    // 500 should cover any block-lag we could reasonably encounter at near-zer memory cost.
    let mut block_times: LruCache<u64, u64> = LruCache::new(NonZeroUsize::new(50).unwrap());

    loop {
        let iteration_start = Instant::now();

        if !is_running() {
            break;
        }

        let latest_block = cached_provider.get_latest_block().await;
        match latest_block {
            Ok(latest_block) => {
                if let Some(latest_block) = latest_block {
                    block_times.put(latest_block.header.number, latest_block.header.timestamp);

                    let latest_block_number = log_response_to_large_to_block
                        .unwrap_or(U64::from(latest_block.header.number));

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
                    } else {
                        debug!(
                            "{} - {} - New block seen {} - Last seen block {}",
                            info_log_name,
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            last_seen_block_number
                        );

                        let safe_block_number = latest_block_number - reorg_safe_distance;
                        let from_block = current_filter.from_block();
                        if from_block > safe_block_number {
                            if reorg_safe_distance.is_zero() {
                                let block_distance = latest_block_number - from_block;
                                let is_outside_reorg_range = block_distance
                                    > reorg_safe_distance_for_chain(cached_provider.chain.id());

                                // it should never get under normal conditions outside the reorg range,
                                // therefore, we log an error as means RCP state is not in sync with the blockchain
                                if is_outside_reorg_range {
                                    error!(
                                        "{} - {} - LIVE INDEXING STEAM - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                                        info_log_name,
                                        IndexingEventProgressStatus::Live.log(),
                                        latest_block_number,
                                        from_block
                                    );
                                } else {
                                    info!(
                                        "{} - {} - LIVE INDEXING STEAM - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                                        info_log_name,
                                        IndexingEventProgressStatus::Live.log(),
                                        latest_block_number,
                                        from_block
                                    );
                                }
                            } else {
                                info!(
                                    "{} - {} - LIVE INDEXING STEAM - not in safe reorg block range yet block: {} > range: {}",
                                    info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block,
                                    safe_block_number
                                );
                            }
                        } else {
                            let contract_address = current_filter.contract_addresses().await;

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

                                debug!(
                                    "{} - {} - Processing live filter: {:?}",
                                    info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    current_filter
                                );

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

                                        // Attach timestamp from current latest_block to the logs
                                        // to prevent any further fetches.
                                        let logs = logs
                                            .into_iter()
                                            .map(|mut log| {
                                                if let Some(n) = log.block_number {
                                                    if let Some(time) = block_times.get(&n) {
                                                        log.block_timestamp = Some(*time);
                                                    }
                                                }
                                                log
                                            })
                                            .collect::<Vec<_>>();

                                        if tx.capacity() == 0 {
                                            warn!(
                                                "{} - {} - Log channel full, live indexer will wait for events to be processed.",
                                                info_log_name,
                                                IndexingEventProgressStatus::Live.log(),
                                            );
                                        }

                                        let logs = if timestamps {
                                            if let Ok(logs_with_ts) =
                                                block_clock.attach_log_timestamps(logs).await
                                            {
                                                logs_with_ts
                                            } else {
                                                error!(
                                                    "Error getting blocktime, will try again in 1s"
                                                );
                                                tokio::time::sleep(Duration::from_secs(1)).await;
                                                continue;
                                            }
                                        } else {
                                            logs
                                        };

                                        if let Err(e) = tx
                                            .send(Ok(FetchLogsResult {
                                                logs,
                                                from_block,
                                                to_block,
                                            }))
                                            .await
                                        {
                                            error!(
                                                "{} - {} - Failed to send logs to stream consumer! Err: {}",
                                                info_log_name,
                                                IndexingEventProgressStatus::Live.log(),
                                                e
                                            );
                                            break;
                                        }

                                        // Clear any remaining references to reduce memory pressure
                                        log_response_to_large_to_block = None;

                                        if logs_empty {
                                            current_filter = current_filter
                                                .set_from_block(to_block + U64::from(1));
                                            debug!(
                                                "{} - {} - No events found between blocks {} - {}",
                                                info_log_name,
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
                                    }
                                    Err(err) => {
                                        if let Some(retry_result) = retry_with_block_range(
                                            info_log_name,
                                            &err,
                                            from_block,
                                            to_block,
                                            original_max_limit,
                                        )
                                        .await
                                        {
                                            debug!(
                                                    "{} - {} - Overfetched from {} to {} - shrinking to block range: from {} to {}",
                                                    info_log_name,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    retry_result.to
                                                    );

                                            log_response_to_large_to_block = Some(retry_result.to);
                                        } else {
                                            let halved_to_block =
                                                halved_block_number(to_block, from_block);

                                            error!(
                                                    "{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                                                    info_log_name,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    halved_to_block,
                                                    err
                                                );

                                            log_response_to_large_to_block = Some(halved_to_block);
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
                    "Error getting latest block, will try again in 1 second - err: {}",
                    e.to_string()
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
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
async fn retry_with_block_range(
    info_log_name: &str,
    error: &ProviderError,
    from_block: U64,
    to_block: U64,
    max_block_range_limitation: Option<U64>,
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
        let trimmed = error_message.chars().take(5000).collect::<String>();

        (trimmed.to_lowercase(), error_data.to_lowercase())
    } else {
        let str_err = error.to_string();
        let trimmed = str_err.chars().take(5000).collect::<String>();
        debug!("Failed to parse structured error, trying with raw string: {}", &str_err);
        (trimmed.to_lowercase(), "".to_string())
    };

    // Thanks Ponder for the regex patterns - https://github.com/ponder-sh/ponder/blob/889096a3ef5f54a0c5a06df82b0da9cf9a113996/packages/utils/src/getLogsRetryHelper.ts#L34
    // Alchemy
    if let Ok(re) =
        Regex::new(r"this block range should work: \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]")
    {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = start_block.as_str();
                let end_block_str = end_block.as_str();
                if let (Ok(from), Ok(to)) = (
                    u64::from_str_radix(start_block_str, 16),
                    u64::from_str_radix(end_block_str, 16),
                ) {
                    if from > to {
                        warn!(
                            "{} Alchemy returned a negative block range {} to {}. Inverting.",
                            info_log_name, from, to
                        );

                        // Negative range fixed by inverting.
                        let to = U64::from(from);

                        return Some(RetryWithBlockRangeResult {
                            from: from_block,
                            to,
                            max_block_range: max_block_range_limitation,
                        });
                    }

                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
                        max_block_range: max_block_range_limitation,
                    });
                } else {
                    info!(
                        "{} Failed to parse block numbers {} and {}",
                        info_log_name, start_block_str, end_block_str
                    );
                }
            }
        }
    }

    // Infura, Thirdweb, zkSync, Tenderly
    if let Ok(re) =
        Regex::new(r"try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]")
    {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                if let (Ok(from), Ok(to)) = (
                    u64::from_str_radix(start_block.as_str(), 16),
                    u64::from_str_radix(end_block.as_str(), 16),
                ) {
                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
                        max_block_range: max_block_range_limitation,
                    });
                }
            }
        }
    }

    // Ankr
    if error_message.contains("block range is too wide") {
        // Use the minimum of original config or 3000
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, U64::from(3000)))
            .unwrap_or(U64::from(3000));

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
        });
    }

    // QuickNode, 1RPC, zkEVM, Blast, BlockPI
    if let Ok(re) = Regex::new(r"limited to a ([\d,.]+)") {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let Some(range_str_match) = captures.get(1) {
                let range_str = range_str_match.as_str().replace(&['.', ','][..], "");
                if let Ok(range) = U64::from_str(&range_str) {
                    // Use the minimum of original config or provider suggestion
                    let suggested_range = max_block_range_limitation
                        .map(|original| std::cmp::min(original, range))
                        .unwrap_or(range);

                    return Some(RetryWithBlockRangeResult {
                        from: from_block,
                        to: from_block + suggested_range,
                        max_block_range: Some(suggested_range),
                    });
                }
            }
        }
    }

    // Base
    if error_message.contains("block range too large") {
        // Use the minimum of original config or 2000
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, U64::from(2000)))
            .unwrap_or(U64::from(2000));

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
        });
    }

    // Transient response errors, likely solved by halving the range or just retrying
    if error_message.contains("response is too big")
        || error_message.contains("error decoding response body")
    {
        let halved_to_block = halved_block_number(to_block, from_block);
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: halved_to_block,
            max_block_range: max_block_range_limitation,
        });
    }

    // We can't keep up with our own sending rate. This is rare, but we must backoff throughput.
    if error_message.contains("error sending request") {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: halved_block_number(to_block, from_block),
            max_block_range: max_block_range_limitation,
        });
    }

    // Fallback range
    if to_block > from_block {
        let diff = to_block - from_block;

        let mut block_range = FallbackBlockRange::from_diff(diff);
        let mut next_to_block = from_block + block_range.value();

        warn!(
            "{} Computed a fallback block range {:?}. Provider did not provide information in error: {:?}",
            info_log_name, block_range, error_message
        );

        if next_to_block == to_block {
            block_range = block_range.lower();
            next_to_block = from_block + block_range.value();
        }

        if next_to_block < from_block {
            error!(
                "{} Computed a negative fallback block range. Overriding to single block fetch.",
                info_log_name
            );

            return Some(RetryWithBlockRangeResult {
                from: from_block,
                to: halved_block_number(to_block, from_block),
                max_block_range: max_block_range_limitation,
            });
        }

        // Use the minimum of original config or fallback range
        let fallback_range = U64::from(block_range.value());
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, fallback_range))
            .unwrap_or(fallback_range);

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
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
