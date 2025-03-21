use std::{str::FromStr, sync::Arc, time::Duration};

use ethers::types::{Action, Address, Bytes, U256, U64};
use futures::future::try_join_all;
use serde::Serialize;
use tokio::time::sleep;
use tracing::{error, info};

use super::start::StartIndexingError;
use crate::{
    event::{
        callback_registry::{TraceResult, TxInformation},
        config::TraceProcessingConfig,
    },
    provider::JsonRpcCachedProvider,
};

#[derive(Serialize)]
pub struct NativeTransfer {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub transaction_information: TxInformation,
}

/// Block publisher.
///
/// This is a long-running process designed to accept a [`Sender`] handle and publish blocks
/// in an efficient manner which respects the user defined manifest block ranges.
///
/// This process respects channel backpressure and will only complete once the `end_block` is
/// reached.
pub async fn native_transfer_block_fetch(
    publisher: Arc<JsonRpcCachedProvider>,
    block_tx: tokio::sync::mpsc::Sender<U64>,
    start_block: U64,
    end_block: Option<U64>,
    indexing_distance_from_head: U64,
    network: String,
) -> Result<(), StartIndexingError> {
    let mut last_seen_block = start_block;

    // Push a range of blocks to the back-pressured channel and block producer when full.
    let push_range = async |last: U64, latest: U64| {
        let range = last.as_u64()..=latest.as_u64();
        for block in range {
            block_tx.send(U64::from(block)).await.expect("should send");
        }
    };

    loop {
        sleep(Duration::from_millis(200)).await;

        let latest_block = publisher.get_latest_block().await;

        match latest_block {
            Ok(Some(latest_block)) => {
                if let Some(block) = latest_block.number {
                    let safe_block_number = block - indexing_distance_from_head;

                    if block > safe_block_number {
                        info!(
                            "{} - not in safe reorg block range yet block: {} > range: {}",
                            "NativeEvmTraces", block, safe_block_number
                        );
                        continue;
                    }

                    if block > last_seen_block {
                        let to_block = end_block.map(|end| block.min(end)).unwrap_or(block);

                        let from_block = block.min(last_seen_block + 1);

                        info!("Pushing blocks {} - {}", from_block, to_block);

                        push_range(from_block, to_block).await;
                        last_seen_block = to_block;
                    }

                    if end_block.is_some() && block > end_block.expect("must have block") {
                        info!("Finished {} HISTORICAL INDEXING NativeEvmTraces. No more blocks to push.", network);
                        return Ok(());
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                error!("Error fetching trace_block: {}", e.to_string());
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn native_transfer_block_consumer(
    provider: Arc<JsonRpcCachedProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &TraceProcessingConfig,
) -> Result<(), StartIndexingError> {
    let trace_futures: Vec<_> = block_numbers.iter().map(|n| provider.trace_block(*n)).collect();
    let trace_calls = try_join_all(trace_futures).await?;
    let (from_block, to_block) =
        block_numbers.iter().fold((U64::MAX, U64::zero()), |(min, max), &num| {
            (std::cmp::min(min, num), std::cmp::max(max, num))
        });

    let native_transfers = trace_calls
        .into_iter()
        .flatten()
        .filter_map(|trace| {
            let action = match &trace.action {
                Action::Call(call) => Some(call),
                _ => None,
            }?;

            // TODO: Replace with `Bytes::new()`
            let no_input = action.input == Bytes::from_str("0x").unwrap();
            let has_value = !action.value.is_zero();
            let is_native_transfer = has_value && no_input;

            if is_native_transfer {
                Some(TraceResult::new_native_transfer(
                    action,
                    &trace,
                    network_name,
                    from_block,
                    to_block,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if native_transfers.is_empty() {
        return Ok(());
    }

    config.trigger_event(native_transfers).await;

    Ok(())
}
