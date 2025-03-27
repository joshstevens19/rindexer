use std::{sync::Arc, time::Duration};

use ethers::{
    providers::ProviderError,
    types::{Action, Address, Bytes, Trace, U256, U64},
};
use futures::future::try_join_all;
use serde::Serialize;
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, info};

use crate::{
    event::{
        callback_registry::{TraceResult, TxInformation},
        config::TraceProcessingConfig,
    },
    indexer::process::ProcessEventError,
    manifest::native_transfer::TraceProcessingMethod,
    provider::JsonRpcCachedProvider,
};

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const NATIVE_TRANSFER_CONTRACT_NAME: &str = "EvmTraces";

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const EVENT_NAME: &str = "NativeTransfer";

/// Invent an ABI to mimic an ERC0 Transfer.
///
/// This will allow indexer consumers, which will typically be configured to consume contract events
/// to simply ingest an ERC20 compatible event for the native tokens.
///
/// In order to reduce name conflicts we will call the Transfer event `NativeTransfer`.
pub const NATIVE_TRANSFER_ABI: &str = r#"[{
    "anonymous": false,
    "inputs": [
        {
            "indexed": true,
            "name": "from",
            "type": "address"
        },
        {
            "indexed": true,
            "name": "to",
            "type": "address"
        },
        {
            "indexed": false,
            "name": "value",
            "type": "uint256"
        }
    ],
    "name": "NativeTransfer",
    "type": "event"
}]"#;

/// Refer to [`NATIVE_TRANSFER_ABI`] as an imaginary associated ABI for this Native Transfer
/// "event" struct.
#[derive(Serialize)]
pub struct NativeTransfer {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub transaction_information: TxInformation,
}

/// Push a range of blocks to the back-pressured channel and block producer when full.
async fn push_range(block_tx: &mpsc::Sender<U64>, last: U64, latest: U64) {
    let range = last.as_u64()..=latest.as_u64();

    for block in range {
        block_tx.send(U64::from(block)).await.expect("should send");
    }
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
    block_tx: mpsc::Sender<U64>,
    start_block: U64,
    end_block: Option<U64>,
    indexing_distance_from_head: U64,
    network: String,
) -> Result<(), ProcessEventError> {
    let mut last_seen_block = start_block;

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

                        push_range(&block_tx, from_block, to_block).await;
                        last_seen_block = to_block;
                    }

                    if end_block.is_some() && block > end_block.expect("must have block") {
                        info!("Finished HISTORICAL INDEXING for {} NativeEvmTraces. No more blocks to push.", network);
                        debug!("Dropping {} 'NativeEvmTraces' block Sender handle", network);
                        drop(block_tx);
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

async fn provider_call(
    provider: Arc<JsonRpcCachedProvider>,
    config: &TraceProcessingConfig,
    block: U64,
) -> Result<Vec<Trace>, ProviderError> {
    if config.method == TraceProcessingMethod::DebugTraceBlockByNumber {
        provider.debug_trace_block_by_number(block).await
    } else {
        provider.trace_block(block).await
    }
}

pub async fn native_transfer_block_consumer(
    provider: Arc<JsonRpcCachedProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &TraceProcessingConfig,
) -> Result<(), ProcessEventError> {
    let trace_futures: Vec<_> =
        block_numbers.iter().map(|n| provider_call(provider.clone(), config, *n)).collect();
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

            let no_input = action.input == Bytes::new();
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
