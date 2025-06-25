use std::{cmp, collections::VecDeque, ops::RangeInclusive, sync::Arc, time::Duration};

use alloy::consensus::Transaction;
use alloy::transports::{RpcError, TransportErrorKind};
use alloy::{
    primitives::{Address, Bytes, U256, U64},
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace},
};
use futures::future::try_join_all;
use serde::Serialize;
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, info, warn};

use crate::provider::RPC_CHUNK_SIZE;
use crate::{
    event::{
        callback_registry::{TraceResult, TxInformation},
        config::TraceProcessingConfig,
    },
    indexer::{
        last_synced::evm_trace_update_progress_and_last_synced_task,
        process::ProcessEventError,
        task_tracker::{indexing_event_processed, indexing_event_processing},
    },
    manifest::native_transfer::TraceProcessingMethod,
    provider::{JsonRpcCachedProvider, ProviderError},
};

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const NATIVE_TRANSFER_CONTRACT_NAME: &str = "EvmTraces";

/// An imaginary contract name to ensure native transfer "debug trace" indexing is compatible
/// with the streams and sinks to which rindexer writes.
pub const EVENT_NAME: &str = "NativeTransfer";

/// Invent an ABI to mimic an ERC20 Transfer.
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
#[derive(Debug, Clone, Serialize)]
pub struct NativeTransfer {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub transaction_information: TxInformation,
}

/// Push a range of blocks to the back-pressured channel and block producer when full.
async fn push_range(block_tx: &mpsc::Sender<U64>, last: U64, latest: U64) {
    let range: RangeInclusive<u64> =
        last.try_into().expect("U64 fits u64")..=latest.try_into().expect("U64 fits u64");
    let mut range = range.collect::<VecDeque<_>>();

    while let Some(block) = range.pop_front() {
        if let Err(e) = block_tx.send(U64::from(block)).await {
            if block_tx.is_closed() {
                error!("Failed to send block via channel. Channel closed: {}", e.to_string());
                break;
            }

            error!("Failed to send block via channel. Re-queuing: {}", e.to_string());
            range.push_front(block);
        }
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
                let block = U64::from(latest_block.header.number);

                // Always trim back to the safe indexing threshold (which is zero if disabled)
                let block = block - indexing_distance_from_head;

                if block > last_seen_block {
                    let to_block = end_block.map(|end| block.min(end)).unwrap_or(block);
                    let from_block = block.min(last_seen_block + U64::from(1));

                    debug!("Pushing trace blocks {} - {}", from_block, to_block);

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
            Ok(None) => {}
            Err(e) => {
                error!("Error fetching '{}' blocks: {}", network, e.to_string());
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn native_transfer_block_processor(
    network_name: String,
    provider: Arc<JsonRpcCachedProvider>,
    config: Arc<TraceProcessingConfig>,
    mut block_rx: mpsc::Receiver<U64>,
) -> Result<(), ProcessEventError> {
    let is_rcp_batchable = config.method == TraceProcessingMethod::EthGetBlockByNumber;

    // Set the concurrency used to make requests based on the method.
    //
    // Currently, `eth_getBlockByNumber` is a single JSON-RPC batch, and others are individual
    // network calls so can be treated differently.
    let (initial_concurrent_requests, limit_concurrent_requests) =
        if is_rcp_batchable { (50, RPC_CHUNK_SIZE) } else { (5, 100) };

    let mut concurrent_requests: usize = initial_concurrent_requests;
    let mut buffer: Vec<U64> = Vec::with_capacity(limit_concurrent_requests);

    loop {
        let Ok(permit) = config.semaphore.clone().acquire_owned().await else {
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        let recv = block_rx.recv_many(&mut buffer, concurrent_requests).await;

        if recv == 0 {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let processed_block = native_transfer_block_consumer(
            provider.clone(),
            &buffer[..recv],
            &network_name,
            &config,
        )
        .await;

        // If this has an error we need to not and reconsume the blocks. We don't have
        // to worry about double-publish because the failure point is on the provider
        // call itself, which is before publish.
        if let Err(e) = processed_block {
            // On error, drop the block query range. We want a slow increase in concurrency and a
            // relatively aggressive backoff.
            concurrent_requests = cmp::max(1, (concurrent_requests as f64 * 0.8) as usize);

            let is_rate_limit_error = matches!(&e, ProcessEventError::ProviderCallError(
                ProviderError::RequestFailed(
                    RpcError::Transport(
                        TransportErrorKind::HttpError(http_err)
                    )
                )) if http_err.status == 429
            );

            if is_rate_limit_error {
                error!(
                    "Rate-limited 429 '{}' block requests. Retrying in 2s: {}",
                    network_name,
                    e.to_string(),
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            } else {
                warn!(
                    "Could not process '{}' block requests. Likely too early for {}..{}, Retrying in 500ms: {}",
                    network_name,
                    &buffer.first().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                    &buffer.last().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                    e.to_string(),
                );
            }

            sleep(Duration::from_millis(500)).await;
            continue;
        } else {
            buffer.clear();

            // A random chance of increasing the request count helps us not overload
            // the ratelimit too rapidly across multi-network trace indexing and have a
            // slow ramp-up time (if rpc batching isn't available)
            if rand::random_bool(0.05) {
                concurrent_requests = (concurrent_requests + 2).min(limit_concurrent_requests);
            }
        };

        drop(permit)
    }
}

async fn provider_trace_call(
    provider: Arc<JsonRpcCachedProvider>,
    config: &TraceProcessingConfig,
    block: U64,
) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
    match config.method {
        TraceProcessingMethod::TraceBlock => provider.trace_block(block).await,
        TraceProcessingMethod::DebugTraceBlockByNumber => {
            provider.debug_trace_block_by_number(block).await
        }
        _ => unimplemented!("Unsupported trace method"),
    }
}

/// Index native transfers via batched rpc block call method
pub async fn native_transfer_block_consumer(
    provider: Arc<JsonRpcCachedProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &Arc<TraceProcessingConfig>,
) -> Result<(), ProcessEventError> {
    let blocks = provider.get_block_by_number_batch(block_numbers, true).await?;
    let (from_block, to_block) = block_numbers
        .iter()
        .fold((U64::MAX, U64::ZERO), |(min, max), &num| (cmp::min(min, num), cmp::max(max, num)));

    let native_transfers = blocks
        .into_iter()
        .flat_map(|b| {
            b.transactions.clone().into_transactions().map(move |tx| (b.header.timestamp, tx))
        })
        .filter_map(|(ts, tx)| {
            let is_empty_input = tx.input().is_empty();
            let is_value_zero = tx.value().is_zero();
            let has_to_address = tx.to().is_some();

            if has_to_address && is_empty_input && !is_value_zero {
                let to = tx.to().unwrap();
                Some(TraceResult::new_native_transfer(
                    tx,
                    ts,
                    to,
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

    indexing_event_processing();
    config.trigger_event(native_transfers.clone()).await;
    evm_trace_update_progress_and_last_synced_task(
        config.clone(),
        to_block,
        indexing_event_processed,
    );

    Ok(())
}

// Indexing native transfers if debug or trace indexing is enabled.
///
/// NOTE: This is currently unused as we have temporarily migrated to exclusively using
/// `eth_getBlockByNumber` calls instead, this is being retained for posterity should we
/// choose to continue to support `debug` and `trace` based native transfer indexing.
#[allow(unused)]
pub async fn native_transfer_block_consumer_debug(
    provider: Arc<JsonRpcCachedProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: Arc<TraceProcessingConfig>,
) -> Result<(), ProcessEventError> {
    let trace_futures: Vec<_> =
        block_numbers.iter().map(|n| provider_trace_call(provider.clone(), &config, *n)).collect();
    let trace_calls = try_join_all(trace_futures).await?;
    let (from_block, to_block) = block_numbers
        .iter()
        .fold((U64::MAX, U64::ZERO), |(min, max), &num| (cmp::min(min, num), cmp::max(max, num)));

    // We're not ready to support complete "trace" indexing for zksync chains. So we can
    // effectively only get what we need for native transfers by removing calls to "system
    // contracts".
    //
    // As an example, a Zksync ETH transfer will have a complex set of interactions with system
    // contracts. But, there will be only one deeply-nested "transfer" call for the actual two EOAs,
    // so filtering everything else will allow us to grab that without noise.
    //
    // Read more:
    // - https://docs.zksync.io/zksync-protocol/contracts/system-contracts#l2basetoken-msgvaluesimulator
    // - https://github.com/matter-labs/zksync-era/blob/7f36ed98fc6066c1224ff07c95282b647a8114fc/infrastructure/zk/src/verify-upgrade.ts#L24
    let zksync_system_contracts: [Address; 13] = [
        "0x0000000000000000000000000000000000008001".parse().unwrap(), // Native Token
        "0x0000000000000000000000000000000000008002".parse().unwrap(), // AccountCodeStorage
        "0x0000000000000000000000000000000000008003".parse().unwrap(), // NonceHolder
        "0x0000000000000000000000000000000000008004".parse().unwrap(), // KnownCodesStorage
        "0x0000000000000000000000000000000000008005".parse().unwrap(), // ImmutableSimulator
        "0x0000000000000000000000000000000000008006".parse().unwrap(), // ContractDeployer
        "0x0000000000000000000000000000000000008008".parse().unwrap(), // L1Messenger
        "0x0000000000000000000000000000000000008009".parse().unwrap(), // MsgValueSimulator
        "0x000000000000000000000000000000000000800a".parse().unwrap(), // L2BaseToken
        "0x000000000000000000000000000000000000800b".parse().unwrap(), // SystemContext
        "0x000000000000000000000000000000000000800c".parse().unwrap(), // BootloaderUtilities
        "0x000000000000000000000000000000000000800e".parse().unwrap(), // BytecodeCompressor
        "0x000000000000000000000000000000000000800f".parse().unwrap(), // ComplexUpgrader
    ];

    let native_transfers = trace_calls
        .into_iter()
        .flatten()
        .filter_map(|trace| {
            let action = match &trace.trace.action {
                Action::Call(call) => Some(call),
                _ => None,
            }?;

            let no_input = action.input == Bytes::new();
            let has_value = !action.value.is_zero();
            let is_zksync_system_transfer = zksync_system_contracts.contains(&action.from)
                || zksync_system_contracts.contains(&action.to);
            let is_native_transfer = has_value && no_input && !is_zksync_system_transfer;

            if is_native_transfer {
                Some(TraceResult::new_debug_native_transfer(
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

    indexing_event_processing();
    config.trigger_event(native_transfers).await;
    evm_trace_update_progress_and_last_synced_task(config, to_block, indexing_event_processed);

    Ok(())
}
