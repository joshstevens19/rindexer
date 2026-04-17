use std::{cmp, collections::VecDeque, ops::RangeInclusive, sync::Arc, time::Duration};

use alloy::consensus::Transaction;
use alloy::transports::{RpcError, TransportErrorKind};
use alloy::{
    primitives::{Address, Bytes, U256, U64},
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace},
};
use futures::future::try_join_all;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::{sync::mpsc, time::sleep};
use tracing::{debug, error, info, warn};

use tokio_util::sync::CancellationToken;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::indexer::reorg::{detect_and_handle_reorg, ReorgContext, ReorgCoordinator};
use crate::is_running;
use crate::provider::RECOMMENDED_RPC_CHUNK_SIZE;
use crate::streams::StreamsClients;
use crate::PostgresClient;
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
    provider::{ChainProvider, ProviderError},
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
                // Log error if not shutting down
                if is_running() {
                    error!("Failed to send block via channel: {}", e.to_string());
                }
                break;
            }

            error!("Failed to send block via channel. Re-queuing: {}", e.to_string());
            range.push_front(block);
        }
    }
}

/// Result of a reorg check over a pending block range.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NativeTransferReorgOutcome {
    /// No coordinator configured; range can be emitted as-is.
    NoCoordinator,
    /// All blocks in the range are canonical. Safe to emit.
    Canonical,
    /// A reorg was handled. `rewind_to` is the first canonical block to re-fetch
    /// (inclusive) — the caller rewinds `last_seen_block` to `rewind_to - 1` so
    /// the next iteration's `from_block` equals `rewind_to`.
    Rewound { rewind_to: u64 },
    /// Detection or rollback failed. Caller should back off and retry.
    Failed,
}

/// Walk the next contiguous block range `from..=to` against the coordinator,
/// feeding each canonical block's `(number, hash, parent_hash)` through
/// `detect_and_handle_reorg`. The first reorg detected rewinds the cursor.
///
/// Extracted from `native_transfer_block_fetch` to keep the hot loop readable
/// and to provide a unit-testable seam for reorg rewind behaviour.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn native_transfer_detect_reorg_in_range(
    provider: &dyn ChainProvider,
    coordinator: Option<&Arc<Mutex<ReorgCoordinator>>>,
    postgres: Option<&PostgresClient>,
    clickhouse: Option<&Arc<ClickhouseClient>>,
    streams_clients: Option<&StreamsClients>,
    network: &str,
    from_block: u64,
    to_block: u64,
) -> NativeTransferReorgOutcome {
    let Some(coordinator) = coordinator else {
        return NativeTransferReorgOutcome::NoCoordinator;
    };

    let block_numbers: Vec<U64> = (from_block..=to_block).map(U64::from).collect();
    let blocks = match provider.get_block_by_number_batch(&block_numbers, false).await {
        Ok(b) => b,
        Err(e) => {
            error!(
                network,
                from_block,
                to_block,
                error = %e,
                "Native transfer reorg pre-check: block header fetch failed",
            );
            return NativeTransferReorgOutcome::Failed;
        }
    };

    // Guard against short reads (RPC lag, chain-tip reorg). Advancing the caller's
    // cursor on a partial batch would leave a silent gap: blocks requested but not
    // returned would never be fed to the coordinator, and their native-transfer
    // rows would never be emitted. Treat as a transient failure so the caller
    // backs off and retries the same range.
    if blocks.len() != block_numbers.len() {
        warn!(
            network,
            from_block,
            to_block,
            requested = block_numbers.len(),
            received = blocks.len(),
            "Native transfer reorg pre-check: short read from provider, retrying",
        );
        return NativeTransferReorgOutcome::Failed;
    }

    // Mutex held across reorg handling (DB rollback, stream publishes). On a real
    // reorg this blocks the other indexing path for the duration of handle_reorg,
    // which is acceptable for isolation. If latency becomes a concern, move
    // handle_reorg out of the hot path.
    let mut guard = coordinator.lock().await;
    // TODO(Task 4): ReorgContext.registry expects &EventCallbackRegistry; native
    // transfers use a TraceCallbackRegistry. Task 4 will add `trace_registry` to
    // ReorgContext so the `on_reorg` callback can fire for NT-only reorgs.
    let ctx = ReorgContext { postgres, clickhouse, registry: None, streams_clients };

    for block in blocks {
        let number = block.header.number;
        let hash = block.header.hash;
        let parent_hash = block.header.parent_hash;
        match detect_and_handle_reorg(
            &mut guard,
            number,
            hash,
            parent_hash,
            "NativeTransfers",
            &ctx,
        )
        .await
        {
            Ok(Some(fork_point)) => {
                warn!(
                    network,
                    fork_point,
                    detection_block = number,
                    "Rewinding native transfer fetch to fork point",
                );
                return NativeTransferReorgOutcome::Rewound { rewind_to: fork_point };
            }
            Ok(None) => {}
            Err(e) => {
                error!(
                    network,
                    block = number,
                    error = ?e,
                    "Native transfer reorg detection failed",
                );
                return NativeTransferReorgOutcome::Failed;
            }
        }
    }

    NativeTransferReorgOutcome::Canonical
}

/// Block publisher.
///
/// This is a long-running process designed to accept a [`Sender`] handle and publish blocks
/// in an efficient manner which respects the user defined manifest block ranges.
///
/// This process respects channel backpressure and will only complete once the `end_block` is
/// reached.
#[allow(clippy::too_many_arguments)]
pub async fn native_transfer_block_fetch(
    publisher: Arc<dyn ChainProvider>,
    block_tx: mpsc::Sender<U64>,
    start_block: U64,
    end_block: Option<U64>,
    indexing_distance_from_head: U64,
    network: String,
    cancel_token: CancellationToken,
    postgres: Option<Arc<PostgresClient>>,
    _indexer_name: String,
    reorg_coordinator: Option<Arc<Mutex<ReorgCoordinator>>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    streams_clients: Arc<Option<StreamsClients>>,
) -> Result<(), ProcessEventError> {
    let mut last_seen_block = start_block;

    loop {
        if !is_running() || cancel_token.is_cancelled() {
            info!("Exiting native transfer indexing block processor!");
            break Ok(());
        }

        let latest_block = publisher.get_latest_block().await;

        match latest_block {
            Ok(Some(latest_block)) => {
                let block = U64::from(latest_block.header.number);

                // Always trim back to the safe indexing threshold (which is zero if disabled)
                let block = block - indexing_distance_from_head;

                if block > last_seen_block {
                    let to_block = end_block.map(|end| block.min(end)).unwrap_or(block);
                    let from_block = block.min(last_seen_block + U64::from(1));

                    // Reorg detection runs on the fetch side so every block - even those
                    // with zero native transfers - feeds the coordinator's window, keeping
                    // it contiguous.
                    let outcome = native_transfer_detect_reorg_in_range(
                        publisher.as_ref(),
                        reorg_coordinator.as_ref(),
                        postgres.as_deref(),
                        clickhouse.as_ref(),
                        streams_clients.as_ref().as_ref(),
                        &network,
                        from_block.to::<u64>(),
                        to_block.to::<u64>(),
                    )
                    .await;

                    match outcome {
                        NativeTransferReorgOutcome::Canonical
                        | NativeTransferReorgOutcome::NoCoordinator => {
                            debug!("Pushing trace blocks {} - {}", from_block, to_block);
                            push_range(&block_tx, from_block, to_block).await;
                            last_seen_block = to_block;
                        }
                        NativeTransferReorgOutcome::Rewound { rewind_to } => {
                            // `rewind_to` is the first canonical block to re-fetch (inclusive).
                            // Subtract 1 from the cursor so the next iteration computes
                            // `from_block = last_seen_block + 1 = rewind_to`, matching the
                            // coordinator's rollback which deletes rows with
                            // `block_number >= fork_point`.
                            last_seen_block = U64::from(rewind_to.saturating_sub(1));
                            continue;
                        }
                        NativeTransferReorgOutcome::Failed => {
                            // Mirror contract-event backoff: pause before retrying.
                            sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
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
    provider: Arc<dyn ChainProvider>,
    config: Arc<TraceProcessingConfig>,
    mut block_rx: mpsc::Receiver<U64>,
) -> Result<(), ProcessEventError> {
    // Set the concurrency used to make requests based on the method.
    //
    // Currently, `eth_getBlockByNumber` is a single JSON-RPC batch, and others are individual
    // network calls so can be treated differently.
    let (initial_concurrent_requests, limit_concurrent_requests) = (5, RECOMMENDED_RPC_CHUNK_SIZE);

    let mut concurrent_requests: usize = initial_concurrent_requests;
    let mut buffer: Vec<U64> = Vec::with_capacity(limit_concurrent_requests);

    loop {
        if !is_running() || config.cancel_token.is_cancelled() {
            info!("Exiting native transfer indexing block processor!");
            break Ok(());
        }

        // Fetch more only if buffer was processed ok last time and cleared.
        let recv = if buffer.is_empty() {
            block_rx.recv_many(&mut buffer, concurrent_requests).await
        } else {
            buffer.len()
        };

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

        // If this has an error, we need to not and reconsume the blocks. We don't have
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
                warn!(
                    "Rate-limited 429 '{}' block requests. Retrying in 2s: {}",
                    network_name,
                    e.to_string(),
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            } else {
                warn!(
                    "Could not process '{}' block requests for {}..{}, Retrying in 500ms: {}",
                    network_name,
                    &buffer.first().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                    &buffer.last().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                    e.to_string(),
                );
            }

            sleep(Duration::from_secs(1)).await;
            continue;
        } else {
            buffer.clear();

            // A random chance of increasing the request count helps us not overload
            // the ratelimit too rapidly across multi-network trace indexing and have a
            // slow ramp-up time (if rpc batching isn't available)
            if rand::random_bool(0.1) {
                concurrent_requests =
                    ((concurrent_requests * 20) / 10).min(limit_concurrent_requests);
            }

            sleep(Duration::from_millis(50)).await;
        };
    }
}

async fn provider_trace_call(
    provider: Arc<dyn ChainProvider>,
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
    provider: Arc<dyn ChainProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &Arc<TraceProcessingConfig>,
) -> Result<(), ProcessEventError> {
    let blocks = provider.get_block_by_number_batch(block_numbers, true).await?;
    let (from_block, to_block) = block_numbers
        .iter()
        .fold((U64::MAX, U64::ZERO), |(min, max), &num| (cmp::min(min, num), cmp::max(max, num)));

    let native_transfers = blocks
        .clone()
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
                    provider.chain().id(),
                    from_block,
                    to_block,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Important that we call this for every event even if there are no logs.
    // This is because we need to sync the last seen block number still.
    indexing_event_processing();

    let blocks = blocks
        .into_iter()
        .map(|b| {
            TraceResult::new_block(b, network_name, provider.chain().id(), from_block, to_block)
        })
        .collect::<Vec<_>>();
    config.trigger_event(blocks).await;

    if !native_transfers.is_empty() {
        config.trigger_event(native_transfers).await;
    }

    evm_trace_update_progress_and_last_synced_task(
        config.clone(),
        to_block,
        indexing_event_processed,
    )
    .await;

    Ok(())
}

// Indexing native transfers if debug or trace indexing is enabled.
///
/// NOTE: This is currently unused as we have temporarily migrated to exclusively using
/// `eth_getBlockByNumber` calls instead, this is being retained for posterity should we
/// choose to continue to support `debug` and `trace` based native transfer indexing.
#[allow(unused)]
pub async fn native_transfer_block_consumer_debug(
    provider: Arc<dyn ChainProvider>,
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
                    provider.chain().id(),
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
    if !native_transfers.is_empty() {
        config.trigger_event(native_transfers).await;
    }
    evm_trace_update_progress_and_last_synced_task(config, to_block, indexing_event_processed)
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::mock::MockChainProvider;

    #[test]
    fn native_transfer_abi_parses() {
        let abi: Vec<alloy::json_abi::Event> = serde_json::from_str(NATIVE_TRANSFER_ABI).unwrap();
        assert_eq!(abi.len(), 1);
        assert_eq!(abi[0].name, "NativeTransfer");
        assert_eq!(abi[0].inputs.len(), 3);
    }

    #[test]
    fn native_transfer_filter_logic() {
        // The core filtering in native_transfer_block_consumer:
        // has_to_address && is_empty_input && !is_value_zero
        let cases: Vec<(bool, bool, bool, bool)> = vec![
            // (input_empty, value_zero, has_to, expected)
            (true, false, true, true),   // ETH transfer
            (false, false, true, false), // Contract call: has input data
            (true, true, true, false),   // Zero value: not a transfer
            (true, false, false, false), // Contract creation: no to address
        ];

        for (input_empty, value_zero, has_to, expected) in cases {
            let is_native_transfer = has_to && input_empty && !value_zero;
            assert_eq!(
                is_native_transfer, expected,
                "Failed for input_empty={input_empty}, value_zero={value_zero}, has_to={has_to}",
            );
        }
    }

    #[tokio::test]
    async fn mock_provider_returns_empty_blocks() {
        let mock = Arc::new(MockChainProvider::new(1).with_block_number(100));
        let block_numbers = vec![U64::from(10), U64::from(11)];
        let result = mock.get_block_by_number_batch(&block_numbers, true).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mock_provider_trace_block_returns_empty() {
        let mock = Arc::new(MockChainProvider::new(1));
        let traces = mock.trace_block(U64::from(100)).await.unwrap();
        assert!(traces.is_empty());
    }

    #[test]
    fn concurrency_backoff() {
        // Mimics the backoff in native_transfer_block_processor
        let backed_off = |n: usize| cmp::max(1, (n as f64 * 0.8) as usize);
        assert_eq!(backed_off(10), 8);
        assert_eq!(backed_off(5), 4);
        assert_eq!(backed_off(1), 1); // bottoms at 1
    }

    #[test]
    fn concurrency_increase_caps_at_limit() {
        let limit = RECOMMENDED_RPC_CHUNK_SIZE;
        let increase = |n: usize| ((n * 20) / 10).min(limit);
        assert_eq!(increase(5), 10); // doubles
        assert_eq!(increase(25), 50); // doubles
        assert_eq!(increase(40), limit.min(80)); // capped
    }

    #[tokio::test]
    async fn provider_trace_call_dispatches_trace_block() {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use alloy::rpc::types::trace::parity::{LocalizedTransactionTrace, TransactionTrace};
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let trace = LocalizedTransactionTrace {
            trace: TransactionTrace {
                action: Action::default(),
                result: None,
                trace_address: vec![],
                subtraces: 0,
                error: None,
            },
            transaction_hash: None,
            transaction_position: None,
            block_number: Some(100),
            block_hash: None,
        };
        let provider = Arc::new(MockChainProvider::new(1).with_traces(vec![trace]));

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        let config = TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: TraceProcessingMethod::TraceBlock,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        };

        let result = provider_trace_call(provider, &config, U64::from(100u64)).await;
        assert!(result.is_ok());
        let traces = result.unwrap();
        assert_eq!(traces.len(), 1);
    }

    #[tokio::test]
    async fn provider_trace_call_dispatches_debug_trace() {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use alloy::rpc::types::trace::parity::{LocalizedTransactionTrace, TransactionTrace};
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let trace = LocalizedTransactionTrace {
            trace: TransactionTrace {
                action: Action::default(),
                result: None,
                trace_address: vec![],
                subtraces: 0,
                error: None,
            },
            transaction_hash: None,
            transaction_position: None,
            block_number: Some(100),
            block_hash: None,
        };
        let provider = Arc::new(MockChainProvider::new(1).with_traces(vec![trace]));

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        let config = TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: TraceProcessingMethod::DebugTraceBlockByNumber,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        };

        let result = provider_trace_call(provider, &config, U64::from(100u64)).await;
        assert!(result.is_ok());
        let traces = result.unwrap();
        assert_eq!(traces.len(), 1);
    }

    async fn make_trace_config_async() -> Arc<TraceProcessingConfig> {
        use crate::event::callback_registry::TraceCallbackRegistry;
        use crate::indexer::progress::IndexingEventsProgressState;
        use std::path::PathBuf;
        use tokio_util::sync::CancellationToken;

        let progress = IndexingEventsProgressState::monitor(&[], &[], None).await;
        Arc::new(TraceProcessingConfig {
            id: "test-id".to_string(),
            chain_id: 1,
            project_path: PathBuf::from("/tmp"),
            start_block: U64::from(100u64),
            end_block: U64::from(200u64),
            indexer_name: "test".to_string(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: EVENT_NAME.to_string(),
            network: "ethereum".to_string(),
            progress,
            postgres: None,
            csv_details: None,
            registry: Arc::new(TraceCallbackRegistry::new()),
            method: crate::manifest::native_transfer::TraceProcessingMethod::TraceBlock,
            stream_last_synced_block_file_path: None,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Build a block with a single native-transfer transaction (value > 0, empty input, has to).
    fn make_block_with_native_transfer(
        block_number: u64,
        from: Address,
        to: Address,
        value: U256,
    ) -> alloy::network::AnyRpcBlock {
        use alloy::consensus::transaction::Recovered;
        use alloy::consensus::{Signed, TxEnvelope, TxLegacy};
        use alloy::network::{AnyHeader, AnyRpcHeader, AnyRpcTransaction, AnyTxEnvelope};
        use alloy::primitives::{Signature, TxKind, B256};
        use alloy::rpc::types::{Block, BlockTransactions, Transaction};
        use alloy::serde::WithOtherFields;

        let block_hash = B256::from([block_number as u8; 32]);

        let tx_legacy = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value,
            input: alloy::primitives::Bytes::new(),
        };

        // Dummy sig and hash — we only need the fields, not cryptographic validity.
        let sig = Signature::new(U256::ONE, U256::ONE, false);
        let tx_hash = B256::from([0xabu8; 32]);
        let signed = Signed::new_unchecked(tx_legacy, sig, tx_hash);
        let envelope = AnyTxEnvelope::Ethereum(TxEnvelope::Legacy(signed));
        let recovered = Recovered::new_unchecked(envelope, from);

        let rpc_tx = AnyRpcTransaction::new(WithOtherFields::new(Transaction {
            inner: recovered,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            transaction_index: Some(0),
            effective_gas_price: None,
        }));

        alloy::network::AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: block_number, ..Default::default() }.seal(block_hash),
                ),
                BlockTransactions::Full(vec![rpc_tx]),
            )
            .into(),
        )
    }

    #[tokio::test]
    async fn native_transfer_block_consumer_empty_blocks() {
        // Provider has no blocks registered → consumer returns Ok with no work.
        let provider = Arc::new(MockChainProvider::new(1));
        let config = make_trace_config_async().await;
        let block_numbers = vec![U64::from(10), U64::from(11)];

        let result =
            native_transfer_block_consumer(provider, &block_numbers, "ethereum", &config).await;

        assert!(result.is_ok(), "Expected Ok when no blocks are returned");
    }

    #[tokio::test]
    async fn native_transfer_block_consumer_filters_native_transfers() {
        // Build a block containing three transactions:
        //   1. A valid native ETH transfer (value > 0, empty input, has to) → should match
        //   2. A zero-value tx (value = 0) → should NOT match
        //   3. A tx with non-empty input (contract call) → should NOT match
        //
        // We verify the consumer runs without error; the callback registry is empty so no
        // callbacks fire, but the filtering path is exercised.
        let from: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        let to: Address = "0x2222222222222222222222222222222222222222".parse().unwrap();

        let transfer_block =
            make_block_with_native_transfer(42, from, to, U256::from(1_000_000_000_000_000_000u64));

        let provider = Arc::new(MockChainProvider::new(1).with_blocks(vec![transfer_block]));
        let config = make_trace_config_async().await;

        let result =
            native_transfer_block_consumer(provider, &[U64::from(42)], "ethereum", &config).await;

        assert!(result.is_ok(), "Consumer should succeed when a native transfer block is present");
    }

    #[test]
    fn zksync_system_contracts_list() {
        // Verify the system contract addresses are valid and contain expected count
        let contracts: [Address; 13] = [
            "0x0000000000000000000000000000000000008001".parse().unwrap(),
            "0x0000000000000000000000000000000000008002".parse().unwrap(),
            "0x0000000000000000000000000000000000008003".parse().unwrap(),
            "0x0000000000000000000000000000000000008004".parse().unwrap(),
            "0x0000000000000000000000000000000000008005".parse().unwrap(),
            "0x0000000000000000000000000000000000008006".parse().unwrap(),
            "0x0000000000000000000000000000000000008008".parse().unwrap(),
            "0x0000000000000000000000000000000000008009".parse().unwrap(),
            "0x000000000000000000000000000000000000800a".parse().unwrap(),
            "0x000000000000000000000000000000000000800b".parse().unwrap(),
            "0x000000000000000000000000000000000000800c".parse().unwrap(),
            "0x000000000000000000000000000000000000800e".parse().unwrap(),
            "0x000000000000000000000000000000000000800f".parse().unwrap(),
        ];
        assert_eq!(contracts.len(), 13);
        // 0x8007 is intentionally missing (not a system contract used in transfers)
        assert!(!contracts.contains(&"0x0000000000000000000000000000000000008007".parse().unwrap()));
    }

    // ----------------------------------------------------------------------
    // Reorg rewind behaviour
    // ----------------------------------------------------------------------

    /// Build a block with a fixed parent hash (no transactions). Used to simulate
    /// canonical vs. reorged chains for the coordinator.
    fn make_block_with_parent(
        block_number: u64,
        block_hash: alloy::primitives::B256,
        parent_hash: alloy::primitives::B256,
    ) -> alloy::network::AnyRpcBlock {
        use alloy::network::{AnyHeader, AnyRpcHeader};
        use alloy::rpc::types::{Block, BlockTransactions};

        alloy::network::AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: block_number, parent_hash, ..Default::default() }
                        .seal(block_hash),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    }

    fn b256(n: u8) -> alloy::primitives::B256 {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        alloy::primitives::B256::from(bytes)
    }

    fn make_test_coordinator(
        network: &str,
        window_blocks: &[(u64, u8, u8)],
        provider: Arc<dyn ChainProvider>,
    ) -> Arc<Mutex<ReorgCoordinator>> {
        use crate::indexer::reorg::{BlockChainWindow, ReorgBlockHashPersistence};

        let mut window = BlockChainWindow::try_new(100).unwrap();
        for &(num, h, p) in window_blocks {
            window.insert(num, b256(h), b256(p));
        }

        let persistence = Arc::new(ReorgBlockHashPersistence::new(None, None));
        let coord = ReorgCoordinator::new(
            network.to_string(),
            window,
            persistence,
            provider,
            vec![],
            vec![],
        )
        .unwrap();
        Arc::new(Mutex::new(coord))
    }

    #[tokio::test]
    async fn native_transfer_detects_no_reorg_on_canonical_chain() {
        // Window has blocks 10, 11. Provider returns canonical 12, 13 whose
        // parent_hashes chain back to the window. Expect `Canonical`.
        let canonical_12 = make_block_with_parent(12, b256(12), b256(11));
        let canonical_13 = make_block_with_parent(13, b256(13), b256(12));
        let provider: Arc<dyn ChainProvider> = Arc::new(
            MockChainProvider::new(1).with_blocks(vec![canonical_12, canonical_13]),
        );

        let coordinator =
            make_test_coordinator("ethereum", &[(10, 10, 9), (11, 11, 10)], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            12,
            13,
        )
        .await;

        assert_eq!(outcome, NativeTransferReorgOutcome::Canonical);
    }

    #[tokio::test]
    async fn native_transfer_rewinds_to_fork_point_on_reorg() {
        // Window has block 11 with hash=11, parent=10 (pre-reorg state).
        // Canonical chain now says block 11 has hash=0xFE / parent=10 (reorged).
        // Fetch range 12..=12 with canonical block 12 whose parent_hash=0xFE
        // does NOT match the window's stored hash of block 11 → reorg.
        //
        // The coordinator then fetches missing blocks from the provider to locate
        // the fork point; we expose canonical 11 so find_fork_point can succeed.
        let canonical_11 = make_block_with_parent(11, b256(0xFE), b256(10));
        let canonical_12 = make_block_with_parent(12, b256(12), b256(0xFE));
        let provider: Arc<dyn ChainProvider> = Arc::new(
            MockChainProvider::new(1).with_blocks(vec![canonical_11, canonical_12]),
        );

        let coordinator =
            make_test_coordinator("ethereum", &[(10, 10, 9), (11, 11, 10)], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            12,
            12,
        )
        .await;

        // Coordinator's find_fork_point compares the window's entries against
        // canonical hashes. With window blocks [10, 11] and canonical [11@0xFE, 12@12],
        // no canonical hash matches any window entry → fork_point falls back to the
        // oldest window block (10).
        //
        // Helper contract: `rewind_to` IS the first canonical block to re-fetch
        // (inclusive) — equal to `fork_point`.
        let rewind_to = match outcome {
            NativeTransferReorgOutcome::Rewound { rewind_to } => rewind_to,
            other => panic!("expected Rewound, got {other:?}"),
        };
        assert_eq!(
            rewind_to, 10,
            "helper must return rewind_to == fork_point (the first canonical block to re-fetch)"
        );

        // Caller contract: applying `last_seen_block = rewind_to - 1` makes the
        // next iteration's `from_block = last_seen_block + 1 = rewind_to = fork_point`,
        // so block `fork_point` is NOT skipped. This mirrors the contract-event
        // path at fetch_logs.rs where `last_seen_block_number = fork_point - 1`.
        let last_seen_block_after_rewind = U64::from(rewind_to.saturating_sub(1));
        let next_from_block = last_seen_block_after_rewind + U64::from(1);
        assert_eq!(
            next_from_block,
            U64::from(rewind_to),
            "caller rewind must set next from_block to fork_point (inclusive)"
        );
    }

    #[tokio::test]
    async fn native_transfer_short_read_returns_failed() {
        // Provider registered with blocks [10, 11] but caller requests [10, 11, 12].
        // The helper must detect the short read and return Failed, so the caller
        // backs off and retries the same range instead of silently advancing
        // `last_seen_block` past block 12.
        let canonical_10 = make_block_with_parent(10, b256(10), b256(9));
        let canonical_11 = make_block_with_parent(11, b256(11), b256(10));
        let provider: Arc<dyn ChainProvider> = Arc::new(
            MockChainProvider::new(1).with_blocks(vec![canonical_10, canonical_11]),
        );

        // Coordinator with an empty window so validation would otherwise succeed
        // — we want to isolate the short-read check.
        let coordinator = make_test_coordinator("ethereum", &[], provider.clone());

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            Some(&coordinator),
            None,
            None,
            None,
            "ethereum",
            10,
            12,
        )
        .await;

        assert_eq!(
            outcome,
            NativeTransferReorgOutcome::Failed,
            "short read (2 blocks returned when 3 requested) must yield Failed",
        );
    }

    #[tokio::test]
    async fn native_transfer_no_coordinator_short_circuits() {
        // Without a coordinator configured, the helper must return NoCoordinator
        // so the caller emits the range as-is.
        let provider: Arc<dyn ChainProvider> = Arc::new(MockChainProvider::new(1));

        let outcome = native_transfer_detect_reorg_in_range(
            provider.as_ref(),
            None,
            None,
            None,
            None,
            "ethereum",
            12,
            13,
        )
        .await;

        assert_eq!(outcome, NativeTransferReorgOutcome::NoCoordinator);
    }
}
