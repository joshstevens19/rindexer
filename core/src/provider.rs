use crate::notifications::ChainStateNotification;
use alloy::network::{AnyNetwork, AnyRpcBlock, AnyTransactionReceipt};
use alloy::rpc::types::{Filter, ValueOrArray};
use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, Bytes, TxHash, U256, U64},
    providers::{
        ext::TraceApi,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, IpcConnect, Provider, ProviderBuilder, RootProvider,
    },
    rpc::{
        client::RpcClient,
        types::{
            trace::parity::{
                Action, CallAction, CallType, LocalizedTransactionTrace, TransactionTrace,
            },
            Log,
        },
    },
    transports::{
        http::{
            reqwest::{header::HeaderMap, Client, Error as ReqwestError},
            Http,
        },
        layers::RetryBackoffLayer,
        RpcError, TransportErrorKind,
    },
};
use alloy_chains::{Chain, NamedChain};
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::future::IntoFuture;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::{broadcast::Sender, Mutex, Semaphore};
use tokio::task::JoinError;
use tracing::{debug, debug_span, error, Instrument};
use url::Url;

use crate::helpers::chunk_hashset;
use crate::layer_extensions::RpcLoggingLayer;
use crate::manifest::network::{AddressFiltering, BlockPollFrequency};
use crate::{event::RindexerEventFilter, manifest::core::Manifest};

/// An alias type for a complex alloy Provider
pub type RindexerProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider<AnyNetwork>,
    AnyNetwork,
>;

// RPC providers have maximum supported addresses that can be provided in a filter
// We play safe and limit to 1000 by default, but that can be overridden in the configuration
const DEFAULT_RPC_SUPPORTED_ACCOUNT_FILTERS: usize = 1000;

/// Maximum RPC batching size available for the provider.
pub const RPC_CHUNK_SIZE: usize = 1000;

/// Recommended chunk sizes for batch RPC requests.
/// See: https://www.alchemy.com/docs/best-practices-when-using-alchemy#2-avoid-high-batch-cardinality
pub const RECOMMENDED_RPC_CHUNK_SIZE: usize = 50;

#[derive(Debug)]
pub struct JsonRpcCachedProvider {
    provider: Arc<RindexerProvider>,
    client: RpcClient,
    cache: Mutex<Option<(Instant, Arc<AnyRpcBlock>)>>,
    is_zk_chain: bool,
    pub chain: Chain,
    block_poll_frequency: Option<BlockPollFrequency>,
    address_filtering: Option<AddressFiltering>,
    pub max_block_range: Option<U64>,
    pub chain_state_notification: Option<Sender<ChainStateNotification>>,
}

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("Failed to make rpc request: {0}")]
    RequestFailed(#[from] RpcError<TransportErrorKind>),

    #[error("Failed to make batched rpc request: {0}")]
    BatchRequestFailed(#[from] JoinError),

    #[error("Failed to serialize rpc request data: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Unknown error: {0}")]
    CustomError(String),
}

/// TODO: This is a temporary type until we migrate to alloy
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedLog {
    #[serde(flatten)]
    pub inner: Log,
    #[serde(rename = "blockTimestamp")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCall {
    pub from: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas: Option<String>,
    #[serde(rename = "gasUsed")]
    pub gas_used: U256,
    pub to: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub input: Bytes,
    #[serde(default)]
    pub value: U256,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub calls: Vec<TraceCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCallFrame {
    /// Zksync chains do not return `tx_hash` in their call trace response.
    #[serde(rename = "txHash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<TxHash>,
    pub result: TraceCall,
}

/// A faster than network-call method for determining if a chain is Zk-Rollup.
fn is_known_zk_evm_compatible_chain(chain: Chain) -> Option<bool> {
    if let Some(name) = chain.named() {
        match name {
            // Known zkEVM-compatible chains
            NamedChain::Lens
            | NamedChain::ZkSync
            | NamedChain::Sophon
            | NamedChain::Abstract
            | NamedChain::Scroll
            | NamedChain::Linea => Some(true),

            // Known non-zkEVM chains
            NamedChain::Mainnet
            | NamedChain::Sepolia
            | NamedChain::Arbitrum
            | NamedChain::Soneium
            | NamedChain::Avalanche
            | NamedChain::Polygon
            | NamedChain::Hyperliquid
            | NamedChain::Blast
            | NamedChain::World
            | NamedChain::Unichain
            | NamedChain::Base
            | NamedChain::Optimism
            | NamedChain::ApeChain
            | NamedChain::BinanceSmartChain
            | NamedChain::Fantom
            | NamedChain::Cronos
            | NamedChain::Gnosis
            | NamedChain::BaseSepolia
            | NamedChain::Sonic
            | NamedChain::Metis
            | NamedChain::Celo
            | NamedChain::Plasma => Some(false),

            // Fallback for unknown chains
            _ => None,
        }
    } else {
        None
    }
}

impl JsonRpcCachedProvider {
    /// Return a duration for block poll caching based on user configuration.
    fn block_poll_frequency(&self) -> Duration {
        let Some(block_poll_frequency) = self.block_poll_frequency else {
            return Duration::from_millis(50);
        };

        match block_poll_frequency {
            BlockPollFrequency::Rapid => Duration::from_millis(50),
            BlockPollFrequency::PollRateMs { millis } => Duration::from_millis(millis),
            BlockPollFrequency::Division { divisor } => self
                .chain
                .average_blocktime_hint()
                .and_then(|t| t.checked_div(divisor))
                .unwrap_or(Duration::from_millis(50)),
            BlockPollFrequency::RpcOptimized => self
                .chain
                .average_blocktime_hint()
                .and_then(|t| t.checked_div(3))
                .map(|t| t.max(Duration::from_millis(500)))
                .unwrap_or(Duration::from_millis(1000)),
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn get_latest_block(&self) -> Result<Option<Arc<AnyRpcBlock>>, ProviderError> {
        let mut cache_guard = self.cache.lock().await;
        let cache_time = self.block_poll_frequency();

        // Fetches the latest block only if it is likely that a new block has been produced for
        // this specific network. Consider this to be equal to half the block-time.
        //
        // If we want to reduce RPC calls further at the cost of we could consider indexing delay we
        // could set this to block-time directly.
        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < cache_time {
                return Ok(Some(Arc::clone(block)));
            }
        }

        let latest_block = self
            .provider
            .get_block(BlockId::Number(BlockNumberOrTag::Latest))
            .into_future()
            .instrument(debug_span!("fetching latest block", name = ?self.chain.named()))
            .await?;

        if let Some(block) = latest_block {
            let arc_block = Arc::new(block);
            *cache_guard = Some((Instant::now(), Arc::clone(&arc_block)));
            return Ok(Some(arc_block));
        } else {
            *cache_guard = None;
        }

        Ok(None)
    }

    #[tracing::instrument(skip_all)]
    pub async fn get_block_number(&self) -> Result<U64, ProviderError> {
        let number = self.provider.get_block_number().await?;
        Ok(U64::from(number))
    }

    /// Prefer using `trace_block` where possible as it returns more information.
    ///
    /// The current ethers version does not allow batching, we should upgrade to alloy.
    ///
    /// # Example of `alloy` supported fetch
    ///
    /// ```rs
    ///  let options = if self.is_zk_chain {
    ///       GethDebugTracingOptions::call_tracer(CallConfig::default())
    ///   } else {
    ///       GethDebugTracingOptions::call_tracer(CallConfig::default().only_top_call())
    ///   };
    ///
    ///   let valid_traces = self.provider.debug_trace_block_by_number(
    ///       BlockNumberOrTag::Number(block_number.as_limbs()[0]),
    ///       options,
    ///   ).await?;
    /// ```
    #[tracing::instrument(skip_all)]
    pub async fn debug_trace_block_by_number(
        &self,
        block_number: U64,
    ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
        // TODO: Consider the need to use `arbtrace_block` for early arbitrum blocks?
        let block = json!(serde_json::to_string_pretty(&block_number)?.replace("\"", ""));
        let options = if self.is_zk_chain {
            json!({ "tracer": "callTracer" })
        } else {
            json!({ "tracer": "callTracer", "tracerConfig": { "onlyTopCall": true } })
        };

        let valid_traces: Vec<TraceCallFrame> =
            self.provider.raw_request("debug_traceBlockByNumber".into(), [block, options]).await?;

        let mut flattened_calls = Vec::new();

        for trace in valid_traces {
            flattened_calls.push(TraceCallFrame {
                tx_hash: trace.tx_hash,
                result: TraceCall { calls: vec![], ..trace.result },
            });

            let mut stack = vec![];
            stack.extend(trace.result.calls.into_iter());

            while let Some(call) = stack.pop() {
                flattened_calls.push(TraceCallFrame {
                    tx_hash: None,
                    result: TraceCall { calls: vec![], ..call },
                });
                stack.extend(call.calls.into_iter());
            }
        }

        let traces = flattened_calls
            .into_iter()
            .filter_map(|frame| {
                // It's not clear in what situation this is None, but it does happen so it's
                // better to avoid deserialization errors for now and remove them from the list.
                //
                // We know they cannot be a valid native token transfer.
                if let Some(to) = frame.result.to {
                    Some(LocalizedTransactionTrace {
                        trace: TransactionTrace {
                            action: Action::Call(CallAction {
                                from: frame.result.from,
                                to,
                                value: frame.result.value,
                                gas: frame
                                    .result
                                    .gas
                                    .and_then(|a| {
                                        U64::from_str_radix(a.trim_start_matches("0x"), 16).ok()
                                    })
                                    .unwrap_or_default()
                                    .as_limbs()[0],
                                input: frame.result.input,
                                call_type: CallType::Call,
                            }),
                            result: None,
                            trace_address: vec![],
                            subtraces: 0,
                            error: frame.result.error,
                        },
                        transaction_hash: frame.tx_hash,
                        transaction_position: None, // not provided by debug_trace
                        block_number: Some(block_number.as_limbs()[0]),
                        block_hash: None, // not provided by debug_trace
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(traces)
    }

    /// Request `trace_block` information. This currently does not support batched multi-calls.
    #[tracing::instrument(skip_all)]
    pub async fn trace_block(
        &self,
        block_number: U64,
    ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
        let traces = self
            .provider
            .trace_block(BlockId::Number(BlockNumberOrTag::Number(block_number.as_limbs()[0])))
            .await?;

        Ok(traces)
    }

    /// Makes an `eth_call` request at a specific block for view function calls.
    ///
    /// # Arguments
    /// * `to` - The contract address to call
    /// * `data` - The encoded calldata
    /// * `block_number` - The block number at which to execute the call
    ///
    /// # Returns
    /// The raw hex-encoded result bytes from the call
    #[tracing::instrument(skip_all)]
    pub async fn eth_call(
        &self,
        to: alloy::primitives::Address,
        data: alloy::primitives::Bytes,
        block_number: u64,
    ) -> Result<String, ProviderError> {
        let result: String = self
            .provider
            .raw_request(
                "eth_call".into(),
                (
                    serde_json::json!({
                        "to": format!("{:?}", to),
                        "data": format!("0x{}", hex::encode(&data)),
                    }),
                    format!("0x{:x}", block_number),
                ),
            )
            .await?;

        Ok(result)
    }

    /// Fetches blocks in concurrent rpc batches.
    #[tracing::instrument(skip_all, fields(len = block_numbers.len()))]
    pub async fn get_block_by_number_batch(
        &self,
        block_numbers: &[U64],
        include_txs: bool,
    ) -> Result<Vec<AnyRpcBlock>, ProviderError> {
        let chain_id = self.chain.id();

        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }

        let mut block_numbers = block_numbers.to_vec();
        block_numbers.dedup();

        // Max concurrency within an oversized batch request
        let semaphore = Arc::new(Semaphore::new(2));

        let futures = block_numbers
            .chunks(RECOMMENDED_RPC_CHUNK_SIZE)
            .map(|chunk| {
                let client = self.client.clone();
                let owned_chunk = chunk.to_vec();
                let semaphore = semaphore.clone();

                tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.expect("Semaphore closed");
                    let mut batch = client.new_batch();
                    let mut request_futures = Vec::with_capacity(owned_chunk.len());

                    for block_num in owned_chunk {
                        let params = (BlockNumberOrTag::Number(block_num.to()), include_txs);
                        let call = batch.add_call("eth_getBlockByNumber", &params)?;
                        request_futures.push(call)
                    }

                    if let Err(e) = batch.send().await {
                        error!(
                            "Failed to send {} batch 'eth_getBlockByNumber' request for {}: {:?}",
                            request_futures.len(),
                            chain_id,
                            e
                        );
                        return Err(e);
                    }

                    try_join_all(request_futures).await
                })
            })
            .collect::<Vec<_>>();

        let chunk_results: Vec<Result<Vec<AnyRpcBlock>, _>> = try_join_all(futures).await?;
        let results = chunk_results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(results)
    }

    /// Fetch tx receipts in a batch rpc call
    #[tracing::instrument(skip_all)]
    pub async fn get_tx_receipts_batch(
        &self,
        hashes: &[TxHash],
    ) -> Result<Vec<AnyTransactionReceipt>, ProviderError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let futures = hashes
            .chunks(RPC_CHUNK_SIZE)
            .map(|chunk| {
                let client = self.client.clone();
                let owned_chunk = chunk.to_vec();

                tokio::spawn(async move {
                    let mut batch = client.new_batch();
                    let mut request_futures = Vec::with_capacity(owned_chunk.len());

                    for hash in owned_chunk {
                        let call = batch.add_call(
                            "eth_getTransactionReceipt",
                            &(
                                hash,
                                /* one element tuple from dangling comma */
                            ),
                        )?;
                        request_futures.push(call)
                    }

                    if let Err(e) = batch.send().await {
                        error!("Failed to send batch tx receipt request: {:?}", e);
                        return Err(e);
                    }

                    try_join_all(request_futures).await
                })
            })
            .collect::<Vec<_>>();

        let chunk_results: Vec<Result<Vec<AnyTransactionReceipt>, _>> =
            try_join_all(futures).await?;
        let results = chunk_results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(results)
    }

    #[tracing::instrument(skip_all)]
    pub async fn get_logs(
        &self,
        event_filter: &RindexerEventFilter,
    ) -> Result<Vec<Log>, ProviderError> {
        let addresses = event_filter.contract_addresses().await;

        let base_filter = Filter::new()
            .event_signature(event_filter.event_signature())
            .topic1(event_filter.topic1())
            .topic2(event_filter.topic2())
            .topic3(event_filter.topic3())
            .from_block(event_filter.from_block())
            .to_block(event_filter.to_block());

        let logs = match addresses {
            // no addresses, which means nothing to get
            // different rpc providers implement an empty array differently,
            // therefore, we assume an empty addresses array means no events to fetch
            Some(addresses) if addresses.is_empty() => Ok(vec![]),
            Some(addresses) => match self.address_filtering {
                Some(AddressFiltering::InMemory) => {
                    self.get_logs_for_address_in_memory(&base_filter, addresses).await
                }
                Some(AddressFiltering::MaxAddressPerGetLogsRequest(
                    max_address_per_get_logs_request,
                )) => {
                    self.get_logs_for_address_in_batches(
                        &base_filter,
                        addresses,
                        max_address_per_get_logs_request,
                    )
                    .await
                }
                None => {
                    self.get_logs_for_address_in_batches(
                        &base_filter,
                        addresses,
                        DEFAULT_RPC_SUPPORTED_ACCOUNT_FILTERS,
                    )
                    .await
                }
            },
            None => Ok(self.provider.get_logs(&base_filter).await?),
        };

        logs
    }

    /// Get logs by chunking addresses and fetching asynchronously in batches
    #[tracing::instrument(skip_all)]
    async fn get_logs_for_address_in_batches(
        &self,
        filter: &Filter,
        addresses: HashSet<Address>,
        chunk_size: usize,
    ) -> Result<Vec<Log>, ProviderError> {
        let address_chunks = chunk_hashset(addresses, chunk_size);

        let logs_futures = address_chunks.into_iter().map(|chunk| async move {
            let filter =
                filter.clone().address(ValueOrArray::Array(chunk.into_iter().collect::<Vec<_>>()));
            self.provider.get_logs(&filter).await
        });

        let chunked_logs = try_join_all(logs_futures).await?;

        Ok(chunked_logs.concat())
    }

    /// Gets all logs for a given filter and then filters by addresses in memory
    #[tracing::instrument(skip_all)]
    async fn get_logs_for_address_in_memory(
        &self,
        filter: &Filter,
        addresses: HashSet<Address>,
    ) -> Result<Vec<Log>, ProviderError> {
        let logs = self.provider.get_logs(filter).await?;

        let filtered_logs =
            logs.into_iter().filter(|log| addresses.contains(&log.address())).collect::<Vec<_>>();

        Ok(filtered_logs)
    }

    pub fn get_inner_provider(&self) -> Arc<RindexerProvider> {
        Arc::clone(&self.provider)
    }

    pub fn get_chain_state_notification(&self) -> Option<Sender<ChainStateNotification>> {
        self.chain_state_notification.clone()
    }

    #[cfg(test)]
    pub fn mock(chain_id: u64) -> Arc<Self> {
        let chain = Chain::from(chain_id);
        let client = RpcClient::new_http(
            Url::parse("http://localhost:8545").expect("mock URL must be valid"),
        );
        let provider =
            ProviderBuilder::new().network::<AnyNetwork>().connect_client(client.clone());
        let is_zk_chain = is_known_zk_evm_compatible_chain(chain).unwrap_or(false);

        Arc::new(Self {
            provider: Arc::new(provider),
            client,
            cache: Mutex::new(None),
            is_zk_chain,
            chain,
            block_poll_frequency: None,
            address_filtering: None,
            max_block_range: None,
            chain_state_notification: None,
        })
    }
}
#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("Provider can't be created for {0}: {1}")]
    ProviderCantBeCreated(String, String),

    #[error("Invalid client chain id for {0}. Expected {1}, received {2}")]
    InvalidClientChainId(String, u64, u64),

    #[error("Could not build client: {0}")]
    CouldNotBuildClient(#[from] ReqwestError),

    #[error("Could not connect to client for chain_id: {0}")]
    CouldNotConnectClient(#[from] RpcError<TransportErrorKind>),

    #[error("Could not start reth node for network {0}: {1}")]
    RethNodeStartError(String, String),
}

#[allow(clippy::too_many_arguments)]
pub async fn create_client(
    rpc_url: &str,
    chain_id: u64,
    compute_units_per_second: Option<u64>,
    max_block_range: Option<U64>,
    block_poll_frequency: Option<BlockPollFrequency>,
    custom_headers: HeaderMap,
    address_filtering: Option<AddressFiltering>,
    chain_state_notification: Option<Sender<ChainStateNotification>>,
) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
    let chain = Chain::from(chain_id);

    let (client, provider) = if rpc_url.ends_with(".ipc") {
        let ipc = IpcConnect::new(rpc_url.to_string());
        let retry_layer =
            RetryBackoffLayer::new(5000, 1000, compute_units_per_second.unwrap_or(660));
        let logging_layer = RpcLoggingLayer::new(chain_id, rpc_url.to_string());

        let rpc_client =
            RpcClient::builder().layer(retry_layer).layer(logging_layer).ipc(ipc.clone()).await?;

        let provider =
            ProviderBuilder::new().network::<AnyNetwork>().connect_ipc(ipc).await.map_err(|e| {
                RetryClientError::ProviderCantBeCreated(
                    rpc_url.to_string(),
                    format!("IPC connection failed: {e}"),
                )
            })?;

        (rpc_client, provider)
    } else {
        let rpc_url = Url::parse(rpc_url).map_err(|e| {
            RetryClientError::ProviderCantBeCreated(rpc_url.to_string(), e.to_string())
        })?;

        let client_with_auth = Client::builder()
            .default_headers(custom_headers)
            .timeout(Duration::from_secs(90))
            .build()?;

        let logging_layer = RpcLoggingLayer::new(chain_id, rpc_url.to_string());
        let http = Http::with_client(client_with_auth, rpc_url);
        let retry_layer =
            RetryBackoffLayer::new(5000, 1000, compute_units_per_second.unwrap_or(660));
        let rpc_client =
            RpcClient::builder().layer(retry_layer).layer(logging_layer).transport(http, false);
        let provider =
            ProviderBuilder::new().network::<AnyNetwork>().connect_client(rpc_client.clone());

        (rpc_client, provider)
    };

    let real_rpc_chain_id = provider.get_chain_id().await.map_err(|e| {
        RetryClientError::CouldNotConnectClient(RpcError::LocalUsageError(Box::new(e)))
    })?;

    if real_rpc_chain_id != chain_id {
        return Err(RetryClientError::InvalidClientChainId(
            rpc_url.to_string(),
            chain_id,
            real_rpc_chain_id,
        ));
    }

    let is_zk_chain = match is_known_zk_evm_compatible_chain(chain) {
        Some(zk) => zk,
        None => {
            let response: Result<String, _> =
                provider.raw_request("zks_L1ChainId".into(), [&()]).await;
            let is_zk_chain = response.is_ok();
            if is_zk_chain {
                debug!("Chain {} is zk chain. Trace indexing adjusted if enabled.", chain_id);
            }

            is_zk_chain
        }
    };

    Ok(Arc::new(JsonRpcCachedProvider {
        provider: Arc::new(provider),
        cache: Mutex::new(None),
        max_block_range,
        client,
        chain,
        is_zk_chain,
        block_poll_frequency,
        address_filtering,
        chain_state_notification,
    }))
}

pub async fn get_chain_id(rpc_url: &str) -> Result<U256, RpcError<TransportErrorKind>> {
    let url = Url::parse(rpc_url).map_err(|e| RpcError::LocalUsageError(Box::new(e)))?;
    let provider = ProviderBuilder::new().connect_http(url);
    let call = provider.get_chain_id().await?;

    Ok(U256::from(call))
}

#[derive(Debug)]
pub struct CreateNetworkProvider {
    pub network_name: String,
    pub disable_logs_bloom_checks: bool,
    pub client: Arc<JsonRpcCachedProvider>,
}

impl CreateNetworkProvider {
    pub async fn create(
        manifest: &Manifest,
    ) -> Result<Vec<CreateNetworkProvider>, RetryClientError> {
        let provider_futures = manifest.networks.iter().map(|network| async move {
            #[cfg(not(feature = "reth"))]
            let provider_url = network.rpc.clone();

            #[cfg(not(feature = "reth"))]
            let reth_tx: Option<Sender<ChainStateNotification>> = None;

            #[cfg(feature = "reth")]
            // if reth is enabled for this network, we need to start the reth node.
            // once reth is started, we can use the reth ipc path to create a provider.
            let reth_tx = network.try_start_reth_node().await.map_err(|e| {
                RetryClientError::RethNodeStartError(network.name.clone(), e.to_string())
            })?;

            // if reth is enabled and started successfully, we can use the reth ipc path to create a provider.
            // else, we will use the rpc url provided in the manifest.
            #[cfg(feature = "reth")]
            let provider_url = if reth_tx.is_some() {
                network.get_reth_ipc_path().unwrap()
            } else {
                network.rpc.clone()
            };

            // create the provider
            let provider = create_client(
                &provider_url,
                network.chain_id,
                network.compute_units_per_second,
                network.max_block_range,
                network.block_poll_frequency,
                manifest.get_custom_headers(),
                network.get_logs_settings.clone().map(|settings| settings.address_filtering),
                reth_tx.clone(),
            )
            .await?;

            Ok::<_, RetryClientError>(CreateNetworkProvider {
                network_name: network.name.clone(),
                disable_logs_bloom_checks: network.disable_logs_bloom_checks.unwrap_or_default(),
                client: provider,
            })
        });

        try_join_all(provider_futures).await
    }

    /// Get the chain state notification for this network
    pub fn chain_state_notification(&self) -> Option<Sender<ChainStateNotification>> {
        self.client.chain_state_notification.clone()
    }
}

/// Get a provider for a specific network
pub fn get_network_provider<'a>(
    network: &str,
    providers: &'a [CreateNetworkProvider],
) -> Option<&'a CreateNetworkProvider> {
    providers.iter().find(|item| item.network_name == network)
}
