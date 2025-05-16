use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::{Address, Bytes, TxHash, U256, U64},
    providers::{
        ext::TraceApi,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
        Identity, Provider, ProviderBuilder, RootProvider,
    },
    rpc::{
        client::RpcClient,
        types::{
            trace::parity::{
                Action, CallAction, CallType, LocalizedTransactionTrace, TransactionTrace,
            },
            Block, Log,
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
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{error, info};
use url::Url;

use crate::{event::RindexerEventFilter, manifest::core::Manifest};

/// An alias type for a complex alloy Provider
pub type RindexerProvider = FillProvider<
    JoinFill<
        Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

#[derive(Debug)]
pub struct JsonRpcCachedProvider {
    provider: Arc<RindexerProvider>,
    cache: Mutex<Option<(Instant, Arc<Block>)>>,
    is_zk_chain: bool,
    #[allow(unused)]
    chain_id: u64,
    pub max_block_range: Option<U64>,
}

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("Failed to make rpc request: {0}")]
    RequestFailed(#[from] RpcError<TransportErrorKind>),

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
    pub gas: String,
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

impl JsonRpcCachedProvider {
    pub async fn new(
        provider: RindexerProvider,
        chain_id: u64,
        max_block_range: Option<U64>,
    ) -> Self {
        let response: Result<String, _> = provider.raw_request("zks_L1ChainId".into(), [&()]).await;
        let is_zk_chain = response.is_ok();

        if is_zk_chain {
            info!("Chain {} is zk chain. Trace indexing adjusted.", chain_id);
        }

        JsonRpcCachedProvider {
            provider: Arc::new(provider),
            cache: Mutex::new(None),
            max_block_range,
            chain_id,
            is_zk_chain,
        }
    }

    pub async fn get_latest_block(&self) -> Result<Option<Arc<Block>>, ProviderError> {
        let mut cache_guard = self.cache.lock().await;

        // TODO-TODO
        //
        // This is potentially called way too much. Consider dropping it back.
        // Also ensure this isn't duplicated by each contract-event.
        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < Duration::from_millis(50) {
                return Ok(Some(Arc::clone(block)));
            }
        }

        let latest_block =
            self.provider.get_block(BlockId::Number(BlockNumberOrTag::Latest)).await?;

        if let Some(block) = latest_block {
            let arc_block = Arc::new(block);
            *cache_guard = Some((Instant::now(), Arc::clone(&arc_block)));
            return Ok(Some(arc_block));
        } else {
            *cache_guard = None;
        }

        Ok(None)
    }

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
                                gas: U64::from_str_radix(
                                    frame.result.gas.trim_start_matches("0x"),
                                    16,
                                )
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

    pub async fn get_logs(&self, filter: &RindexerEventFilter) -> Result<Vec<Log>, ProviderError> {
        let logs = self.provider.get_logs(filter.raw_filter()).await?;
        Ok(logs)

        // rindexer_info!("get_logs DEBUG [{:?}]", filter.raw_filter());
        // LEAVING FOR NOW CONTEXT: TEMP FIX TO MAKE SURE FROM BLOCK IS ALWAYS SET
        // let mut filter = filter.raw_filter().clone();
        // if filter.get_from_block().is_none() {
        //     filter = filter.from_block(BlockNumber::Earliest);
        // }
        // rindexer_info!("get_logs DEBUG AFTER [{:?}]", filter);
        // let result = self.provider.raw_request("eth_getLogs".into(),
        // [filter.raw_filter()]).await?; rindexer_info!("get_logs RESULT [{:?}]", result);
        // Ok(result)
    }

    pub async fn get_chain_id(&self) -> Result<U256, ProviderError> {
        let chain_id = self.provider.get_chain_id().await?;
        Ok(U256::from(chain_id))
    }

    pub fn get_inner_provider(&self) -> Arc<RindexerProvider> {
        Arc::clone(&self.provider)
    }
}
#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("http provider can't be created for {0}: {1}")]
    HttpProviderCantBeCreated(String, String),

    #[error("Could not build client: {0}")]
    CouldNotBuildClient(#[from] ReqwestError),

    #[error("Could not connect to client for chain_id: {0}")]
    CouldNotConnectClient(#[from] RpcError<TransportErrorKind>),
}

pub async fn create_client(
    rpc_url: &str,
    chain_id: u64,
    compute_units_per_second: Option<u64>,
    max_block_range: Option<U64>,
    custom_headers: HeaderMap,
) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
    let rpc_url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;

    let client_with_auth = Client::builder().default_headers(custom_headers).build()?;
    let http = Http::with_client(client_with_auth, rpc_url);
    let retry_layer = RetryBackoffLayer::new(5000, 500, compute_units_per_second.unwrap_or(660));
    let rpc_client = RpcClient::builder().layer(retry_layer).transport(http, false);
    let provider = ProviderBuilder::new().connect_client(rpc_client);

    Ok(Arc::new(JsonRpcCachedProvider::new(provider, chain_id, max_block_range).await))
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
        let mut result: Vec<CreateNetworkProvider> = vec![];
        for network in &manifest.networks {
            let provider = create_client(
                &network.rpc,
                network.chain_id,
                network.compute_units_per_second,
                network.max_block_range,
                manifest.get_custom_headers(),
            )
            .await?;
            result.push(CreateNetworkProvider {
                network_name: network.name.clone(),
                disable_logs_bloom_checks: network.disable_logs_bloom_checks.unwrap_or_default(),
                client: provider,
            });
        }

        Ok(result)
    }
}

/// Get a provider for a specific network
pub fn get_network_provider<'a>(
    network: &str,
    providers: &'a [CreateNetworkProvider],
) -> Option<&'a CreateNetworkProvider> {
    providers.iter().find(|item| item.network_name == network)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_retry_client() {
        let rpc_url = "http://localhost:8545";
        let result = create_client(rpc_url, 1, Some(660), None, HeaderMap::new()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_client(rpc_url, 1, Some(660), None, HeaderMap::new()).await;
        assert!(result.is_err());
        if let Err(RetryClientError::HttpProviderCantBeCreated(url, _)) = result {
            assert_eq!(url, rpc_url);
        } else {
            panic!("Expected HttpProviderCantBeCreated error");
        }
    }
}
