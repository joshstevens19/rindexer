use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use ethers::{
    middleware::Middleware,
    prelude::{Bytes, Log},
    providers::{Http, Provider, ProviderError, RetryClient, RetryClientBuilder},
    types::{
        Action, ActionType, Address, Block, BlockNumber, Call, CallType, Trace, H256, U256, U64,
    },
};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::error;
use url::Url;

use crate::{event::RindexerEventFilter, manifest::core::Manifest};

#[derive(Debug)]
pub struct JsonRpcCachedProvider {
    provider: Arc<Provider<RetryClient<Http>>>,
    cache: Mutex<Option<(Instant, Arc<Block<H256>>)>>,
    pub max_block_range: Option<U64>,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct TraceCall {
    pub from: Address,
    pub gas: String,
    #[serde(rename = "gasUsed")]
    pub gas_used: U256,
    pub to: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub input: Bytes,
    pub value: U256,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TraceCallFrame {
    #[serde(rename = "txHash")]
    pub tx_hash: H256,
    pub result: TraceCall,
}

impl JsonRpcCachedProvider {
    pub fn new(provider: Provider<RetryClient<Http>>, max_block_range: Option<U64>) -> Self {
        JsonRpcCachedProvider {
            provider: Arc::new(provider),
            cache: Mutex::new(None),
            max_block_range,
        }
    }

    pub async fn get_latest_block(&self) -> Result<Option<Arc<Block<H256>>>, ProviderError> {
        let mut cache_guard = self.cache.lock().await;

        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < Duration::from_millis(50) {
                return Ok(Some(Arc::clone(block)));
            }
        }

        let latest_block = self.provider.get_block(BlockNumber::Latest).await?;

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
        self.provider.get_block_number().await
    }

    /// Prefer using `trace_block` where possible as it returns more information.
    ///
    /// The current ethers version does not allow batching, we should upgrade to alloy.
    pub async fn debug_trace_block_by_number(
        &self,
        block_number: U64,
    ) -> Result<Vec<Trace>, ProviderError> {
        let block = json!(serde_json::to_string_pretty(&block_number)?.replace("\"", ""));
        let options = json!({
            "tracer": "callTracer",
            "tracerConfig": {
                "onlyTopCall": true
            }
        });

        let valid_traces: Vec<TraceCallFrame> =
            self.provider.request("debug_traceBlockByNumber", [block, options]).await?;

        let traces = valid_traces
            .into_iter()
            .map(|frame| Trace {
                action: Action::Call(Call {
                    from: frame.result.from,
                    to: frame.result.to,
                    value: frame.result.value,
                    gas: U256::from_str_radix(frame.result.gas.trim_start_matches("0x"), 16)
                        .unwrap_or_default(),
                    input: frame.result.input,
                    call_type: CallType::Call,
                }),
                result: None,
                trace_address: vec![],
                subtraces: 0,
                transaction_hash: Some(frame.tx_hash),
                transaction_position: None, // not provided by debug_trace
                block_number: block_number.as_u64(),
                block_hash: H256::zero(), // not provided by debug_trace
                error: frame.result.error,
                action_type: ActionType::Call,
            })
            .collect();

        Ok(traces)
    }

    /// Request `trace_block` information. This currently does not support batched multi-calls.
    pub async fn trace_block(&self, block_number: U64) -> Result<Vec<Trace>, ProviderError> {
        self.provider.trace_block(BlockNumber::Number(block_number)).await
    }

    pub async fn get_logs(
        &self,
        filter: &RindexerEventFilter,
    ) -> Result<Vec<WrappedLog>, ProviderError> {
        // rindexer_info!("get_logs DEBUG [{:?}]", filter.raw_filter());
        // LEAVING FOR NOW CONTEXT: TEMP FIX TO MAKE SURE FROM BLOCK IS ALWAYS SET
        // let mut filter = filter.raw_filter().clone();
        // if filter.get_from_block().is_none() {
        //     filter = filter.from_block(BlockNumber::Earliest);
        // }
        // rindexer_info!("get_logs DEBUG AFTER [{:?}]", filter);
        let result = self.provider.request("eth_getLogs", [filter.raw_filter()]).await?;
        // rindexer_info!("get_logs RESULT [{:?}]", result);
        Ok(result)
    }

    pub async fn get_chain_id(&self) -> Result<U256, ProviderError> {
        self.provider.get_chainid().await
    }

    pub fn get_inner_provider(&self) -> Arc<Provider<RetryClient<Http>>> {
        Arc::clone(&self.provider)
    }
}
#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("http provider can't be created for {0}: {1}")]
    HttpProviderCantBeCreated(String, String),

    #[error("Could not build client: {0}")]
    CouldNotBuildClient(#[from] reqwest::Error),
}

pub fn create_client(
    rpc_url: &str,
    compute_units_per_second: Option<u64>,
    max_block_range: Option<U64>,
    custom_headers: HeaderMap,
) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;
    let client = reqwest::Client::builder().default_headers(custom_headers).build()?;

    let provider = Http::new_with_client(url, client);

    let retry_client = RetryClientBuilder::default()
        // assume minimum compute units per second if not provided as growth plan standard
        .compute_units_per_second(compute_units_per_second.unwrap_or(660))
        .rate_limit_retries(5000)
        .timeout_retries(1000)
        .initial_backoff(Duration::from_millis(500))
        .build(provider, Box::<ethers::providers::HttpRateLimitRetryPolicy>::default());
    let instance = Provider::new(retry_client);

    Ok(Arc::new(JsonRpcCachedProvider::new(instance, max_block_range)))
}

pub async fn get_chain_id(rpc_url: &str) -> Result<U256, ProviderError> {
    let url = Url::parse(rpc_url).map_err(|_| ProviderError::UnsupportedRPC)?;
    let provider = Provider::new(Http::new(url));

    provider.get_chainid().await
}

#[derive(Debug)]
pub struct CreateNetworkProvider {
    pub network_name: String,
    pub disable_logs_bloom_checks: bool,
    pub client: Arc<JsonRpcCachedProvider>,
}

impl CreateNetworkProvider {
    pub fn create(manifest: &Manifest) -> Result<Vec<CreateNetworkProvider>, RetryClientError> {
        let mut result: Vec<CreateNetworkProvider> = vec![];
        for network in &manifest.networks {
            let provider = create_client(
                &network.rpc,
                network.compute_units_per_second,
                network.max_block_range,
                manifest.get_custom_headers(),
            )?;
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

    #[test]
    fn test_create_retry_client() {
        let rpc_url = "http://localhost:8545";
        let result = create_client(rpc_url, Some(660), None, HeaderMap::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_client(rpc_url, Some(660), None, HeaderMap::new());
        assert!(result.is_err());
        if let Err(RetryClientError::HttpProviderCantBeCreated(url, _)) = result {
            assert_eq!(url, rpc_url);
        } else {
            panic!("Expected HttpProviderCantBeCreated error");
        }
    }
}
