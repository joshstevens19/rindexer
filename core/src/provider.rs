use std::{
    any::Any,
    collections::BTreeSet,
    fmt::Debug,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use arrayvec::ArrayVec;
use async_trait::async_trait;
use ethers::{
    middleware::Middleware,
    prelude::{Log, ValueOrArray},
    providers::{Http, Provider, ProviderError, RetryClient, RetryClientBuilder},
    types::{Block, BlockNumber, FilterBlockOption, H256, U256, U64},
};
use hypersync_client::{
    net_types::{FieldSelection, Query},
    preset_query::blocks_and_transactions,
    to_ethers::TryIntoEthers,
    Client, ClientConfig,
};
use hypersync_format::{FixedSizeData, LogArgument};
use hypersync_net_types::{BlockSelection, JoinMode, LogSelection};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    event::RindexerEventFilter,
    manifest::{core::Manifest, network::ProviderType},
};

#[async_trait]
pub trait ProviderInterface: Send + Sync + Debug {
    async fn get_latest_block(&self) -> Result<Option<Arc<Block<H256>>>, ProviderError>;

    async fn get_chain_id(&self) -> Result<U256, ProviderError>;

    async fn get_block_number(&self) -> Result<U64, ProviderError>;

    async fn get_logs(&self, filter: &RindexerEventFilter) -> Result<Vec<Log>, ProviderError>;

    fn max_block_range(&self) -> Option<U64>;

    fn as_any(&self) -> &dyn Any;
}

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

impl JsonRpcCachedProvider {
    pub fn new(provider: Provider<RetryClient<Http>>, max_block_range: Option<U64>) -> Self {
        JsonRpcCachedProvider {
            provider: Arc::new(provider),
            cache: Mutex::new(None),
            max_block_range,
        }
    }

    pub fn get_inner_provider(&self) -> Arc<Provider<RetryClient<Http>>> {
        Arc::clone(&self.provider)
    }
}

#[async_trait]
impl ProviderInterface for JsonRpcCachedProvider {
    async fn get_latest_block(&self) -> Result<Option<Arc<Block<H256>>>, ProviderError> {
        let mut cache_guard = self.cache.lock().await;

        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < Duration::from_millis(300) {
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

    async fn get_chain_id(&self) -> Result<U256, ProviderError> {
        self.provider.get_chainid().await
    }

    async fn get_block_number(&self) -> Result<U64, ProviderError> {
        self.provider.get_block_number().await
    }

    async fn get_logs(
        &self,
        filter: &RindexerEventFilter,
    ) -> Result<Vec<WrappedLog>, ProviderError> {
        self.provider.request("eth_getLogs", [filter.raw_filter()]).await
    }

    fn max_block_range(&self) -> Option<U64> {
        self.max_block_range
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
#[derive(Debug, Clone)]
pub struct HyperSyncProvider {
    provider: Arc<Client>,
    pub max_block_range: Option<U64>,
}

impl HyperSyncProvider {
    pub fn new(provider: Client, max_block_range: Option<U64>) -> Self {
        HyperSyncProvider { provider: Arc::new(provider), max_block_range }
    }
}

#[async_trait]
impl ProviderInterface for HyperSyncProvider {
    async fn get_latest_block(&self) -> Result<Option<Arc<Block<H256>>>, ProviderError> {
        let latest_block = self
            .provider
            .get_height()
            .await
            .map_err(|err| ProviderError::CustomError(err.to_string()))?;

        let query = blocks_and_transactions(latest_block, Some(latest_block + 1));

        let data = self
            .provider
            .clone()
            .collect(query, Default::default())
            .await
            .map_err(|err| ProviderError::CustomError(err.to_string()))?;

        let block = data.data.blocks.iter().flatten().last().cloned();
        let txs = data.data.transactions.into_iter().flatten().collect::<Vec<_>>();

        block
            .map(|b| {
                b.try_into_ethers_hash(txs.into_iter().filter_map(|tx| tx.hash).collect())
                    .map(Arc::new)
            })
            .transpose()
            .map_err(|err| ProviderError::CustomError(err.to_string()))
    }

    async fn get_chain_id(&self) -> Result<U256, ProviderError> {
        self.provider
            .get_chain_id()
            .await
            .map(U256::from)
            .map_err(|err| ProviderError::CustomError(err.to_string()))
    }

    async fn get_block_number(&self) -> Result<U64, ProviderError> {
        self.provider
            .get_height()
            .await
            .map(U64::from)
            .map_err(|err| ProviderError::CustomError(err.to_string()))
    }

    async fn get_logs(&self, filter: &RindexerEventFilter) -> Result<Vec<WrappedLog>, ProviderError> {
        let raw_filter = filter.raw_filter().clone();

        let all_log_fields: BTreeSet<String> =
            hypersync_schema::log().fields.iter().map(|x| x.name.clone()).collect();

        let mut query = match raw_filter.block_option {
            FilterBlockOption::Range { from_block, to_block } => {
                let from_block = from_block
                    .map(|n| n.as_number().expect("from_block should be set as a number"))
                    .unwrap_or_default();
                let to_block =
                    to_block.map(|n| n.as_number().expect("to_block should be set as a number"));
                Query {
                    from_block: from_block.as_u64(),
                    to_block: to_block.map(|n| n.as_u64() + 1),
                    field_selection: FieldSelection { log: all_log_fields, ..Default::default() },
                    ..Default::default()
                }
            }
            FilterBlockOption::AtBlockHash(block_hash) => Query {
                from_block: 0,
                to_block: None,
                blocks: vec![BlockSelection {
                    hash: vec![block_hash.into()],
                    ..Default::default()
                }],
                field_selection: FieldSelection { log: all_log_fields, ..Default::default() },
                join_mode: JoinMode::JoinAll,
                ..Default::default()
            },
        };

        let addresses = raw_filter
            .address
            .map(|addr| match addr {
                ValueOrArray::Value(a) => vec![a],
                ValueOrArray::Array(arr) => arr,
            })
            .unwrap_or_default();

        let hypersync_topics: ArrayVec<Vec<LogArgument>, 4> = raw_filter
            .topics
            .into_iter()
            .map(|topic| match topic {
                None => vec![],
                Some(ValueOrArray::Value(None)) => vec![],
                Some(ValueOrArray::Value(Some(topic))) => vec![topic.into()],
                Some(ValueOrArray::Array(topics)) => topics
                    .into_iter()
                    .filter_map(|topic| topic.map(Into::into))
                    .collect::<Vec<FixedSizeData<32>>>(),
            })
            .collect::<ArrayVec<Vec<LogArgument>, 4>>();

        query.logs = vec![LogSelection {
            address: addresses.clone().into_iter().map(|a| a.into()).collect(),
            address_filter: None,
            topics: hypersync_topics,
        }];

        query.join_mode = JoinMode::JoinNothing;

        let resp = self
            .provider
            .clone()
            .collect(query, Default::default())
            .await
            .map_err(|err| ProviderError::CustomError(err.to_string()))?;

        Ok(resp
            .data
            .logs
            .into_iter()
            .flatten()
            .filter_map(|log| {
                let log = log.try_into().ok()?;
                Some(WrappedLog{inner: log, block_timestamp: None})
            })
            .collect::<Vec<WrappedLog>>())

    }

    fn max_block_range(&self) -> Option<U64> {
        self.max_block_range
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("http provider can't be created for {0}: {1}")]
    HttpProviderCantBeCreated(String, String),

    #[error("Could not build client: {0}")]
    CouldNotBuildClient(#[from] reqwest::Error),

    #[error("Could not build hypersync client: {0}")]
    CouldNotBuildHypersyncClient(String),
}

pub fn create_client(
    rpc_url: &str,
    kind: ProviderType,
    compute_units_per_second: Option<u64>,
    max_block_range: Option<U64>,
    custom_headers: HeaderMap,
) -> Result<Arc<dyn ProviderInterface>, RetryClientError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;
    match kind {
        ProviderType::Rpc => {
            create_jsonrpc_client(url, compute_units_per_second, max_block_range, custom_headers)
                .and_then(|client| Ok(client as Arc<dyn ProviderInterface>))
        }
        ProviderType::Hypersync => create_hypersync_client(url, max_block_range)
            .and_then(|client| Ok(client as Arc<dyn ProviderInterface>)),
    }
}

pub fn create_jsonrpc_client(
    url: Url,
    compute_units_per_second: Option<u64>,
    max_block_range: Option<U64>,
    custom_headers: HeaderMap,
) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
    let client = reqwest::Client::builder().default_headers(custom_headers).build()?;

    let provider = Http::new_with_client(url, client);
    let instance = Provider::new(
        RetryClientBuilder::default()
            // assume minimum compute units per second if not provided as growth plan
            // standard
            .compute_units_per_second(compute_units_per_second.unwrap_or(660))
            .rate_limit_retries(5000)
            .timeout_retries(1000)
            .initial_backoff(Duration::from_millis(500))
            .build(provider, Box::<ethers::providers::HttpRateLimitRetryPolicy>::default()),
    );
    Ok(Arc::new(JsonRpcCachedProvider::new(instance, max_block_range)))
}

pub fn create_hypersync_client(
    url: Url,
    max_block_range: Option<U64>,
) -> Result<Arc<HyperSyncProvider>, RetryClientError> {
    let config = ClientConfig {
        url: Some(url),
        http_req_timeout_millis: NonZeroU64::new(30000),
        max_num_retries: 3.into(),
        ..Default::default()
    };
    let client = Client::new(config)
        .map_err(|err| RetryClientError::CouldNotBuildHypersyncClient(err.to_string()))?;
    Ok(Arc::new(HyperSyncProvider::new(client, max_block_range)))
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
    pub client: Arc<dyn ProviderInterface>,
}

impl CreateNetworkProvider {
    pub fn create(manifest: &Manifest) -> Result<Vec<CreateNetworkProvider>, RetryClientError> {
        let mut result: Vec<CreateNetworkProvider> = vec![];
        for network in &manifest.networks {
            let provider = create_client(
                &network.rpc,
                network.kind,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_retry_client() {
        let rpc_url = "http://localhost:8545";
        let result = create_client(rpc_url, ProviderType::Rpc, Some(660), None, HeaderMap::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_client(rpc_url, ProviderType::Rpc, Some(660), None, HeaderMap::new());
        assert!(result.is_err());
        if let Err(RetryClientError::HttpProviderCantBeCreated(url, _)) = result {
            assert_eq!(url, rpc_url);
        } else {
            panic!("Expected HttpProviderCantBeCreated error");
        }
    }
}
