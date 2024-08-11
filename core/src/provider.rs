use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    eips::BlockNumberOrTag,
    primitives::{BlockNumber, B256},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::{
        client::ClientBuilder,
        types::{Block, Log},
    },
    transports::{
        http::{Client, Http},
        layers::{RetryBackoffLayer, RetryBackoffService},
        RpcError, TransportErrorKind, TransportResult,
    },
};
use reqwest::header::HeaderMap;
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

use crate::{event::RindexerEventFilter, manifest::core::Manifest};

pub type RindexerHttpProvider = RootProvider<RetryBackoffService<Http<Client>>>;

#[derive(Debug)]
pub struct JsonRpcCachedProvider {
    provider: Arc<RindexerHttpProvider>,
    cache: Mutex<Option<(Instant, Arc<Block>)>>,
    pub max_block_range: Option<u64>,
}

impl JsonRpcCachedProvider {
    pub fn new(provider: RindexerHttpProvider, max_block_range: Option<u64>) -> Self {
        JsonRpcCachedProvider {
            provider: Arc::new(provider),
            cache: Mutex::new(None),
            max_block_range,
        }
    }

    pub async fn get_latest_block(
        &self,
    ) -> Result<Option<Arc<Block>>, RpcError<TransportErrorKind>> {
        let mut cache_guard = self.cache.lock().await;

        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < Duration::from_millis(300) {
                return Ok(Some(Arc::clone(block)));
            }
        }

        let latest_block =
            self.provider.get_block_by_number(BlockNumberOrTag::Latest, false).await?;

        if let Some(block) = latest_block {
            let arc_block = Arc::new(block);
            *cache_guard = Some((Instant::now(), Arc::clone(&arc_block)));
            return Ok(Some(arc_block));
        } else {
            *cache_guard = None;
        }

        Ok(None)
    }

    pub async fn get_block_number(&self) -> TransportResult<BlockNumber> {
        self.provider.get_block_number().await
    }

    pub async fn get_logs(&self, filter: &RindexerEventFilter) -> TransportResult<Vec<Log>> {
        self.provider.get_logs(filter.raw_filter()).await
    }

    pub async fn get_chain_id(&self) -> TransportResult<u64> {
        self.provider.get_chain_id().await
    }

    pub fn get_inner_provider(&self) -> Arc<RindexerHttpProvider> {
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
    max_block_range: Option<u64>,
    custom_headers: HeaderMap,
) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;
    // let client = reqwest::Client::builder().default_headers(custom_headers).build()?;
    // missing timeout_retries

    let retry_layer = RetryBackoffLayer::new(5000, 500, compute_units_per_second.unwrap_or(660));
    let client = ClientBuilder::default().layer(retry_layer).http(url.clone());

    let provider = ProviderBuilder::new().on_client(client);

    Ok(Arc::new(JsonRpcCachedProvider::new(provider, max_block_range)))
}

pub async fn get_chain_id(rpc_url: &str) -> Result<u64, RpcError<TransportErrorKind>> {
    let provider = create_client(rpc_url, None, None, HeaderMap::new())
        .map_err(|e| TransportErrorKind::custom(Box::new(e)))?;
    provider.get_chain_id().await
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
