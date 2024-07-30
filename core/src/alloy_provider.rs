use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    primitives::*,
    providers::{network::AnyNetwork, Provider, ProviderBuilder, RootProvider},
    rpc::types::{Block, BlockTransactionsKind, Log},
    transports::{http::Http, RpcError, TransportErrorKind},
};
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

use crate::event::RindexerEventFilter;
#[derive(Error, Debug)]
pub enum AlloyProviderError {
    #[error("Provider error for {0}: {1} ")]
    ProviderCreatorError(String, String),
}
#[derive(Debug)]
pub struct AlloyProvider {
    provider: Arc<RootProvider<Http<reqwest::Client>>>,
    cache: Mutex<Option<(Instant, Arc<Block>)>>,
    pub max_block_range: Option<U64>,
}

impl AlloyProvider {
    pub fn new(
        provider: Arc<RootProvider<Http<reqwest::Client>>>,
        max_block_range: Option<U64>,
    ) -> Self {
        AlloyProvider { provider, cache: Mutex::new(None), max_block_range }
    }

    pub async fn get_latest_block(&self) -> Result<Option<Arc<Block>>, AlloyProviderError> {
        let mut cache_guard = self.cache.lock().await;

        if let Some((timestamp, block)) = &*cache_guard {
            if timestamp.elapsed() < Duration::from_millis(300) {
                return Ok(Some(Arc::clone(block)));
                // return Ok(Some(Arc::clone(block)));
            }
        }

        let latest_block =
            self.provider.get_block(BlockId::latest(), BlockTransactionsKind::Full).await.unwrap();
        if let Some(block) = latest_block {
            let arc_block = Arc::new(block);
            *cache_guard = Some((Instant::now(), Arc::clone(&arc_block)));
            return Ok(Some(arc_block))
        } else {
            *cache_guard = None;
        }
        Ok(None)
    }

    pub async fn get_block_number(&self) -> Result<u64, RpcError<TransportErrorKind>> {
        self.provider.get_block_number().await
    }

    pub async fn get_logs(
        &self,
        filter: &RindexerEventFilter,
    ) -> Result<Vec<Log>, AlloyProviderError> {
        todo!()
    }

    pub async fn get_chain_id(&self) -> Result<U256, AlloyProviderError> {
        todo!()
    }
    pub async fn get_provider(&self) -> Arc<RootProvider<Http<reqwest::Client>>> {
        Arc::clone(&self.provider)
    }
}

pub fn create_client(
    rpc_url: &str,
    max_block_range: Option<U64>,
) -> Result<AlloyProvider, AlloyProviderError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        AlloyProviderError::ProviderCreatorError(rpc_url.to_string(), e.to_string())
    })?;
    let instance = ProviderBuilder::new().on_http(url);
    Ok(AlloyProvider::new(Arc::new(instance), max_block_range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_alloy_provider() {
        use alloy::{
            primitives::*,
            providers::{Provider, ProviderBuilder},
        };
        let rpc_url = "https://eth.merkle.io";
        let result = create_client(rpc_url, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_client(rpc_url, None);
        assert!(result.is_err());
    }
}
