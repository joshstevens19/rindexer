use ethers::middleware::Middleware;
use ethers::providers::{Http, Provider, ProviderError, RetryClient, RetryClientBuilder};
use ethers::types::U256;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("http provider can't be created for {0}: {1}")]
    HttpProviderCantBeCreated(String, String),
}

pub fn create_retry_client(
    rpc_url: &str,
    compute_units_per_second: Option<u64>,
) -> Result<Arc<Provider<RetryClient<Http>>>, RetryClientError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;
    let provider = Http::new(url);
    // Configure the retry client with intense retry settings
    Ok(Arc::new(Provider::new(
        RetryClientBuilder::default()
            // assume minimum compute units per second if not provided as growth plan standard
            .compute_units_per_second(compute_units_per_second.unwrap_or(660))
            .rate_limit_retries(5000)
            .timeout_retries(1000)
            .initial_backoff(Duration::from_millis(500))
            .build(
                provider,
                Box::<ethers::providers::HttpRateLimitRetryPolicy>::default(),
            ),
    )))
}

pub async fn get_chain_id(rpc_url: &str) -> Result<U256, ProviderError> {
    let url = Url::parse(rpc_url).map_err(|_| ProviderError::UnsupportedRPC)?;
    let provider = Provider::new(Http::new(url));

    provider.get_chainid().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_retry_client() {
        let rpc_url = "http://localhost:8545";
        let result = create_retry_client(rpc_url, Some(660));
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_retry_client(rpc_url, Some(660));
        assert!(result.is_err());
        if let Err(RetryClientError::HttpProviderCantBeCreated(url, _)) = result {
            assert_eq!(url, rpc_url);
        } else {
            panic!("Expected HttpProviderCantBeCreated error");
        }
    }
}
