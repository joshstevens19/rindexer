use ethers::middleware::Middleware;
use ethers::providers::{Http, Provider, ProviderError, RetryClient, RetryClientBuilder};
use ethers::types::U256;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url::Url;

/// Custom error type for RetryClient creation errors.
#[derive(Error, Debug)]
pub enum RetryClientError {
    #[error("http provider can't be created for {0}: {1}")]
    HttpProviderCantBeCreated(String, String),
}

/// Creates a retry-enabled HTTP provider client.
///
/// This function sets up a `Provider` with a `RetryClient` to handle retries in case of request failures.
/// It uses a predefined retry policy.
///
/// # Arguments
///
/// * `rpc_url` - The RPC URL for the Ethereum provider.
///
/// # Returns
///
/// A `Result` containing an `Arc<Provider<RetryClient<Http>>>` or a `RetryClientError`.
pub fn create_retry_client(
    rpc_url: &str,
) -> Result<Arc<Provider<RetryClient<Http>>>, RetryClientError> {
    let url = Url::parse(rpc_url).map_err(|e| {
        RetryClientError::HttpProviderCantBeCreated(rpc_url.to_string(), e.to_string())
    })?;
    let provider = Http::new(url);
    // Configure the retry client with intense retry settings
    Ok(Arc::new(Provider::new(
        RetryClientBuilder::default()
            .rate_limit_retries(50)
            .timeout_retries(10)
            .initial_backoff(Duration::from_millis(500))
            .build(
                provider,
                Box::<ethers::providers::HttpRateLimitRetryPolicy>::default(),
            ),
    )))
}

/// Retrieves the chain ID from the specified Ethereum provider.
///
/// # Arguments
///
/// * `rpc_url` - The RPC URL for the Ethereum provider.
///
/// # Returns
///
/// A `Result` containing the `U256` chain ID or a `ProviderError`.
pub async fn get_chain_id(rpc_url: &str) -> Result<U256, ProviderError> {
    let url = Url::parse(rpc_url).unwrap();
    let provider = Provider::new(Http::new(url));

    provider.get_chainid().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_retry_client() {
        let rpc_url = "http://localhost:8545";
        let result = create_retry_client(rpc_url);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_chain_id() {
        let rpc_url = "http://localhost:8545";
        let result = get_chain_id(rpc_url).await;
        // Assuming the local node returns chain ID 1 (Ethereum mainnet)
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), U256::from(1));
    }

    #[test]
    fn test_create_retry_client_invalid_url() {
        let rpc_url = "invalid_url";
        let result = create_retry_client(rpc_url);
        assert!(result.is_err());
        if let Err(RetryClientError::HttpProviderCantBeCreated(url, _)) = result {
            assert_eq!(url, rpc_url);
        } else {
            panic!("Expected HttpProviderCantBeCreated error");
        }
    }
}
