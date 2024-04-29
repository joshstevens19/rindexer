use ethers::middleware::{Middleware, MiddlewareError};
use ethers::prelude::{Filter, JsonRpcError, Log};
use ethers::types::{BlockNumber, U64};
use regex::Regex;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

struct RetryWithBlockRangeResult {
    from: BlockNumber,
    to: BlockNumber,
    range: u64,
}

fn retry_with_block_range(error: &JsonRpcError) -> Option<RetryWithBlockRangeResult> {
    let error_message = &error.message;

    // alchemy - https://github.com/ponder-sh/ponder/blob/main/packages/utils/src/getLogsRetryHelper.ts
    let re = Regex::new(r"this block range should work: \[(0x[0-9a-fA-F]+),\s*(0x[0-9a-fA-F]+)]").unwrap();
    if let Some(captures) = re.captures(error_message) {
        let start_block = captures.get(1).unwrap().as_str();
        println!("start_block: {:?}", start_block);

        let end_block = captures.get(2).unwrap().as_str();
        println!("end_block: {:?}", end_block);

        // let range = end_block.as_number().unwrap() - start_block.as_number().unwrap();
        // println!("range: {:?}", range);

        return Some(RetryWithBlockRangeResult {
            from: BlockNumber::from_str(start_block).unwrap(),
            to: BlockNumber::from_str(end_block).unwrap(),
            range: 10u64,
        });
    }

    None
}

pub fn fetch_logs<M: Middleware + Clone + Send + 'static>(
    provider: Arc<M>,
    filter: Filter,
) -> Pin<Box<dyn Future<Output = Result<Vec<Log>, Box<dyn Error>>> + Send>> {
    async fn inner_fetch_logs<M: Middleware + Clone + Send + 'static>(
        provider: Arc<M>,
        filter: Filter,
    ) -> Result<Vec<Log>, Box<dyn Error>> {
        println!("Fetching logs for filter: {:?}", filter);
        let logs_result = provider.get_logs(&filter).await;
        match logs_result {
            Ok(logs) => {
                println!("Fetched logs: {:?}", logs.len());
                Ok(logs)
            }
            Err(err) => {
                println!("Failed to fetch logs: {:?}", err);
                let json_rpc_error = err.as_error_response();
                if let Some(json_rpc_error) = json_rpc_error {
                    let retry_result = retry_with_block_range(json_rpc_error);
                    if let Some(retry_result) = retry_result {
                        let filter = filter
                            .from_block(retry_result.from)
                            .to_block(retry_result.to);
                        println!("Retrying with block range: {}", retry_result.range);
                        let future = Box::pin(inner_fetch_logs(provider.clone(), filter));
                        future.await
                    } else {
                        Err(Box::new(err))
                    }
                } else {
                    Err(Box::new(err))
                }
            }
        }
    }

    Box::pin(inner_fetch_logs(provider, filter))
}
