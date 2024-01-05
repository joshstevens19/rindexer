use ethers::providers::{Http as HttpProvider, Middleware, Provider, ProviderError, RetryClient};
use ethers::types::{Filter, Log};

// https://github.com/a16z/magi/blob/a1d09c1dea0bb2a1b72e0b5c22ad71f658a604da/src/l1/mod.rs#L23
// let rpc_url = "https://eth.llamarpc.com";
// let provider = Provider::new_client(rpc_url)?;
pub async fn get_logs(
    filter: Filter,
    provider: Provider<HttpProvider>,
) -> Result<Vec<Log>, ProviderError> {
    // let filter = Filter::new()
    //             .address(self.config.chain.system_config_contract)
    //             .topic0(*CONFIG_UPDATE_TOPIC)
    //             .from_block(last_update_block + 1)
    //             .to_block(to_block);

    Ok(provider.get_logs(&filter).await?)
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }
