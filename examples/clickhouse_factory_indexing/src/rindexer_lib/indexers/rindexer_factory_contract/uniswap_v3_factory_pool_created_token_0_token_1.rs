#![allow(non_snake_case)]
use rindexer::{
                event::callback_registry::EventCallbackRegistry,
                EthereumSqlTypeWrapper, PgType, rindexer_error, rindexer_info
            };
        use std::sync::Arc;
use std::path::PathBuf;
        use alloy::primitives::{U64, U256, I256};
        use super::super::super::typings::rindexer_factory_contract::events::uniswap_v3_factory_pool_created_token_0_token_1::{no_extensions, UniswapV3FactoryPoolCreatedToken0Token1EventType,PoolCreatedEvent};

async fn pool_created_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = PoolCreatedEvent::handler(|results, context| async move {
                                if results.is_empty() {
                                    return Ok(());
                                }



                    let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];

                    for result in results.iter() {

                        let data = vec![
EthereumSqlTypeWrapper::Address(result.tx_information.address),
EthereumSqlTypeWrapper::Address(result.event_data.token0),
EthereumSqlTypeWrapper::Address(result.event_data.token1),
EthereumSqlTypeWrapper::U32(result.event_data.fee.to()),
EthereumSqlTypeWrapper::I32(result.event_data.tickSpacing.unchecked_into()),
EthereumSqlTypeWrapper::Address(result.event_data.pool),
EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
EthereumSqlTypeWrapper::DateTimeNullable(result.tx_information.block_timestamp_to_datetime()),
EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
EthereumSqlTypeWrapper::U256(result.tx_information.log_index)
];
                        postgres_bulk_data.push(data);
                    }



                    if postgres_bulk_data.is_empty() {
                        return Ok(());
                    }

                    let rows = ["contract_address".to_string(), "token_0".to_string(), "token_1".to_string(), "fee".to_string(), "tick_spacing".to_string(), "pool".to_string(), "tx_hash".to_string(), "block_number".to_string(), "block_timestamp".to_string(), "block_hash".to_string(), "network".to_string(), "tx_index".to_string(), "log_index".to_string()];

                    let result = context
                        .database
                        .insert_bulk(
                            "rindexer_factory_contract_uniswap_v3_factory_pool_created_token_0_token_1.pool_created",
                            &rows,
                            &postgres_bulk_data,
                        )
                        .await;

                    if let Err(e) = result {
                        rindexer_error!("UniswapV3FactoryPoolCreatedToken0Token1EventType::PoolCreated inserting bulk data: {:?}", e);
                        return Err(e.to_string());
                    }


                                rindexer_info!(
                                    "UniswapV3FactoryPoolCreatedToken0Token1::PoolCreated - INDEXED - {} events",
                                    results.len(),
                                );

                                Ok(())
                            },
                            no_extensions(),
                          )
                          .await;

    UniswapV3FactoryPoolCreatedToken0Token1EventType::PoolCreated(handler)
        .register(manifest_path, registry)
        .await;
}
pub async fn uniswap_v3_factory_pool_created_token_0_token_1_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    pool_created_handler(manifest_path, registry).await;
}
