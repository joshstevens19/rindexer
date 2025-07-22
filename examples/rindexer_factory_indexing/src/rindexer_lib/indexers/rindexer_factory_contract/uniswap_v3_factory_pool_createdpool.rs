#![allow(non_snake_case)]
use rindexer::{
                event::callback_registry::EventCallbackRegistry,
                EthereumSqlTypeWrapper, PgType, RindexerColorize, rindexer_error, rindexer_info
            };
        use std::sync::Arc;
use std::path::PathBuf;
        use alloy::primitives::{U256, I256};
        use super::super::super::typings::rindexer_factory_contract::events::uniswap_v3_factory_pool_createdpool::{no_extensions, UniswapV3FactoryPoolCreatedpoolEventType,PoolCreatedEvent};

async fn pool_created_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = PoolCreatedEvent::handler(|results, context| async move {
                                if results.is_empty() {
                                    return Ok(());
                                }



                    let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
                    let mut csv_bulk_data: Vec<Vec<String>> = vec![];
                    for result in results.iter() {
                        csv_bulk_data.push(vec![result.tx_information.address.to_string(),result.event_data.token0.to_string(),
result.event_data.token1.to_string(),
result.event_data.fee.to_string(),
result.event_data.tickSpacing.to_string(),
result.event_data.pool.to_string(),
result.tx_information.transaction_hash.to_string(),result.tx_information.block_number.to_string(),result.tx_information.block_hash.to_string(),result.tx_information.network.to_string(),result.tx_information.transaction_index.to_string(),result.tx_information.log_index.to_string()]);
                        let data = vec![
EthereumSqlTypeWrapper::Address(result.tx_information.address),
EthereumSqlTypeWrapper::Address(result.event_data.token0),
EthereumSqlTypeWrapper::Address(result.event_data.token1),
EthereumSqlTypeWrapper::U32(result.event_data.fee.to()),
EthereumSqlTypeWrapper::I32(result.event_data.tickSpacing.as_i32()),
EthereumSqlTypeWrapper::Address(result.event_data.pool),
EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
EthereumSqlTypeWrapper::U256(result.tx_information.log_index)
];
                        postgres_bulk_data.push(data);
                    }

                    if !csv_bulk_data.is_empty() {
                        let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                        if let Err(e) = csv_result {
                            rindexer_error!("UniswapV3FactoryPoolCreatedpoolEventType::PoolCreated inserting csv data: {:?}", e);
                            return Err(e.to_string());
                        }
                    }

                    if postgres_bulk_data.is_empty() {
                        return Ok(());
                    }

                    let rows = ["contract_address".to_string(), "token_0".to_string(), "token_1".to_string(), "fee".to_string(), "tick_spacing".to_string(), "pool".to_string(), "tx_hash".to_string(), "block_number".to_string(), "block_hash".to_string(), "network".to_string(), "tx_index".to_string(), "log_index".to_string()];

                    if postgres_bulk_data.len() > 100 {
                        let result = context
                            .database
                            .bulk_insert_via_copy(
                                "rindexer_factory_contract_uniswap_v3_factory_pool_createdpool.pool_created",
                                &rows,
                                &postgres_bulk_data
                                    .first()
                                    .ok_or("No first element in bulk data, impossible")?
                                    .iter()
                                    .map(|param| param.to_type())
                                    .collect::<Vec<PgType>>(),
                                &postgres_bulk_data,
                            )
                            .await;

                        if let Err(e) = result {
                            rindexer_error!("UniswapV3FactoryPoolCreatedpoolEventType::PoolCreated inserting bulk data via COPY: {:?}", e);
                            return Err(e.to_string());
                        }
                        } else {
                            let result = context
                                .database
                                .bulk_insert(
                                    "rindexer_factory_contract_uniswap_v3_factory_pool_createdpool.pool_created",
                                    &rows,
                                    &postgres_bulk_data,
                                )
                                .await;

                            if let Err(e) = result {
                                rindexer_error!("UniswapV3FactoryPoolCreatedpoolEventType::PoolCreated inserting bulk data via INSERT: {:?}", e);
                                return Err(e.to_string());
                            }
                    }


                                rindexer_info!(
                                    "UniswapV3FactoryPoolCreatedpool::PoolCreated - {} - {} events",
                                    "INDEXED".green(),
                                    results.len(),
                                );

                                Ok(())
                            },
                            no_extensions(),
                          )
                          .await;

    UniswapV3FactoryPoolCreatedpoolEventType::PoolCreated(handler)
        .register(manifest_path, registry)
        .await;
}
pub async fn uniswap_v3_factory_pool_createdpool_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    pool_created_handler(manifest_path, registry).await;
}
