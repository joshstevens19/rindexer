#![allow(non_snake_case)]
use std::{path::PathBuf, sync::Arc};

use rindexer::{
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
};

use super::super::super::typings::rindexer_playground::events::uniswap_v3_pool_filter::{
    no_extensions, SwapEvent, UniswapV3PoolFilterEventType,
};

async fn swap_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    UniswapV3PoolFilterEventType::Swap(
        SwapEvent::handler(|results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    format!("{:?}", result.tx_information.address),
                    format!("{:?}", result.event_data.sender,),
                    format!("{:?}", result.event_data.recipient,),
                    result.event_data.amount_0.to_string(),
                    result.event_data.amount_1.to_string(),
                    result.event_data.sqrt_price_x96.to_string(),
                    result.event_data.liquidity.to_string(),
                    result.event_data.tick.to_string(),
                    format!("{:?}", result.tx_information.transaction_hash),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string()
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Address(result.event_data.sender),
                    EthereumSqlTypeWrapper::Address(result.event_data.recipient),
                    EthereumSqlTypeWrapper::I256(result.event_data.amount_0),
                    EthereumSqlTypeWrapper::I256(result.event_data.amount_1),
                    EthereumSqlTypeWrapper::U256(result.event_data.sqrt_price_x96),
                    EthereumSqlTypeWrapper::U128(result.event_data.liquidity),
                    EthereumSqlTypeWrapper::I32(result.event_data.tick),
                    EthereumSqlTypeWrapper::H256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::H256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index)
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!("UniswapV3PoolFilterEventType::Swap inserting csv data: {:?}", e);
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

             if postgres_bulk_data.len() > 100 {
                let result = context
                    .database
                    .bulk_insert_via_copy(
                        "rindexer_playground_uniswap_v3_pool_filter.swap",
                        &[
                            "contract_address".to_string(),
                            "sender".to_string(),
                            "recipient".to_string(),
                            "amount_0".to_string(),
                            "amount_1".to_string(),
                            "sqrt_price_x96".to_string(),
                            "liquidity".to_string(),
                            "tick".to_string(),
                            "tx_hash".to_string(),
                            "block_number".to_string(),
                            "block_hash".to_string(),
                            "network".to_string(),
                            "tx_index".to_string(),
                            "log_index".to_string()
                        ],
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
                        rindexer_error!("UniswapV3PoolFilterEventType::Swap inserting bulk data via COPY: {:?}", e);
                        return Err(e.to_string());
                    }
                } else {
                    let result = context
                        .database
                        .bulk_insert(
                            "rindexer_playground_uniswap_v3_pool_filter.swap",
                            &[
                                "contract_address".to_string(),
                                "sender".to_string(),
                                "recipient".to_string(),
                                "amount_0".to_string(),
                                "amount_1".to_string(),
                                "sqrt_price_x96".to_string(),
                                "liquidity".to_string(),
                                "tick".to_string(),
                                "tx_hash".to_string(),
                                "block_number".to_string(),
                                "block_hash".to_string(),
                                "network".to_string(),
                                "tx_index".to_string(),
                                "log_index".to_string()
                            ],
                            &postgres_bulk_data,
                        )
                        .await;
                    if let Err(e) = result {
                        rindexer_error!("UniswapV3PoolFilterEventType::Swap inserting bulk data via INSERT: {:?}", e);
                        return Err(e.to_string());
                    }
                }

                rindexer_info!(
                    "UniswapV3PoolFilter::Swap - {} - {} events",
                    "INDEXED".green(),
                    results.len(),
                );

                Ok(())
            },
            no_extensions(),
          )
          .await,
    )
    .register(manifest_path, registry);
}
pub async fn uniswap_v3_pool_filter_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    swap_handler(manifest_path, registry).await;
}
