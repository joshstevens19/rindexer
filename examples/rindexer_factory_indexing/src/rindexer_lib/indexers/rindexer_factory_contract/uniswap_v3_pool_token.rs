#![allow(non_snake_case)]
use super::super::super::typings::rindexer_factory_contract::events::uniswap_v3_pool_token::{
    no_extensions, TransferEvent, UniswapV3PoolTokenEventType,
};
use alloy::primitives::{I256, U256, U64};
use rindexer::{
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
};
use std::path::PathBuf;
use std::sync::Arc;

async fn transfer_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = TransferEvent::handler(
        |results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![
                    result.tx_information.address.to_string(),
                    result.event_data.from.to_string(),
                    result.event_data.to.to_string(),
                    result.event_data.value.to_string(),
                    result.tx_information.transaction_hash.to_string(),
                    result.tx_information.block_number.to_string(),
                    result.tx_information.block_hash.to_string(),
                    result.tx_information.network.to_string(),
                    result.tx_information.transaction_index.to_string(),
                    result.tx_information.log_index.to_string(),
                ]);
                let data = vec![
                    EthereumSqlTypeWrapper::Address(result.tx_information.address),
                    EthereumSqlTypeWrapper::Address(result.event_data.from),
                    EthereumSqlTypeWrapper::Address(result.event_data.to),
                    EthereumSqlTypeWrapper::U256(U256::from(result.event_data.value)),
                    EthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),
                    EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                    EthereumSqlTypeWrapper::DateTimeNullable(
                        result.tx_information.block_timestamp_to_datetime(),
                    ),
                    EthereumSqlTypeWrapper::B256(result.tx_information.block_hash),
                    EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),
                    EthereumSqlTypeWrapper::U256(result.tx_information.log_index),
                ];
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!(
                        "UniswapV3PoolTokenEventType::Transfer inserting csv data: {:?}",
                        e
                    );
                    return Err(e.to_string());
                }
            }

            if postgres_bulk_data.is_empty() {
                return Ok(());
            }

            let rows = [
                "contract_address".to_string(),
                "from".to_string(),
                "to".to_string(),
                "value".to_string(),
                "tx_hash".to_string(),
                "block_number".to_string(),
                "block_timestamp".to_string(),
                "block_hash".to_string(),
                "network".to_string(),
                "tx_index".to_string(),
                "log_index".to_string(),
            ];

            let result = context
                .database
                .insert_bulk(
                    "rindexer_factory_contract_uniswap_v3_pool_token.transfer",
                    &rows,
                    &postgres_bulk_data,
                )
                .await;

            if let Err(e) = result {
                rindexer_error!(
                    "UniswapV3PoolTokenEventType::Transfer inserting bulk data: {:?}",
                    e
                );
                return Err(e.to_string());
            }

            rindexer_info!(
                "UniswapV3PoolToken::Transfer - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
    .await;

    UniswapV3PoolTokenEventType::Transfer(handler).register(manifest_path, registry).await;
}
pub async fn uniswap_v3_pool_token_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    transfer_handler(manifest_path, registry).await;
}
