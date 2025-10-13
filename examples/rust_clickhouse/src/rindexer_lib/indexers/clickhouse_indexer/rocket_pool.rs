#![allow(non_snake_case)]
use super::super::super::typings::clickhouse_indexer::events::rocket_pool::{
    RocketPoolEventType, TransferEvent, no_extensions,
};
use alloy::primitives::{I256, U64, U256};
use rindexer::{
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
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

            for result in results.iter() {
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
                .insert_bulk("clickhouse_indexer_rocket_pool.transfer", &rows, &postgres_bulk_data)
                .await;

            if let Err(e) = result {
                rindexer_error!("RocketPoolEventType::Transfer inserting bulk data: {:?}", e);
                return Err(e.to_string());
            }

            rindexer_info!(
                "RocketPool::Transfer - {} - {} events",
                "INDEXED".green(),
                results.len(),
            );

            Ok(())
        },
        no_extensions(),
    )
    .await;

    RocketPoolEventType::Transfer(handler).register(manifest_path, registry).await;
}
pub async fn rocket_pool_handlers(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    transfer_handler(manifest_path, registry).await;
}
