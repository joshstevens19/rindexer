#![allow(non_snake_case)]
use super::super::super::typings::rindexer_factory_contract::events::uniswap_v3_factory::{
    no_extensions, OwnerChangedEvent, UniswapV3FactoryEventType,
};
use alloy::primitives::{I256, U256};
use rindexer::{
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
};
use std::path::PathBuf;
use std::sync::Arc;

async fn owner_changed_handler(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    let handler = OwnerChangedEvent::handler(|results, context| async move {
                                if results.is_empty() {
                                    return Ok(());
                                }



                    let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
                    let mut csv_bulk_data: Vec<Vec<String>> = vec![];
                    for result in results.iter() {
                        csv_bulk_data.push(vec![result.tx_information.address.to_string(),result.event_data.oldOwner.to_string(),
result.event_data.newOwner.to_string(),
result.tx_information.transaction_hash.to_string(),result.tx_information.block_number.to_string(),result.tx_information.block_hash.to_string(),result.tx_information.network.to_string(),result.tx_information.transaction_index.to_string(),result.tx_information.log_index.to_string()]);
                        let data = vec![
EthereumSqlTypeWrapper::Address(result.tx_information.address),
EthereumSqlTypeWrapper::Address(result.event_data.oldOwner),
EthereumSqlTypeWrapper::Address(result.event_data.newOwner),
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
                            rindexer_error!("UniswapV3FactoryEventType::OwnerChanged inserting csv data: {:?}", e);
                            return Err(e.to_string());
                        }
                    }

                    if postgres_bulk_data.is_empty() {
                        return Ok(());
                    }

                    let rows = ["contract_address".to_string(), "old_owner".to_string(), "new_owner".to_string(), "tx_hash".to_string(), "block_number".to_string(), "block_hash".to_string(), "network".to_string(), "tx_index".to_string(), "log_index".to_string()];

                    if postgres_bulk_data.len() > 100 {
                        let result = context
                            .database
                            .bulk_insert_via_copy(
                                "rindexer_factory_contract_uniswap_v3_factory.owner_changed",
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
                            rindexer_error!("UniswapV3FactoryEventType::OwnerChanged inserting bulk data via COPY: {:?}", e);
                            return Err(e.to_string());
                        }
                        } else {
                            let result = context
                                .database
                                .bulk_insert(
                                    "rindexer_factory_contract_uniswap_v3_factory.owner_changed",
                                    &rows,
                                    &postgres_bulk_data,
                                )
                                .await;

                            if let Err(e) = result {
                                rindexer_error!("UniswapV3FactoryEventType::OwnerChanged inserting bulk data via INSERT: {:?}", e);
                                return Err(e.to_string());
                            }
                    }


                                rindexer_info!(
                                    "UniswapV3Factory::OwnerChanged - {} - {} events",
                                    "INDEXED".green(),
                                    results.len(),
                                );

                                Ok(())
                            },
                            no_extensions(),
                          )
                          .await;

    UniswapV3FactoryEventType::OwnerChanged(handler).register(manifest_path, registry).await;
}
pub async fn uniswap_v3_factory_handlers(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    owner_changed_handler(manifest_path, registry).await;
}
