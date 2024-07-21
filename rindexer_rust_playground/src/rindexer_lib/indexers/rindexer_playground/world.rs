use std::{path::PathBuf, sync::Arc};

use rindexer::{
    event::callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
};

use super::super::super::typings::rindexer_playground::events::world::{
    no_extensions, ComponentValueSetEvent, WorldEventType,
};

async fn component_value_set_handler(
    manifest_path: &PathBuf,
    registry: &mut EventCallbackRegistry,
) {
    WorldEventType::ComponentValueSet(
        ComponentValueSetEvent::handler(|results, context| async move {
            if results.is_empty() {
                return Ok(());
            }

            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
            let mut csv_bulk_data: Vec<Vec<String>> = vec![];
            for result in results.iter() {
                csv_bulk_data.push(vec![format!("{:?}", result.tx_information.address),result.event_data.arg_0.to_string(),format!("{:?}", result.event_data.arg_1,),result.event_data.arg_2.to_string(),result.event_data.arg_3.iter().map(|byte| format!("{:02x}", byte)).collect::<Vec<_>>().join(""),format!("{:?}", result.tx_information.transaction_hash),result.tx_information.block_number.to_string(),result.tx_information.block_hash.to_string(),result.tx_information.network.to_string(),result.tx_information.transaction_index.to_string(),result.tx_information.log_index.to_string()]);
                let data = vec![EthereumSqlTypeWrapper::Address(result.tx_information.address),EthereumSqlTypeWrapper::U256(result.event_data.arg_0),EthereumSqlTypeWrapper::Address(result.event_data.arg_1),EthereumSqlTypeWrapper::U256(result.event_data.arg_2),EthereumSqlTypeWrapper::Bytes(result.event_data.arg_3.clone()),EthereumSqlTypeWrapper::H256(result.tx_information.transaction_hash),EthereumSqlTypeWrapper::U64(result.tx_information.block_number),EthereumSqlTypeWrapper::H256(result.tx_information.block_hash),EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),EthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),EthereumSqlTypeWrapper::U256(result.tx_information.log_index)];;
                postgres_bulk_data.push(data);
            }

            if !csv_bulk_data.is_empty() {
                let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                if let Err(e) = csv_result {
                    rindexer_error!("WorldEventType::ComponentValueSet inserting csv data: {:?}", e);
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
                        "rindexer_playground_world.component_value_set",
                        &["contract_address".to_string(), "arg_0".to_string(), "arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string(), "tx_hash".to_string(), "block_number".to_string(), "block_hash".to_string(), "network".to_string(), "tx_index".to_string(), "log_index".to_string()],
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
                        rindexer_error!("WorldEventType::ComponentValueSet inserting bulk data via COPY: {:?}", e);
                        return Err(e.to_string());
                    }
                } else {
                    let result = context
                        .database
                        .bulk_insert(
                            "rindexer_playground_world.component_value_set",
                            &["contract_address".to_string(), "arg_0".to_string(), "arg_1".to_string(), "arg_2".to_string(), "arg_3".to_string(), "tx_hash".to_string(), "block_number".to_string(), "block_hash".to_string(), "network".to_string(), "tx_index".to_string(), "log_index".to_string()],
                            &postgres_bulk_data,
                        )
                        .await;

                    if let Err(e) = result {
                        rindexer_error!("WorldEventType::ComponentValueSet inserting bulk data via INSERT: {:?}", e);
                        return Err(e.to_string());
                    }
                }
            
                rindexer_info!(
                    "World::ComponentValueSet - {} - {} events",
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
pub async fn world_handlers(manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
    component_value_set_handler(manifest_path, registry).await;
}
