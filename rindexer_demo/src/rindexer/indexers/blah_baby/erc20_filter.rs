use super::super::super::typings::blah_baby::events::erc20_filter::{
    no_extensions, ERC20FilterEventType, TransferEvent,
};
use rindexer_core::{
    generator::event_callback_registry::EventCallbackRegistry, rindexer_error, rindexer_info,
    EthereumSqlTypeWrapper, PgType, RindexerColorize,
};
use std::sync::Arc;

async fn transfer_handler(registry: &mut EventCallbackRegistry) {
    ERC20FilterEventType::Transfer(
        TransferEvent::handler(
            |results, context| async move {
                if results.is_empty() {
                    return;
                }

                let mut bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
                for result in results.iter() {
                    let csv_result = context
                        .csv
                        .append(vec![
                            format!("{:?}", result.tx_information.address),
                            format!("{:?}", result.event_data.from,),
                            format!("{:?}", result.event_data.to,),
                            result.event_data.value.to_string(),
                            format!("{:?}", result.tx_information.transaction_hash),
                            result.tx_information.block_number.to_string(),
                            result.tx_information.block_hash.to_string(),
                            result.tx_information.network.to_string(),
                        ])
                        .await;

                    if let Err(e) = csv_result {
                        rindexer_error!(
                            "ERC20FilterEventType::Transfer inserting csv data: {:?}",
                            e
                        );
                    }

                    let data = vec![
                        EthereumSqlTypeWrapper::Address(result.tx_information.address),
                        EthereumSqlTypeWrapper::Address(result.event_data.from),
                        EthereumSqlTypeWrapper::Address(result.event_data.to),
                        EthereumSqlTypeWrapper::U256(result.event_data.value),
                        EthereumSqlTypeWrapper::H256(result.tx_information.transaction_hash),
                        EthereumSqlTypeWrapper::U64(result.tx_information.block_number),
                        EthereumSqlTypeWrapper::H256(result.tx_information.block_hash),
                        EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),
                    ];
                    bulk_data.push(data);
                }

                if bulk_data.is_empty() {
                    return;
                }

                if bulk_data.len() > 100 {
                    let result = context
                        .database
                        .bulk_insert_via_copy(
                            "blah_baby_erc20_filter.transfer",
                            &[
                                "contract_address".to_string(),
                                "from".to_string(),
                                "to".to_string(),
                                "value".to_string(),
                                "tx_hash".to_string(),
                                "block_number".to_string(),
                                "block_hash".to_string(),
                                "network".to_string(),
                            ],
                            &bulk_data
                                .first()
                                .unwrap()
                                .iter()
                                .map(|param| param.to_type())
                                .collect::<Vec<PgType>>(),
                            &bulk_data,
                        )
                        .await;

                    if let Err(e) = result {
                        rindexer_error!(
                            "ERC20FilterEventType::Transfer inserting bulk data: {:?}",
                            e
                        );
                    }
                } else {
                    let result = context
                        .database
                        .bulk_insert(
                            "blah_baby_erc20_filter.transfer",
                            &[
                                "contract_address".to_string(),
                                "from".to_string(),
                                "to".to_string(),
                                "value".to_string(),
                                "tx_hash".to_string(),
                                "block_number".to_string(),
                                "block_hash".to_string(),
                                "network".to_string(),
                            ],
                            &bulk_data,
                        )
                        .await;

                    if let Err(e) = result {
                        rindexer_error!(
                            "ERC20FilterEventType::Transfer inserting bulk data: {:?}",
                            e
                        );
                    }
                }

                rindexer_info!(
                    "ERC20Filter::Transfer - {} - {} events",
                    "INDEXED".green(),
                    results.len(),
                );
            },
            no_extensions(),
        )
        .await,
    )
    .register(registry);
}
pub async fn erc20_filter_handlers(registry: &mut EventCallbackRegistry) {
    transfer_handler(registry).await;
}
