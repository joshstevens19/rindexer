use super::super::super::typings::lens_registry_example::events::erc20_filter::{
    no_extensions, ERC20FilterEventType, TransferEvent,
};
use rindexer_core::{
    generator::event_callback_registry::EventCallbackRegistry, EthereumSqlTypeWrapper,
};
use std::sync::Arc;

async fn transfer_handler(registry: &mut EventCallbackRegistry) {
    ERC20FilterEventType::Transfer(
                    TransferEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                for result in results {
                                    context.csv.append(vec![format!("{:?}", result.tx_information.address),format!("{:?}", result.event_data.from,),format!("{:?}", result.event_data.to,),result.event_data.value.to_string(),format!("{:?}", result.tx_information.transaction_hash),result.tx_information.block_number.to_string(),result.tx_information.block_hash.to_string()]).await.unwrap();
                                    context.database.execute("INSERT INTO lens_registry_example_erc20_filter.transfer (contract_address, \"from\", \"to\", \"value\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",&[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.from),&EthereumSqlTypeWrapper::Address(&result.event_data.to),&EthereumSqlTypeWrapper::U256(&result.event_data.value),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash)]).await.unwrap();
                                }
                           })
                        }),
                        no_extensions()
                    )
                    .await,
                )
                .register(registry);
}
pub async fn erc20_filter_handlers(registry: &mut EventCallbackRegistry) {
    transfer_handler(registry).await;
}
