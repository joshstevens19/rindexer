
use crate::rindexer::lens_registry_example::events::erc20::{
    no_extensions, ApprovalEvent, ERC20EventType, NewEventOptions, TransferEvent,
};
use rindexer_core::{
    generator::event_callback_registry::EventCallbackRegistry, EthereumSqlTypeWrapper,
};
use std::sync::Arc;

async fn approval_handler(registry: &mut EventCallbackRegistry) {
    ERC20EventType::Approval(
                    ApprovalEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Approval event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.approval (contract_address, \"owner\", \"spender\", \"value\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.owner),&EthereumSqlTypeWrapper::Address(&result.event_data.spender),&EthereumSqlTypeWrapper::U256(&result.event_data.value),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}

async fn transfer_handler(registry: &mut EventCallbackRegistry) {
    ERC20EventType::Transfer(
                    TransferEvent::new(
                        Arc::new(|results, context| {
                            Box::pin(async move {
                                println!("Transfer event: {:?}", results);
                                for result in results {
                    context
                        .database
                        .execute("INSERT INTO lens_registry_example.transfer (contract_address, \"from\", \"to\", \"value\", \"tx_hash\", \"block_number\", \"block_hash\") VALUES($1, $2, $3, $4, $5, $6, $7)",
                        &[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),&EthereumSqlTypeWrapper::Address(&result.event_data.from),&EthereumSqlTypeWrapper::Address(&result.event_data.to),&EthereumSqlTypeWrapper::U256(&result.event_data.value),&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())])
                        .await.unwrap();
                }
        })
                        }),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
}
pub async fn erc20_handlers(registry: &mut EventCallbackRegistry) {
    approval_handler(registry).await;

    transfer_handler(registry).await;
}
