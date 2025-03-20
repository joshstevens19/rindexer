use std::{str::FromStr, sync::Arc};

use colored::Colorize;
use ethers::{
    abi::Hash,
    types::{Action, Address, Bytes, U256, U64},
};
use futures::future::try_join_all;
use serde::Serialize;
use serde_json::json;
use tracing::{error, info};

use super::start::StartIndexingError;
use crate::{
    event::{
        callback_registry::{EventResult, TraceResult, TxInformation},
        config::TraceProcessingConfig,
        EventMessage,
    },
    provider::JsonRpcCachedProvider,
    streams::StreamsClients,
};

#[derive(Serialize)]
pub struct NativeTransfer {
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub transaction_information: TxInformation,
}

pub async fn native_transfer_block_consumer(
    provider: Arc<JsonRpcCachedProvider>,
    block_numbers: &[U64],
    network_name: &str,
    config: &TraceProcessingConfig,
) -> Result<(), StartIndexingError> {
    let trace_futures: Vec<_> = block_numbers.iter().map(|n| provider.trace_block(*n)).collect();

    let trace_calls = try_join_all(trace_futures).await?;

    let native_transfers = trace_calls
        .into_iter()
        .flatten()
        .filter_map(|trace| {
            let action = match trace.action {
                Action::Call(call) => Some(call),
                _ => None,
            }?;

            let has_value = !action.value.is_zero();
            let no_input = action.input == Bytes::from_str("0x").unwrap();
            let is_native_transfer = has_value && no_input;

            if is_native_transfer {
                if trace.transaction_hash.is_none() {
                    error!("Transaction hash should exist for block trace {}", trace.block_number);
                    return None;
                }

                Some(NativeTransfer {
                    from: action.from,
                    to: action.to,
                    value: action.value,
                    transaction_information: TxInformation {
                        network: network_name.to_owned(),
                        address: Address::zero(),
                        block_number: U64::from(trace.block_number),
                        block_timestamp: None,
                        transaction_hash: trace.transaction_hash.expect("checked prior"),
                        block_hash: trace.block_hash,
                        transaction_index: U64::from(trace.transaction_position.unwrap_or(0)),
                        log_index: U256::from(0),
                    },
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if native_transfers.is_empty() {
        return Ok(());
    }

    let from_block = native_transfers.first().map(|n| n.transaction_information.block_number);
    let to_block = native_transfers.first().map(|n| n.transaction_information.block_number);

    let fn_data =
        native_transfers.into_iter().map(|_| TraceResult::new_native_transfer()).collect::<Vec<_>>();

    config.trigger_event(fn_data).await;

    // if let Some(client) = streams_client {
    //     let contract_name = "EvmDebugTrace";
    //     let event_name = "NativeTokenTransfer";
    //     let from_block = native_transfers
    //         .first()
    //         .map(|b| b.transaction_information.block_number)
    //         .unwrap_or_default();
    //     let to_block = native_transfers
    //         .last()
    //         .map(|b| b.transaction_information.block_number)
    //         .unwrap_or_default();
    //
    //     let stream_id = format!(
    //         "{}-{}-{}-{}-{}",
    //         contract_name, event_name, network_name, from_block, to_block
    //     );
    //
    //     let event_message = EventMessage {
    //         event_name: event_name.to_string(),
    //         event_data: json!(native_transfers),
    //         event_signature_hash: Hash::zero(),
    //         network: network_name.to_string(),
    //     };
    //
    //     match client.stream(stream_id, &event_message, false, true).await {
    //         Ok(streamed) => {
    //             if streamed > 0 {
    //                 info!(
    //                     "{}::{} - {} - {} events {}",
    //                     contract_name,
    //                     event_name,
    //                     "STREAMED".green(),
    //                     streamed,
    //                     format!(
    //                         "- trace block: {} - {} - network: {}",
    //                         from_block, to_block, network_name
    //                     )
    //                 );
    //             }
    //         }
    //         Err(e) => {
    //             error!("Error streaming event: {}", e);
    //             return Err(StartIndexingError::UnknownError(e.to_string()));
    //         }
    //     }
    // }

    Ok(())
}
