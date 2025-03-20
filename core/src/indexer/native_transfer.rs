use std::{str::FromStr, sync::Arc};

use ethers::types::{Action, Address, Bytes, U256, U64};
use futures::future::try_join_all;
use serde::Serialize;

use super::start::StartIndexingError;
use crate::{
    event::{
        callback_registry::{TraceResult, TxInformation},
        config::TraceProcessingConfig,
    },
    provider::JsonRpcCachedProvider,
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
    let (from_block, to_block) =
        block_numbers.iter().fold((U64::MAX, U64::zero()), |(min, max), &num| {
            (std::cmp::min(min, num), std::cmp::max(max, num))
        });

    let native_transfers = trace_calls
        .into_iter()
        .flatten()
        .filter_map(|trace| {
            let action = match &trace.action {
                Action::Call(call) => Some(call),
                _ => None,
            }?;

            // TODO: Replace with `Bytes::new()`
            let no_input = action.input == Bytes::from_str("0x").unwrap();
            let has_value = !action.value.is_zero();
            let is_native_transfer = has_value && no_input;

            if is_native_transfer {
                Some(TraceResult::new_native_transfer(
                    action,
                    &trace,
                    network_name,
                    from_block,
                    to_block,
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if native_transfers.is_empty() {
        return Ok(());
    }

    config.trigger_event(native_transfers).await;

    Ok(())
}
