use std::str::FromStr;

use alloy::{
    dyn_abi::{DynSolValue, EventExt},
    json_abi::Event,
    primitives::{keccak256, Address, Bloom, B256, U256, U64},
    rpc::types::{Block, FilterSet, FilteredParams, Log, ValueOrArray},
};
use tracing::error;

use crate::types::core::{LogParam, ParsedLog};

pub fn parse_log(event: &Event, log: &Log) -> Option<ParsedLog> {
    // as topic[0] is the event signature
    let topics_length = log.topics().len() - 1;
    let indexed_inputs_abi_length = event.inputs.iter().filter(|param| param.indexed).count();

    // check if topics and data match the event
    if topics_length == indexed_inputs_abi_length {
        if let Ok(decoded) = event.decode_log(&log.inner) {
            let mut indexed_iter = decoded.indexed.into_iter();
            let mut body_iter = decoded.body.into_iter();

            let params = event
                .inputs
                .iter()
                .map(|input| {
                    let value = if input.indexed {
                        indexed_iter.next().expect("Not enough indexed values")
                    } else {
                        body_iter.next().expect("Not enough body values")
                    };
                    LogParam { name: input.name.clone(), value }
                })
                .collect();

            return Some(ParsedLog { params });
        }
    }

    None
}

fn map_token_to_raw_values(token: &DynSolValue) -> Vec<String> {
    match token {
        DynSolValue::Address(addr) => vec![format!("{:?}", addr)],
        DynSolValue::FixedBytes(bytes, _) => vec![format!("{:?}", bytes)],
        DynSolValue::Bytes(bytes) => vec![format!("{:?}", bytes)],
        DynSolValue::Int(int, _) => {
            vec![int.to_string()]
        }
        DynSolValue::Uint(uint, _) => {
            vec![uint.to_string()]
        }
        DynSolValue::Bool(b) => vec![b.to_string()],
        DynSolValue::String(s) => vec![s.clone()],
        DynSolValue::FixedArray(tokens) | DynSolValue::Array(tokens) => {
            let values: Vec<String> = tokens.iter().flat_map(map_token_to_raw_values).collect();
            vec![format!("[{}]", values.join(", "))]
        }
        DynSolValue::Tuple(tokens) => {
            let mut values = vec![];
            for token in tokens {
                values.extend(map_token_to_raw_values(token));
            }
            values
        }
        _ => {
            error!("Error parsing unsupported token {:?}", token);
            unimplemented!(
                "Functions and CustomStruct are not supported yet for `map_token_to_raw_values`"
            );
        }
    }
}

pub fn map_log_params_to_raw_values(params: &[LogParam]) -> Vec<String> {
    let mut raw_values = vec![];
    for param in params {
        raw_values.extend(map_token_to_raw_values(&param.value));
    }
    raw_values
}

pub fn parse_topic(input: &str) -> B256 {
    match input.to_lowercase().as_str() {
        "true" => B256::from(U256::from(1)),
        "false" => B256::ZERO,
        _ => {
            if let Ok(address) = Address::from_str(input) {
                B256::from(address.into_word())
            } else if let Ok(num) = U256::from_str(input) {
                B256::from(num)
            } else {
                B256::from(keccak256(input))
            }
        }
    }
}

pub fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let filter = FilterSet::from(ValueOrArray::Value(contract_address));
    let address_filter = FilteredParams::address_filter(&filter);
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

pub fn topic_in_bloom(topic_id: B256, logs_bloom: Bloom) -> bool {
    let filter = FilterSet::from(ValueOrArray::Value(Some(topic_id)));
    let topic_filter = FilteredParams::topics_filter(&[filter]);
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

pub fn is_relevant_block(
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &B256,
    latest_block: &Block,
) -> bool {
    let logs_bloom = latest_block.header.logs_bloom;

    if let Some(contract_address) = contract_address {
        match contract_address {
            ValueOrArray::Value(address) => {
                if !contract_in_bloom(*address, logs_bloom) {
                    return false;
                }
            }
            ValueOrArray::Array(addresses) => {
                if addresses.iter().all(|addr| !contract_in_bloom(*addr, logs_bloom)) {
                    return false;
                }
            }
        }
    }

    if !topic_in_bloom(*topic_id, logs_bloom) {
        return false;
    }

    true
}

/// Take either the halved block range, or 2 blocks from the current. This is to prevent possible
/// stalling risk and ensure we always make progress.
pub fn halved_block_number(to_block: U64, from_block: U64) -> U64 {
    let halved_range = (to_block - from_block) / U64::from(2);
    (from_block + halved_range).max(from_block + U64::from(2))
}
