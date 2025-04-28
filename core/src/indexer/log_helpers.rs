use std::str::FromStr;

use alloy::{
    dyn_abi::DynSolValue,
    json_abi::{Event, EventParam},
    primitives::{keccak256, Address, Bloom, B256, U256},
    rpc::types::{Block, FilterSet, FilteredParams, Log, ValueOrArray},
};
#[derive(Debug, PartialEq, Clone)]
pub struct LogParam {
    /// Decoded log name.
    pub name: String,
    /// Decoded log value.
    pub value: DynSolValue,
}

pub fn parse_log(event: &Event, log: &Log) -> Option<Vec<EventParam>> {
    // as topic[0] is the event signature
    let topics_length = log.topics().len() - 1;
    let indexed_inputs_abi_length = event.inputs.iter().filter(|param| param.indexed).count();

    // check if topics and data match the event
    if topics_length == indexed_inputs_abi_length {
        let log = event.clone().inputs;
        return Some(log);
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
