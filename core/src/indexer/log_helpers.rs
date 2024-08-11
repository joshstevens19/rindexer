use std::str::FromStr;

use alloy::{
    primitives::{keccak256, Address, Bloom, B256},
    rpc::types::{Block, FilteredParams, Log, ValueOrArray},
};
use alloy::dyn_abi::{DecodedEvent, EventExt};
use alloy::json_abi::Event;
use alloy::primitives::LogData;

pub fn parse_log(event: &Event, log: &Log) -> Option<DecodedEvent> {
    let topics = log.topics();
    // as topic[0] is the event signature
    let topics_length = topics.len() - 1;
    let indexed_inputs_abi_length = event.inputs.iter().filter(|param| param.indexed).count();
    
    // check if topics and data match the event
    if topics_length == indexed_inputs_abi_length {
        let log_data = LogData::new(topics.to_vec(), log.data().clone().data).unwrap();

        let log = match event.decode_log(&log_data, true) {
            Ok(log) => Some(log),
            Err(_) => None,
        };

        return log;
    }

    None
}

fn map_token_to_raw_values(token: &Token) -> Vec<String> {
    match token {
        Token::Address(addr) => vec![format!("{:?}", addr)],
        Token::FixedBytes(bytes) | Token::Bytes(bytes) => vec![format!("{:?}", bytes)],
        Token::Int(int) => vec![int.to_string()],
        Token::Uint(uint) => vec![uint.to_string()],
        Token::Bool(b) => vec![b.to_string()],
        Token::String(s) => vec![s.clone()],
        Token::FixedArray(tokens) | Token::Array(tokens) => {
            let values: Vec<String> = tokens.iter().flat_map(map_token_to_raw_values).collect();
            vec![format!("[{}]", values.join(", "))]
        }
        Token::Tuple(tokens) => {
            let mut values = vec![];
            for token in tokens {
                values.extend(map_token_to_raw_values(token));
            }
            values
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
        "true" => B256::from_low_u64_be(1),
        "false" => B256::from_low_u64_be(0),
        _ => {
            if let Ok(address) = Address::from_str(input) {
                B256::from(address)
            } else if let Ok(num) = B256::from_dec_str(input) {
                B256::from_uint(&num)
            } else {
                B256::from(keccak256(input))
            }
        }
    }
}

pub fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let address_filter =
        FilteredParams::address_filter(&Some(ValueOrArray::Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

pub fn topic_in_bloom(topic_id: B256, logs_bloom: Bloom) -> bool {
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![ValueOrArray::Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

pub fn is_relevant_block(
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &B256,
    latest_block: &Block<B256>,
) -> bool {
    match latest_block.header.logs_bloom {
        None => false,
        Some(logs_bloom) => {
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
    }
}
