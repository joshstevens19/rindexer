use std::str::FromStr;

use ethers::{
    abi::{Event, Log as ParsedLog, LogParam, RawLog, Token},
    addressbook::Address,
    prelude::{Block, Bloom, FilteredParams, ValueOrArray, H256, U256},
    types::{BigEndianHash, Log},
    utils::keccak256,
};

use crate::helpers::u256_to_i256;

pub fn parse_log(event: &Event, log: &Log) -> Option<ParsedLog> {
    let raw_log = RawLog { topics: log.topics.clone(), data: log.data.to_vec() };

    // as topic[0] is the event signature
    let topics_length = log.topics.len() - 1;
    let indexed_inputs_abi_length = event.inputs.iter().filter(|param| param.indexed).count();

    // check if topics and data match the event
    if topics_length == indexed_inputs_abi_length {
        let log = match event.parse_log(raw_log) {
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
        Token::Int(int) => {
            // handle two’s complement without adding a new type
            let i256_value = u256_to_i256(*int);
            vec![i256_value.to_string()]
        }
        Token::Uint(uint) => {
            // handle two’s complement without adding a new type
            let i256_value = u256_to_i256(*uint);
            vec![i256_value.to_string()]
        }
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

pub fn parse_topic(input: &str) -> H256 {
    match input.to_lowercase().as_str() {
        "true" => H256::from_low_u64_be(1),
        "false" => H256::from_low_u64_be(0),
        _ => {
            if let Ok(address) = Address::from_str(input) {
                H256::from(address)
            } else if let Ok(num) = U256::from_dec_str(input) {
                H256::from_uint(&num)
            } else {
                H256::from(keccak256(input))
            }
        }
    }
}

pub fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let address_filter =
        FilteredParams::address_filter(&Some(ValueOrArray::Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

pub fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![ValueOrArray::Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

pub fn is_relevant_block(
    contract_address: &Option<ValueOrArray<Address>>,
    topic_id: &H256,
    latest_block: &Block<H256>,
) -> bool {
    match latest_block.logs_bloom {
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
