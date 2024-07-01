use ethers::abi::{Event, Log as ParsedLog, LogParam, RawLog, Token};
use ethers::types::Log;
use serde_json::Value;

pub fn parse_log(event: &Event, log: &Log) -> Option<ParsedLog> {
    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.to_vec(),
    };

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

    // Was exploring advanced log parsing to handle cases where the indexed parameters are a bit different
    // not sure i see a use case for this yet
    // let mut modified_event = event.clone();
    //
    // // try to adjust the log to match an event where the indexed parameters are a bit different
    // // aka - Transfer (indexed address from, indexed address to, indexed uint256 tokenId)
    // // vs - Transfer (indexed address from, indexed address to, uint256 tokenId)
    // // both topic_id = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    // // but ABI will not be able to debug with index mismatch
    //
    // // see if data has been moved to topics
    // // Log - Transfer (indexed address from, indexed address to, indexed uint256 tokenId)
    // // with ABI - Transfer (indexed address from, indexed address to, uint256 tokenId)
    // if data_length == 0 && data_inputs_abi_length > 0 {
    //     // modify the event to have the data classed as a topic
    //     modified_event.inputs = modified_event.inputs.iter().map(|input| {
    //         let mut input = input.clone();
    //         input.indexed = true;
    //         input
    //     }).collect();
    //
    //     let log = match modified_event.parse_log(raw_log) {
    //         Ok(log) => Some(log),
    //         Err(_) => None
    //     };
    //
    //     return log;
    // }

    // println!("topics_length: {:?}", topics_length);
    // println!("indexed_inputs_abi_length: {:?}", indexed_inputs_abi_length);
    // println!("event: {:?}", event);
    // println!("log: {:?}", log);

    // see if value is in data but ABI expects it in topics
    // Log - Transfer (indexed address from, indexed address to, indexed uint256 tokenId)
    // with ABI - Transfer (indexed address from, indexed address to, indexed uint256 tokenId)

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
        },
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
