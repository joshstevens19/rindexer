use std::str::FromStr;

use bytes::BytesMut;
use ethers::{
    abi::{Int, LogParam, Token},
    addressbook::Address,
    prelude::{Bytes, H128, H160, H256, H512, U128, U256, U512, U64},
};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};
use tracing::error;

use crate::{abi::ABIInput, event::callback_registry::TxInformation};

#[derive(Debug, Clone)]
pub enum EthereumSqlTypeWrapper {
    U64(U64),
    VecU64(Vec<U64>),
    U128(U128),
    VecU128(Vec<U128>),
    U256(U256),
    VecU256(Vec<U256>),
    U512(U512),
    VecU512(Vec<U512>),
    H128(H128),
    VecH128(Vec<H128>),
    H160(H160),
    VecH160(Vec<H160>),
    H256(H256),
    VecH256(Vec<H256>),
    H512(H512),
    VecH512(Vec<H512>),
    Address(Address),
    VecAddress(Vec<Address>),
    Bool(bool),
    VecBool(Vec<bool>),
    U32(u32),
    VecU32(Vec<u32>),
    U16(u16),
    VecU16(Vec<u16>),
    U8(u8),
    VecU8(Vec<u8>),
    String(String),
    VecString(Vec<String>),
    Bytes(Bytes),
    VecBytes(Vec<Bytes>),
}

impl EthereumSqlTypeWrapper {
    pub fn raw_name(&self) -> &'static str {
        match self {
            EthereumSqlTypeWrapper::U64(_) => "U64",
            EthereumSqlTypeWrapper::VecU64(_) => "VecU64",
            EthereumSqlTypeWrapper::U128(_) => "U128",
            EthereumSqlTypeWrapper::VecU128(_) => "VecU128",
            EthereumSqlTypeWrapper::U256(_) => "U256",
            EthereumSqlTypeWrapper::VecU256(_) => "VecU256",
            EthereumSqlTypeWrapper::U512(_) => "U512",
            EthereumSqlTypeWrapper::VecU512(_) => "VecU512",
            EthereumSqlTypeWrapper::H128(_) => "H128",
            EthereumSqlTypeWrapper::VecH128(_) => "VecH128",
            EthereumSqlTypeWrapper::H160(_) => "H160",
            EthereumSqlTypeWrapper::VecH160(_) => "VecH160",
            EthereumSqlTypeWrapper::H256(_) => "H256",
            EthereumSqlTypeWrapper::VecH256(_) => "VecH256",
            EthereumSqlTypeWrapper::H512(_) => "H512",
            EthereumSqlTypeWrapper::VecH512(_) => "VecH512",
            EthereumSqlTypeWrapper::Address(_) => "Address",
            EthereumSqlTypeWrapper::VecAddress(_) => "VecAddress",
            EthereumSqlTypeWrapper::Bool(_) => "Bool",
            EthereumSqlTypeWrapper::VecBool(_) => "VecBool",
            EthereumSqlTypeWrapper::U32(_) => "U32",
            EthereumSqlTypeWrapper::VecU32(_) => "VecU32",
            EthereumSqlTypeWrapper::U16(_) => "U16",
            EthereumSqlTypeWrapper::VecU16(_) => "VecU16",
            EthereumSqlTypeWrapper::U8(_) => "U8",
            EthereumSqlTypeWrapper::VecU8(_) => "VecU8",
            EthereumSqlTypeWrapper::String(_) => "String",
            EthereumSqlTypeWrapper::VecString(_) => "VecString",
            EthereumSqlTypeWrapper::Bytes(_) => "Bytes",
            EthereumSqlTypeWrapper::VecBytes(_) => "VecBytes",
        }
    }

    pub fn to_type(&self) -> PgType {
        match self {
            EthereumSqlTypeWrapper::U64(_) => PgType::INT8,
            EthereumSqlTypeWrapper::VecU64(_) => PgType::INT8_ARRAY,
            EthereumSqlTypeWrapper::U128(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::VecU128(_) => PgType::NUMERIC_ARRAY,
            // keep as VARCHAR, so we can keep a decimal string when we return the data
            EthereumSqlTypeWrapper::U256(_) => PgType::VARCHAR,
            // keep as VARCHAR, so we can keep a decimal string when we return the data
            EthereumSqlTypeWrapper::VecU256(_) => PgType::VARCHAR,
            EthereumSqlTypeWrapper::U512(_) => PgType::TEXT,
            EthereumSqlTypeWrapper::VecU512(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::H128(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH128(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H160(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH160(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H256(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH256(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H512(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH512(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::Address(_) => PgType::BPCHAR,
            EthereumSqlTypeWrapper::VecAddress(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::Bool(_) => PgType::BOOL,
            EthereumSqlTypeWrapper::VecBool(_) => PgType::BOOL_ARRAY,
            EthereumSqlTypeWrapper::U16(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU16(_) => PgType::INT2_ARRAY,
            EthereumSqlTypeWrapper::String(_) => PgType::TEXT,
            EthereumSqlTypeWrapper::VecString(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::Bytes(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecBytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::U32(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU32(_) => PgType::INT2_ARRAY,
            EthereumSqlTypeWrapper::U8(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU8(_) => PgType::INT2_ARRAY,
        }
    }
}

impl ToSql for EthereumSqlTypeWrapper {
    fn to_sql(
        &self,
        _ty: &PgType,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            EthereumSqlTypeWrapper::U64(value) => {
                let value = value.to_string();
                Decimal::to_sql(&value.parse::<Decimal>()?, _ty, out)
            }
            EthereumSqlTypeWrapper::VecU64(values) => serialize_vec_decimal(values, _ty, out),
            EthereumSqlTypeWrapper::U128(value) => {
                let value = value.to_string();
                Decimal::to_sql(&value.parse::<Decimal>()?, _ty, out)
            }
            EthereumSqlTypeWrapper::VecU128(values) => serialize_vec_decimal(values, _ty, out),
            EthereumSqlTypeWrapper::U256(value) => {
                let value = value.to_string();
                String::to_sql(&value, _ty, out)
            }
            EthereumSqlTypeWrapper::VecU256(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> =
                        values.iter().map(|v| v.to_string()).collect();
                    let formatted_str = values_strings.join(",");
                    String::to_sql(&formatted_str, _ty, out)
                }
            }
            EthereumSqlTypeWrapper::U512(value) => {
                let value = value.to_string();
                String::to_sql(&value, _ty, out)
            }
            EthereumSqlTypeWrapper::VecU512(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> =
                        values.iter().map(|v| v.to_string()).collect();
                    let formatted_str = values_strings.join(",");
                    String::to_sql(&formatted_str, _ty, out)
                }
            }
            EthereumSqlTypeWrapper::H128(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecH128(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::H160(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecH160(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::H256(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecH256(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::H512(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecH512(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::Address(value) => {
                let hex = format!("{:?}", value);
                String::to_sql(&hex, _ty, out)
            }
            EthereumSqlTypeWrapper::VecAddress(values) => {
                let addresses: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if addresses.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    addresses.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::Bool(value) => bool::to_sql(value, _ty, out),
            EthereumSqlTypeWrapper::VecBool(values) => {
                if values.is_empty() {
                    return Ok(IsNull::Yes);
                }

                // yes this looks mad but only way i could get bool[] working in postgres
                // it correctly serialize the boolean values into the binary format for boolean
                // arrays
                let mut buf = BytesMut::new();
                buf.extend_from_slice(&(1i32.to_be_bytes())); // Number of dimensions
                buf.extend_from_slice(&(0i32.to_be_bytes())); // Has nulls flag
                buf.extend_from_slice(&PgType::BOOL.oid().to_be_bytes()); // Element type OID for boolean

                // Upper and lower bounds for dimensions
                buf.extend_from_slice(&(values.len() as i32).to_be_bytes()); // Length of the array
                buf.extend_from_slice(&(1i32.to_be_bytes())); // Index lower bound

                for value in values {
                    buf.extend_from_slice(&1i32.to_be_bytes()); // Length of the element
                    buf.extend_from_slice(&(*value as u8).to_be_bytes()); // The element itself
                }

                out.extend_from_slice(&buf);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::String(value) => String::to_sql(value, _ty, out),
            EthereumSqlTypeWrapper::VecString(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::Bytes(value) => {
                out.extend_from_slice(value);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecBytes(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U32(value) => {
                let int_value: i32 = *value as i32;
                int_value.to_sql(_ty, out)
            }
            EthereumSqlTypeWrapper::VecU32(values) => {
                let int_values: Vec<i32> = values.iter().map(|&s| s as i32).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U16(value) => {
                let int_value: i16 = *value as i16;
                int_value.to_sql(_ty, out)
            }
            EthereumSqlTypeWrapper::VecU16(values) => {
                let int_values: Vec<i16> = values.iter().map(|&s| s as i16).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U8(value) => {
                let int_value: i16 = *value as i16;
                int_value.to_sql(_ty, out)
            }
            EthereumSqlTypeWrapper::VecU8(values) => {
                let int_values: Vec<i16> = values.iter().map(|&s| s as i16).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(_ty, out)
                }
            }
        }
    }

    fn accepts(_ty: &PgType) -> bool {
        true // We accept all types
    }

    to_sql_checked!();
}

#[allow(clippy::manual_strip)]
pub fn solidity_type_to_ethereum_sql_type_wrapper(
    abi_type: &str,
) -> Option<EthereumSqlTypeWrapper> {
    let is_array = abi_type.ends_with("[]");
    let base_type = abi_type.trim_end_matches("[]");

    match base_type {
        "string" => Some(if is_array {
            EthereumSqlTypeWrapper::VecString(Vec::new())
        } else {
            EthereumSqlTypeWrapper::String(String::new())
        }),
        "address" => Some(if is_array {
            EthereumSqlTypeWrapper::VecAddress(Vec::new())
        } else {
            EthereumSqlTypeWrapper::Address(Address::zero())
        }),
        "bool" => Some(if is_array {
            EthereumSqlTypeWrapper::VecBool(Vec::new())
        } else {
            EthereumSqlTypeWrapper::Bool(false)
        }),
        t if t.starts_with("bytes") => Some(if is_array {
            EthereumSqlTypeWrapper::VecBytes(Vec::new())
        } else {
            EthereumSqlTypeWrapper::Bytes(Bytes::new())
        }),
        t if t.starts_with("int") || t.starts_with("uint") => {
            let size: usize = if t.starts_with("int") {
                t[3..].parse().unwrap_or(256)
            } else {
                t[4..].parse().unwrap_or(256)
            };

            Some(match size {
                8 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU8(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U8(0)
                    }
                }
                16 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU16(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U16(0)
                    }
                }
                24 | 32 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU32(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U32(0)
                    }
                }
                40 | 48 | 56 | 64 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU64(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U64(U64::zero())
                    }
                }
                72 | 80 | 88 | 96 | 104 | 112 | 120 | 128 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU128(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U128(U128::zero())
                    }
                }
                136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232 |
                240 | 248 | 256 => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU256(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U256(U256::zero())
                    }
                }
                _ => return None,
            })
        }
        _ => None,
    }
}

pub fn map_log_params_to_ethereum_wrapper(
    abi_inputs: &[ABIInput],
    params: &[LogParam],
) -> Vec<EthereumSqlTypeWrapper> {
    let mut wrappers = vec![];

    for (index, param) in params.iter().enumerate() {
        if let Some(abi_input) = abi_inputs.get(index) {
            match &param.value {
                Token::Tuple(tuple) => {
                    wrappers.extend(process_tuple(
                        abi_input
                            .components
                            .as_ref()
                            .expect("tuple should have a component ABI on"),
                        tuple,
                    ));
                }
                _ => {
                    wrappers.push(map_log_token_to_ethereum_wrapper(abi_input, &param.value));
                }
            }
        } else {
            panic!("No ABI input found for log param at index: {}", index)
        }
    }

    wrappers
}

fn process_tuple(abi_inputs: &[ABIInput], tokens: &[Token]) -> Vec<EthereumSqlTypeWrapper> {
    let mut wrappers = vec![];

    for (index, token) in tokens.iter().enumerate() {
        if let Some(abi_input) = abi_inputs.get(index) {
            match token {
                Token::Tuple(tuple) => {
                    wrappers.extend(process_tuple(
                        abi_input
                            .components
                            .as_ref()
                            .expect("tuple should have a component ABI on"),
                        tuple,
                    ));
                }
                _ => {
                    wrappers.push(map_log_token_to_ethereum_wrapper(abi_input, token));
                }
            }
        } else {
            panic!("No ABI input found for log param at index: {}", index)
        }
    }

    wrappers
}

fn convert_int(value: &Int, target_type: &EthereumSqlTypeWrapper) -> EthereumSqlTypeWrapper {
    match target_type {
        EthereumSqlTypeWrapper::U256(_) | EthereumSqlTypeWrapper::VecU256(_) => {
            EthereumSqlTypeWrapper::U256(*value)
        }
        EthereumSqlTypeWrapper::U128(_) | EthereumSqlTypeWrapper::VecU128(_) => {
            EthereumSqlTypeWrapper::U128(U128::from(value.low_u128()))
        }
        EthereumSqlTypeWrapper::U64(_) | EthereumSqlTypeWrapper::VecU64(_) => {
            EthereumSqlTypeWrapper::U64(value.as_u64().into())
        }
        EthereumSqlTypeWrapper::U32(_) | EthereumSqlTypeWrapper::VecU32(_) => {
            EthereumSqlTypeWrapper::U32(value.low_u32())
        }
        EthereumSqlTypeWrapper::U16(_) | EthereumSqlTypeWrapper::VecU16(_) => {
            EthereumSqlTypeWrapper::U16(value.low_u32() as u16)
        }
        EthereumSqlTypeWrapper::U8(_) | EthereumSqlTypeWrapper::VecU8(_) => {
            EthereumSqlTypeWrapper::U8(value.low_u32() as u8)
        }
        _ => {
            let error_message = format!("Unsupported target type - {:?}", target_type);
            error!("{}", error_message);
            panic!("{}", error_message)
        }
    }
}

fn map_dynamic_int_to_ethereum_sql_type_wrapper(
    abi_input: &ABIInput,
    value: &Int,
) -> EthereumSqlTypeWrapper {
    let sql_type_wrapper = solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_);
    if let Some(target_type) = sql_type_wrapper {
        convert_int(value, &target_type)
    } else {
        let error_message = format!("Unknown int type for abi input: {:?}", abi_input);
        error!("{}", error_message);
        panic!("{}", error_message);
    }
}

fn map_log_token_to_ethereum_wrapper(
    abi_input: &ABIInput,
    token: &Token,
) -> EthereumSqlTypeWrapper {
    match &token {
        Token::Address(address) => EthereumSqlTypeWrapper::Address(*address),
        Token::Int(value) | Token::Uint(value) => {
            map_dynamic_int_to_ethereum_sql_type_wrapper(abi_input, value)
        }
        Token::Bool(b) => EthereumSqlTypeWrapper::Bool(*b),
        Token::String(s) => EthereumSqlTypeWrapper::String(s.clone()),
        Token::FixedBytes(bytes) | Token::Bytes(bytes) => {
            EthereumSqlTypeWrapper::Bytes(Bytes::from(bytes.clone()))
        }
        Token::FixedArray(tokens) | Token::Array(tokens) => {
            match tokens.first() {
                None => EthereumSqlTypeWrapper::VecString(vec![]),
                Some(first_token) => {
                    // events arrays can only be one type so get it from the first one
                    let token_type = first_token;
                    match token_type {
                        Token::Address(_) => {
                            let mut vec: Vec<Address> = vec![];
                            for token in tokens {
                                if let Token::Address(address) = token {
                                    vec.push(*address);
                                }
                            }

                            EthereumSqlTypeWrapper::VecAddress(vec)
                        }
                        Token::FixedBytes(_) | Token::Bytes(_) => {
                            let mut vec: Vec<Bytes> = vec![];
                            for token in tokens {
                                if let Token::FixedBytes(bytes) = token {
                                    vec.push(Bytes::from(bytes.clone()));
                                }
                            }

                            EthereumSqlTypeWrapper::VecBytes(vec)
                        }
                        Token::Int(_) | Token::Uint(_) => {
                            let sql_type_wrapper =
                                solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_)
                                    .unwrap_or_else(|| {
                                        panic!("Unknown int type for abi input: {:?}", abi_input)
                                    });

                            let vec_wrapper = tokens
                                .iter()
                                .map(|token| {
                                    if let Token::Uint(uint) = token {
                                        return convert_int(uint, &sql_type_wrapper);
                                    }

                                    if let Token::Int(uint) = token {
                                        return convert_int(uint, &sql_type_wrapper);
                                    }

                                    panic!(
                                        "Expected uint or int token in array for abi input: {:?}",
                                        abi_input
                                    );
                                })
                                .collect::<Vec<_>>();

                            match sql_type_wrapper {
                                EthereumSqlTypeWrapper::U256(_) |
                                EthereumSqlTypeWrapper::VecU256(_) => {
                                    EthereumSqlTypeWrapper::VecU256(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U256(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )
                                }
                                EthereumSqlTypeWrapper::U128(_) |
                                EthereumSqlTypeWrapper::VecU128(_) => {
                                    EthereumSqlTypeWrapper::VecU128(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U128(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )
                                }
                                EthereumSqlTypeWrapper::U64(_) |
                                EthereumSqlTypeWrapper::VecU64(_) => {
                                    EthereumSqlTypeWrapper::VecU64(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U64(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )
                                }
                                EthereumSqlTypeWrapper::U32(_) |
                                EthereumSqlTypeWrapper::VecU32(_) => {
                                    EthereumSqlTypeWrapper::VecU32(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U32(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )
                                }
                                EthereumSqlTypeWrapper::U16(_) |
                                EthereumSqlTypeWrapper::VecU16(_) => {
                                    EthereumSqlTypeWrapper::VecU16(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U16(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )
                                }
                                EthereumSqlTypeWrapper::U8(_) |
                                EthereumSqlTypeWrapper::VecU8(_) => EthereumSqlTypeWrapper::VecU8(
                                    vec_wrapper
                                        .into_iter()
                                        .map(|w| match w {
                                            EthereumSqlTypeWrapper::U8(v) => v,
                                            _ => unreachable!(),
                                        })
                                        .collect(),
                                ),
                                _ => panic!("Unknown int type for abi input: {:?}", abi_input),
                            }
                        }
                        Token::Bool(_) => {
                            let mut vec: Vec<bool> = vec![];
                            for token in tokens {
                                if let Token::Bool(b) = token {
                                    vec.push(*b);
                                }
                            }

                            EthereumSqlTypeWrapper::VecBool(vec)
                        }
                        Token::String(_) => {
                            let mut vec: Vec<String> = vec![];
                            for token in tokens {
                                if let Token::String(s) = token {
                                    vec.push(s.clone());
                                }
                            }

                            EthereumSqlTypeWrapper::VecString(vec)
                        }
                        Token::FixedArray(_) | Token::Array(_) => {
                            unreachable!("Nested arrays are not supported by the EVM")
                        }
                        Token::Tuple(_) => {
                            // TODO - this is not supported yet
                            panic!("Array tuple not supported yet - please raise issue in github with ABI to recreate and we will fix")
                        }
                    }
                }
            }
        }
        Token::Tuple(_tuple) => {
            panic!("You should not be calling a tuple type in this function!")
        }
    }
}

impl From<&Address> for EthereumSqlTypeWrapper {
    fn from(address: &Address) -> Self {
        EthereumSqlTypeWrapper::Address(*address)
    }
}

fn serialize_vec_decimal<T: ToString>(
    values: &Vec<T>,
    ty: &PgType,
    out: &mut BytesMut,
) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
    if values.is_empty() {
        return Ok(IsNull::Yes);
    }

    let mut buf = BytesMut::new();
    buf.extend_from_slice(&(1i32.to_be_bytes())); // Number of dimensions
    buf.extend_from_slice(&(0i32.to_be_bytes())); // Has nulls flag
    buf.extend_from_slice(&PgType::NUMERIC.oid().to_be_bytes()); // Element type OID for numeric

    // Upper and lower bounds for dimensions
    buf.extend_from_slice(&(values.len() as i32).to_be_bytes()); // Length of the array
    buf.extend_from_slice(&(1i32.to_be_bytes())); // Index lower bound

    for value in values {
        let value_str = value.to_string();
        let decimal_value = Decimal::from_str(&value_str)?;
        let mut elem_buf = BytesMut::new();
        Decimal::to_sql(&decimal_value, ty, &mut elem_buf)?;
        buf.extend_from_slice(&(elem_buf.len() as i32).to_be_bytes()); // Length of the element
        buf.extend_from_slice(&elem_buf); // The element itself
    }

    out.extend_from_slice(&buf);
    Ok(IsNull::No)
}

fn count_components(components: &[ABIInput]) -> usize {
    components
        .iter()
        .map(|component| {
            if component.type_ == "tuple" {
                let nested_components =
                    component.components.as_ref().expect("Tuple should have components defined");
                1 + count_components(nested_components)
            } else {
                1
            }
        })
        .sum()
}

pub fn map_ethereum_wrapper_to_json(
    abi_inputs: &[ABIInput],
    wrappers: &[EthereumSqlTypeWrapper],
    transaction_information: &TxInformation,
    is_within_tuple: bool,
) -> Value {
    let mut result = serde_json::Map::new();

    let mut current_wrapper_index = 0;
    let mut wrappers_index_processed = Vec::new();
    for abi_input in abi_inputs.iter() {
        // tuples will take in multiple wrapper indexes, so we need to skip them if processed
        if wrappers_index_processed.contains(&current_wrapper_index) {
            continue;
        }
        if let Some(wrapper) = wrappers.get(current_wrapper_index) {
            if abi_input.type_ == "tuple" {
                let components =
                    abi_input.components.as_ref().expect("Tuple should have components defined");
                let total_properties = count_components(components);
                let tuple_value = map_ethereum_wrapper_to_json(
                    components,
                    &wrappers[current_wrapper_index..total_properties],
                    transaction_information,
                    true,
                );
                result.insert(abi_input.name.clone(), tuple_value);
                for i in current_wrapper_index..total_properties {
                    wrappers_index_processed.push(i);
                }
                current_wrapper_index = total_properties;
            } else {
                let value = match wrapper {
                    EthereumSqlTypeWrapper::U64(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU64(u64s) => json!(u64s),
                    EthereumSqlTypeWrapper::U128(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU128(u128s) => {
                        json!(u128s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::U256(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU256(u256s) => {
                        json!(u256s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::U512(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU512(u512s) => {
                        json!(u512s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::H128(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH128(h128s) => json!(h128s),
                    EthereumSqlTypeWrapper::H160(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH160(h160s) => json!(h160s),
                    EthereumSqlTypeWrapper::H256(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH256(h256s) => json!(h256s),
                    EthereumSqlTypeWrapper::H512(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH512(h512s) => json!(h512s),
                    EthereumSqlTypeWrapper::Address(address) => json!(address),
                    EthereumSqlTypeWrapper::VecAddress(addresses) => json!(addresses),
                    EthereumSqlTypeWrapper::Bool(b) => json!(b),
                    EthereumSqlTypeWrapper::VecBool(bools) => json!(bools),
                    EthereumSqlTypeWrapper::U32(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU32(u32s) => json!(u32s),
                    EthereumSqlTypeWrapper::U16(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU16(u16s) => json!(u16s),
                    EthereumSqlTypeWrapper::U8(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU8(u8s) => json!(u8s),
                    EthereumSqlTypeWrapper::String(s) => json!(s),
                    EthereumSqlTypeWrapper::VecString(strings) => json!(strings),
                    EthereumSqlTypeWrapper::Bytes(bytes) => json!(hex::encode(bytes)),
                    EthereumSqlTypeWrapper::VecBytes(bytes) => {
                        json!(bytes.iter().map(hex::encode).collect::<Vec<_>>())
                    }
                };
                result.insert(abi_input.name.clone(), value);
                wrappers_index_processed.push(current_wrapper_index);
                current_wrapper_index += 1;
            }
        } else {
            panic!(
                "No wrapper found for ABI input {:?} and wrapper index {} - wrappers {:?}",
                abi_input, current_wrapper_index, wrappers
            );
        }
    }

    // only do this at the top level
    if !is_within_tuple {
        result.insert("transaction_information".to_string(), json!(transaction_information));
    }

    Value::Object(result)
}
