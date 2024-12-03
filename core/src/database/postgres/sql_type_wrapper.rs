use std::str::FromStr;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use ethers::{
    abi::{Int, LogParam, Token},
    addressbook::Address,
    prelude::{Bytes, H128, H160, H256, H512, U256, U512, U64},
    types::I256,
};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};
use tracing::error;

use crate::{abi::ABIInput, event::callback_registry::TxInformation, helpers::u256_to_i256};

#[derive(Debug, Clone)]
pub enum EthereumSqlTypeWrapper {
    // Boolean
    Bool(bool),
    VecBool(Vec<bool>),

    // 8-bit integers
    U8(u8),
    I8(i8),
    VecU8(Vec<u8>),
    VecI8(Vec<i8>),

    // 16-bit integers
    U16(u16),
    I16(i16),
    VecU16(Vec<u16>),
    VecI16(Vec<i16>),

    // 32-bit integers
    U32(u32),
    I32(i32),
    VecU32(Vec<u32>),
    VecI32(Vec<i32>),

    // 64-bit integers
    U64(U64),
    I64(i64),
    VecU64(Vec<U64>),
    VecI64(Vec<i64>),

    // 128-bit integers
    U128(u128),
    I128(i128),
    VecU128(Vec<u128>),
    VecI128(Vec<i128>),

    // 256-bit integers
    U256(U256),
    U256Nullable(U256),
    U256Bytes(U256),
    U256BytesNullable(U256),
    I256(I256),
    I256Nullable(I256),
    I256Bytes(I256),
    I256BytesNullable(I256),
    VecU256(Vec<U256>),
    VecU256Bytes(Vec<U256>),
    VecI256(Vec<I256>),
    VecI256Bytes(Vec<I256>),

    // 512-bit integers
    U512(U512),
    VecU512(Vec<U512>),

    // Hashes
    H128(H128),
    H160(H160),
    H256(H256),
    H256Bytes(H256),
    H512(H512),
    VecH128(Vec<H128>),
    VecH160(Vec<H160>),
    VecH256(Vec<H256>),
    VecH256Bytes(Vec<H256>),
    VecH512(Vec<H512>),

    // Address
    Address(Address),
    AddressNullable(Address),
    AddressBytes(Address),
    AddressBytesNullable(Address),
    VecAddress(Vec<Address>),
    VecAddressBytes(Vec<Address>),

    // Strings and Bytes
    String(String),
    StringVarchar(String),
    StringChar(String),
    StringNullable(String),
    StringVarcharNullable(String),
    StringCharNullable(String),
    VecString(Vec<String>),
    VecStringVarchar(Vec<String>),
    VecStringChar(Vec<String>),
    Bytes(Bytes),
    BytesNullable(Bytes),
    VecBytes(Vec<Bytes>),

    DateTime(DateTime<Utc>),
}

impl EthereumSqlTypeWrapper {
    pub fn raw_name(&self) -> &'static str {
        match self {
            // Boolean
            EthereumSqlTypeWrapper::Bool(_) => "Bool",
            EthereumSqlTypeWrapper::VecBool(_) => "VecBool",

            // 8-bit integers
            EthereumSqlTypeWrapper::U8(_) => "U8",
            EthereumSqlTypeWrapper::I8(_) => "I8",
            EthereumSqlTypeWrapper::VecU8(_) => "VecU8",
            EthereumSqlTypeWrapper::VecI8(_) => "VecI8",

            // 16-bit integers
            EthereumSqlTypeWrapper::U16(_) => "U16",
            EthereumSqlTypeWrapper::I16(_) => "I16",
            EthereumSqlTypeWrapper::VecU16(_) => "VecU16",
            EthereumSqlTypeWrapper::VecI16(_) => "VecI16",

            // 32-bit integers
            EthereumSqlTypeWrapper::U32(_) => "U32",
            EthereumSqlTypeWrapper::I32(_) => "I32",
            EthereumSqlTypeWrapper::VecU32(_) => "VecU32",
            EthereumSqlTypeWrapper::VecI32(_) => "VecI32",

            // 64-bit integers
            EthereumSqlTypeWrapper::U64(_) => "U64",
            EthereumSqlTypeWrapper::I64(_) => "I64",
            EthereumSqlTypeWrapper::VecU64(_) => "VecU64",
            EthereumSqlTypeWrapper::VecI64(_) => "VecI64",

            // 128-bit integers
            EthereumSqlTypeWrapper::U128(_) => "U128",
            EthereumSqlTypeWrapper::I128(_) => "I128",
            EthereumSqlTypeWrapper::VecU128(_) => "VecU128",
            EthereumSqlTypeWrapper::VecI128(_) => "VecI128",

            // 256-bit integers
            EthereumSqlTypeWrapper::U256(_) => "U256",
            EthereumSqlTypeWrapper::U256Nullable(_) => "U256Nullable",
            EthereumSqlTypeWrapper::U256Bytes(_) => "U256Bytes",
            EthereumSqlTypeWrapper::U256BytesNullable(_) => "U256BytesNullable",
            EthereumSqlTypeWrapper::I256(_) => "I256",
            EthereumSqlTypeWrapper::I256Nullable(_) => "I256Nullable",
            EthereumSqlTypeWrapper::I256Bytes(_) => "I256Bytes",
            EthereumSqlTypeWrapper::I256BytesNullable(_) => "I256BytesNullable",
            EthereumSqlTypeWrapper::VecU256(_) => "VecU256",
            EthereumSqlTypeWrapper::VecU256Bytes(_) => "VecU256Bytes",
            EthereumSqlTypeWrapper::VecI256(_) => "VecI256",
            EthereumSqlTypeWrapper::VecI256Bytes(_) => "VecI256Bytes",

            // 512-bit integers
            EthereumSqlTypeWrapper::U512(_) => "U512",
            EthereumSqlTypeWrapper::VecU512(_) => "VecU512",

            // Hashes
            EthereumSqlTypeWrapper::H128(_) => "H128",
            EthereumSqlTypeWrapper::H160(_) => "H160",
            EthereumSqlTypeWrapper::H256(_) => "H256",
            EthereumSqlTypeWrapper::H256Bytes(_) => "H256Bytes",
            EthereumSqlTypeWrapper::H512(_) => "H512",
            EthereumSqlTypeWrapper::VecH128(_) => "VecH128",
            EthereumSqlTypeWrapper::VecH160(_) => "VecH160",
            EthereumSqlTypeWrapper::VecH256(_) => "VecH256",
            EthereumSqlTypeWrapper::VecH256Bytes(_) => "VecH256Bytes",
            EthereumSqlTypeWrapper::VecH512(_) => "VecH512",

            // Address
            EthereumSqlTypeWrapper::Address(_) => "Address",
            EthereumSqlTypeWrapper::AddressNullable(_) => "AddressNullable",
            EthereumSqlTypeWrapper::AddressBytes(_) => "AddressBytes",
            EthereumSqlTypeWrapper::AddressBytesNullable(_) => "AddressBytesNullable",
            EthereumSqlTypeWrapper::VecAddress(_) => "VecAddress",
            EthereumSqlTypeWrapper::VecAddressBytes(_) => "VecAddressBytes",

            // Strings and Bytes
            EthereumSqlTypeWrapper::String(_) => "String",
            EthereumSqlTypeWrapper::StringVarchar(_) => "StringVarchar",
            EthereumSqlTypeWrapper::StringChar(_) => "StringChar",
            EthereumSqlTypeWrapper::StringNullable(_) => "StringNullable",
            EthereumSqlTypeWrapper::StringVarcharNullable(_) => "StringVarcharNullable",
            EthereumSqlTypeWrapper::StringCharNullable(_) => "StringCharNullable",
            EthereumSqlTypeWrapper::VecString(_) => "VecString",
            EthereumSqlTypeWrapper::VecStringVarchar(_) => "VecStringVarchar",
            EthereumSqlTypeWrapper::VecStringChar(_) => "VecStringChar",
            EthereumSqlTypeWrapper::Bytes(_) => "Bytes",
            EthereumSqlTypeWrapper::BytesNullable(_) => "BytesNullable",
            EthereumSqlTypeWrapper::VecBytes(_) => "VecBytes",

            EthereumSqlTypeWrapper::DateTime(_) => "DateTime",
        }
    }

    pub fn to_type(&self) -> PgType {
        match self {
            // Boolean
            EthereumSqlTypeWrapper::Bool(_) => PgType::BOOL,
            EthereumSqlTypeWrapper::VecBool(_) => PgType::BOOL_ARRAY,

            // 8-bit integers
            EthereumSqlTypeWrapper::U8(_) => PgType::INT2,
            EthereumSqlTypeWrapper::I8(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU8(_) => PgType::INT2_ARRAY,
            EthereumSqlTypeWrapper::VecI8(_) => PgType::INT2_ARRAY,

            // 16-bit integers
            EthereumSqlTypeWrapper::U16(_) => PgType::INT2,
            EthereumSqlTypeWrapper::I16(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU16(_) => PgType::INT2_ARRAY,
            EthereumSqlTypeWrapper::VecI16(_) => PgType::INT2_ARRAY,

            // 32-bit integers
            EthereumSqlTypeWrapper::U32(_) => PgType::INT4,
            EthereumSqlTypeWrapper::I32(_) => PgType::INT4,
            EthereumSqlTypeWrapper::VecU32(_) => PgType::INT4_ARRAY,
            EthereumSqlTypeWrapper::VecI32(_) => PgType::INT4_ARRAY,

            // 64-bit integers
            EthereumSqlTypeWrapper::U64(_) => PgType::INT8,
            EthereumSqlTypeWrapper::I64(_) => PgType::INT8,
            EthereumSqlTypeWrapper::VecU64(_) => PgType::INT8_ARRAY,
            EthereumSqlTypeWrapper::VecI64(_) => PgType::INT8_ARRAY,

            // 128-bit integers
            EthereumSqlTypeWrapper::U128(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::I128(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::VecU128(_) => PgType::NUMERIC_ARRAY,
            EthereumSqlTypeWrapper::VecI128(_) => PgType::NUMERIC_ARRAY,

            // 256-bit integers (kept as VARCHAR for decimal string representation)
            EthereumSqlTypeWrapper::U256(_) | EthereumSqlTypeWrapper::U256Nullable(_) => {
                PgType::VARCHAR
            }
            EthereumSqlTypeWrapper::U256Bytes(_) | EthereumSqlTypeWrapper::U256BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::I256(_) | EthereumSqlTypeWrapper::I256Nullable(_) => {
                PgType::VARCHAR
            }
            EthereumSqlTypeWrapper::I256Bytes(_) | EthereumSqlTypeWrapper::I256BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::VecU256(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecU256Bytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecI256(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecI256Bytes(_) => PgType::BYTEA_ARRAY,

            // 512-bit integers
            EthereumSqlTypeWrapper::U512(_) => PgType::TEXT,
            EthereumSqlTypeWrapper::VecU512(_) => PgType::TEXT_ARRAY,

            // Hashes
            EthereumSqlTypeWrapper::H128(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::H160(_) => PgType::BYTEA,
            // TODO! LOOK AT THIS TYPE AS IT IS SAVED AS CHAR IN NO CODE
            EthereumSqlTypeWrapper::H256(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::H256Bytes(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::H512(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH128(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecH160(_) => PgType::BYTEA_ARRAY,
            // TODO! LOOK AT THIS TYPE AS IT IS SAVED AS CHAR IN NO CODE
            EthereumSqlTypeWrapper::VecH256(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecH256Bytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecH512(_) => PgType::BYTEA_ARRAY,

            // Address
            EthereumSqlTypeWrapper::Address(_) | EthereumSqlTypeWrapper::AddressNullable(_) => {
                PgType::BPCHAR
            }
            EthereumSqlTypeWrapper::AddressBytes(_) |
            EthereumSqlTypeWrapper::AddressBytesNullable(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecAddress(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::VecAddressBytes(_) => PgType::BYTEA_ARRAY,

            // Strings and Bytes
            EthereumSqlTypeWrapper::String(_) | EthereumSqlTypeWrapper::StringNullable(_) => {
                PgType::TEXT
            }
            EthereumSqlTypeWrapper::StringVarchar(_) |
            EthereumSqlTypeWrapper::StringVarcharNullable(_) => PgType::VARCHAR,
            EthereumSqlTypeWrapper::StringChar(_) |
            EthereumSqlTypeWrapper::StringCharNullable(_) => PgType::CHAR,
            EthereumSqlTypeWrapper::VecString(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::VecStringVarchar(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecStringChar(_) => PgType::CHAR_ARRAY,
            EthereumSqlTypeWrapper::Bytes(_) | EthereumSqlTypeWrapper::BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::VecBytes(_) => PgType::BYTEA_ARRAY,

            // DateTime
            EthereumSqlTypeWrapper::DateTime(_) => PgType::TIMESTAMPTZ,
        }
    }
}

impl ToSql for EthereumSqlTypeWrapper {
    fn to_sql(
        &self,
        ty: &PgType,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            EthereumSqlTypeWrapper::U64(value) => {
                let value = value.to_string();
                Decimal::to_sql(&value.parse::<Decimal>()?, ty, out)
            }
            EthereumSqlTypeWrapper::I64(value) => value.to_sql(ty, out),
            EthereumSqlTypeWrapper::VecU64(values) => serialize_vec_decimal(values, ty, out),
            EthereumSqlTypeWrapper::VecI64(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::U128(value) => {
                let value = value.to_string();
                Decimal::to_sql(&value.parse::<Decimal>()?, ty, out)
            }
            EthereumSqlTypeWrapper::I128(value) => {
                let value = value.to_string();
                Decimal::to_sql(&value.parse::<Decimal>()?, ty, out)
            }
            EthereumSqlTypeWrapper::VecU128(values) => serialize_vec_decimal(values, ty, out),
            EthereumSqlTypeWrapper::VecI128(values) => serialize_vec_decimal(values, ty, out),
            EthereumSqlTypeWrapper::U256(value) => {
                // handle two’s complement without adding a new type
                let i256_value = u256_to_i256(*value);
                String::to_sql(&i256_value.to_string(), ty, out)
            }
            EthereumSqlTypeWrapper::U256Nullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }
                // handle two’s complement without adding a new type
                let i256_value = u256_to_i256(*value);
                String::to_sql(&i256_value.to_string(), ty, out)
            }
            EthereumSqlTypeWrapper::U256Bytes(value) => {
                let mut bytes = [0u8; 32];
                value.to_big_endian(&mut bytes);
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::U256BytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let mut bytes = [0u8; 32];
                value.to_big_endian(&mut bytes);
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU256(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> = values
                        .iter()
                        .map(|v| {
                            // handle two’s complement without adding a new type
                            let i256_value = u256_to_i256(*v);
                            i256_value.to_string()
                        })
                        .collect();
                    let formatted_str = values_strings.join(",");
                    String::to_sql(&formatted_str, ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecU256Bytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let mut bytes = [0u8; 32];
                        value.to_big_endian(&mut bytes);
                        let bytes = Bytes::from(bytes);
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::I256(value) => {
                let value = value.to_string();
                String::to_sql(&value, ty, out)
            }
            EthereumSqlTypeWrapper::I256Nullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let value = value.to_string();
                String::to_sql(&value, ty, out)
            }
            EthereumSqlTypeWrapper::I256Bytes(value) => {
                let mut bytes = [0u8; 32];
                value.to_big_endian(&mut bytes);
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::I256BytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let mut bytes = [0u8; 32];
                value.to_big_endian(&mut bytes);
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecI256(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> =
                        values.iter().map(|v| v.to_string()).collect();
                    let formatted_str = values_strings.join(",");
                    String::to_sql(&formatted_str, ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecI256Bytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let mut bytes = [0u8; 32];
                        value.to_big_endian(&mut bytes);
                        let bytes = Bytes::from(bytes);
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::U512(value) => {
                let value = value.to_string();
                String::to_sql(&value, ty, out)
            }
            EthereumSqlTypeWrapper::VecU512(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> =
                        values.iter().map(|v| v.to_string()).collect();
                    let formatted_str = values_strings.join(",");
                    String::to_sql(&formatted_str, ty, out)
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
                    hexes.to_sql(ty, out)
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
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::H256(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::H256Bytes(value) => {
                let bytes: Bytes = value.as_bytes().to_vec().into();
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecH256(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecH256Bytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let bytes: Bytes = value.as_bytes().to_vec().into();
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
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
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::Address(value) => {
                let hex = format!("{:?}", value);
                String::to_sql(&hex, ty, out)
            }
            EthereumSqlTypeWrapper::AddressNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let hex = format!("{:?}", value);
                String::to_sql(&hex, ty, out)
            }
            EthereumSqlTypeWrapper::AddressBytes(value) => {
                let bytes: Bytes = value.as_bytes().to_vec().into();
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::AddressBytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let bytes: Bytes = value.as_bytes().to_vec().into();
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecAddress(values) => {
                let addresses: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if addresses.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    addresses.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecAddressBytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let bytes: Bytes = value.as_bytes().to_vec().into();
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::Bool(value) => bool::to_sql(value, ty, out),
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
            EthereumSqlTypeWrapper::String(value) |
            EthereumSqlTypeWrapper::StringVarchar(value) |
            EthereumSqlTypeWrapper::StringChar(value) => String::to_sql(value, ty, out),
            EthereumSqlTypeWrapper::StringNullable(value) |
            EthereumSqlTypeWrapper::StringVarcharNullable(value) |
            EthereumSqlTypeWrapper::StringCharNullable(value) => {
                if value.is_empty() {
                    return Ok(IsNull::Yes);
                }

                String::to_sql(value, ty, out)
            }
            EthereumSqlTypeWrapper::VecString(values) |
            EthereumSqlTypeWrapper::VecStringVarchar(values) |
            EthereumSqlTypeWrapper::VecStringChar(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::Bytes(value) => {
                out.extend_from_slice(value);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::BytesNullable(value) => {
                if value.is_empty() {
                    return Ok(IsNull::Yes);
                }

                out.extend_from_slice(value);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecBytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        out.extend_from_slice(value);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::U32(value) => {
                let int_value: i32 = *value as i32;
                int_value.to_sql(ty, out)
            }
            EthereumSqlTypeWrapper::I32(value) => value.to_sql(ty, out),
            EthereumSqlTypeWrapper::VecU32(values) => {
                let int_values: Vec<i32> = values.iter().map(|&s| s as i32).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecI32(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::U16(value) => {
                let int_value: i16 = *value as i16;
                int_value.to_sql(ty, out)
            }
            EthereumSqlTypeWrapper::I16(value) => value.to_sql(ty, out),
            EthereumSqlTypeWrapper::VecU16(values) => {
                let int_values: Vec<i16> = values.iter().map(|&s| s as i16).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecI16(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::U8(value) => {
                let int_value: i16 = *value as i16;
                int_value.to_sql(ty, out)
            }
            EthereumSqlTypeWrapper::I8(value) => {
                let int_value: i16 = *value as i16;
                int_value.to_sql(ty, out)
            }
            EthereumSqlTypeWrapper::VecU8(values) => {
                let int_values: Vec<i16> = values.iter().map(|&s| s as i16).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecI8(values) => {
                let int_values: Vec<i16> = values.iter().map(|&s| s as i16).collect();
                if int_values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    int_values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::DateTime(value) => value.to_sql(ty, out),
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
            let is_signed = t.starts_with("int");

            Some(match (size, is_signed) {
                (8, false) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU8(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U8(0)
                    }
                }
                (8, true) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI8(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I8(0)
                    }
                }
                (16, false) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU16(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U16(0)
                    }
                }
                (16, true) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI16(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I16(0)
                    }
                }
                (24 | 32, false) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU32(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U32(0)
                    }
                }
                (24 | 32, true) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI32(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I32(0)
                    }
                }
                (40 | 48 | 56 | 64, false) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU64(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U64(U64::zero())
                    }
                }
                (40 | 48 | 56 | 64, true) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI64(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I64(0)
                    }
                }
                (72 | 80 | 88 | 96 | 104 | 112 | 120 | 128, false) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU128(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U128(0)
                    }
                }
                (72 | 80 | 88 | 96 | 104 | 112 | 120 | 128, true) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI128(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I128(0)
                    }
                }
                (
                    136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232 |
                    240 | 248 | 256,
                    false,
                ) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU256(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U256(U256::zero())
                    }
                }
                (
                    136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232 |
                    240 | 248 | 256,
                    true,
                ) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI256(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I256(I256::zero())
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
        EthereumSqlTypeWrapper::I256(_) | EthereumSqlTypeWrapper::VecI256(_) => {
            // proxy back to U256 which handles I25 to avoid odd parsing issues
            EthereumSqlTypeWrapper::U256(*value)
        }
        EthereumSqlTypeWrapper::U128(_) | EthereumSqlTypeWrapper::VecU128(_) => {
            EthereumSqlTypeWrapper::U128(value.low_u128())
        }
        EthereumSqlTypeWrapper::I128(_) | EthereumSqlTypeWrapper::VecI128(_) => {
            EthereumSqlTypeWrapper::I128(value.low_u128() as i128)
        }
        EthereumSqlTypeWrapper::U64(_) | EthereumSqlTypeWrapper::VecU64(_) => {
            EthereumSqlTypeWrapper::U64(value.as_u64().into())
        }
        EthereumSqlTypeWrapper::I64(_) | EthereumSqlTypeWrapper::VecI64(_) => {
            EthereumSqlTypeWrapper::I64(value.as_u64() as i64)
        }
        EthereumSqlTypeWrapper::U32(_) | EthereumSqlTypeWrapper::VecU32(_) => {
            EthereumSqlTypeWrapper::U32(value.low_u32())
        }
        EthereumSqlTypeWrapper::I32(_) | EthereumSqlTypeWrapper::VecI32(_) => {
            EthereumSqlTypeWrapper::I32(value.low_u32() as i32)
        }
        EthereumSqlTypeWrapper::U16(_) | EthereumSqlTypeWrapper::VecU16(_) => {
            EthereumSqlTypeWrapper::U16(value.low_u32() as u16)
        }
        EthereumSqlTypeWrapper::I16(_) | EthereumSqlTypeWrapper::VecI16(_) => {
            EthereumSqlTypeWrapper::I16(value.low_u32() as i16)
        }
        EthereumSqlTypeWrapper::U8(_) | EthereumSqlTypeWrapper::VecU8(_) => {
            EthereumSqlTypeWrapper::U8(value.low_u32() as u8)
        }
        EthereumSqlTypeWrapper::I8(_) | EthereumSqlTypeWrapper::VecI8(_) => {
            EthereumSqlTypeWrapper::I8(value.low_u32() as i8)
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
                                EthereumSqlTypeWrapper::I256(_) |
                                EthereumSqlTypeWrapper::VecI256(_) => {
                                    EthereumSqlTypeWrapper::VecI256(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I256(v) => v,
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
                                EthereumSqlTypeWrapper::I128(_) |
                                EthereumSqlTypeWrapper::VecI128(_) => {
                                    EthereumSqlTypeWrapper::VecI128(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I128(v) => v,
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
                                EthereumSqlTypeWrapper::I64(_) |
                                EthereumSqlTypeWrapper::VecI64(_) => {
                                    EthereumSqlTypeWrapper::VecI64(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I64(v) => v,
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
                                EthereumSqlTypeWrapper::I32(_) |
                                EthereumSqlTypeWrapper::VecI32(_) => {
                                    EthereumSqlTypeWrapper::VecI32(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I32(v) => v,
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
                                EthereumSqlTypeWrapper::I16(_) |
                                EthereumSqlTypeWrapper::VecI16(_) => {
                                    EthereumSqlTypeWrapper::VecI16(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I16(v) => v,
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
                                EthereumSqlTypeWrapper::I8(_) |
                                EthereumSqlTypeWrapper::VecI8(_) => EthereumSqlTypeWrapper::VecI8(
                                    vec_wrapper
                                        .into_iter()
                                        .map(|w| match w {
                                            EthereumSqlTypeWrapper::I8(v) => v,
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
                    EthereumSqlTypeWrapper::I64(i) => json!(i),
                    EthereumSqlTypeWrapper::VecI64(i64s) => json!(i64s),
                    EthereumSqlTypeWrapper::U128(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU128(u128s) => {
                        json!(u128s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::I128(i) => json!(i.to_string()),
                    EthereumSqlTypeWrapper::VecI128(i128s) => {
                        json!(i128s.iter().map(|i| i.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::U256(u) |
                    EthereumSqlTypeWrapper::U256Bytes(u) |
                    EthereumSqlTypeWrapper::U256Nullable(u) |
                    EthereumSqlTypeWrapper::U256BytesNullable(u) => {
                        // handle two's complement without adding a new type
                        let i256_value = u256_to_i256(*u);
                        json!(i256_value.to_string())
                    }
                    EthereumSqlTypeWrapper::VecU256(u256s) |
                    EthereumSqlTypeWrapper::VecU256Bytes(u256s) => {
                        json!(u256s
                            .iter()
                            .map(|u| {
                                // handle two's complement without adding a new type
                                let i256_value = u256_to_i256(*u);
                                i256_value.to_string()
                            })
                            .collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::I256(i) |
                    EthereumSqlTypeWrapper::I256Bytes(i) |
                    EthereumSqlTypeWrapper::I256Nullable(i) |
                    EthereumSqlTypeWrapper::I256BytesNullable(i) => {
                        json!(i.to_string())
                    }
                    EthereumSqlTypeWrapper::VecI256(i256s) |
                    EthereumSqlTypeWrapper::VecI256Bytes(i256s) => {
                        json!(i256s.iter().map(|i| i.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::U512(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU512(u512s) => {
                        json!(u512s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::H128(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH128(h128s) => json!(h128s),
                    EthereumSqlTypeWrapper::H160(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH160(h160s) => json!(h160s),
                    EthereumSqlTypeWrapper::H256(h) | EthereumSqlTypeWrapper::H256Bytes(h) => {
                        json!(h)
                    }
                    EthereumSqlTypeWrapper::VecH256(h256s) |
                    EthereumSqlTypeWrapper::VecH256Bytes(h256s) => json!(h256s),
                    EthereumSqlTypeWrapper::H512(h) => json!(h),
                    EthereumSqlTypeWrapper::VecH512(h512s) => json!(h512s),
                    EthereumSqlTypeWrapper::Address(address) |
                    EthereumSqlTypeWrapper::AddressBytes(address) |
                    EthereumSqlTypeWrapper::AddressBytesNullable(address) |
                    EthereumSqlTypeWrapper::AddressNullable(address) => json!(address),
                    EthereumSqlTypeWrapper::VecAddress(addresses) |
                    EthereumSqlTypeWrapper::VecAddressBytes(addresses) => json!(addresses),
                    EthereumSqlTypeWrapper::Bool(b) => json!(b),
                    EthereumSqlTypeWrapper::VecBool(bools) => json!(bools),
                    EthereumSqlTypeWrapper::U32(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU32(u32s) => json!(u32s),
                    EthereumSqlTypeWrapper::I32(i) => json!(i),
                    EthereumSqlTypeWrapper::VecI32(i32s) => json!(i32s),
                    EthereumSqlTypeWrapper::U16(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU16(u16s) => json!(u16s),
                    EthereumSqlTypeWrapper::I16(i) => json!(i),
                    EthereumSqlTypeWrapper::VecI16(i16s) => json!(i16s),
                    EthereumSqlTypeWrapper::U8(u) => json!(u),
                    EthereumSqlTypeWrapper::VecU8(u8s) => json!(u8s),
                    EthereumSqlTypeWrapper::I8(i) => json!(i),
                    EthereumSqlTypeWrapper::VecI8(i8s) => json!(i8s),
                    EthereumSqlTypeWrapper::String(s) |
                    EthereumSqlTypeWrapper::StringNullable(s) |
                    EthereumSqlTypeWrapper::StringVarchar(s) |
                    EthereumSqlTypeWrapper::StringVarcharNullable(s) |
                    EthereumSqlTypeWrapper::StringChar(s) |
                    EthereumSqlTypeWrapper::StringCharNullable(s) => json!(s),
                    EthereumSqlTypeWrapper::VecString(strings) |
                    EthereumSqlTypeWrapper::VecStringVarchar(strings) |
                    EthereumSqlTypeWrapper::VecStringChar(strings) => json!(strings),
                    EthereumSqlTypeWrapper::Bytes(bytes) |
                    EthereumSqlTypeWrapper::BytesNullable(bytes) => json!(hex::encode(bytes)),
                    EthereumSqlTypeWrapper::VecBytes(bytes) => {
                        json!(bytes.iter().map(hex::encode).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::DateTime(date_time) => {
                        json!(date_time.to_rfc3339())
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
