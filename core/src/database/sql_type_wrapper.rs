use std::str::FromStr;

use crate::helpers::parse_solidity_integer_type;
use crate::{abi::ABIInput, event::callback_registry::TxInformation, types::core::LogParam};
#[allow(deprecated)]
use alloy::{
    dyn_abi::DynSolValue,
    primitives::{Address, Bytes, B128, B160, B256, B512, I256, U256, U512},
};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};
use tracing::error;
use uuid::Uuid;

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
    U64(u64),
    U64Nullable(u64),
    U64BigInt(u64),
    I64(i64),
    VecU64(Vec<u64>),
    VecI64(Vec<i64>),

    // 128-bit integers
    U128(u128),
    I128(i128),
    VecU128(Vec<u128>),
    VecI128(Vec<i128>),

    // 256-bit integers
    U256(U256),
    U256Numeric(U256),
    U256NumericNullable(Option<U256>),
    U256Nullable(U256),
    U256Bytes(U256),
    U256BytesNullable(U256),
    I256(I256),
    I256Numeric(I256),
    I256Nullable(I256),
    I256Bytes(I256),
    I256BytesNullable(I256),
    VecU256(Vec<U256>),
    VecU256Bytes(Vec<U256>),
    VecU256Numeric(Vec<U256>),
    VecI256(Vec<I256>),
    VecI256Bytes(Vec<I256>),

    // 512-bit integers
    U512(U512),
    VecU512(Vec<U512>),

    // Hashes
    B128(B128),
    #[deprecated(note = "Use Address instead")]
    #[allow(deprecated)]
    H160(B160),
    B256(B256),
    B256Bytes(B256),
    B512(B512),
    VecB128(Vec<B128>),
    #[deprecated(note = "Use Address instead")]
    #[allow(deprecated)]
    VecH160(Vec<B160>),
    VecB256(Vec<B256>),
    VecB256Bytes(Vec<B256>),
    VecB512(Vec<B512>),

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
    Uuid(Uuid),

    DateTime(DateTime<Utc>),
    DateTimeNullable(Option<DateTime<Utc>>),

    JSONB(Value),
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
            EthereumSqlTypeWrapper::U64Nullable(_) => "U64Nullable",
            EthereumSqlTypeWrapper::U64BigInt(_) => "U64BigInt",
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
            EthereumSqlTypeWrapper::U256Numeric(_) => "U256Numeric",
            EthereumSqlTypeWrapper::U256NumericNullable(_) => "U256NumericNullable",
            EthereumSqlTypeWrapper::U256Bytes(_) => "U256Bytes",
            EthereumSqlTypeWrapper::U256BytesNullable(_) => "U256BytesNullable",
            EthereumSqlTypeWrapper::I256(_) => "I256",
            EthereumSqlTypeWrapper::I256Numeric(_) => "I256Numeric",
            EthereumSqlTypeWrapper::I256Nullable(_) => "I256Nullable",
            EthereumSqlTypeWrapper::I256Bytes(_) => "I256Bytes",
            EthereumSqlTypeWrapper::I256BytesNullable(_) => "I256BytesNullable",
            EthereumSqlTypeWrapper::VecU256(_) => "VecU256",
            EthereumSqlTypeWrapper::VecU256Bytes(_) => "VecU256Bytes",
            EthereumSqlTypeWrapper::VecU256Numeric(_) => "VecU256Numeric",
            EthereumSqlTypeWrapper::VecI256(_) => "VecI256",
            EthereumSqlTypeWrapper::VecI256Bytes(_) => "VecI256Bytes",

            // 512-bit integers
            EthereumSqlTypeWrapper::U512(_) => "U512",
            EthereumSqlTypeWrapper::VecU512(_) => "VecU512",

            // Hashes
            EthereumSqlTypeWrapper::B128(_) => "B128",
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::H160(_) => "H160",
            EthereumSqlTypeWrapper::B256(_) => "B256",
            EthereumSqlTypeWrapper::B256Bytes(_) => "B256Bytes",
            EthereumSqlTypeWrapper::B512(_) => "B512",
            EthereumSqlTypeWrapper::VecB128(_) => "VecB128",
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::VecH160(_) => "VecH160",
            EthereumSqlTypeWrapper::VecB256(_) => "VecB256",
            EthereumSqlTypeWrapper::VecB256Bytes(_) => "VecB256Bytes",
            EthereumSqlTypeWrapper::VecB512(_) => "VecB512",

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
            EthereumSqlTypeWrapper::Uuid(_) => "Uuid",

            EthereumSqlTypeWrapper::DateTime(_) => "DateTime",
            EthereumSqlTypeWrapper::DateTimeNullable(_) => "DateTimeNullable",

            EthereumSqlTypeWrapper::JSONB(_) => "JSONB",
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
            EthereumSqlTypeWrapper::U64(_)
            | EthereumSqlTypeWrapper::U64Nullable(_)
            | EthereumSqlTypeWrapper::U64BigInt(_) => PgType::INT8,
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
            // 256-bit unsigned integers opt in numeric representation (numeric(78))
            EthereumSqlTypeWrapper::U256Numeric(_)
            | EthereumSqlTypeWrapper::U256NumericNullable(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::U256Bytes(_) | EthereumSqlTypeWrapper::U256BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::I256(_) | EthereumSqlTypeWrapper::I256Nullable(_) => {
                PgType::VARCHAR
            }
            EthereumSqlTypeWrapper::I256Numeric(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::I256Bytes(_) | EthereumSqlTypeWrapper::I256BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::VecU256(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecU256Bytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecU256Numeric(_) => PgType::NUMERIC_ARRAY,
            EthereumSqlTypeWrapper::VecI256(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecI256Bytes(_) => PgType::BYTEA_ARRAY,

            // 512-bit integers
            EthereumSqlTypeWrapper::U512(_) => PgType::TEXT,
            EthereumSqlTypeWrapper::VecU512(_) => PgType::TEXT_ARRAY,

            // Hashes
            EthereumSqlTypeWrapper::B128(_) => PgType::BYTEA,
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::H160(_) => PgType::BYTEA,
            // TODO! LOOK AT THIS TYPE AS IT IS SAVED AS CHAR IN NO CODE
            EthereumSqlTypeWrapper::B256(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::B256Bytes(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::B512(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecB128(_) => PgType::BYTEA_ARRAY,
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::VecH160(_) => PgType::BYTEA_ARRAY,
            // TODO! LOOK AT THIS TYPE AS IT IS SAVED AS CHAR IN NO CODE
            EthereumSqlTypeWrapper::VecB256(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecB256Bytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::VecB512(_) => PgType::BYTEA_ARRAY,

            // Address
            EthereumSqlTypeWrapper::Address(_) | EthereumSqlTypeWrapper::AddressNullable(_) => {
                PgType::BPCHAR
            }
            EthereumSqlTypeWrapper::AddressBytes(_)
            | EthereumSqlTypeWrapper::AddressBytesNullable(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecAddress(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::VecAddressBytes(_) => PgType::BYTEA_ARRAY,

            // Strings and Bytes
            EthereumSqlTypeWrapper::String(_) | EthereumSqlTypeWrapper::StringNullable(_) => {
                PgType::TEXT
            }
            EthereumSqlTypeWrapper::StringVarchar(_)
            | EthereumSqlTypeWrapper::StringVarcharNullable(_) => PgType::VARCHAR,
            EthereumSqlTypeWrapper::StringChar(_)
            | EthereumSqlTypeWrapper::StringCharNullable(_) => PgType::CHAR,
            EthereumSqlTypeWrapper::VecString(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::VecStringVarchar(_) => PgType::VARCHAR_ARRAY,
            EthereumSqlTypeWrapper::VecStringChar(_) => PgType::CHAR_ARRAY,
            EthereumSqlTypeWrapper::Bytes(_) | EthereumSqlTypeWrapper::BytesNullable(_) => {
                PgType::BYTEA
            }
            EthereumSqlTypeWrapper::VecBytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::Uuid(_) => PgType::UUID,

            // DateTime
            EthereumSqlTypeWrapper::DateTime(_) | EthereumSqlTypeWrapper::DateTimeNullable(_) => {
                PgType::TIMESTAMPTZ
            }

            EthereumSqlTypeWrapper::JSONB(_) => PgType::JSONB,
        }
    }

    pub fn to_sqlite_string_value(&self) -> String {
        // SQLite will store everything as TEXT, INTEGER, REAL, or BLOB
        // For simplicity, we convert most things to text representation
        match self {
            // Boolean
            EthereumSqlTypeWrapper::Bool(value) => if *value { "1" } else { "0" }.to_string(),
            EthereumSqlTypeWrapper::VecBool(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            // Integers - store as text to avoid overflow issues
            EthereumSqlTypeWrapper::U8(value) => value.to_string(),
            EthereumSqlTypeWrapper::I8(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU8(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }
            EthereumSqlTypeWrapper::VecI8(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U16(value) => value.to_string(),
            EthereumSqlTypeWrapper::I16(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU16(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }
            EthereumSqlTypeWrapper::VecI16(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U32(value) => value.to_string(),
            EthereumSqlTypeWrapper::I32(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU32(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }
            EthereumSqlTypeWrapper::VecI32(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U64(value)
            | EthereumSqlTypeWrapper::U64Nullable(value)
            | EthereumSqlTypeWrapper::U64BigInt(value) => value.to_string(),
            EthereumSqlTypeWrapper::I64(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU64(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }
            EthereumSqlTypeWrapper::VecI64(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U128(value) => value.to_string(),
            EthereumSqlTypeWrapper::I128(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU128(values) => {
                serde_json::to_string(&values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            }
            EthereumSqlTypeWrapper::VecI128(values) => {
                serde_json::to_string(&values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U256(value)
            | EthereumSqlTypeWrapper::U256Nullable(value)
            | EthereumSqlTypeWrapper::U256Numeric(value)
            | EthereumSqlTypeWrapper::U256Bytes(value)
            | EthereumSqlTypeWrapper::U256BytesNullable(value) => value.to_string(),
            EthereumSqlTypeWrapper::U256NumericNullable(value) => {
                value.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string())
            }
            EthereumSqlTypeWrapper::VecU256(values)
            | EthereumSqlTypeWrapper::VecU256Bytes(values)
            | EthereumSqlTypeWrapper::VecU256Numeric(values) => {
                serde_json::to_string(&values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            }

            EthereumSqlTypeWrapper::I256(value)
            | EthereumSqlTypeWrapper::I256Nullable(value)
            | EthereumSqlTypeWrapper::I256Numeric(value)
            | EthereumSqlTypeWrapper::I256Bytes(value)
            | EthereumSqlTypeWrapper::I256BytesNullable(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecI256(values)
            | EthereumSqlTypeWrapper::VecI256Bytes(values) => {
                serde_json::to_string(&values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            }

            EthereumSqlTypeWrapper::U512(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU512(values) => {
                serde_json::to_string(&values.iter().map(|v| v.to_string()).collect::<Vec<_>>())
                    .unwrap_or_default()
            }

            // Hashes and bytes - store as hex strings
            EthereumSqlTypeWrapper::B128(value) => format!("{:?}", value),
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::H160(value) => format!("{:?}", value),
            EthereumSqlTypeWrapper::B256(value) | EthereumSqlTypeWrapper::B256Bytes(value) => {
                format!("{:?}", value)
            }
            EthereumSqlTypeWrapper::B512(value) => format!("{:?}", value),
            EthereumSqlTypeWrapper::VecB128(values) => serde_json::to_string(
                &values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::VecH160(values) => serde_json::to_string(
                &values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),
            EthereumSqlTypeWrapper::VecB256(values)
            | EthereumSqlTypeWrapper::VecB256Bytes(values) => serde_json::to_string(
                &values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),
            EthereumSqlTypeWrapper::VecB512(values) => serde_json::to_string(
                &values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),

            // Address
            EthereumSqlTypeWrapper::Address(value)
            | EthereumSqlTypeWrapper::AddressNullable(value)
            | EthereumSqlTypeWrapper::AddressBytes(value)
            | EthereumSqlTypeWrapper::AddressBytesNullable(value) => format!("{:?}", value),
            EthereumSqlTypeWrapper::VecAddress(values)
            | EthereumSqlTypeWrapper::VecAddressBytes(values) => serde_json::to_string(
                &values.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),

            // Strings
            EthereumSqlTypeWrapper::String(value)
            | EthereumSqlTypeWrapper::StringVarchar(value)
            | EthereumSqlTypeWrapper::StringChar(value)
            | EthereumSqlTypeWrapper::StringNullable(value)
            | EthereumSqlTypeWrapper::StringVarcharNullable(value)
            | EthereumSqlTypeWrapper::StringCharNullable(value) => value.clone(),
            EthereumSqlTypeWrapper::VecString(values)
            | EthereumSqlTypeWrapper::VecStringVarchar(values)
            | EthereumSqlTypeWrapper::VecStringChar(values) => {
                serde_json::to_string(values).unwrap_or_default()
            }

            // Bytes
            EthereumSqlTypeWrapper::Bytes(value) | EthereumSqlTypeWrapper::BytesNullable(value) => {
                format!("0x{}", hex::encode(value))
            }
            EthereumSqlTypeWrapper::VecBytes(values) => serde_json::to_string(
                &values.iter().map(|v| format!("0x{}", hex::encode(v))).collect::<Vec<_>>(),
            )
            .unwrap_or_default(),

            // DateTime
            EthereumSqlTypeWrapper::DateTime(value) => value.to_rfc3339(),
            EthereumSqlTypeWrapper::DateTimeNullable(value) => {
                value.as_ref().map(|v| v.to_rfc3339()).unwrap_or_else(|| "NULL".to_string())
            }

            // UUID
            EthereumSqlTypeWrapper::Uuid(value) => value.to_string(),

            // JSONB
            EthereumSqlTypeWrapper::JSONB(value) => {
                serde_json::to_string(value).unwrap_or_default()
            }
        }
    }

    pub fn to_clickhouse_value(&self) -> String {
        match self {
            // Boolean
            EthereumSqlTypeWrapper::Bool(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecBool(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 8-bit integers
            EthereumSqlTypeWrapper::U8(value) => value.to_string(),
            EthereumSqlTypeWrapper::I8(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU8(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI8(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 16-bit integers
            EthereumSqlTypeWrapper::U16(value) => value.to_string(),
            EthereumSqlTypeWrapper::I16(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU16(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI16(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 32-bit integers
            EthereumSqlTypeWrapper::U32(value) => value.to_string(),
            EthereumSqlTypeWrapper::I32(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU32(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI32(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 64-bit integers
            EthereumSqlTypeWrapper::U64(value) => value.to_string(),
            EthereumSqlTypeWrapper::I64(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU64(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI64(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 128-bit integers
            EthereumSqlTypeWrapper::U128(value) => value.to_string(),
            EthereumSqlTypeWrapper::I128(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU128(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI128(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 256-bit integers
            EthereumSqlTypeWrapper::U256(value) => value.to_string(),
            EthereumSqlTypeWrapper::I256(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU256(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecI256(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // 512-bit integers
            EthereumSqlTypeWrapper::U512(value) => value.to_string(),
            EthereumSqlTypeWrapper::VecU512(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            // Hashes
            EthereumSqlTypeWrapper::B128(value) => format!("'{value:?}'"),

            EthereumSqlTypeWrapper::B256(value) => format!("'{value:?}'"),
            EthereumSqlTypeWrapper::B512(value) => format!("'{value:?}'"),
            EthereumSqlTypeWrapper::VecB128(values) => format!(
                "[{}]",
                values.iter().map(|v| format!("'{v:?}'")).collect::<Vec<_>>().join(", ")
            ),
            EthereumSqlTypeWrapper::VecB256(values) => format!(
                "[{}]",
                values.iter().map(|v| format!("'{v:?}'")).collect::<Vec<_>>().join(", ")
            ),
            EthereumSqlTypeWrapper::VecB512(values) => format!(
                "[{}]",
                values.iter().map(|v| format!("'{v:?}'")).collect::<Vec<_>>().join(", ")
            ),

            // Address
            EthereumSqlTypeWrapper::Address(address) => format!("'{address}'"),
            EthereumSqlTypeWrapper::VecAddress(addresses) => format!(
                "[{}]",
                addresses.iter().map(|addr| format!("'{}'", addr)).collect::<Vec<_>>().join(", ")
            ),

            // Strings and Bytes
            EthereumSqlTypeWrapper::String(value) => format!("'{}'", value.replace("'", "\\'")),
            EthereumSqlTypeWrapper::VecString(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|v| format!("'{}'", v.replace("'", "\\'")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            EthereumSqlTypeWrapper::Bytes(value) => format!("'0x{}'", hex::encode(value)),
            EthereumSqlTypeWrapper::VecBytes(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(|v| format!("'0x{}'", hex::encode(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),

            // DateTime
            EthereumSqlTypeWrapper::DateTime(value) => {
                let timestamp = value.to_rfc3339();
                let (datetime, _) =
                    timestamp.split_once('+').expect("DateTime should have a timezone");
                format!("'{datetime}'",)
            }
            EthereumSqlTypeWrapper::DateTimeNullable(value) => {
                if let Some(value) = value {
                    let timestamp = value.to_rfc3339();
                    let (datetime, _) =
                        timestamp.split_once('+').expect("DateTime should have a timezone");

                    format!("'{datetime}'")
                } else {
                    "NULL".to_string()
                }
            }

            EthereumSqlTypeWrapper::I256Nullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::U64Nullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::U256Nullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::U64BigInt(v) => v.to_string(),
            EthereumSqlTypeWrapper::StringVarchar(v) => v.to_string(),
            EthereumSqlTypeWrapper::StringChar(v) => v.to_string(),
            EthereumSqlTypeWrapper::StringNullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::StringVarcharNullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::StringCharNullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::AddressNullable(v) => v.to_string(),
            EthereumSqlTypeWrapper::BytesNullable(v) => format!("'0x{}'", hex::encode(v)),

            #[allow(deprecated)]
            EthereumSqlTypeWrapper::H160(v) => v.to_string(),
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::VecH160(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            EthereumSqlTypeWrapper::VecStringVarchar(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }
            EthereumSqlTypeWrapper::VecStringChar(values) => {
                format!("[{}]", values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))
            }

            EthereumSqlTypeWrapper::Uuid(_)
            | EthereumSqlTypeWrapper::VecB256Bytes(_)
            | EthereumSqlTypeWrapper::VecI256Bytes(_)
            | EthereumSqlTypeWrapper::B256Bytes(_)
            | EthereumSqlTypeWrapper::JSONB(_)
            | EthereumSqlTypeWrapper::U256Numeric(_)
            | EthereumSqlTypeWrapper::U256NumericNullable(_)
            | EthereumSqlTypeWrapper::VecU256Bytes(_)
            | EthereumSqlTypeWrapper::VecU256Numeric(_)
            | EthereumSqlTypeWrapper::U256Bytes(_)
            | EthereumSqlTypeWrapper::U256BytesNullable(_)
            | EthereumSqlTypeWrapper::I256Numeric(_)
            | EthereumSqlTypeWrapper::AddressBytes(_)
            | EthereumSqlTypeWrapper::AddressBytesNullable(_)
            | EthereumSqlTypeWrapper::VecAddressBytes(_)
            | EthereumSqlTypeWrapper::I256Bytes(_)
            | EthereumSqlTypeWrapper::I256BytesNullable(_) => {
                panic!(
                    "Clickhouse in no-code should never encounter these types. Clickhouse rust projects should use prefer the native-protocol. Unsupported '{}' EthereumSqlTypeWrapper variant for ClickHouse serialization",
                    self.raw_name()
                )
            }
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

    fn convert_to_base_10000_numeric_digits<T: Into<u128> + Copy>(value: T) -> Vec<i16> {
        let mut groups = Vec::new();
        let mut num: u128 = value.into();
        while num > 0 {
            groups.push((num % 10000) as i16);
            num /= 10000;
        }
        groups.reverse();
        groups
    }

    fn convert_u256_to_base_10000_numeric_digits(value: &U256) -> Vec<i16> {
        let mut groups = Vec::new();
        let mut num = *value;
        if num.is_zero() {
            return vec![0];
        }
        while !num.is_zero() {
            let remainder = num % U256::from(10000);
            let bytes: [u8; 32] = remainder.to_be_bytes();
            let bytes: [u8; 2] = bytes[30..].try_into().unwrap();
            let remainder_i16 = i16::from_be_bytes(bytes);
            groups.push(remainder_i16);
            num /= U256::from(10000);
        }

        groups.reverse();
        groups
    }

    fn write_numeric_to_postgres<T>(
        value: T,
        is_negative: bool,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        T: Into<u128> + Copy,
    {
        let groups = Self::convert_to_base_10000_numeric_digits(value);

        if groups.is_empty() {
            // Handle zero case
            out.put_i16(0); // ndigits
            out.put_i16(0); // weight
            out.put_i16(0x0000); // sign
            out.put_i16(0); // dscale
            return Ok(IsNull::No);
        }

        out.put_i16(groups.len() as i16); // ndigits
        out.put_i16((groups.len() - 1) as i16); // weight - safe now as we checked for empty
        out.put_i16(if is_negative { 0x4000 } else { 0x0000 }); // sign
        out.put_i16(0); // dscale

        for group in groups {
            out.put_i16(group);
        }

        Ok(IsNull::No)
    }

    fn write_u256_numeric_to_postgres<T>(
        value: T,
        is_negative: bool,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        T: Into<U256>,
    {
        let groups = Self::convert_u256_to_base_10000_numeric_digits(&value.into());

        if groups.is_empty() {
            // Handle zero case
            out.put_i16(0); // ndigits
            out.put_i16(0); // weight
            out.put_i16(0x0000); // sign
            out.put_i16(0); // dscale
            return Ok(IsNull::No);
        }

        out.put_i16(groups.len() as i16); // ndigits
        out.put_i16((groups.len() - 1) as i16); // weight - safe now as we checked for empty
        out.put_i16(if is_negative { 0x4000 } else { 0x0000 }); // sign
        out.put_i16(0); // dscale

        for group in groups {
            out.put_i16(group);
        }

        Ok(IsNull::No)
    }

    fn serialize_numeric_array<T>(
        values: &[T],
        out: &mut BytesMut,
        value_converter: impl Fn(&T) -> (u128, bool), // (absolute value, is_negative)
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if values.is_empty() {
            return Ok(IsNull::Yes);
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(1i32.to_be_bytes()));
        buf.extend_from_slice(&(0i32.to_be_bytes()));
        buf.extend_from_slice(&PgType::NUMERIC.oid().to_be_bytes());
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());
        buf.extend_from_slice(&(1i32.to_be_bytes()));

        for value in values {
            let (abs_value, is_negative) = value_converter(value);
            let mut elem_buf = BytesMut::new();
            Self::write_numeric_to_postgres(abs_value, is_negative, &mut elem_buf)?;

            buf.extend_from_slice(&(elem_buf.len() as i32).to_be_bytes());
            buf.extend_from_slice(&elem_buf);
        }

        out.extend_from_slice(&buf);
        Ok(IsNull::No)
    }

    fn serialize_numeric_u256_array<T>(
        values: &[T],
        out: &mut BytesMut,
        value_converter: impl Fn(&T) -> (U256, bool), // (absolute value, is_negative)
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if values.is_empty() {
            return Ok(IsNull::Yes);
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(1i32.to_be_bytes()));
        buf.extend_from_slice(&(0i32.to_be_bytes()));
        buf.extend_from_slice(&PgType::NUMERIC.oid().to_be_bytes());
        buf.extend_from_slice(&(values.len() as i32).to_be_bytes());
        buf.extend_from_slice(&(1i32.to_be_bytes()));

        for value in values {
            let (abs_value, is_negative) = value_converter(value);
            let mut elem_buf = BytesMut::new();
            Self::write_u256_numeric_to_postgres(abs_value, is_negative, &mut elem_buf)?;

            buf.extend_from_slice(&(elem_buf.len() as i32).to_be_bytes());
            buf.extend_from_slice(&elem_buf);
        }

        out.extend_from_slice(&buf);
        Ok(IsNull::No)
    }
}

impl ToSql for EthereumSqlTypeWrapper {
    fn to_sql(
        &self,
        ty: &PgType,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            EthereumSqlTypeWrapper::U64(value) => Decimal::to_sql(&Decimal::from(*value), ty, out),
            EthereumSqlTypeWrapper::U64BigInt(value) => {
                // Convert u64 directly to i64 for BIGINT
                let pg_value = *value as i64;
                pg_value.to_sql(ty, out)
            }
            EthereumSqlTypeWrapper::U64Nullable(value) => {
                if *value == 0 {
                    return Ok(IsNull::Yes);
                }
                Decimal::to_sql(&Decimal::from(*value), ty, out)
            }
            EthereumSqlTypeWrapper::I64(value) => value.to_sql(ty, out),
            EthereumSqlTypeWrapper::VecU64(values) => Self::serialize_vec_decimal(values, ty, out),
            EthereumSqlTypeWrapper::VecI64(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    values.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::U128(value) => {
                Self::write_numeric_to_postgres(*value, false, out)
            }
            EthereumSqlTypeWrapper::I128(value) => {
                Self::write_numeric_to_postgres(value.unsigned_abs(), *value < 0, out)
            }
            EthereumSqlTypeWrapper::VecU128(values) => {
                Self::serialize_numeric_array(values, out, |v| (*v, false))
            }
            EthereumSqlTypeWrapper::VecI128(values) => {
                Self::serialize_numeric_array(values, out, |v| (v.unsigned_abs(), *v < 0))
            }
            EthereumSqlTypeWrapper::U256(value) => String::to_sql(&value.to_string(), ty, out),
            EthereumSqlTypeWrapper::U256Nullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }
                String::to_sql(&value.to_string(), ty, out)
            }
            EthereumSqlTypeWrapper::U256Numeric(value) => {
                Self::write_u256_numeric_to_postgres(*value, false, out)
            }
            EthereumSqlTypeWrapper::U256NumericNullable(value) => {
                if let Some(v) = value {
                    Self::write_u256_numeric_to_postgres(*v, false, out)
                } else {
                    Ok(IsNull::Yes)
                }
            }
            EthereumSqlTypeWrapper::U256Bytes(value) => {
                let bytes: [u8; 32] = value.to_be_bytes();
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::U256BytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let bytes: [u8; 32] = value.to_be_bytes();
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU256(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    let values_strings: Vec<String> =
                        values.iter().map(|v| v.to_string()).collect();
                    EthereumSqlTypeWrapper::VecStringVarchar(values_strings).to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecU256Bytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let bytes: [u8; 32] = value.to_be_bytes();
                        let bytes = Bytes::from(bytes);
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::VecU256Numeric(values) => {
                Self::serialize_numeric_u256_array(values, out, |v| (*v, false))
            }
            EthereumSqlTypeWrapper::I256(value) => {
                let value = value.to_string();
                String::to_sql(&value, ty, out)
            }
            EthereumSqlTypeWrapper::I256Numeric(value) => {
                let is_negative = value.is_negative();
                let abs_value: U256 =
                    if is_negative { value.abs().into_raw() } else { value.into_raw() };
                Self::write_u256_numeric_to_postgres(abs_value, is_negative, out)
            }
            EthereumSqlTypeWrapper::I256Nullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let value = value.to_string();
                String::to_sql(&value, ty, out)
            }
            EthereumSqlTypeWrapper::I256Bytes(value) => {
                let bytes: [u8; 32] = value.to_be_bytes();
                let bytes = Bytes::from(bytes);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::I256BytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let bytes: [u8; 32] = value.to_be_bytes();
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
                        let bytes: [u8; 32] = value.to_be_bytes();
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
            EthereumSqlTypeWrapper::B128(value) => {
                let hex = format!("{value:?}");
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecB128(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{s:?}")).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(ty, out)
                }
            }
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::H160(value) => {
                let hex = format!("{value:?}");
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            #[allow(deprecated)]
            EthereumSqlTypeWrapper::VecH160(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{s:?}")).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::B256(value) => {
                let hex = format!("{value:?}");
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::B256Bytes(value) => {
                let bytes = Bytes::from(value.0);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecB256(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{s:?}")).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::VecB256Bytes(values) => {
                if values.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    for value in values {
                        let bytes = Bytes::from(value.0);
                        out.extend_from_slice(&bytes);
                    }
                    Ok(IsNull::No)
                }
            }
            EthereumSqlTypeWrapper::B512(value) => {
                let hex = format!("{value:?}");
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecB512(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{s:?}")).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::Address(value) => {
                let hex = format!("{value:?}");
                String::to_sql(&hex, ty, out)
            }
            EthereumSqlTypeWrapper::AddressNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let hex = format!("{value:?}");
                String::to_sql(&hex, ty, out)
            }
            EthereumSqlTypeWrapper::AddressBytes(value) => {
                let bytes = Bytes::from(value.0);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::AddressBytesNullable(value) => {
                if value.is_zero() {
                    return Ok(IsNull::Yes);
                }

                let bytes = Bytes::from(value.0);
                out.extend_from_slice(&bytes);
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecAddress(values) => {
                let addresses: Vec<String> = values.iter().map(|s| format!("{s:?}")).collect();
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
                        let bytes = Bytes::from(value.0);
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
            EthereumSqlTypeWrapper::String(value)
            | EthereumSqlTypeWrapper::StringVarchar(value)
            | EthereumSqlTypeWrapper::StringChar(value) => String::to_sql(value, ty, out),
            EthereumSqlTypeWrapper::StringNullable(value)
            | EthereumSqlTypeWrapper::StringVarcharNullable(value)
            | EthereumSqlTypeWrapper::StringCharNullable(value) => {
                if value.is_empty() {
                    return Ok(IsNull::Yes);
                }

                String::to_sql(value, ty, out)
            }
            EthereumSqlTypeWrapper::VecString(values)
            | EthereumSqlTypeWrapper::VecStringVarchar(values)
            | EthereumSqlTypeWrapper::VecStringChar(values) => {
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
            EthereumSqlTypeWrapper::DateTimeNullable(value) => {
                if value.is_none() {
                    Ok(IsNull::Yes)
                } else {
                    value.to_sql(ty, out)
                }
            }
            EthereumSqlTypeWrapper::JSONB(value) => value.to_sql(ty, out),
            EthereumSqlTypeWrapper::Uuid(value) => value.to_sql(ty, out),
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
            EthereumSqlTypeWrapper::Address(Address::ZERO)
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
            let (prefix, size) = parse_solidity_integer_type(t);
            let is_signed = prefix.eq("int");

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
                        EthereumSqlTypeWrapper::U64(0)
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
                    136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232
                    | 240 | 248 | 256,
                    false,
                ) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecU256(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::U256(U256::ZERO)
                    }
                }
                (
                    136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232
                    | 240 | 248 | 256,
                    true,
                ) => {
                    if is_array {
                        EthereumSqlTypeWrapper::VecI256(Vec::new())
                    } else {
                        EthereumSqlTypeWrapper::I256(I256::ZERO)
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
            wrappers.extend(map_log_token_to_ethereum_wrapper(abi_input, &param.value))
        } else {
            panic!("No ABI input found for log param at index: {index}")
        }
    }

    wrappers
}

fn process_tuple(abi_inputs: &[ABIInput], tokens: &[DynSolValue]) -> Vec<EthereumSqlTypeWrapper> {
    let mut wrappers = vec![];

    for (index, token) in tokens.iter().enumerate() {
        if let Some(abi_input) = abi_inputs.get(index) {
            wrappers.extend(map_log_token_to_ethereum_wrapper(abi_input, token));
        } else {
            panic!("No ABI input found for log param at index: {index}")
        }
    }

    wrappers
}

fn tuple_solidity_type_to_ethereum_sql_type_wrapper(
    abi_inputs: &[ABIInput],
) -> Option<Vec<EthereumSqlTypeWrapper>> {
    let mut wrappers = vec![];

    for abi_input in abi_inputs {
        match &abi_input.components {
            Some(components) => {
                wrappers.extend(tuple_solidity_type_to_ethereum_sql_type_wrapper(components)?)
            }
            None => {
                wrappers.push(solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_)?);
            }
        }
    }

    Some(wrappers)
}

fn low_u128(value: &U256) -> u128 {
    // Referenced from: https://github.com/paritytech/parity-common/blob/a2b580d9fd5a340cea817bc9ed829320d2c9cd73/uint/src/uint.rs#L499
    let arr = value.as_limbs();

    ((arr[1] as u128) << 64) + arr[0] as u128
}

fn low_u128_from_int(value: &I256) -> u128 {
    // Referenced from: https://github.com/paritytech/parity-common/blob/a2b580d9fd5a340cea817bc9ed829320d2c9cd73/uint/src/uint.rs#L499
    let arr = value.as_limbs();

    ((arr[1] as u128) << 64) + arr[0] as u128
}

fn low_u32(value: &U256) -> u32 {
    value.to::<u32>()
}

fn as_u64(value: &U256) -> u64 {
    let low = value.into_limbs()[0];
    if value > &U256::from(low) {
        panic!("Integer overflow when casting to u64")
    }
    low
}

fn convert_int(value: &I256, target_type: &EthereumSqlTypeWrapper) -> EthereumSqlTypeWrapper {
    match target_type {
        EthereumSqlTypeWrapper::I256(_) | EthereumSqlTypeWrapper::VecI256(_) => {
            EthereumSqlTypeWrapper::I256(*value)
        }
        EthereumSqlTypeWrapper::U128(_) | EthereumSqlTypeWrapper::VecU128(_) => {
            EthereumSqlTypeWrapper::U128(low_u128_from_int(value))
        }
        EthereumSqlTypeWrapper::I128(_) | EthereumSqlTypeWrapper::VecI128(_) => {
            EthereumSqlTypeWrapper::I128(low_u128_from_int(value) as i128)
        }
        EthereumSqlTypeWrapper::U64(_) | EthereumSqlTypeWrapper::VecU64(_) => {
            EthereumSqlTypeWrapper::U64(value.as_u64())
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
            let error_message = format!("Unsupported target type - {target_type:?}");
            error!("{}", error_message);
            panic!("{}", error_message)
        }
    }
}

fn convert_uint(value: &U256, target_type: &EthereumSqlTypeWrapper) -> EthereumSqlTypeWrapper {
    match target_type {
        EthereumSqlTypeWrapper::U256(_) | EthereumSqlTypeWrapper::VecU256(_) => {
            EthereumSqlTypeWrapper::U256(*value)
        }
        EthereumSqlTypeWrapper::I256(_) | EthereumSqlTypeWrapper::VecI256(_) => {
            EthereumSqlTypeWrapper::I256(I256::from_raw(*value))
        }
        EthereumSqlTypeWrapper::U128(_) | EthereumSqlTypeWrapper::VecU128(_) => {
            EthereumSqlTypeWrapper::U128(low_u128(value))
        }
        EthereumSqlTypeWrapper::I128(_) | EthereumSqlTypeWrapper::VecI128(_) => {
            EthereumSqlTypeWrapper::I128(low_u128(value) as i128)
        }
        EthereumSqlTypeWrapper::U64(_) | EthereumSqlTypeWrapper::VecU64(_) => {
            EthereumSqlTypeWrapper::U64(as_u64(value))
        }
        EthereumSqlTypeWrapper::I64(_) | EthereumSqlTypeWrapper::VecI64(_) => {
            EthereumSqlTypeWrapper::I64(as_u64(value) as i64)
        }
        EthereumSqlTypeWrapper::U32(_) | EthereumSqlTypeWrapper::VecU32(_) => {
            EthereumSqlTypeWrapper::U32(low_u32(value))
        }
        EthereumSqlTypeWrapper::I32(_) | EthereumSqlTypeWrapper::VecI32(_) => {
            EthereumSqlTypeWrapper::I32(low_u32(value) as i32)
        }
        EthereumSqlTypeWrapper::U16(_) | EthereumSqlTypeWrapper::VecU16(_) => {
            EthereumSqlTypeWrapper::U16(low_u32(value) as u16)
        }
        EthereumSqlTypeWrapper::I16(_) | EthereumSqlTypeWrapper::VecI16(_) => {
            EthereumSqlTypeWrapper::I16(low_u32(value) as i16)
        }
        EthereumSqlTypeWrapper::U8(_) | EthereumSqlTypeWrapper::VecU8(_) => {
            EthereumSqlTypeWrapper::U8(low_u32(value) as u8)
        }
        EthereumSqlTypeWrapper::I8(_) | EthereumSqlTypeWrapper::VecI8(_) => {
            EthereumSqlTypeWrapper::I8(low_u32(value) as i8)
        }
        _ => {
            let error_message = format!("Unsupported target type - {target_type:?}");
            error!("{}", error_message);
            panic!("{}", error_message)
        }
    }
}

fn map_dynamic_int_to_ethereum_sql_type_wrapper(
    abi_input: &ABIInput,
    value: &I256,
) -> EthereumSqlTypeWrapper {
    let sql_type_wrapper = solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_);
    if let Some(target_type) = sql_type_wrapper {
        convert_int(value, &target_type)
    } else {
        let error_message = format!("Unknown int type for abi input: {abi_input:?}");
        error!("{}", error_message);
        panic!("{}", error_message);
    }
}

fn map_dynamic_uint_to_ethereum_sql_type_wrapper(
    abi_input: &ABIInput,
    value: &U256,
) -> EthereumSqlTypeWrapper {
    let sql_type_wrapper = solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_);
    if let Some(target_type) = sql_type_wrapper {
        convert_uint(value, &target_type)
    } else {
        let error_message = format!("Unknown int type for abi input: {abi_input:?}");
        error!("{}", error_message);
        panic!("{}", error_message);
    }
}

fn map_log_token_to_ethereum_wrapper(
    abi_input: &ABIInput,
    token: &DynSolValue,
) -> Vec<EthereumSqlTypeWrapper> {
    match &token {
        DynSolValue::Address(address) => vec![EthereumSqlTypeWrapper::Address(*address)],
        DynSolValue::Int(value, _) => {
            vec![map_dynamic_int_to_ethereum_sql_type_wrapper(abi_input, value)]
        }
        DynSolValue::Uint(value, _) => {
            vec![map_dynamic_uint_to_ethereum_sql_type_wrapper(abi_input, value)]
        }
        DynSolValue::Bool(b) => vec![EthereumSqlTypeWrapper::Bool(*b)],
        DynSolValue::String(s) => vec![EthereumSqlTypeWrapper::String(s.clone())],
        DynSolValue::FixedBytes(bytes, _) => {
            vec![EthereumSqlTypeWrapper::Bytes(Bytes::from(*bytes))]
        }
        DynSolValue::Bytes(bytes) => {
            vec![EthereumSqlTypeWrapper::Bytes(Bytes::from(bytes.clone()))]
        }
        DynSolValue::FixedArray(tokens) | DynSolValue::Array(tokens) => {
            match tokens.first() {
                None => match &abi_input.components {
                    Some(components) => tuple_solidity_type_to_ethereum_sql_type_wrapper(
                        components,
                    )
                    .unwrap_or_else(|| {
                        panic!(
                            "map_log_token_to_ethereum_wrapper:: Unknown type: {}",
                            abi_input.type_
                        )
                    }),
                    None => {
                        vec![solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_)
                            .unwrap_or_else(|| {
                                panic!(
                                    "map_log_token_to_ethereum_wrapper:: Unknown type: {}",
                                    abi_input.type_
                                )
                            })]
                    }
                },
                Some(first_token) => {
                    // events arrays can only be one type so get it from the first one
                    let token_type = first_token;
                    match token_type {
                        DynSolValue::Address(_) => {
                            let mut vec: Vec<Address> = vec![];
                            for token in tokens {
                                if let DynSolValue::Address(address) = token {
                                    vec.push(*address);
                                }
                            }

                            vec![EthereumSqlTypeWrapper::VecAddress(vec)]
                        }
                        DynSolValue::FixedBytes(_, _) | DynSolValue::Bytes(_) => {
                            let mut vec: Vec<Bytes> = vec![];
                            for token in tokens {
                                if let DynSolValue::FixedBytes(bytes, _) = token {
                                    vec.push(Bytes::from(*bytes));
                                }
                            }

                            vec![EthereumSqlTypeWrapper::VecBytes(vec)]
                        }
                        DynSolValue::Int(_, _) | DynSolValue::Uint(_, _) => {
                            let sql_type_wrapper =
                                solidity_type_to_ethereum_sql_type_wrapper(&abi_input.type_)
                                    .unwrap_or_else(|| {
                                        panic!("Unknown int type for abi input: {abi_input:?}")
                                    });

                            let vec_wrapper = tokens
                                .iter()
                                .map(|token| {
                                    if let DynSolValue::Uint(uint, _) = token {
                                        return convert_uint(uint, &sql_type_wrapper);
                                    }

                                    if let DynSolValue::Int(uint, _) = token {
                                        return convert_int(uint, &sql_type_wrapper);
                                    }

                                    panic!(
                                        "Expected uint or int token in array for abi input: {abi_input:?}"
                                    );
                                })
                                .collect::<Vec<_>>();

                            match sql_type_wrapper {
                                EthereumSqlTypeWrapper::U256(_)
                                | EthereumSqlTypeWrapper::VecU256(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU256(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U256(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I256(_)
                                | EthereumSqlTypeWrapper::VecI256(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI256(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I256(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::U128(_)
                                | EthereumSqlTypeWrapper::VecU128(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU128(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U128(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I128(_)
                                | EthereumSqlTypeWrapper::VecI128(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI128(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I128(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::U64(_)
                                | EthereumSqlTypeWrapper::VecU64(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU64(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U64(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I64(_)
                                | EthereumSqlTypeWrapper::VecI64(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI64(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I64(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::U32(_)
                                | EthereumSqlTypeWrapper::VecU32(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU32(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U32(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I32(_)
                                | EthereumSqlTypeWrapper::VecI32(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI32(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I32(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::U16(_)
                                | EthereumSqlTypeWrapper::VecU16(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU16(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U16(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I16(_)
                                | EthereumSqlTypeWrapper::VecI16(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI16(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I16(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::U8(_)
                                | EthereumSqlTypeWrapper::VecU8(_) => {
                                    vec![EthereumSqlTypeWrapper::VecU8(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::U8(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                EthereumSqlTypeWrapper::I8(_)
                                | EthereumSqlTypeWrapper::VecI8(_) => {
                                    vec![EthereumSqlTypeWrapper::VecI8(
                                        vec_wrapper
                                            .into_iter()
                                            .map(|w| match w {
                                                EthereumSqlTypeWrapper::I8(v) => v,
                                                _ => unreachable!(),
                                            })
                                            .collect(),
                                    )]
                                }
                                _ => panic!("Unknown int type for abi input: {abi_input:?}"),
                            }
                        }
                        DynSolValue::Bool(_) => {
                            let mut vec: Vec<bool> = vec![];
                            for token in tokens {
                                if let DynSolValue::Bool(b) = token {
                                    vec.push(*b);
                                }
                            }

                            vec![EthereumSqlTypeWrapper::VecBool(vec)]
                        }
                        DynSolValue::String(_) => {
                            let mut vec: Vec<String> = vec![];
                            for token in tokens {
                                if let DynSolValue::String(s) = token {
                                    vec.push(s.clone());
                                }
                            }

                            vec![EthereumSqlTypeWrapper::VecString(vec)]
                        }
                        DynSolValue::FixedArray(_) | DynSolValue::Array(_) => {
                            unreachable!("Nested arrays are not supported by the EVM")
                        }
                        DynSolValue::Tuple(tuple) => process_tuple(
                            abi_input
                                .components
                                .as_ref()
                                .expect("Tuple should have a component ABI on"),
                            tuple,
                        ),
                        _ => {
                            unimplemented!("CustomStruct and Function are not supported yet - please raise issue in github with ABI to recreate")
                        }
                    }
                }
            }
        }
        DynSolValue::Tuple(tuple) => process_tuple(
            abi_input.components.as_ref().expect("Tuple should have a component ABI on"),
            tuple,
        ),
        _ => {
            unimplemented!("CustomStruct and Function are not supported yet - please raise issue in github with ABI to recreate")
        }
    }
}

impl From<&Address> for EthereumSqlTypeWrapper {
    fn from(address: &Address) -> Self {
        EthereumSqlTypeWrapper::Address(*address)
    }
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
                // Extract the correct slice of wrappers for this tuple.
                // We need wrappers[current_index..current_index + count], not wrappers[current_index..count]
                // because current_index is the starting position, and total_properties is the number of components.
                // For example: if we're at index 3 and need 12 components, we want indices 3-14 (12 items),
                // not indices 3-11 (9 items).
                let tuple_value = map_ethereum_wrapper_to_json(
                    components,
                    &wrappers[current_wrapper_index..current_wrapper_index + total_properties],
                    transaction_information,
                    true,
                );
                result.insert(abi_input.name.clone(), tuple_value);
                for i in current_wrapper_index..current_wrapper_index + total_properties {
                    wrappers_index_processed.push(i);
                }
                current_wrapper_index += total_properties;
            } else {
                let value = match wrapper {
                    EthereumSqlTypeWrapper::U64(u)
                    | EthereumSqlTypeWrapper::U64Nullable(u)
                    | EthereumSqlTypeWrapper::U64BigInt(u) => {
                        json!(u)
                    }
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
                    EthereumSqlTypeWrapper::U256(u)
                    | EthereumSqlTypeWrapper::U256Numeric(u)
                    | EthereumSqlTypeWrapper::U256Bytes(u)
                    | EthereumSqlTypeWrapper::U256Nullable(u)
                    | EthereumSqlTypeWrapper::U256BytesNullable(u) => {
                        json!(u.to_string())
                    }
                    EthereumSqlTypeWrapper::U256NumericNullable(u) => {
                        json!(u.map(|v| v.to_string()))
                    }
                    EthereumSqlTypeWrapper::VecU256(u256s)
                    | EthereumSqlTypeWrapper::VecU256Numeric(u256s)
                    | EthereumSqlTypeWrapper::VecU256Bytes(u256s) => {
                        json!(u256s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::I256(i)
                    | EthereumSqlTypeWrapper::I256Numeric(i)
                    | EthereumSqlTypeWrapper::I256Bytes(i)
                    | EthereumSqlTypeWrapper::I256Nullable(i)
                    | EthereumSqlTypeWrapper::I256BytesNullable(i) => {
                        json!(i.to_string())
                    }
                    EthereumSqlTypeWrapper::VecI256(i256s)
                    | EthereumSqlTypeWrapper::VecI256Bytes(i256s) => {
                        json!(i256s.iter().map(|i| i.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::U512(u) => json!(u.to_string()),
                    EthereumSqlTypeWrapper::VecU512(u512s) => {
                        json!(u512s.iter().map(|u| u.to_string()).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::B128(h) => json!(h),
                    EthereumSqlTypeWrapper::VecB128(h128s) => json!(h128s),
                    #[allow(deprecated)]
                    EthereumSqlTypeWrapper::H160(h) => json!(h),
                    #[allow(deprecated)]
                    EthereumSqlTypeWrapper::VecH160(h160s) => json!(h160s),
                    EthereumSqlTypeWrapper::B256(h) | EthereumSqlTypeWrapper::B256Bytes(h) => {
                        json!(h)
                    }
                    EthereumSqlTypeWrapper::VecB256(h256s)
                    | EthereumSqlTypeWrapper::VecB256Bytes(h256s) => json!(h256s),
                    EthereumSqlTypeWrapper::B512(h) => json!(h),
                    EthereumSqlTypeWrapper::VecB512(h512s) => json!(h512s),
                    EthereumSqlTypeWrapper::Address(address)
                    | EthereumSqlTypeWrapper::AddressBytes(address)
                    | EthereumSqlTypeWrapper::AddressBytesNullable(address)
                    | EthereumSqlTypeWrapper::AddressNullable(address) => json!(address),
                    EthereumSqlTypeWrapper::VecAddress(addresses)
                    | EthereumSqlTypeWrapper::VecAddressBytes(addresses) => json!(addresses),
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
                    EthereumSqlTypeWrapper::String(s)
                    | EthereumSqlTypeWrapper::StringNullable(s)
                    | EthereumSqlTypeWrapper::StringVarchar(s)
                    | EthereumSqlTypeWrapper::StringVarcharNullable(s)
                    | EthereumSqlTypeWrapper::StringChar(s)
                    | EthereumSqlTypeWrapper::StringCharNullable(s) => json!(s),
                    EthereumSqlTypeWrapper::VecString(strings)
                    | EthereumSqlTypeWrapper::VecStringVarchar(strings)
                    | EthereumSqlTypeWrapper::VecStringChar(strings) => json!(strings),
                    EthereumSqlTypeWrapper::Bytes(bytes)
                    | EthereumSqlTypeWrapper::BytesNullable(bytes) => json!(hex::encode(bytes)),
                    EthereumSqlTypeWrapper::VecBytes(bytes) => {
                        json!(bytes.iter().map(hex::encode).collect::<Vec<_>>())
                    }
                    EthereumSqlTypeWrapper::DateTime(date_time) => {
                        json!(date_time.to_rfc3339())
                    }
                    EthereumSqlTypeWrapper::DateTimeNullable(date_time) => {
                        json!(date_time.map(|d| d.to_rfc3339()))
                    }
                    EthereumSqlTypeWrapper::JSONB(json) => json.clone(),
                    EthereumSqlTypeWrapper::Uuid(uuid) => json!(uuid.to_string()),
                };
                result.insert(abi_input.name.clone(), value);
                wrappers_index_processed.push(current_wrapper_index);
                current_wrapper_index += 1;
            }
        } else {
            panic!(
                "No wrapper found for ABI input {abi_input:?} and wrapper index {current_wrapper_index} - wrappers {wrappers:?}"
            );
        }
    }

    // only do this at the top level
    if !is_within_tuple {
        result.insert("transaction_information".to_string(), json!(transaction_information));
    }

    Value::Object(result)
}
