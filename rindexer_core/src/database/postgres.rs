use dotenv::dotenv;
use ethers::types::{Address, Bytes, H128, H160, H256, H512, U128, U256, U512, U64};
use std::{env, str};
use thiserror::Error;

use crate::generator::{
    extract_event_names_and_signatures_from_abi, generate_abi_name_properties, read_abi_file,
    ABIInput, EventInfo, GenerateAbiPropertiesType,
};
use crate::helpers::camel_to_snake;
use crate::manifest::yaml::Indexer;
use bytes::BytesMut;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tokio_postgres::{Client, Error, NoTls, Row, Statement, Transaction};

fn connection_string() -> Result<String, env::VarError> {
    dotenv().ok();
    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        env::var("DATABASE_USER")?,
        env::var("DATABASE_PASSWORD")?,
        env::var("DATABASE_HOST")?,
        env::var("DATABASE_PORT")?,
        env::var("DATABASE_NAME")?
    ))
}

pub struct PostgresClient {
    db: Client,
}

#[derive(Error, Debug)]
pub enum PostgresConnectionError {
    #[error("The database connection string is wrong please check your environment: {0}")]
    DatabaseConnectionConfigWrong(env::VarError),

    #[error("Can not connect to the database: {0}")]
    CanNotConnectToDb(tokio_postgres::Error),
}

impl PostgresClient {
    /// Creates a new `PostgresClient` instance and establishes a connection to the Postgres database.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `PostgresClient` instance if the connection is successful, or an `Error` if an error occurs.
    pub async fn new() -> Result<Self, PostgresConnectionError> {
        let (db, connection) = tokio_postgres::connect(
            &connection_string().map_err(PostgresConnectionError::DatabaseConnectionConfigWrong)?,
            NoTls,
        )
        .await
        .map_err(PostgresConnectionError::CanNotConnectToDb)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(Self { db })
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), tokio_postgres::Error> {
        self.db.batch_execute(sql).await
    }

    /// Executes a SQL query on the PostgreSQL database.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute.
    /// * `params` - The parameters to bind to the query.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the number of rows affected by the query, or an `Error` if an error occurs.
    pub async fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        self.db.execute(query, params).await
    }

    pub async fn prepare(&self, query: &str, parameter_types: &[Type]) -> Result<Statement, Error> {
        self.db.prepare_typed(query, parameter_types).await
    }

    pub async fn transaction(&mut self) -> Result<Transaction, tokio_postgres::Error> {
        self.db.transaction().await
    }

    /// Reads rows from the database based on the given query and parameters.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute. It must implement `tokio_postgres::ToStatement`.
    /// * `params` - The parameters to bind to the query. Each parameter must implement `tokio_postgres::types::ToSql + Sync`.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `Row` on success, or an `Error` if the query execution fails.
    pub async fn query<T>(
        &self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        let rows = self.db.query(query, params).await?;
        Ok(rows)
    }

    pub async fn query_one<T>(
        &self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        let rows = self.db.query_one(query, params).await?;
        Ok(rows)
    }

    pub async fn query_one_or_none<T>(
        &self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        let rows = self.db.query_opt(query, params).await?;
        Ok(rows)
    }
}

// TODO do better types for the db
pub fn solidity_type_to_db_type(abi_type: &str) -> String {
    let result = match abi_type.replace("[]", "").as_str() {
        "address" => "CHAR(42)".to_string(),
        "bool" => "BOOLEAN".to_string(),
        "int256" | "uint256" => "VARCHAR(78)".to_string(),
        "int64" | "uint64" | "int128" | "uint128" => "BIGINT".to_string(),
        "int32" | "uint32" => "INTEGER".to_string(),
        "string" => "TEXT".to_string(),
        t if t.starts_with("bytes") => "BYTEA".to_string(),
        "uint8" | "uint16" | "int8" | "int16" => "SMALLINT".to_string(),
        _ => panic!("Unsupported type {}", abi_type),
    };

    if abi_type.contains("[]") {
        return format!("{}[]", result);
    }

    result
}

pub fn generate_columns_with_data_types(inputs: &[ABIInput]) -> Vec<String> {
    generate_abi_name_properties(
        inputs,
        &GenerateAbiPropertiesType::PostgresWithDataTypes,
        None,
    )
    .iter()
    .map(|m| m.value.clone())
    .collect()
}

pub fn generate_columns_names_only(inputs: &[ABIInput]) -> Vec<String> {
    generate_abi_name_properties(
        inputs,
        &GenerateAbiPropertiesType::PostgresColumnsNamesOnly,
        None,
    )
    .iter()
    .map(|m| m.value.clone())
    .collect()
}

fn generate_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
    let mut queries = String::new();

    for event_info in abi_inputs {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (rindexer_id SERIAL PRIMARY KEY, contract_address CHAR(66), {}, \"tx_hash\" CHAR(66), \"block_number\" BIGINT, \"block_hash\" CHAR(66))",
            schema_name, camel_to_snake(&event_info.name), generate_columns_with_data_types(&event_info.inputs).join(", ")
        );

        queries.push_str(&query);
        queries.push(';');
        queries.push('\n');
    }

    queries
}

fn generate_internal_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
    let mut queries = String::new();

    for event_info in abi_inputs {
        let query = format!(
            r#"CREATE TABLE IF NOT EXISTS rindexer_internal.{}_{} ("network" TEXT PRIMARY KEY, "last_seen_block" BIGINT)"#,
            schema_name,
            camel_to_snake(&event_info.name)
        );

        queries.push_str(&query);
        queries.push(';');
        queries.push('\n');
    }

    queries
}

pub fn create_tables_for_indexer_sql(indexer: &Indexer) -> String {
    let mut sql = String::new();
    let schema_name = camel_to_snake(&indexer.name);
    sql.push_str(&format!(
        r#"
            CREATE SCHEMA IF NOT EXISTS {};
            "#,
        &schema_name
    ));

    // also create internal table lookups
    sql.push_str("CREATE SCHEMA IF NOT EXISTS rindexer_internal;\n");

    for contract in &indexer.contracts {
        let abi_items = read_abi_file(&contract.abi).unwrap();
        let event_names = extract_event_names_and_signatures_from_abi(&abi_items).unwrap();

        sql.push_str(&generate_event_table_sql(&event_names, &schema_name));
        sql.push_str(&generate_internal_event_table_sql(
            &event_names,
            &schema_name,
        ));
    }

    sql
}

pub fn generate_injected_param(count: usize) -> String {
    let params: Vec<_> = (1..=count).map(|i| format!("${}", i)).collect();
    format!("VALUES({})", params.join(", "))
}

#[derive(Debug)]
pub enum EthereumSqlTypeWrapper<'a> {
    U64(&'a U64),
    VecU64(&'a Vec<U64>),
    U128(&'a U128),
    VecU128(&'a Vec<U128>),
    U256(&'a U256),
    VecU256(&'a Vec<U256>),
    U512(&'a U512),
    VecU512(Vec<U512>),
    H128(&'a H128),
    VecH128(&'a Vec<H128>),
    H160(&'a H160),
    VecH160(&'a Vec<H160>),
    H256(&'a H256),
    VecH256(&'a Vec<H256>),
    H512(&'a H512),
    VecH512(&'a Vec<H512>),
    Address(&'a Address),
    VecAddress(&'a Vec<Address>),
    Bool(&'a bool),
    VecBool(&'a Vec<bool>),
    U32(&'a u32),
    VecU32(&'a Vec<u32>),
    U16(&'a u16),
    VecU16(&'a Vec<u16>),
    U8(&'a u8),
    VecU8(&'a Vec<u8>),
    String(&'a String),
    VecString(&'a Vec<String>),
    Bytes(&'a Bytes),
    VecBytes(&'a Vec<Bytes>),
}

pub fn solidity_type_to_ethereum_sql_type(abi_type: &str) -> Option<String> {
    match abi_type {
        "string" => Some("EthereumSqlTypeWrapper::String".to_string()),
        "string[]" => Some("EthereumSqlTypeWrapper::VecString".to_string()),
        "address" => Some("EthereumSqlTypeWrapper::Address".to_string()),
        "address[]" => Some("EthereumSqlTypeWrapper::VecAddress".to_string()),
        "bool" => Some("EthereumSqlTypeWrapper::Bool".to_string()),
        "bool[]" => Some("EthereumSqlTypeWrapper::VecBool".to_string()),
        "int256" | "uint256" => Some("EthereumSqlTypeWrapper::U256".to_string()),
        "int256[]" | "uint256[]" => Some("EthereumSqlTypeWrapper::VecU256".to_string()),
        "int128" | "uint128" => Some("EthereumSqlTypeWrapper::U128".to_string()),
        "int128[]" | "uint128[]" => Some("EthereumSqlTypeWrapper::VecU128".to_string()),
        "int64" | "uint64" => Some("EthereumSqlTypeWrapper::U64".to_string()),
        "int64[]" | "uint64[]" => Some("EthereumSqlTypeWrapper::VecU64".to_string()),
        "int32" | "uint32" => Some("EthereumSqlTypeWrapper::U32".to_string()),
        "int32[]" | "uint32[]" => Some("EthereumSqlTypeWrapper::VecU32".to_string()),
        "int16" | "uint16" => Some("EthereumSqlTypeWrapper::U16".to_string()),
        "int16[]" | "uint16[]" => Some("EthereumSqlTypeWrapper::VecU16".to_string()),
        "int8" | "uint8" => Some("EthereumSqlTypeWrapper::U8".to_string()),
        "int8[]" | "uint8[]" => Some("EthereumSqlTypeWrapper::VecU8".to_string()),
        t if t.starts_with("bytes") && t.contains("[]") => {
            Some("EthereumSqlTypeWrapper::VecBytes".to_string())
        }
        t if t.starts_with("bytes") => Some("EthereumSqlTypeWrapper::Bytes".to_string()),
        _ => None,
    }
}

impl<'a> From<&'a Address> for EthereumSqlTypeWrapper<'a> {
    fn from(address: &'a Address) -> Self {
        EthereumSqlTypeWrapper::Address(address)
    }
}

impl<'a> ToSql for EthereumSqlTypeWrapper<'a> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            EthereumSqlTypeWrapper::U64(value) => {
                let value = value.to_string();
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU64(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U128(value) => {
                let value = value.to_string();
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU128(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U256(value) => {
                let value_str = value.to_string();
                out.extend_from_slice(value_str.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU256(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U512(value) => {
                let hex = format!("{:?}", value);
                out.extend_from_slice(hex.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU512(values) => {
                let hexes: Vec<String> = values.iter().map(|s| format!("{:?}", s)).collect();
                if hexes.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    hexes.to_sql(_ty, out)
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
                let bools: Vec<i8> = values.iter().map(|&b| if b { 1 } else { 0 }).collect();
                if bools.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    bools.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U16(value) => {
                let value = value.to_string();
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU16(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
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
                let value = value.to_string();
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU32(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
            }
            EthereumSqlTypeWrapper::U8(value) => {
                let value = value.to_string();
                out.extend_from_slice(value.as_bytes());
                Ok(IsNull::No)
            }
            EthereumSqlTypeWrapper::VecU8(values) => {
                let results: Vec<String> = values.iter().map(|s| s.to_string()).collect();
                if results.is_empty() {
                    Ok(IsNull::Yes)
                } else {
                    results.to_sql(_ty, out)
                }
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true // We accept all types
    }

    to_sql_checked!();
}
