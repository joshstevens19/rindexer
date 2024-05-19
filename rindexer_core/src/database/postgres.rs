// External crates
use bytes::BytesMut;
use dotenv::dotenv;
use ethers::types::{Address, Bytes, H128, H160, H256, H512, U128, U256, U512, U64};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use rust_decimal::Decimal;
use std::{env, str};
use thiserror::Error;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tokio_postgres::{Client, Error as PgError, NoTls, Row, Statement, Transaction};

// Internal modules
use crate::generator::{
    extract_event_names_and_signatures_from_abi, generate_abi_name_properties, read_abi_file,
    ABIInput, EventInfo, GenerateAbiPropertiesType,
};
use crate::helpers::camel_to_snake;
use crate::manifest::yaml::Indexer;

pub fn database_user() -> Result<String, env::VarError> {
    dotenv().ok();
    env::var("DATABASE_USER")
}

/// Constructs a PostgresSQL connection string using environment variables.
///
/// This function reads database connection details from environment variables
/// and constructs a connection string in the format required by PostgresSQL.
///
/// # Environment Variables
///
/// - `DATABASE_USER`: The username for the database.
/// - `DATABASE_PASSWORD`: The password for the database user.
/// - `DATABASE_HOST`: The hostname or IP address of the database server.
/// - `DATABASE_PORT`: The port number on which the database server is listening.
/// - `DATABASE_NAME`: The name of the database to connect to.
///
/// # Returns
///
/// A `Result` containing the connection string on success, or an `env::VarError` on failure.
fn connection_string() -> Result<String, env::VarError> {
    dotenv().ok();
    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        database_user()?,
        env::var("DATABASE_PASSWORD")?,
        env::var("DATABASE_HOST")?,
        env::var("DATABASE_PORT")?,
        env::var("DATABASE_NAME")?
    ))
}
/// Constructs a PostgreSQL connection string from environment variables,
/// encoding the password to be URL-safe.
///
/// The following environment variables are expected:
/// - `DATABASE_USER`: The database username.
/// - `DATABASE_PASSWORD`: The database user's password.
/// - `DATABASE_HOST`: The database host (e.g., `localhost`).
/// - `DATABASE_PORT`: The database port (e.g., `5432`).
/// - `DATABASE_NAME`: The name of the database.
///
/// # Returns
///
/// Returns a `Result` containing the PostgreSQL connection string if successful,
/// or an `env::VarError` if any of the required environment variables are not set.
pub fn connection_string_as_url() -> Result<String, env::VarError> {
    dotenv().ok();
    let password =
        utf8_percent_encode(&env::var("DATABASE_PASSWORD")?, NON_ALPHANUMERIC).to_string();
    Ok(format!(
        "postgresql://{}:{}@{}:{}/{}",
        database_user()?,
        password,
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

    pub async fn prepare(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, PgError> {
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

/// Converts a Solidity ABI type to a corresponding SQL data type.
///
/// This function maps various Solidity types to their appropriate SQL types.
/// If the Solidity type is an array, the corresponding SQL type will also be an array.
///
/// # Arguments
///
/// * `abi_type` - A string slice that holds the Solidity ABI type.
///
/// # Returns
///
/// A `String` representing the corresponding SQL data type.
///
/// # Panics
///
/// The function will panic if it encounters an unsupported Solidity type.
pub fn solidity_type_to_db_type(abi_type: &str) -> String {
    let is_array = abi_type.ends_with("[]");
    let base_type = abi_type.trim_end_matches("[]");

    let sql_type = match base_type {
        "address" => "CHAR(42)",
        "bool" => "BOOLEAN",
        "int256" | "uint256" => "VARCHAR(78)",
        "int64" | "uint64" | "int128" | "uint128" => "NUMERIC",
        "int32" | "uint32" => "INTEGER",
        "string" => "TEXT",
        t if t.starts_with("bytes") => "BYTEA",
        "uint8" | "uint16" | "int8" | "int16" => "SMALLINT",
        _ => panic!("Unsupported type {}", abi_type),
    };

    // Return the SQL type, appending array brackets if necessary
    if is_array {
        format!("{}[]", sql_type)
    } else {
        sql_type.to_string()
    }
}

/// Generates a vector of strings based on ABI input properties and a specified property type.
///
/// # Arguments
///
/// * `inputs` - A slice of `ABIInput` containing the ABI input details.
/// * `property_type` - The type of property generation (e.g., with data types or names only).
///
/// # Returns
///
/// A `Vec<String>` containing the generated properties as strings.
fn generate_columns(inputs: &[ABIInput], property_type: &GenerateAbiPropertiesType) -> Vec<String> {
    generate_abi_name_properties(inputs, property_type, None)
        .iter()
        .map(|m| m.value.clone())
        .collect()
}

/// Generates a vector of columns with data types based on ABI input properties.
///
/// # Arguments
///
/// * `inputs` - A slice of `ABIInput` containing the ABI input details.
///
/// # Returns
///
/// A `Vec<String>` containing the column definitions with data types.
pub fn generate_columns_with_data_types(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresWithDataTypes)
}

/// Generates a vector of column names based on ABI input properties.
///
/// # Arguments
///
/// * `inputs` - A slice of `ABIInput` containing the ABI input details.
///
/// # Returns
///
/// A `Vec<String>` containing the column names.
pub fn generate_columns_names_only(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresColumnsNamesOnly)
}

/// Generates SQL queries to create tables based on provided event information.
///
/// # Arguments
///
/// * `abi_inputs` - A slice of `EventInfo` containing the event details.
/// * `schema_name` - The name of the database schema.
///
/// # Returns
///
/// A `String` containing the SQL queries to create the tables.
fn generate_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (\
                rindexer_id SERIAL PRIMARY KEY, \
                contract_address CHAR(66), \
                {}, \
                tx_hash CHAR(66), \
                block_number NUMERIC, \
                block_hash CHAR(66)\
            );",
                schema_name,
                camel_to_snake(&event_info.name),
                generate_columns_with_data_types(&event_info.inputs).join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generates SQL queries to create internal event tables and insert initial data.
///
/// This function creates SQL tables to track the last synced block for each event on different networks.
/// It constructs a table for each event and inserts an initial record for each network.
///
/// # Arguments
///
/// * `abi_inputs` - A slice of `EventInfo` containing the event details.
/// * `schema_name` - The name of the schema.
/// * `networks` - A vector of strings representing the network names.
///
/// # Returns
///
/// A `String` containing the SQL queries to create the tables and insert initial data.
fn generate_internal_event_table_sql(
    abi_inputs: &[EventInfo],
    schema_name: &str,
    networks: Vec<String>,
) -> String {
    abi_inputs.iter().map(|event_info| {
        let table_name = format!(
            "rindexer_internal.{}_{}",
            schema_name,
            camel_to_snake(&event_info.name)
        );

        let create_table_query = format!(
            r#"CREATE TABLE IF NOT EXISTS {} ("network" TEXT PRIMARY KEY, "last_synced_block" NUMERIC);"#,
            table_name
        );

        let insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT INTO {} ("network", "last_synced_block") VALUES ('{}', 0) ON CONFLICT ("network") DO NOTHING;"#,
                table_name,
                network
            )
        }).collect::<Vec<_>>().join("\n");

        format!("{}\n{}", create_table_query, insert_queries)
    }).collect::<Vec<_>>().join("\n")
}

/// Generates the schema name for the given indexer.
pub fn indexer_schema_name(indexer: &Indexer) -> String {
    camel_to_snake(&indexer.name)
}

/// Generates SQL queries to create tables and schemas for the given indexer.
///
/// This function constructs SQL queries to create the necessary schemas and tables based on the provided indexer configuration.
///
/// # Arguments
///
/// * `indexer` - A reference to the `Indexer` containing the configuration details.
///
/// # Returns
///
/// A `String` containing the SQL queries to create the schemas and tables.
pub fn create_tables_for_indexer_sql(indexer: &Indexer) -> String {
    let schema_name = indexer_schema_name(indexer);

    let mut sql = format!(
        r#"
        CREATE SCHEMA IF NOT EXISTS {};
        CREATE SCHEMA IF NOT EXISTS rindexer_internal;
        "#,
        schema_name
    );

    for contract in &indexer.contracts {
        if let Ok(abi_items) = read_abi_file(&contract.abi) {
            if let Ok(event_names) = extract_event_names_and_signatures_from_abi(&abi_items) {
                let networks: Vec<String> =
                    contract.details.iter().map(|d| d.network.clone()).collect();
                sql.push_str(&generate_event_table_sql(&event_names, &schema_name));
                sql.push_str(&generate_internal_event_table_sql(
                    &event_names,
                    &schema_name,
                    networks,
                ));
            }
        }
    }

    sql
}

/// Generates a SQL VALUES clause with injected parameters.
///
/// This function constructs a VALUES clause for a SQL statement with a specified number of
/// parameters, formatted as `$1, $2, ..., $count`.
///
/// # Arguments
///
/// * `count` - The number of parameters to generate.
///
/// # Returns
///
/// A `String` containing the SQL VALUES clause with the injected parameters.
pub fn generate_injected_param(count: usize) -> String {
    let params = (1..=count)
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");
    format!("VALUES({})", params)
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

/// Converts a Solidity ABI type to a corresponding Ethereum SQL type wrapper.
///
/// This function maps various Solidity types to their appropriate Ethereum SQL type wrappers.
///
/// # Arguments
///
/// * `abi_type` - A string slice that holds the Solidity ABI type.
///
/// # Returns
///
/// An `Option<String>` containing the corresponding Ethereum SQL type wrapper if the type is supported, or `None` if the type is unsupported.
///
pub fn solidity_type_to_ethereum_sql_type(abi_type: &str) -> Option<String> {
    let type_wrapper = match abi_type {
        "string" => "String",
        "string[]" => "VecString",
        "address" => "Address",
        "address[]" => "VecAddress",
        "bool" => "Bool",
        "bool[]" => "VecBool",
        "int256" | "uint256" => "U256",
        "int256[]" | "uint256[]" => "VecU256",
        "int128" | "uint128" => "U128",
        "int128[]" | "uint128[]" => "VecU128",
        "int64" | "uint64" => "U64",
        "int64[]" | "uint64[]" => "VecU64",
        "int32" | "uint32" => "U32",
        "int32[]" | "uint32[]" => "VecU32",
        "int16" | "uint16" => "U16",
        "int16[]" | "uint16[]" => "VecU16",
        "int8" | "uint8" => "U8",
        "int8[]" | "uint8[]" => "VecU8",
        t if t.starts_with("bytes") && t.contains("[]") => "VecBytes",
        t if t.starts_with("bytes") => "Bytes",
        _ => return None,
    };

    Some(format!("EthereumSqlTypeWrapper::{}", type_wrapper))
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
                Decimal::to_sql(&value.parse::<Decimal>().unwrap(), _ty, out)
            }
            EthereumSqlTypeWrapper::VecU64(values) => {
                let results: Vec<Decimal> = values
                    .iter()
                    .map(|s| s.to_string().parse::<Decimal>().unwrap())
                    .collect();
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
