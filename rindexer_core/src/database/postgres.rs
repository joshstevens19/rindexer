use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;
use bytes::{Buf, BytesMut};
use dotenv::dotenv;
use ethers::abi::{LogParam, Token};
use ethers::types::{Address, Bytes, H128, H160, H256, H512, U128, U256, U512, U64};
use futures::pin_mut;
use rust_decimal::Decimal;
use std::{env, str};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type as PgType};
use tokio_postgres::{
    CopyInSink, Error as PgError, NoTls, Row, Statement, ToStatement, Transaction as PgTransaction,
};
use tracing::{debug, error, info};

use crate::generator::build::{contract_name_to_filter_name, is_filter};
use crate::generator::{
    extract_event_names_and_signatures_from_abi, generate_abi_name_properties, read_abi_items,
    ABIInput, EventInfo, GenerateAbiPropertiesType, ParamTypeError, ReadAbiError,
};
use crate::helpers::camel_to_snake;
use crate::indexer::Indexer;
use crate::manifest::yaml::{Manifest, ProjectType};
use crate::types::code::Code;

pub fn connection_string() -> Result<String, env::VarError> {
    dotenv().ok();
    let connection = env::var("DATABASE_URL")?;
    Ok(connection)
}

pub struct PostgresClient {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresConnectionError {
    #[error("The database connection string is wrong please check your environment: {0}")]
    DatabaseConnectionConfigWrong(env::VarError),

    #[error("Connection pool error: {0}")]
    ConnectionPoolError(tokio_postgres::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresError {
    #[error("PgError {0}")]
    PgError(PgError),

    #[error("Connection pool error: {0}")]
    ConnectionPoolError(RunError<tokio_postgres::Error>),
}

pub struct PostgresTransaction {
    pub transaction: PgTransaction<'static>,
}

impl PostgresClient {
    pub async fn new() -> Result<Self, PostgresConnectionError> {
        let manager = PostgresConnectionManager::new_from_stringlike(
            connection_string().map_err(PostgresConnectionError::DatabaseConnectionConfigWrong)?,
            NoTls,
        )
        .map_err(PostgresConnectionError::ConnectionPoolError)?;

        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(PostgresConnectionError::ConnectionPoolError)?;
        Ok(Self { pool })
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), PostgresError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        conn.batch_execute(sql)
            .await
            .map_err(PostgresError::PgError)
    }

    pub async fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        conn.execute(query, params)
            .await
            .map_err(PostgresError::PgError)
    }

    pub async fn prepare(
        &self,
        query: &str,
        parameter_types: &[PgType],
    ) -> Result<Statement, PostgresError> {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        conn.prepare_typed(query, parameter_types)
            .await
            .map_err(PostgresError::PgError)
    }

    pub async fn transaction(&self) -> Result<PostgresTransaction, PostgresError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        let transaction = conn.transaction().await.map_err(PostgresError::PgError)?;

        // Wrap the transaction in a static lifetime
        let boxed_transaction: Box<PgTransaction<'static>> =
            unsafe { std::mem::transmute(Box::new(transaction)) };
        Ok(PostgresTransaction {
            transaction: *boxed_transaction,
        })
    }

    pub async fn query<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        let rows = conn
            .query(query, params)
            .await
            .map_err(PostgresError::PgError)?;
        Ok(rows)
    }

    pub async fn query_one<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        let row = conn
            .query_one(query, params)
            .await
            .map_err(PostgresError::PgError)?;
        Ok(row)
    }

    pub async fn query_one_or_none<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        let row = conn
            .query_opt(query, params)
            .await
            .map_err(PostgresError::PgError)?;
        Ok(row)
    }

    pub async fn batch_insert<T>(
        &self,
        query: &T,
        params_list: Vec<Vec<Box<dyn ToSql + Send + Sync>>>,
    ) -> Result<(), PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;
        let transaction = conn.transaction().await.map_err(PostgresError::PgError)?;

        for params in params_list {
            let params_refs: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|param| param.as_ref() as &(dyn ToSql + Sync))
                .collect();
            transaction
                .execute(query, &params_refs)
                .await
                .map_err(PostgresError::PgError)?;
        }

        transaction.commit().await.map_err(PostgresError::PgError)?;
        Ok(())
    }

    pub async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, PostgresError>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static + Send,
    {
        let conn = self
            .pool
            .get()
            .await
            .map_err(PostgresError::ConnectionPoolError)?;

        conn.copy_in(statement)
            .await
            .map_err(PostgresError::PgError)
    }

    pub async fn bulk_insert_via_copy(
        &self,
        table_name: &str,
        column_names: &[String],
        column_types: &[PgType],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), BulkInsertPostgresError> {
        let stmt = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            table_name,
            generate_event_table_columns_names_sql(column_names),
        );

        let prepared_data: Vec<Vec<&(dyn ToSql + Sync)>> = data
            .iter()
            .map(|row| {
                row.iter()
                    .map(|param| param as &(dyn ToSql + Sync))
                    .collect()
            })
            .collect();

        let sink = self
            .copy_in(&stmt)
            .await
            .map_err(BulkInsertPostgresError::PostgresError)?;

        let writer = BinaryCopyInWriter::new(sink, column_types);
        pin_mut!(writer);

        for row in prepared_data.iter() {
            writer
                .as_mut()
                .write(row)
                .await
                .map_err(BulkInsertPostgresError::CouldNotWriteDataToPostgres)?;
        }

        writer
            .finish()
            .await
            .map_err(BulkInsertPostgresError::CouldNotWriteDataToPostgres)?;

        Ok(())
    }

    pub async fn bulk_insert<'a>(
        &self,
        table_name: &str,
        column_names: &[String],
        bulk_data: &'a [Vec<EthereumSqlTypeWrapper>],
    ) -> Result<u64, PostgresError> {
        let total_columns = column_names.len();

        let mut query = format!(
            "INSERT INTO {} ({}) VALUES ",
            table_name,
            generate_event_table_columns_names_sql(column_names),
        );
        let mut params: Vec<&'a (dyn ToSql + Sync + 'a)> = Vec::new();

        for (i, row) in bulk_data.iter().enumerate() {
            if i > 0 {
                query.push(',');
            }
            let mut placeholders = vec![];
            for j in 0..total_columns {
                placeholders.push(format!("${}", i * total_columns + j + 1));
            }
            query.push_str(&format!("({})", placeholders.join(",")));

            for param in row {
                params.push(param as &'a (dyn ToSql + Sync + 'a));
            }
        }

        self.execute(&query, &params).await
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SetupPostgresError {
    #[error("{0}")]
    PostgresConnection(PostgresConnectionError),

    #[error("{0}")]
    PostgresError(PostgresError),

    #[error("Error creating tables for indexer: {0}")]
    CreateTables(CreateTablesForIndexerSqlError),
}

pub async fn setup_postgres(manifest: &Manifest) -> Result<PostgresClient, SetupPostgresError> {
    info!("Setting up postgres");
    let client = PostgresClient::new()
        .await
        .map_err(SetupPostgresError::PostgresConnection)?;

    // No-code will ignore this as it must have tables if postgres used
    if !manifest.storage.postgres_disable_create_tables()
        || manifest.project_type == ProjectType::NoCode
    {
        info!("Creating tables for {}", manifest.name);
        let sql = create_tables_for_indexer_sql(&manifest.to_indexer())
            .map_err(SetupPostgresError::CreateTables)?;
        debug!("{}", sql);
        client
            .batch_execute(sql.as_str())
            .await
            .map_err(SetupPostgresError::PostgresError)?;
        info!("Created tables for {}", manifest.name);
    }

    Ok(client)
}

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
        _ => panic!("Unsupported type: {}", base_type),
    };

    // Return the SQL type, appending array brackets if necessary
    if is_array {
        format!("{}[]", sql_type)
    } else {
        sql_type.to_string()
    }
}

/// Generates an array of strings based on ABI input properties and a specified property type.
fn generate_columns(inputs: &[ABIInput], property_type: &GenerateAbiPropertiesType) -> Vec<String> {
    generate_abi_name_properties(inputs, property_type, None)
        .iter()
        .map(|m| m.value.clone())
        .collect()
}

/// Generates an array of columns with data types based on ABI input properties.
pub fn generate_columns_with_data_types(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresWithDataTypes)
}

/// Generates an array of column names based on ABI input properties.
fn generate_columns_names_only(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresColumnsNamesOnly)
}

pub fn generate_column_names_only_with_base_properties(inputs: &[ABIInput]) -> Vec<String> {
    let mut column_names: Vec<String> = vec!["contract_address".to_string()];
    column_names.extend(generate_columns_names_only(inputs));
    column_names.extend(vec![
        "tx_hash".to_string(),
        "block_number".to_string(),
        "block_hash".to_string(),
        "network".to_string(),
    ]);
    column_names
}

/// Generates SQL queries to create tables based on provided event information.
fn generate_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            let table_name = format!("{}.{}", schema_name, camel_to_snake(&event_info.name));
            info!("Creating table if not exists: {}", table_name);
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                rindexer_id SERIAL PRIMARY KEY NOT NULL, \
                contract_address CHAR(66) NOT NULL, \
                {}, \
                tx_hash CHAR(66) NOT NULL, \
                block_number NUMERIC NOT NULL, \
                block_hash CHAR(66) NOT NULL, \
                network VARCHAR(50) NOT NULL\
            );",
                table_name,
                generate_columns_with_data_types(&event_info.inputs).join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generates SQL queries to create internal event tables and insert initial data.
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
pub fn indexer_contract_schema_name(indexer_name: &str, contract_name: &str) -> String {
    format!(
        "{}_{}",
        camel_to_snake(indexer_name),
        camel_to_snake(contract_name)
    )
}

#[derive(thiserror::Error, Debug)]
pub enum CreateTablesForIndexerSqlError {
    #[error("{0}")]
    ReadAbiError(ReadAbiError),

    #[error("{0}")]
    ParamTypeError(ParamTypeError),
}

/// Generates SQL queries to create tables and schemas for the given indexer.
pub fn create_tables_for_indexer_sql(
    indexer: &Indexer,
) -> Result<Code, CreateTablesForIndexerSqlError> {
    let mut sql = "CREATE SCHEMA IF NOT EXISTS rindexer_internal;".to_string();

    for contract in &indexer.contracts {
        let contract_name = if is_filter(contract) {
            contract_name_to_filter_name(&contract.name)
        } else {
            contract.name.clone()
        };
        let abi_items =
            read_abi_items(contract).map_err(CreateTablesForIndexerSqlError::ReadAbiError)?;
        let event_names = extract_event_names_and_signatures_from_abi(&abi_items)
            .map_err(CreateTablesForIndexerSqlError::ParamTypeError)?;
        let schema_name = indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("CREATE SCHEMA IF NOT EXISTS {};", schema_name).as_str());
        info!("Creating schema if not exists: {}", schema_name);

        let networks: Vec<String> = contract.details.iter().map(|d| d.network.clone()).collect();
        sql.push_str(&generate_event_table_sql(&event_names, &schema_name));
        sql.push_str(&generate_internal_event_table_sql(
            &event_names,
            &schema_name,
            networks,
        ));
    }

    Ok(Code::new(sql))
}

pub fn drop_tables_for_indexer_sql(indexer: &Indexer) -> Code {
    let mut sql = "DROP SCHEMA IF EXISTS rindexer_internal CASCADE;".to_string();

    for contract in &indexer.contracts {
        let contract_name = if is_filter(contract) {
            contract_name_to_filter_name(&contract.name)
        } else {
            contract.name.clone()
        };
        let schema_name = indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("DROP SCHEMA IF EXISTS {} CASCADE;", schema_name).as_str());
    }

    Code::new(sql)
}

pub fn event_table_full_name(indexer_name: &str, contract_name: &str, event_name: &str) -> String {
    let schema_name = indexer_contract_schema_name(indexer_name, contract_name);
    format!("{}.{}", schema_name, camel_to_snake(event_name))
}

fn generate_event_table_columns_names_sql(column_names: &[String]) -> String {
    column_names
        .iter()
        .map(|name| format!("\"{}\"", name))
        .collect::<Vec<String>>()
        .join(", ")
}

#[derive(thiserror::Error, Debug)]
pub enum BulkInsertPostgresError {
    #[error("{0}")]
    PostgresError(PostgresError),

    #[error("{0}")]
    CouldNotWriteDataToPostgres(tokio_postgres::Error),
}

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
            EthereumSqlTypeWrapper::U256(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::VecU256(_) => PgType::NUMERIC_ARRAY,
            EthereumSqlTypeWrapper::U512(_) => PgType::NUMERIC,
            EthereumSqlTypeWrapper::VecU512(_) => PgType::NUMERIC_ARRAY,
            EthereumSqlTypeWrapper::H128(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH128(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H160(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH160(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H256(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH256(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::H512(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecH512(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::Address(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecAddress(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::Bool(_) => PgType::BOOL,
            EthereumSqlTypeWrapper::VecBool(_) => PgType::BOOL_ARRAY,
            EthereumSqlTypeWrapper::U16(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU16(_) => PgType::INT2_ARRAY,
            EthereumSqlTypeWrapper::String(_) => PgType::TEXT,
            EthereumSqlTypeWrapper::VecString(_) => PgType::TEXT_ARRAY,
            EthereumSqlTypeWrapper::Bytes(_) => PgType::BYTEA,
            EthereumSqlTypeWrapper::VecBytes(_) => PgType::BYTEA_ARRAY,
            EthereumSqlTypeWrapper::U32(_) => PgType::INT4,
            EthereumSqlTypeWrapper::VecU32(_) => PgType::INT4_ARRAY,
            EthereumSqlTypeWrapper::U8(_) => PgType::INT2,
            EthereumSqlTypeWrapper::VecU8(_) => PgType::INT2_ARRAY,
        }
    }
}

pub fn solidity_type_to_ethereum_sql_type_wrapper(
    abi_type: &str,
) -> Option<EthereumSqlTypeWrapper> {
    match abi_type {
        "string" => Some(EthereumSqlTypeWrapper::String(String::new())),
        "string[]" => Some(EthereumSqlTypeWrapper::VecString(Vec::new())),
        "address" => Some(EthereumSqlTypeWrapper::Address(Address::zero())),
        "address[]" => Some(EthereumSqlTypeWrapper::VecAddress(Vec::new())),
        "bool" => Some(EthereumSqlTypeWrapper::Bool(false)),
        "bool[]" => Some(EthereumSqlTypeWrapper::VecBool(Vec::new())),
        "int256" | "uint256" => Some(EthereumSqlTypeWrapper::U256(U256::zero())),
        "int256[]" | "uint256[]" => Some(EthereumSqlTypeWrapper::VecU256(Vec::new())),
        "int128" | "uint128" => Some(EthereumSqlTypeWrapper::U128(U128::zero())),
        "int128[]" | "uint128[]" => Some(EthereumSqlTypeWrapper::VecU128(Vec::new())),
        "int64" | "uint64" => Some(EthereumSqlTypeWrapper::U64(U64::zero())),
        "int64[]" | "uint64[]" => Some(EthereumSqlTypeWrapper::VecU64(Vec::new())),
        "int32" | "uint32" => Some(EthereumSqlTypeWrapper::U32(0)),
        "int32[]" | "uint32[]" => Some(EthereumSqlTypeWrapper::VecU32(Vec::new())),
        "int16" | "uint16" => Some(EthereumSqlTypeWrapper::U16(0)),
        "int16[]" | "uint16[]" => Some(EthereumSqlTypeWrapper::VecU16(Vec::new())),
        "int8" | "uint8" => Some(EthereumSqlTypeWrapper::U8(0)),
        "int8[]" | "uint8[]" => Some(EthereumSqlTypeWrapper::VecU8(Vec::new())),
        t if t.starts_with("bytes") && t.contains("[]") => {
            Some(EthereumSqlTypeWrapper::VecBytes(Vec::new()))
        }
        t if t.starts_with("bytes") => Some(EthereumSqlTypeWrapper::Bytes(Bytes::new())),
        _ => None,
    }
}

pub fn map_log_params_to_ethereum_wrapper(params: &Vec<LogParam>) -> Vec<EthereumSqlTypeWrapper> {
    let mut wrappers = vec![];

    for param in params {
        match &param.value {
            Token::Tuple(tuple) => {
                wrappers.extend(process_tuple(tuple));
            }
            _ => {
                wrappers.push(map_log_token_to_ethereum_wrapper(&param.value));
            }
        }
    }

    wrappers
}

fn process_tuple(tokens: &Vec<Token>) -> Vec<EthereumSqlTypeWrapper> {
    let mut wrappers = vec![];

    for token in tokens {
        match token {
            Token::Tuple(tuple) => {
                wrappers.extend(process_tuple(tuple));
            }
            _ => {
                wrappers.push(map_log_token_to_ethereum_wrapper(token));
            }
        }
    }

    wrappers
}

fn map_log_token_to_ethereum_wrapper(token: &Token) -> EthereumSqlTypeWrapper {
    match &token {
        Token::Address(address) => EthereumSqlTypeWrapper::Address(*address),
        Token::Int(uint) | Token::Uint(uint) => EthereumSqlTypeWrapper::U256(*uint),
        Token::Bool(b) => EthereumSqlTypeWrapper::Bool(*b),
        Token::String(s) => EthereumSqlTypeWrapper::String(s.clone()),
        Token::FixedBytes(bytes) | Token::Bytes(bytes) => {
            EthereumSqlTypeWrapper::Bytes(Bytes::from(bytes.clone()))
        }
        Token::FixedArray(tokens) | Token::Array(tokens) => {
            if tokens.is_empty() {
                return EthereumSqlTypeWrapper::VecString(vec![]);
            }

            // events arrays can only be one type so get it from the first one
            let token_type = tokens.first().unwrap();
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
                    let mut vec: Vec<U256> = vec![];
                    for token in tokens {
                        if let Token::Int(uint) = token {
                            vec.push(*uint);
                        }
                    }

                    EthereumSqlTypeWrapper::VecU256(vec)
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
                    // TODO!
                    panic!("Array tuple not supported yet - please raise issue in github with ABI to recreate and we will fix")
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
            EthereumSqlTypeWrapper::VecU64(values) => {
                let results: Result<Vec<Decimal>, _> = values
                    .iter()
                    .map(|s| s.to_string().parse::<Decimal>())
                    .collect();

                match results {
                    Ok(decimals) => {
                        if decimals.is_empty() {
                            Ok(IsNull::Yes)
                        } else {
                            decimals.to_sql(_ty, out)
                        }
                    }
                    Err(e) => Err(Box::new(e)),
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

    fn accepts(_ty: &PgType) -> bool {
        true // We accept all types
    }

    to_sql_checked!();
}
