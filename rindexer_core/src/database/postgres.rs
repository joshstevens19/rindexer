use dotenv::dotenv;
use std::{env, str};
use thiserror::Error;

use crate::generator::{extract_event_names_and_signatures_from_abi, ABIInput, EventInfo};
use crate::helpers::camel_to_snake;
use crate::manifest::yaml::Indexer;
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

    pub async fn batch_execute(
        &self,
        sql: &str,
    ) -> Result<(), tokio_postgres::Error> {
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

    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.db.prepare_typed(query, &[]).await
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
    if abi_type.contains("[]") {
        return "TEXT[]".to_string();
    }

    match abi_type {
        "address" => "CHAR(42)".to_string(),
        "bool" | "string" | "int256" | "uint256" => "TEXT".to_string(),
        t if t.starts_with("bytes") => "BYTEA".to_string(), // Use BYTEA for dynamic bytes
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "int8" | "int16" | "int32"
        | "int64" | "int128" => "NUMERIC".to_string(),
        _ => panic!("Unsupported type {}", abi_type),
    }
}

pub fn generate_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
    let mut queries = String::new();

    fn generate_columns(inputs: &[ABIInput], prefix: Option<String>) -> Vec<String> {
        inputs
            .iter()
            .flat_map(|input| {
                if let Some(components) = &input.components {
                    generate_columns(components, Some(camel_to_snake(&input.name)))
                } else {
                    vec![format!(
                        "\"{}{}\" {}",
                        if prefix.is_some() { format!("{}_", prefix.as_ref().unwrap()) } else { "".to_string() },
                        camel_to_snake(&input.name),
                        solidity_type_to_db_type(&input.type_)
                    )]
                }
            })
            .collect()
    }

    for event_info in abi_inputs {
        let table_name = camel_to_snake(&event_info.name);
        let columns = generate_columns(&event_info.inputs, Option::default());

        let columns_str = columns.join(", ");

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (rindexer_id SERIAL PRIMARY KEY, contract_address CHAR(66), {}, \"tx_hash\" CHAR(66), \"block_number\" INT, \"block_hash\" CHAR(66))",
            schema_name, table_name, columns_str
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

    for contract in &indexer.contracts {
        let event_names = extract_event_names_and_signatures_from_abi(&contract.abi).unwrap();

        sql.push_str(&generate_event_table_sql(&event_names, &schema_name));
    }

    sql
}
