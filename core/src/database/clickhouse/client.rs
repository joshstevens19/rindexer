use std::{env, time::Instant};

use clickhouse::{Client, Row};
use dotenv::dotenv;
use serde::Deserialize;
use tracing::info;

use crate::metrics::database::{self as db_metrics, ops};
use crate::EthereumSqlTypeWrapper;

pub struct ClickhouseConnection {
    url: String,
    user: String,
    password: String,
    db: String,
}

pub fn clickhouse_connection() -> Result<ClickhouseConnection, env::VarError> {
    dotenv().ok();

    let connection = ClickhouseConnection {
        url: env::var("CLICKHOUSE_URL")?,
        user: env::var("CLICKHOUSE_USER")?,
        password: env::var("CLICKHOUSE_PASSWORD")?,
        db: env::var("CLICKHOUSE_DB")?,
    };

    Ok(connection)
}

#[derive(thiserror::Error, Debug)]
pub enum ClickhouseConnectionError {
    #[error("The clickhouse env vars are wrong please check your environment: {0}")]
    ClickhouseConnectionConfigWrong(#[from] env::VarError),

    #[error("Could not connect to clickhouse database: {0}")]
    ClickhouseNetworkError(#[from] clickhouse::error::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum ClickhouseError {
    #[error("ClickhouseError: {0}")]
    ClickhouseError(#[from] clickhouse::error::Error),
}

pub struct ClickhouseClient {
    pub(crate) conn: Client,
}

impl ClickhouseClient {
    pub async fn new() -> Result<Self, ClickhouseConnectionError> {
        let connection = clickhouse_connection()?;

        let client = Client::default()
            .with_url(connection.url)
            .with_user(connection.user)
            .with_database(connection.db)
            .with_password(connection.password);

        client.query("select 1").execute().await?;
        info!("Clickhouse client connected successfully!");

        Ok(ClickhouseClient { conn: client })
    }

    pub async fn query_one<T>(&self, sql: &str) -> Result<T, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let start = Instant::now();
        let result = self.conn.query(sql).fetch_one().await;
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());

        Ok(result?)
    }

    pub async fn query<T>(&self, sql: &str) -> Result<T, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let start = Instant::now();
        let result = self.conn.query(sql).fetch_one().await;
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());

        Ok(result?)
    }

    pub async fn query_all<T>(&self, sql: &str) -> Result<Vec<T>, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let start = Instant::now();
        let result = self.conn.query(sql).fetch_all().await;
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());

        Ok(result?)
    }

    pub async fn query_optional<T>(&self, sql: &str) -> Result<Option<T>, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let start = Instant::now();
        let result = self.conn.query(sql).fetch_optional().await;
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());

        Ok(result?)
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickhouseError> {
        let start = Instant::now();
        let result = self.conn.query(sql).execute().await;
        db_metrics::record_db_operation(
            ops::BATCH_EXECUTE,
            result.is_ok(),
            start.elapsed().as_secs_f64(),
        );

        result?;
        Ok(())
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<(), ClickhouseError> {
        let start = Instant::now();
        let statements: Vec<&str> =
            sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();

        for statement in statements {
            if let Err(e) = self.conn.query(statement).execute().await {
                db_metrics::record_db_operation(
                    ops::BATCH_EXECUTE,
                    false,
                    start.elapsed().as_secs_f64(),
                );
                return Err(ClickhouseError::ClickhouseError(e));
            }
        }

        db_metrics::record_db_operation(ops::BATCH_EXECUTE, true, start.elapsed().as_secs_f64());
        Ok(())
    }

    pub(crate) async fn bulk_insert_via_query(
        &self,
        table_name: &str,
        column_names: &[String],
        bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<u64, ClickhouseError> {
        let start = Instant::now();
        let values = bulk_data
            .iter()
            .map(|row| row.iter().map(|v| v.to_clickhouse_value()).collect::<Vec<_>>().join(", "))
            .map(|row| format!("({})", row))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_name,
            column_names.join(", "),
            values
        );

        let result = self.conn.query(&sql).execute().await;
        db_metrics::record_db_operation(
            ops::BATCH_INSERT,
            result.is_ok(),
            start.elapsed().as_secs_f64(),
        );

        result?;
        Ok(bulk_data.len() as u64)
    }

    pub async fn insert_bulk(
        &self,
        table_name: &str,
        column_names: &[String],
        bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<u64, ClickhouseError> {
        self.bulk_insert_via_query(table_name, column_names, bulk_data).await
    }
}
