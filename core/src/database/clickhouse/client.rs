use crate::EthereumSqlTypeWrapper;
use clickhouse::{Client, Row};
use dotenv::dotenv;
use serde::Deserialize;
use std::env;
use tracing::info;

pub struct ClickhouseConnection {
    pub url: String,
    pub user: String,
    pub password: String,
    pub db: String,
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
    pub(crate) database_name: String,
}

impl ClickhouseClient {
    pub async fn new() -> Result<Self, ClickhouseConnectionError> {
        let connection = clickhouse_connection()?;
        let database_name = connection.db.clone();

        let client = Client::default()
            .with_url(connection.url)
            .with_user(connection.user)
            .with_database(connection.db)
            .with_password(connection.password);

        client.query("select 1").execute().await?;
        info!("Clickhouse client connected successfully!");

        Ok(ClickhouseClient { conn: client, database_name })
    }

    pub fn get_database_name(&self) -> &str {
        &self.database_name
    }

    pub async fn query_one<T>(&self, sql: &str) -> Result<T, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let data = self.conn.query(sql).fetch_one().await?;

        Ok(data)
    }

    pub async fn query<T>(&self, sql: &str) -> Result<T, ClickhouseError>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        let data = self.conn.query(sql).fetch_one().await?;

        Ok(data)
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickhouseError> {
        self.conn.query(sql).execute().await?;

        Ok(())
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<(), ClickhouseError> {
        let statements: Vec<&str> =
            sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();

        for statement in statements {
            self.execute(statement).await?;
        }

        Ok(())
    }

    pub(crate) async fn bulk_insert_via_query(
        &self,
        table_name: &str,
        column_names: &[String],
        bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<u64, ClickhouseError> {
        let values = bulk_data
            .iter()
            .map(|row| row.iter().map(|v| v.to_clickhouse_value()).collect::<Vec<_>>().join(", "))
            .map(|row| format!("({})", row))
            .collect::<Vec<_>>()
            .join(", ");

        self.execute(&format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_name,
            column_names.join(", "),
            values
        ))
        .await?;

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
