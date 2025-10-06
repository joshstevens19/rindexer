use crate::EthereumSqlTypeWrapper;
use clickhouse::{Client, Row};
use dotenv::dotenv;
use serde::Deserialize;
use std::env;
use tracing::info;

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
    #[error("ClickhouseError {0}")]
    ClickhouseError(String),
}

pub struct ClickhouseClient {
    conn: Client,
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
        self.conn
            .query(sql)
            .fetch_one()
            .await
            .map_err(|e| ClickhouseError::ClickhouseError(e.to_string()))
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickhouseError> {
        self.conn
            .query(sql)
            .execute()
            .await
            .map_err(|e| ClickhouseError::ClickhouseError(e.to_string()))
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
        let column_names_str = column_names.join(", ");
        let query = format!("INSERT INTO {} ({}) VALUES", table_name, column_names_str);
        let mut values = Vec::new();

        for row in bulk_data.iter() {
            let row_values: Vec<String> =
                row.iter().map(|value| value.to_clickhouse_value()).collect();
            values.push(format!("({})", row_values.join(", ")));
        }

        let full_query = format!("{} {}", query, values.join(", "));

        self.execute(&full_query).await?;

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
