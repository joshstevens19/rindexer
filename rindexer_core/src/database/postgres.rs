use std::env;
use dotenv::dotenv;
use thiserror::Error;

use tokio_postgres::{Client, NoTls, Row, Transaction};

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
