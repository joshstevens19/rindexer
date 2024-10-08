use std::{env, future::Future, time::Duration};

use bb8::{Pool, RunError};
use bb8_postgres::PostgresConnectionManager;
use bytes::Buf;
use dotenv::dotenv;
use futures::pin_mut;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio::{task, time::timeout};
pub use tokio_postgres::types::{ToSql, Type as PgType};
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter, config::SslMode, Config, CopyInSink, Error as PgError, Row,
    Statement, ToStatement, Transaction as PgTransaction,
};
use tracing::{debug, error};

use crate::database::postgres::{
    generate::generate_event_table_columns_names_sql, sql_type_wrapper::EthereumSqlTypeWrapper,
};

pub fn connection_string() -> Result<String, env::VarError> {
    dotenv().ok();
    let connection = env::var("DATABASE_URL")?;
    Ok(connection)
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresConnectionError {
    #[error("The database connection string is wrong please check your environment: {0}")]
    DatabaseConnectionConfigWrong(#[from] env::VarError),

    #[error("Connection pool error: {0}")]
    ConnectionPoolError(#[from] tokio_postgres::Error),

    #[error("Connection pool runtime error: {0}")]
    ConnectionPoolRuntimeError(#[from] RunError<tokio_postgres::Error>),

    #[error("Can not connect to the database please make sure your connection string is correct")]
    CanNotConnectToDatabase,

    #[error("Could not parse connection string make sure it is correctly formatted")]
    CouldNotParseConnectionString,

    #[error("Could not create tls connector")]
    CouldNotCreateTlsConnector,
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresError {
    #[error("PgError {0}")]
    PgError(#[from] PgError),

    #[error("Connection pool error: {0}")]
    ConnectionPoolError(#[from] RunError<tokio_postgres::Error>),
}

pub struct PostgresTransaction<'a> {
    pub transaction: PgTransaction<'a>,
}
impl<'a> PostgresTransaction<'a> {
    pub async fn execute(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError> {
        self.transaction.execute(query, params).await.map_err(PostgresError::PgError)
    }

    pub async fn commit(self) -> Result<(), PostgresError> {
        self.transaction.commit().await.map_err(PostgresError::PgError)
    }

    pub async fn rollback(self) -> Result<(), PostgresError> {
        self.transaction.rollback().await.map_err(PostgresError::PgError)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BulkInsertPostgresError {
    #[error("{0}")]
    PostgresError(#[from] PostgresError),

    #[error("{0}")]
    CouldNotWriteDataToPostgres(#[from] tokio_postgres::Error),
}

pub struct PostgresClient {
    pool: Pool<PostgresConnectionManager<MakeTlsConnector>>,
}

impl PostgresClient {
    pub async fn new() -> Result<Self, PostgresConnectionError> {
        async fn _new(disable_ssl: bool) -> Result<PostgresClient, PostgresConnectionError> {
            let connection_str = connection_string()?;
            let mut config: Config = connection_str
                .parse()
                .map_err(|_| PostgresConnectionError::CouldNotParseConnectionString)?;

            if disable_ssl {
                config.ssl_mode(SslMode::Disable);
            }

            let connector = TlsConnector::builder()
                .build()
                .map_err(|_| PostgresConnectionError::CouldNotCreateTlsConnector)?;
            let tls_connector = MakeTlsConnector::new(connector);

            // Perform a direct connection test
            let (client, connection) =
                match timeout(Duration::from_millis(5000), config.connect(tls_connector.clone()))
                    .await
                {
                    Ok(Ok((client, connection))) => (client, connection),
                    Ok(Err(e)) => {
                        // retry without ssl if ssl has been attempted and failed
                        if !disable_ssl &&
                            config.get_ssl_mode() != SslMode::Disable &&
                            !connection_str.contains("sslmode=require")
                        {
                            return Box::pin(_new(true)).await;
                        }
                        error!("Error connecting to database: {}", e);
                        return Err(PostgresConnectionError::CanNotConnectToDatabase);
                    }
                    Err(e) => {
                        error!("Timeout connecting to database: {}", e);
                        return Err(PostgresConnectionError::CanNotConnectToDatabase);
                    }
                };

            // Spawn the connection future to ensure the connection is established
            let connection_handle = task::spawn(connection);

            // Perform a simple query to check the connection
            match client.query_one("SELECT 1", &[]).await {
                Ok(_) => {}
                Err(_) => return Err(PostgresConnectionError::CanNotConnectToDatabase),
            };

            // Drop the client and ensure the connection handle completes
            drop(client);
            match connection_handle.await {
                Ok(Ok(())) => (),
                Ok(Err(_)) => return Err(PostgresConnectionError::CanNotConnectToDatabase),
                Err(_) => return Err(PostgresConnectionError::CanNotConnectToDatabase),
            }

            let manager = PostgresConnectionManager::new(config, tls_connector);

            let pool = Pool::builder().build(manager).await?;

            Ok(PostgresClient { pool })
        }

        _new(false).await
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), PostgresError> {
        let conn = self.pool.get().await?;
        conn.batch_execute(sql).await.map_err(PostgresError::PgError)
    }

    pub async fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self.pool.get().await?;
        conn.execute(query, params).await.map_err(PostgresError::PgError)
    }

    pub async fn prepare(
        &self,
        query: &str,
        parameter_types: &[PgType],
    ) -> Result<Statement, PostgresError> {
        let conn = self.pool.get().await?;
        conn.prepare_typed(query, parameter_types).await.map_err(PostgresError::PgError)
    }

    pub async fn with_transaction<F, Fut, T, Q>(
        &self,
        query: &Q,
        params: &[&(dyn ToSql + Sync)],
        f: F,
    ) -> Result<T, PostgresError>
    where
        F: FnOnce(u64) -> Fut + Send,
        Fut: Future<Output = Result<T, PostgresError>> + Send,
        Q: ?Sized + ToStatement,
    {
        let mut conn = self.pool.get().await.map_err(PostgresError::ConnectionPoolError)?;
        let transaction = conn.transaction().await.map_err(PostgresError::PgError)?;

        let count = transaction.execute(query, params).await.map_err(PostgresError::PgError)?;

        let result = f(count).await?;

        transaction.commit().await.map_err(PostgresError::PgError)?;

        Ok(result)
    }

    pub async fn query<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self.pool.get().await?;
        let rows = conn.query(query, params).await.map_err(PostgresError::PgError)?;
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
        let conn = self.pool.get().await?;
        let row = conn.query_one(query, params).await.map_err(PostgresError::PgError)?;
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
        let conn = self.pool.get().await?;
        let row = conn.query_opt(query, params).await.map_err(PostgresError::PgError)?;
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
        let mut conn = self.pool.get().await?;
        let transaction = conn.transaction().await.map_err(PostgresError::PgError)?;

        for params in params_list {
            let params_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|param| param.as_ref() as &(dyn ToSql + Sync)).collect();
            transaction.execute(query, &params_refs).await.map_err(PostgresError::PgError)?;
        }

        transaction.commit().await.map_err(PostgresError::PgError)?;
        Ok(())
    }

    pub async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, PostgresError>
    where
        T: ?Sized + ToStatement,
        U: Buf + 'static + Send,
    {
        let conn = self.pool.get().await?;

        conn.copy_in(statement).await.map_err(PostgresError::PgError)
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

        debug!("Bulk insert statement: {}", stmt);

        let prepared_data: Vec<Vec<&(dyn ToSql + Sync)>> = data
            .iter()
            .map(|row| row.iter().map(|param| param as &(dyn ToSql + Sync)).collect())
            .collect();

        //debug!("Prepared data: {:?}", prepared_data);

        let sink = self.copy_in(&stmt).await?;

        let writer = BinaryCopyInWriter::new(sink, column_types);
        pin_mut!(writer);

        for row in prepared_data.iter() {
            writer.as_mut().write(row).await?;
        }

        writer.finish().await?;

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

    /// This will use COPY to insert the data into the database
    /// or use the normal bulk inserts if the data is not large enough to
    /// need a COPY. This uses `bulk_insert` and `bulk_insert_via_copy` under the hood
    pub async fn insert_bulk(
        &self,
        table_name: &str,
        columns: &[String],
        postgres_bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        if postgres_bulk_data.is_empty() {
            return Ok(());
        }

        if postgres_bulk_data.len() > 100 {
            let column_types: Vec<PgType> =
                postgres_bulk_data[0].iter().map(|param| param.to_type()).collect();

            self.bulk_insert_via_copy(table_name, columns, &column_types, postgres_bulk_data)
                .await
                .map_err(|e| e.to_string())
        } else {
            self.bulk_insert(table_name, columns, postgres_bulk_data)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }
}
