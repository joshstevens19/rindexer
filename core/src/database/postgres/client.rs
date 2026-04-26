use std::{
    env,
    future::Future,
    time::{Duration, Instant},
};

use bb8::{Pool, PooledConnection, RunError};
use bb8_postgres::PostgresConnectionManager;
use bytes::Buf;
use dotenv::dotenv;
use futures::pin_mut;
use rust_decimal::Decimal;
use tokio::{task, time::timeout};
pub use tokio_postgres::types::{ToSql, Type as PgType};
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter, config::SslMode, Config, CopyInSink, Error as PgError, Row,
    Statement, ToStatement, Transaction as PgTransaction,
};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::error;

use crate::database::generate::generate_event_table_columns_names_sql;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::metrics::database::{self as db_metrics, ops};

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

    #[error("{0}")]
    Custom(String),
}

#[allow(unused)]
pub struct PostgresTransaction<'a> {
    pub transaction: PgTransaction<'a>,
}

impl PostgresTransaction<'_> {
    #[allow(unused)]
    pub async fn execute(
        &mut self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError> {
        self.transaction.execute(query, params).await.map_err(PostgresError::PgError)
    }

    #[allow(unused)]
    pub async fn commit(self) -> Result<(), PostgresError> {
        self.transaction.commit().await.map_err(PostgresError::PgError)
    }

    #[allow(unused)]
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
    pool: Pool<PostgresConnectionManager<MakeRustlsConnect>>,
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

            let mut root_store = rustls::RootCertStore::empty();
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let native = rustls_native_certs::load_native_certs();
            for e in &native.errors {
                tracing::debug!("Native cert load error (skipped): {}", e);
            }
            for cert in native.certs {
                if let Err(e) = root_store.add(cert) {
                    tracing::debug!("Skipped malformed native cert: {}", e);
                }
            }
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            let tls_connector = MakeRustlsConnect::new(tls_config);

            // Perform a direct connection test
            let (client, connection) =
                match timeout(Duration::from_millis(5000), config.connect(tls_connector.clone()))
                    .await
                {
                    Ok(Ok((client, connection))) => (client, connection),
                    Ok(Err(e)) => {
                        // retry without ssl if ssl has been attempted and failed
                        if !disable_ssl
                            && config.get_ssl_mode() != SslMode::Disable
                            && !connection_str.contains("sslmode=require")
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

            // Pool size: configurable via DATABASE_POOL_SIZE env var, defaults to 10.
            let pool_size: u32 =
                env::var("DATABASE_POOL_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(10);
            let pool = Pool::builder().max_size(pool_size).build(manager).await?;

            Ok(PostgresClient { pool })
        }

        _new(false).await
    }

    pub async fn from_connection(
        pool: Pool<PostgresConnectionManager<MakeRustlsConnect>>,
    ) -> Result<Self, PostgresConnectionError> {
        Ok(Self { pool })
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), PostgresError> {
        let start = Instant::now();
        let conn = self.pool.get().await?;
        let result = conn.batch_execute(sql).await.map_err(PostgresError::PgError);
        db_metrics::record_db_operation(
            ops::BATCH_EXECUTE,
            result.is_ok(),
            start.elapsed().as_secs_f64(),
        );
        result
    }

    pub async fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, PostgresError>
    where
        T: ?Sized + ToStatement,
    {
        let start = Instant::now();
        let conn = self.pool.get().await?;
        let result = conn.execute(query, params).await.map_err(PostgresError::PgError);
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());
        result
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
        let start = Instant::now();
        let conn = self.pool.get().await?;
        let result = conn.query(query, params).await.map_err(PostgresError::PgError);
        db_metrics::record_db_operation(ops::QUERY, result.is_ok(), start.elapsed().as_secs_f64());
        result
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

    // Internal method used by insert_bulk for large datasets (>100 rows).
    // Uses PostgreSQL COPY command for optimal performance with large data.
    // Made pub(crate) to allow crate-internal access while keeping insert_bulk as the primary API.
    pub(crate) async fn bulk_insert_via_copy(
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

        // info!("Bulk insert statement: {}", stmt);

        let prepared_data: Vec<Vec<&(dyn ToSql + Sync)>> = data
            .iter()
            .map(|row| row.iter().map(|param| param as &(dyn ToSql + Sync)).collect())
            .collect();

        // info!("Prepared data: {:?}", prepared_data);

        let sink = self.copy_in(&stmt).await?;

        let writer = BinaryCopyInWriter::new(sink, column_types);
        pin_mut!(writer);

        // This can cause issues with Binary Copy command not completing and leaving hanging
        // processes. See similar: https://github.com/sfackler/rust-postgres/issues/1109
        //
        // We have to call `finish` manually on any write error.
        for row in prepared_data.iter() {
            if let Err(e) = writer.as_mut().write(row).await {
                error!("Error writing binary data, aborting early: {}", e);
                writer.finish().await?;
                return Err(e)?;
            };
        }

        writer.finish().await?;

        Ok(())
    }

    // Internal method used by insert_bulk for small datasets (≤100 rows).
    // Uses standard INSERT queries which are more efficient for smaller data volumes.
    // Made pub(crate) to allow crate-internal access while keeping insert_bulk as the primary API.
    pub(crate) async fn bulk_insert_via_query(
        &self,
        table_name: &str,
        column_names: &[String],
        bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<u64, PostgresError> {
        let total_columns = column_names.len();

        // good for debugging
        // for (i, row) in bulk_data.iter().enumerate() {
        //     for (j, param) in row.iter().enumerate() {
        //         tracing::info!(
        //             "Row {} Column {} ({:?}) -> Value: {:?}, Type: {:?}",
        //             i,
        //             j,
        //             column_names.get(j),
        //             param,
        //             param.to_type()
        //         );
        //     }
        // }

        let mut query = format!(
            "INSERT INTO {} ({}) VALUES ",
            table_name,
            generate_event_table_columns_names_sql(column_names),
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();

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
                params.push(param as &(dyn ToSql + Sync));
            }
        }

        // Good for debugging
        // tracing::info!("query: {:?}", query);
        // tracing::info!(
        //     "params original types: {:?}",
        //     bulk_data.iter().flat_map(|row| row.iter().map(|p|
        // p.to_type())).collect::<Vec<_>>()     );

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

        let total_params = postgres_bulk_data.len() * columns.len();

        // PostgreSQL has a maximum of 65535 parameters in a single query
        // (see https://www.postgresql.org/docs/current/limits.html#LIMITS-TABLE)
        // If we exceed this limit, force use of COPY method
        if postgres_bulk_data.len() > 100 || total_params > 65535 {
            let column_types: Vec<PgType> =
                postgres_bulk_data[0].iter().map(|param| param.to_type()).collect();

            self.bulk_insert_via_copy(table_name, columns, &column_types, postgres_bulk_data)
                .await
                .map_err(|e| e.to_string())
        } else {
            self.bulk_insert_via_query(table_name, columns, postgres_bulk_data)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
    }

    pub async fn raw_connection(
        &self,
    ) -> Result<PooledConnection<'_, PostgresConnectionManager<MakeRustlsConnect>>, PostgresError>
    {
        let conn = self.pool.get().await?;

        Ok(conn)
    }

    /// Delete events in a block range for a given network from a specific table.
    /// Returns the number of rows deleted.
    pub async fn delete_by_block_range(
        &self,
        table_name: &str,
        network: &str,
        fork_point: u64,
        detection_point: u64,
    ) -> Result<u64, String> {
        let query = format!(
            "DELETE FROM {} WHERE network = $1 AND block_number >= $2 AND block_number <= $3",
            table_name
        );
        let fork_point =
            i64::try_from(fork_point).map_err(|_| "fork_point exceeds i64 range".to_string())?;
        let detection_point = i64::try_from(detection_point)
            .map_err(|_| "detection_point exceeds i64 range".to_string())?;
        self.execute(&query, &[&network, &fork_point, &detection_point])
            .await
            .map_err(|e| e.to_string())
    }

    /// Execute a full reorg rollback atomically in a single PostgreSQL transaction:
    /// 1. Delete stale events from all given event tables (returning affected tx hashes)
    /// 2. Delete stale entries from `rindexer_internal.reorg_block_hashes` for the block range
    /// 3. Insert corrected reorg_block_hashes entries (marking reorg as handled)
    /// 4. Rewind checkpoint cursors
    ///
    /// Returns `(total_rows_deleted, affected_tx_hashes)`.
    pub async fn reorg_rollback_transaction(
        &self,
        event_table_names: &[&str],
        network: &str,
        fork_point: u64,
        detection_point: u64,
        corrected_blocks: &[(u64, &str, &str)], // (block_number, block_hash, parent_hash)
        checkpoint_tables: &[&str],
    ) -> Result<(u64, Vec<String>), PostgresError> {
        let mut conn = self.pool.get().await?;
        let transaction = conn.transaction().await?;

        let fork_point_i64 = i64::try_from(fork_point)
            .map_err(|_| PostgresError::Custom("fork_point exceeds i64 range".to_string()))?;
        let detection_point_i64 = i64::try_from(detection_point)
            .map_err(|_| PostgresError::Custom("detection_point exceeds i64 range".to_string()))?;
        let fork_point_decimal = Decimal::from(fork_point);
        let detection_point_decimal = Decimal::from(detection_point);
        let mut total_deleted: u64 = 0;
        let mut all_affected_tx_hashes: Vec<String> = Vec::new();

        // 1. Delete stale events and collect affected tx hashes in one round-trip per table
        for table_name in event_table_names {
            let query = format!(
                "DELETE FROM {} WHERE network = $1 AND block_number >= $2 AND block_number <= $3 RETURNING tx_hash",
                table_name
            );
            let rows = transaction
                .query(&query, &[&network, &fork_point_decimal, &detection_point_decimal])
                .await?;
            total_deleted += rows.len() as u64;
            let hashes: Vec<String> = rows.iter().map(|r| r.get::<_, String>("tx_hash")).collect();
            all_affected_tx_hashes.extend(hashes);
        }

        all_affected_tx_hashes.sort();
        all_affected_tx_hashes.dedup();

        // 2. Delete stale entries from rindexer_internal.reorg_block_hashes
        let delete_reorg_hashes_query = "DELETE FROM rindexer_internal.reorg_block_hashes \
             WHERE network = $1 AND block_number >= $2 AND block_number <= $3";
        transaction
            .execute(delete_reorg_hashes_query, &[&network, &fork_point_i64, &detection_point_i64])
            .await?;

        // 3. Insert corrected reorg_block_hashes entries
        let insert_query = "INSERT INTO rindexer_internal.reorg_block_hashes \
             (network, block_number, block_hash, parent_hash) \
             VALUES ($1, $2, $3, $4)";
        for &(block_number, block_hash, parent_hash) in corrected_blocks {
            let block_number_i64 = i64::try_from(block_number).map_err(|_| {
                PostgresError::Custom(format!("block_number {} exceeds i64 range", block_number))
            })?;
            transaction
                .execute(insert_query, &[&network, &block_number_i64, &block_hash, &parent_hash])
                .await?;
        }

        // 4. Rewind last_synced_block checkpoints to fork_point - 1
        let rewind_block = Decimal::from(fork_point.saturating_sub(1));
        for table in checkpoint_tables {
            let query = format!(
                "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2",
                table
            );
            transaction.execute(&query, &[&rewind_block, &network]).await?;
        }

        transaction.commit().await?;

        Ok((total_deleted, all_affected_tx_hashes))
    }
}

#[async_trait::async_trait]
impl crate::database::Database for PostgresClient {
    async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        // Delegates to the inherent method (same signature).
        self.insert_bulk(table, columns, data).await
    }

    fn backend_name(&self) -> &'static str {
        "postgres"
    }
}
