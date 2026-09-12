use std::{
    env,
    future::Future,
    time::{Duration, Instant},
};

use bb8::{Pool, PooledConnection, RunError};
use bb8_postgres::PostgresConnectionManager;
use bytes::Buf;
use dotenvy::dotenv;
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

/// DATABASE_URL is process-global while tests run in parallel, so tests that
/// point it at a per-test container must hold this lock from `set_var` until
/// the client has connected.
#[cfg(test)]
pub(crate) static TEST_DATABASE_URL_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

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
    #[error("PgError {}", pg_error_to_string(.0))]
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

    #[error("{}", pg_error_to_string(.0))]
    CouldNotWriteDataToPostgres(#[from] tokio_postgres::Error),
}

/// Cursor advance committed atomically with a bulk event insert — the
/// `rindexer_internal.{internal_table_name}` last-synced tracker for one network.
pub struct BulkCursorUpdate {
    pub internal_table_name: String,
    pub network: String,
    pub to_block: u64,
}

/// What the cursor UPDATE did inside the caller's transaction.
#[derive(Debug)]
pub enum CursorAdvance {
    /// The UPDATE matched: the cursor is at `to_block` once the caller commits.
    Advanced { updated_rows: u64 },
    /// Row present but already at or past `to_block` because a concurrent
    /// live/historic loop is ahead. Rows and table effects must still commit.
    AlreadyAhead { current: String },
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
        let stmt = build_copy_statement(table_name, column_names);

        let sink = self.copy_in(&stmt).await?;

        write_binary_copy_rows(sink, column_types, data).await
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
        let query = build_multi_row_insert_sql(table_name, column_names, bulk_data.len());

        let params: Vec<&(dyn ToSql + Sync)> =
            bulk_data.iter().flatten().map(|param| param as &(dyn ToSql + Sync)).collect();

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

    /// Same as `insert_bulk`, but commits the batch AND the
    /// `rindexer_internal.{table}` last-synced cursor in ONE transaction.
    ///
    /// This closes the double-index race (`process.rs` `trigger_event` TODO):
    /// with the cursor committed atomically with the rows, a crash/restart
    /// either sees neither (clean re-fetch) or both (resume past the batch).
    ///
    /// PRECONDITIONS (enforced by the caller, `no_code_callback`): Postgres is
    /// the SOLE raw-event sink; a single writer process; effective callback
    /// concurrency 1 (batches commit in rid order). A reorg rewind cannot
    /// overlap this commit: every callback holds the per-network writer barrier
    /// (`indexer::reorg::event_writer_barrier`, taken as a read guard in
    /// `indexer/process.rs`) while it writes, and the reorg coordinator holds
    /// its write guard from snapshot through rollback. The remaining seam,
    /// custom-table effects committed apart from the raw rows and this cursor,
    /// is closed by the atomic arm through `insert_bulk_with_cursor_in`.
    pub async fn insert_bulk_with_cursor(
        &self,
        table_name: &str,
        columns: &[String],
        postgres_bulk_data: &[Vec<EthereumSqlTypeWrapper>],
        cursor: &BulkCursorUpdate,
    ) -> Result<(), String> {
        if postgres_bulk_data.is_empty() {
            return Ok(());
        }

        let mut conn = self.raw_connection().await.map_err(|e| e.to_string())?;
        let transaction = conn.transaction().await.map_err(|e| pg_error_to_string(&e))?;

        let advance = Self::insert_bulk_with_cursor_in(
            &transaction,
            table_name,
            columns,
            postgres_bulk_data,
            cursor,
        )
        .await?;

        transaction.commit().await.map_err(|e| pg_error_to_string(&e))?;

        match advance {
            CursorAdvance::Advanced { updated_rows } => tracing::debug!(
                "ATOMIC-CURSOR commit: {} rows={} cursor[{}]={} (updated={})",
                table_name,
                postgres_bulk_data.len(),
                cursor.internal_table_name,
                cursor.to_block,
                updated_rows
            ),
            CursorAdvance::AlreadyAhead { current } => tracing::debug!(
                "ATOMIC-CURSOR commit: {} rows={} cursor[{}] to_block={} not advanced (already at {} — concurrent live/historic loop ahead)",
                table_name,
                postgres_bulk_data.len(),
                cursor.internal_table_name,
                cursor.to_block,
                current
            ),
        }
        Ok(())
    }

    /// Body of `insert_bulk_with_cursor` without BEGIN/COMMIT, for a caller that
    /// owns the transaction and commits other effects with it.
    ///
    /// Writes the raw rows (binary COPY above 100 rows or 65535 parameters, one
    /// multi-row INSERT otherwise), then advances the cursor under the monotonic
    /// guard. Callers pass at least one row; the wrapper returns early on an
    /// empty batch. A missing cursor row is an `Err`: dropping the transaction
    /// then rolls back everything staged in it.
    pub(crate) async fn insert_bulk_with_cursor_in(
        transaction: &PgTransaction<'_>,
        table_name: &str,
        columns: &[String],
        postgres_bulk_data: &[Vec<EthereumSqlTypeWrapper>],
        cursor: &BulkCursorUpdate,
    ) -> Result<CursorAdvance, String> {
        let total_params = postgres_bulk_data.len() * columns.len();

        if postgres_bulk_data.len() > 100 || total_params > 65535 {
            let column_types: Vec<PgType> = postgres_bulk_data
                .first()
                .map(|row| row.iter().map(|param| param.to_type()).collect())
                .unwrap_or_default();
            copy_in_via_transaction(
                transaction,
                table_name,
                columns,
                &column_types,
                postgres_bulk_data,
            )
            .await
            .map_err(|e| e.to_string())?;
        } else {
            let query = build_multi_row_insert_sql(table_name, columns, postgres_bulk_data.len());
            let params: Vec<&(dyn ToSql + Sync)> = postgres_bulk_data
                .iter()
                .flatten()
                .map(|param| param as &(dyn ToSql + Sync))
                .collect();
            // Metrics parity with the non-atomic path (bulk_insert_via_query goes
            // through self.execute, which records; the COPY path records nothing
            // there either, so only this branch records).
            let start = Instant::now();
            let result = transaction.execute(&query, &params).await;
            db_metrics::record_db_operation(
                ops::QUERY,
                result.is_ok(),
                start.elapsed().as_secs_f64(),
            );
            result.map_err(|e| pg_error_to_string(&e))?;
        }

        // Same statement + binding shape as update_progress_and_last_synced_task,
        // monotonic guard included, but inside the caller's transaction.
        let cursor_query = format!(
            "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
            cursor.internal_table_name
        );
        let cursor_rows = transaction
            .execute(
                &cursor_query,
                &[&EthereumSqlTypeWrapper::U64(cursor.to_block), &cursor.network],
            )
            .await
            .map_err(|e| pg_error_to_string(&e))?;

        if cursor_rows > 0 {
            return Ok(CursorAdvance::Advanced { updated_rows: cursor_rows });
        }

        // Zero updated rows is one of two very different situations; probe the
        // row (same transaction) to tell them apart:
        //  - row present, already at/past to_block: benign. The historic and
        //    live loops of the SAME event share this cursor, and the live loop
        //    commits at head while historic backfill is still behind; the
        //    monotonic guard correctly refuses to rewind. The rows must still
        //    commit (they were never inserted), so this cannot error: the batch
        //    would retry forever against a cursor that stays ahead.
        //  - row missing: the seeded (network, 0) row is gone / setup never ran.
        //    Committing rows while no cursor can ever advance would restart
        //    indexing from the manifest start forever (duplicate storm), so
        //    fail loudly and let the caller's drop roll everything back.
        let probe = format!(
            "SELECT last_synced_block::TEXT FROM rindexer_internal.{} WHERE network = $1",
            cursor.internal_table_name
        );
        let row = transaction
            .query_opt(&probe, &[&cursor.network])
            .await
            .map_err(|e| pg_error_to_string(&e))?;
        match row {
            Some(row) => {
                let current: String = row.try_get(0).map_err(|e| pg_error_to_string(&e))?;
                Ok(CursorAdvance::AlreadyAhead { current })
            }
            None => Err(format!(
                "ATOMIC-CURSOR: no cursor row for network={} in rindexer_internal.{} — \
                 seeded row missing, rolling back {} rows for {} (cursor could never \
                 advance; committing would re-index from the manifest start forever)",
                cursor.network,
                cursor.internal_table_name,
                postgres_bulk_data.len(),
                table_name
            )),
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

        let result = Self::reorg_rollback_in_transaction(
            &transaction,
            event_table_names,
            network,
            fork_point,
            detection_point,
            corrected_blocks,
            checkpoint_tables,
        )
        .await?;

        transaction.commit().await?;
        Ok(result)
    }

    pub(crate) async fn reorg_rollback_in_transaction(
        transaction: &PgTransaction<'_>,
        event_table_names: &[&str],
        network: &str,
        fork_point: u64,
        detection_point: u64,
        corrected_blocks: &[(u64, &str, &str)],
        checkpoint_tables: &[&str],
    ) -> Result<(u64, Vec<String>), PostgresError> {
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

        Ok((total_deleted, all_affected_tx_hashes))
    }
}

/// Renders a `tokio_postgres::Error` with what its `Display` leaves out, on one line.
///
/// tokio-postgres prints only the error kind (`db error`, `error communicating with
/// the server`); the server message and SQLSTATE live in `as_db_error()` and an IO or
/// TLS cause in `source()`. The write path returns `String` errors, and both the
/// `rindexer_atomic_batches_total` reason classifier and the retry log rely on the
/// message and code being present in them. Server errors render as
/// `<kind>: <severity>: <message> (<SQLSTATE>)`; the `DbError` `Display` is not used
/// because it appends DETAIL and HINT on extra lines (a real deadlock carries both).
pub(crate) fn pg_error_to_string(error: &PgError) -> String {
    if let Some(db) = error.as_db_error() {
        return format!("{error}: {}: {} ({})", db.severity(), db.message(), db.code().code());
    }
    match std::error::Error::source(error) {
        Some(cause) => format!("{error}: {cause}"),
        None => error.to_string(),
    }
}

/// Builds a `COPY ... FROM STDIN WITH (FORMAT binary)` statement.
pub(crate) fn build_copy_statement(table_name: &str, column_names: &[String]) -> String {
    format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
        table_name,
        generate_event_table_columns_names_sql(column_names),
    )
}

/// Builds a multi-row `INSERT INTO ... VALUES ($1, ...), (...)` statement whose
/// parameters are the rows flattened in order.
pub(crate) fn build_multi_row_insert_sql(
    table_name: &str,
    column_names: &[String],
    row_count: usize,
) -> String {
    let total_columns = column_names.len();

    let mut query = format!(
        "INSERT INTO {} ({}) VALUES ",
        table_name,
        generate_event_table_columns_names_sql(column_names),
    );

    for i in 0..row_count {
        if i > 0 {
            query.push(',');
        }
        let placeholders: Vec<String> =
            (0..total_columns).map(|j| format!("${}", i * total_columns + j + 1)).collect();
        query.push_str(&format!("({})", placeholders.join(",")));
    }

    query
}

/// Streams rows into a binary COPY sink.
///
/// `finish()` must run even after a failed write, otherwise the COPY never
/// completes and leaves a hanging backend process
/// (https://github.com/sfackler/rust-postgres/issues/1109).
async fn write_binary_copy_rows(
    sink: CopyInSink<bytes::Bytes>,
    column_types: &[PgType],
    data: &[Vec<EthereumSqlTypeWrapper>],
) -> Result<(), BulkInsertPostgresError> {
    let writer = BinaryCopyInWriter::new(sink, column_types);
    pin_mut!(writer);

    for row in data {
        let row_refs: Vec<&(dyn ToSql + Sync)> =
            row.iter().map(|param| param as &(dyn ToSql + Sync)).collect();
        if let Err(e) = writer.as_mut().write(&row_refs).await {
            error!("Error writing binary data, aborting early: {}", e);
            writer.as_mut().finish().await?;
            return Err(e.into());
        }
    }

    writer.finish().await?;

    Ok(())
}

/// Runs a binary COPY inside an already-open transaction, so the copied rows
/// commit or roll back with everything else staged in it.
pub(crate) async fn copy_in_via_transaction(
    transaction: &PgTransaction<'_>,
    table_name: &str,
    column_names: &[String],
    column_types: &[PgType],
    data: &[Vec<EthereumSqlTypeWrapper>],
) -> Result<(), BulkInsertPostgresError> {
    let stmt = build_copy_statement(table_name, column_names);

    let sink: CopyInSink<bytes::Bytes> =
        transaction.copy_in(&stmt).await.map_err(PostgresError::PgError)?;

    write_binary_copy_rows(sink, column_types, data).await
}
