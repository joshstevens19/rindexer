use std::collections::HashSet;
use std::{env, time::Instant};

use clickhouse::{Client, Row};
use dotenv::dotenv;
use serde::Deserialize;
use tracing::info;

use crate::metrics::database::{self as db_metrics, ops};
use crate::EthereumSqlTypeWrapper;

const DEFAULT_CLICKHOUSE_BATCH_SIZE: usize = 1000;
const CLICKHOUSE_BATCH_SIZE_ENV: &str = "RINDEXER_CLICKHOUSE_BATCH_SIZE";

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

    #[error("{0}")]
    Custom(String),
}

pub struct ClickhouseClient {
    pub(crate) conn: Client,
    batch_size: usize,
    /// ClickHouse server version (major, minor). Used to select lightweight vs mutation deletes.
    version: (u32, u32),
}

fn parse_clickhouse_batch_size() -> usize {
    parse_clickhouse_batch_size_value(env::var(CLICKHOUSE_BATCH_SIZE_ENV).ok())
}

fn parse_clickhouse_batch_size_value(value: Option<String>) -> usize {
    match value {
        Some(raw) => match raw.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            _ => {
                tracing::warn!(
                    "{} is invalid (value: {:?}); using default {}",
                    CLICKHOUSE_BATCH_SIZE_ENV,
                    raw,
                    DEFAULT_CLICKHOUSE_BATCH_SIZE
                );
                DEFAULT_CLICKHOUSE_BATCH_SIZE
            }
        },
        None => DEFAULT_CLICKHOUSE_BATCH_SIZE,
    }
}

impl ClickhouseClient {
    /// Stable identifier exposed to circuit breakers, metrics, and routing.
    /// Renaming this string is a breaking API change.
    pub const BACKEND_NAME: &'static str = "clickhouse";

    pub async fn new() -> Result<Self, ClickhouseConnectionError> {
        let connection = clickhouse_connection()?;
        let batch_size = parse_clickhouse_batch_size();

        let client = Client::default()
            .with_url(connection.url)
            .with_user(connection.user)
            .with_database(connection.db)
            .with_password(connection.password);

        client.query("select 1").execute().await?;
        info!("Clickhouse client connected successfully! dynamic batch size={}", batch_size);

        // Probe server version for feature detection (lightweight deletes >= 23.3)
        let version = Self::probe_version(&client).await;

        Ok(ClickhouseClient { conn: client, batch_size, version })
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    async fn probe_version(client: &Client) -> (u32, u32) {
        #[derive(Row, Deserialize)]
        struct Version {
            v: String,
        }

        match client.query("SELECT version() AS v").fetch_one::<Version>().await {
            Ok(row) => {
                let parts: Vec<&str> = row.v.split('.').collect();
                let major = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                let minor = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                info!("ClickHouse version: {}.{}", major, minor);
                (major, minor)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to probe ClickHouse version: {:?}, defaulting to mutation deletes",
                    e
                );
                (0, 0)
            }
        }
    }

    /// Whether this ClickHouse server supports lightweight DELETE FROM (>= 23.3).
    pub fn supports_lightweight_delete(&self) -> bool {
        self.version >= (23, 3)
    }

    /// Delete rows matching a WHERE clause, using lightweight DELETE when available.
    ///
    /// **IMPORTANT**: Lightweight deletes (CH >= 23.3) mark rows as deleted but they remain
    /// visible until the next merge. All post-delete reads MUST use `FINAL` to exclude
    /// deleted rows. Mutation deletes (CH < 23.3) are synchronous (`mutations_sync = 1`)
    /// and rows are immediately invisible.
    pub async fn delete_where(
        &self,
        table: &str,
        where_clause: &str,
    ) -> Result<(), ClickhouseError> {
        if self.supports_lightweight_delete() {
            self.execute(&format!("DELETE FROM {} WHERE {}", table, where_clause)).await
        } else {
            self.execute(&format!(
                "ALTER TABLE {} DELETE WHERE {} SETTINGS mutations_sync = 1",
                table, where_clause
            ))
            .await
        }
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

        let sql =
            format!("INSERT INTO {} ({}) VALUES {}", table_name, column_names.join(", "), values);

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

    pub async fn delete_by_block_range(
        &self,
        table_name: &str,
        network: &str,
        fork_point: u64,
        detection_point: u64,
    ) -> Result<(), ClickhouseError> {
        let sql = format!(
            "ALTER TABLE {table_name} DELETE WHERE block_number >= {fork_point} AND block_number <= {detection_point} AND network = '{network}' SETTINGS mutations_sync = 1"
        );
        self.execute(&sql).await
    }

    /// Reorg rollback for ClickHouse. Operations are ordered for crash safety:
    /// 1. Rewind checkpoints first (most important — ensures re-indexing on restart)
    /// 2. Count and collect affected tx hashes
    /// 3. Delete stale events (synchronous via mutations_sync = 1)
    /// 4. Update reorg_block_hashes with corrected blocks
    ///
    /// All DELETE operations use `mutations_sync = 1` for synchronous execution.
    /// Each step is idempotent, so a crash mid-sequence can be retried from the start.
    pub async fn reorg_rollback(
        &self,
        event_tables: &[(String, String)], // (database, table_name)
        network: &str,
        fork_point: u64,
        detection_point: u64,
        checkpoint_tables: &[String],
        corrected_blocks: &[(u64, &str, &str)],
    ) -> Result<(u64, Vec<String>), ClickhouseError> {
        #[derive(Row, Deserialize)]
        struct CountAndHashes {
            c: u64,
            hashes: Vec<String>,
        }

        // Step 1: Rewind checkpoint tables first — on restart, the indexer will
        // re-index from the rewound block regardless of whether later steps completed.
        let rewind_block = fork_point.saturating_sub(1);
        for table in checkpoint_tables {
            let delete_sql = format!(
                "ALTER TABLE rindexer_internal.{} DELETE WHERE network = '{}' SETTINGS mutations_sync = 1",
                table, network
            );
            self.execute(&delete_sql).await?;

            let insert_sql = format!(
                "INSERT INTO rindexer_internal.{} (network, last_synced_block) VALUES ('{}', {})",
                table, network, rewind_block
            );
            self.execute(&insert_sql).await?;
        }

        // Step 2: Count affected rows and collect tx hashes before deletion.
        let predicate = format!(
            "block_number >= {} AND block_number <= {} AND network = '{}'",
            fork_point, detection_point, network
        );

        let mut total_deleted: u64 = 0;
        let mut all_tx_hashes: HashSet<String> = HashSet::new();

        for (database, table_name) in event_tables {
            let fq_name = format!("{}.{}", database, table_name);
            let sql = format!(
                "SELECT count() as c, groupArray(DISTINCT tx_hash) as hashes FROM {} WHERE {}",
                fq_name, predicate
            );
            let row: CountAndHashes = self.conn.query(&sql).fetch_one().await?;
            total_deleted += row.c;
            all_tx_hashes.extend(row.hashes);
        }

        // Step 3: Delete stale events (synchronous — mutations_sync = 1 in delete_by_block_range).
        for (database, table_name) in event_tables {
            let fq_name = format!("{}.{}", database, table_name);
            self.delete_by_block_range(&fq_name, network, fork_point, detection_point).await?;
        }

        // Step 4: Update reorg_block_hashes — delete stale, insert corrected.
        self.delete_by_block_range(
            "rindexer_internal.reorg_block_hashes",
            network,
            fork_point,
            detection_point,
        )
        .await?;

        for (block_number, block_hash, parent_hash) in corrected_blocks {
            let sql = format!(
                "INSERT INTO rindexer_internal.reorg_block_hashes (network, block_number, block_hash, parent_hash) VALUES ('{}', {}, '{}', '{}')",
                network, block_number, block_hash, parent_hash
            );
            self.execute(&sql).await?;
        }

        Ok((total_deleted, all_tx_hashes.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_clickhouse_batch_size_value, DEFAULT_CLICKHOUSE_BATCH_SIZE};

    #[test]
    fn clickhouse_batch_size_defaults_when_env_missing() {
        assert_eq!(parse_clickhouse_batch_size_value(None), DEFAULT_CLICKHOUSE_BATCH_SIZE);
    }

    #[test]
    fn clickhouse_batch_size_reads_positive_env_override() {
        assert_eq!(parse_clickhouse_batch_size_value(Some("5000".to_string())), 5000);
    }

    #[test]
    fn clickhouse_batch_size_rejects_zero_or_invalid_values() {
        assert_eq!(
            parse_clickhouse_batch_size_value(Some("0".to_string())),
            DEFAULT_CLICKHOUSE_BATCH_SIZE
        );
        assert_eq!(
            parse_clickhouse_batch_size_value(Some("invalid".to_string())),
            DEFAULT_CLICKHOUSE_BATCH_SIZE
        );
    }
}

#[async_trait::async_trait]
impl crate::database::Database for ClickhouseClient {
    async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        // Wraps the inherent method: discards u64 row count, maps ClickhouseError → String.
        self.insert_bulk(table, columns, data).await.map(|_| ()).map_err(|e| e.to_string())
    }

    fn backend_name(&self) -> &'static str {
        Self::BACKEND_NAME
    }
}
