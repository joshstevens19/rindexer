pub mod batch_operations;
pub mod clickhouse;
pub mod generate;
pub mod postgres;
pub mod sql_type_wrapper;

use std::sync::Arc;

use futures::future::join_all;
use tracing::error;

use self::{
    clickhouse::client::ClickhouseClient, postgres::client::PostgresClient,
    sql_type_wrapper::EthereumSqlTypeWrapper,
};

/// Trait for database backends that support bulk row insertion.
/// Designed for extensibility — future backends (S3, SQLite, etc.) implement this trait.
#[async_trait::async_trait]
pub trait Database: Send + Sync + 'static {
    async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String>;

    fn backend_name(&self) -> &'static str;
}

/// Holds all configured database backends for parallel writes.
///
/// The `backends` vec enables `join_all` over `dyn Database` for the common insert path.
/// The typed `postgres`/`clickhouse` fields provide concrete access for backend-specific
/// operations (checkpoints, reorg cleanup, DDL, table operations).
#[derive(Clone, Default)]
pub struct DatabaseBackends {
    backends: Vec<Arc<dyn Database>>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
}

impl DatabaseBackends {
    pub fn new(
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
    ) -> Self {
        let mut backends: Vec<Arc<dyn Database>> = Vec::new();
        if let Some(pg) = &postgres {
            backends.push(Arc::clone(pg) as Arc<dyn Database>);
        }
        if let Some(ch) = &clickhouse {
            backends.push(Arc::clone(ch) as Arc<dyn Database>);
        }
        Self { backends, postgres, clickhouse }
    }

    /// Parallel insert across all backends using `join_all`.
    /// Never uses `try_join_all` — cancelling in-flight futures can abort PG COPY mid-stream.
    pub async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        if self.backends.is_empty() {
            return Ok(());
        }

        let futs = self
            .backends
            .iter()
            .map(|backend| {
                let backend = Arc::clone(backend);
                async move {
                    let name = backend.backend_name();
                    backend.insert_bulk(table, columns, data).await.map_err(|e| {
                        error!("{} insert_bulk failed: {}", name, e);
                        format!("{}: {}", name, e)
                    })
                }
            })
            .collect::<Vec<_>>();

        let results = join_all(futs).await;
        for result in &results {
            if let Err(e) = result {
                return Err(e.clone());
            }
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }

    pub fn has_any_db(&self) -> bool {
        !self.backends.is_empty()
    }
}
