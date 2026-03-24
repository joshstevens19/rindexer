pub mod batch_operations;
pub mod clickhouse;
pub mod generate;
pub mod postgres;
pub mod sql_type_wrapper;

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use parking_lot::Mutex;
use tracing::{error, info, warn};

use crate::manifest::storage::{CircuitBreakerConfig, WritePolicy};
use crate::metrics::database as db_metrics;

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

// ============================================================================
// Circuit Breaker
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

struct BackendHealth {
    state: CircuitState,
    consecutive_failures: u32,
    last_failure: Option<Instant>,
    failure_threshold: u32,
    cooldown: Duration,
}

impl BackendHealth {
    fn new(config: &CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            last_failure: None,
            failure_threshold: config.failure_threshold,
            cooldown: Duration::from_secs(config.cooldown_seconds),
        }
    }

    fn should_dispatch(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last) = self.last_failure {
                    if last.elapsed() >= self.cooldown {
                        self.state = CircuitState::HalfOpen;
                        true // probe write
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true, // allow probe
        }
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
    }

    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_failure = Some(Instant::now());
        if self.consecutive_failures >= self.failure_threshold
            || self.state == CircuitState::HalfOpen
        {
            self.state = CircuitState::Open;
        }
    }
}

// ============================================================================
// DatabaseBackends
// ============================================================================

/// Holds all configured database backends for parallel writes.
///
/// The `backends` vec enables `join_all` over `dyn Database` for the common insert path.
/// The typed `postgres`/`clickhouse` fields provide concrete access for backend-specific
/// operations (checkpoints, reorg cleanup, DDL, table operations).
#[derive(Clone)]
pub struct DatabaseBackends {
    backends: Vec<Arc<dyn Database>>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    write_policy: WritePolicy,
    max_batch_size: Option<usize>,
    /// Per-backend circuit breaker health. Index matches `backends` vec.
    health: Vec<Arc<Mutex<BackendHealth>>>,
    circuit_breaker_enabled: bool,
}

impl Default for DatabaseBackends {
    fn default() -> Self {
        Self {
            backends: Vec::new(),
            postgres: None,
            clickhouse: None,
            write_policy: WritePolicy::All,
            max_batch_size: None,
            health: Vec::new(),
            circuit_breaker_enabled: false,
        }
    }
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
        Self {
            backends,
            postgres,
            clickhouse,
            write_policy: WritePolicy::All,
            max_batch_size: None,
            health: Vec::new(),
            circuit_breaker_enabled: false,
        }
    }

    /// Configure write policy, circuit breaker, and batch size from storage config.
    pub fn with_config(
        mut self,
        write_policy: Option<WritePolicy>,
        circuit_breaker: Option<CircuitBreakerConfig>,
        max_batch_size: Option<usize>,
    ) -> Self {
        if let Some(policy) = write_policy {
            self.write_policy = policy;
        }
        if let Some(config) = &circuit_breaker {
            if config.enabled {
                self.circuit_breaker_enabled = true;
                self.health =
                    self.backends.iter().map(|_| Arc::new(Mutex::new(BackendHealth::new(config)))).collect();
            }
        }
        self.max_batch_size = max_batch_size;
        self
    }

    /// Parallel insert across all backends using `join_all`.
    /// Respects write policy, circuit breaker, and batch-size caps.
    /// Never uses `try_join_all` — cancelling in-flight futures can abort PG COPY mid-stream.
    pub async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        if self.backends.is_empty() || data.is_empty() {
            return Ok(());
        }

        // Batch-size caps: split into chunks if configured
        if let Some(max) = self.max_batch_size {
            if data.len() > max {
                for chunk in data.chunks(max) {
                    self.insert_bulk_inner(table, columns, chunk).await?;
                }
                return Ok(());
            }
        }

        self.insert_bulk_inner(table, columns, data).await
    }

    async fn insert_bulk_inner(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        let start = Instant::now();

        let futs: Vec<_> = self
            .backends
            .iter()
            .enumerate()
            .filter_map(|(i, backend)| {
                // Circuit breaker check
                if self.circuit_breaker_enabled {
                    if let Some(health) = self.health.get(i) {
                        let mut h = health.lock();
                        if !h.should_dispatch() {
                            warn!(
                                "{} circuit open, skipping write to {}",
                                backend.backend_name(),
                                table
                            );
                            return None;
                        }
                    }
                }

                let backend = Arc::clone(backend);
                let health = self.health.get(i).cloned();
                let table_owned = table.to_string();
                Some(async move {
                    let name = backend.backend_name();
                    let backend_start = Instant::now();
                    let result =
                        backend.insert_bulk(&table_owned, columns, data).await.map_err(|e| {
                            error!("{} insert_bulk failed: {}", name, e);
                            format!("{}: {}", name, e)
                        });

                    // Per-backend metrics
                    let elapsed = backend_start.elapsed().as_secs_f64();
                    db_metrics::record_backend_insert(name, &table_owned, elapsed);
                    if result.is_err() {
                        db_metrics::record_backend_insert_error(name, &table_owned);
                    }

                    // Update circuit breaker state
                    if let Some(health) = health {
                        let mut h = health.lock();
                        match &result {
                            Ok(_) => h.record_success(),
                            Err(_) => {
                                h.record_failure();
                                if h.state == CircuitState::Open {
                                    warn!("{} circuit breaker tripped after {} consecutive failures",
                                        name, h.consecutive_failures);
                                    db_metrics::set_circuit_state(name, 1.0);
                                }
                            }
                        }
                        // Update circuit state metric
                        let state_val = match h.state {
                            CircuitState::Closed => 0.0,
                            CircuitState::Open => 1.0,
                            CircuitState::HalfOpen => 2.0,
                        };
                        db_metrics::set_circuit_state(name, state_val);
                    }

                    (name, result)
                })
            })
            .collect();

        if futs.is_empty() {
            // All backends have open circuits
            warn!("All backend circuits are open — no writes dispatched for {}", table);
            return Ok(());
        }

        let results = join_all(futs).await;
        let duration = start.elapsed();

        // Collect successes and failures
        let mut successes = Vec::new();
        let mut failures = Vec::new();
        for (name, result) in &results {
            match result {
                Ok(_) => successes.push(*name),
                Err(e) => failures.push((*name, e.as_str())),
            }
        }

        if !failures.is_empty() && !successes.is_empty() {
            info!(
                "Partial write: {} succeeded ({:?}), {} failed ({:?}) in {:.1}ms",
                successes.len(),
                successes,
                failures.len(),
                failures.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
                duration.as_secs_f64() * 1000.0
            );
        }

        // Apply write policy
        match self.write_policy {
            WritePolicy::All => {
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(failures.iter().map(|(n, e)| format!("{}: {}", n, e)).collect::<Vec<_>>().join("; "))
                }
            }
            WritePolicy::Any => {
                if successes.is_empty() {
                    Err(failures.iter().map(|(n, e)| format!("{}: {}", n, e)).collect::<Vec<_>>().join("; "))
                } else {
                    Ok(())
                }
            }
            WritePolicy::PrimaryWithShadow => {
                // PG is primary — error only if PG failed
                let pg_failed = failures.iter().any(|(name, _)| *name == "postgres");
                if pg_failed {
                    Err(failures.iter().map(|(n, e)| format!("{}: {}", n, e)).collect::<Vec<_>>().join("; "))
                } else {
                    Ok(())
                }
            }
        }
    }

    pub fn has_any_db(&self) -> bool {
        !self.backends.is_empty()
    }

    pub fn write_policy(&self) -> &WritePolicy {
        &self.write_policy
    }

    /// Get circuit state for a backend by name (for metrics/observability).
    pub fn circuit_state(&self, backend_name: &str) -> Option<CircuitState> {
        if !self.circuit_breaker_enabled {
            return None;
        }
        for (i, backend) in self.backends.iter().enumerate() {
            if backend.backend_name() == backend_name {
                return self.health.get(i).map(|h| h.lock().state);
            }
        }
        None
    }
}
