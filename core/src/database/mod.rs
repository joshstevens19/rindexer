pub mod batch_operations;
pub mod clickhouse;
pub mod generate;
pub mod postgres;
pub mod sql_type_wrapper;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{error, info, warn};

use crate::manifest::storage::{CircuitBreakerConfig, WritePolicy};
use crate::metrics::database as db_metrics;

use self::{
    clickhouse::client::ClickhouseClient, postgres::client::PostgresClient,
    sql_type_wrapper::EthereumSqlTypeWrapper,
};

const PG_NAME: &str = "postgres";
const CH_NAME: &str = "clickhouse";

/// Trait for database backends that support bulk row insertion.
/// Implemented by PostgresClient and ClickhouseClient; provided as an extension
/// hook for future backends, not used for runtime fan-out (which is typed-pair).
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

/// Update per-backend metrics, circuit breaker state, and state gauge from a write outcome.
fn record_backend_outcome(
    name: &'static str,
    elapsed_secs: f64,
    health: Option<&Mutex<BackendHealth>>,
    result: &Result<(), String>,
) {
    db_metrics::record_backend_insert(name, elapsed_secs);
    if result.is_err() {
        db_metrics::record_backend_insert_error(name);
    }
    let Some(health) = health else { return };
    let mut guard = health.lock();
    match result {
        Ok(_) => {
            let was_half_open = guard.state == CircuitState::HalfOpen;
            guard.record_success();
            if was_half_open {
                info!("{} circuit breaker recovered — backend is healthy", name);
            }
        }
        Err(_) => {
            guard.record_failure();
            if guard.state == CircuitState::Open {
                warn!(
                    "{} circuit breaker tripped after {} consecutive failures",
                    name, guard.consecutive_failures
                );
            }
        }
    }
    let state_val = match guard.state {
        CircuitState::Closed => 0.0,
        CircuitState::Open => 1.0,
        CircuitState::HalfOpen => 2.0,
    };
    db_metrics::set_circuit_state(name, state_val);
}

// ============================================================================
// DatabaseBackends
// ============================================================================

/// Holds the configured database backends. The current set (postgres, clickhouse)
/// is fixed by the YAML schema, so backends are stored as typed `Option`s rather
/// than a heterogeneous Vec.
#[derive(Default, Clone)]
pub struct DatabaseBackends {
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    write_policy: WritePolicy,
    max_batch_size: Option<usize>,
    pg_health: Option<Arc<Mutex<BackendHealth>>>,
    ch_health: Option<Arc<Mutex<BackendHealth>>>,
    circuit_breaker_enabled: bool,
}

impl DatabaseBackends {
    pub fn new(
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
    ) -> Self {
        Self { postgres, clickhouse, ..Self::default() }
    }

    /// Configure write policy, circuit breaker, and batch size from storage config.
    pub fn with_config(
        mut self,
        write_policy: Option<WritePolicy>,
        circuit_breaker: Option<CircuitBreakerConfig>,
        max_batch_size: Option<usize>,
    ) -> Self {
        if let Some(policy) = &write_policy {
            // PrimaryWithShadow needs both backends; with one configured the
            // policy degenerates into All. Be explicit rather than silently no-op it.
            if *policy == WritePolicy::PrimaryWithShadow
                && (self.postgres.is_none() || self.clickhouse.is_none())
            {
                warn!("WritePolicy::PrimaryWithShadow requires both postgres and clickhouse — falling back to WritePolicy::All");
                self.write_policy = WritePolicy::All;
            } else {
                self.write_policy = policy.clone();
            }
        }
        if let Some(config) = &circuit_breaker {
            if config.enabled {
                self.circuit_breaker_enabled = true;
                // Clamp to safe minimums: threshold >= 1, cooldown >= 1s
                let safe_config = CircuitBreakerConfig {
                    enabled: true,
                    failure_threshold: config.failure_threshold.max(1),
                    cooldown_seconds: config.cooldown_seconds.max(1),
                };
                let new_health = || Arc::new(Mutex::new(BackendHealth::new(&safe_config)));
                self.pg_health = self.postgres.as_ref().map(|_| new_health());
                self.ch_health = self.clickhouse.as_ref().map(|_| new_health());
            }
        }
        // Clamp max_batch_size to >= 1 to prevent panic in chunks(0)
        self.max_batch_size = max_batch_size.map(|v| v.max(1));
        self
    }

    /// Parallel insert across configured backends.
    /// Respects write policy, circuit breaker, and batch-size caps.
    /// Never uses `try_join_all` — cancelling in-flight futures can abort PG COPY mid-stream.
    pub async fn insert_bulk(
        &self,
        table: &str,
        columns: &[String],
        data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        if !self.has_any_db() || data.is_empty() {
            return Ok(());
        }

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
        let pg_op = self.postgres.as_ref().map(|pg| {
            let pg = Arc::clone(pg);
            async move {
                Database::insert_bulk(&*pg, table, columns, data).await.map_err(|e| {
                    error!("postgres insert_bulk failed: {}", e);
                    format!("postgres: {}", e)
                })
            }
        });
        let ch_op = self.clickhouse.as_ref().map(|ch| {
            let ch = Arc::clone(ch);
            async move {
                Database::insert_bulk(&*ch, table, columns, data).await.map_err(|e| {
                    error!("clickhouse insert_bulk failed: {}", e);
                    format!("clickhouse: {}", e)
                })
            }
        });
        self.dispatch_paired(table, pg_op, ch_op).await
    }

    /// Run paired per-backend writes through the shared circuit-breaker, metrics,
    /// and write-policy framework. Either op may be `None` (backend not configured
    /// or skipped); the helper short-circuits cleanly.
    pub async fn dispatch_paired<PgFut, ChFut>(
        &self,
        context: &str,
        pg_op: Option<PgFut>,
        ch_op: Option<ChFut>,
    ) -> Result<(), String>
    where
        PgFut: Future<Output = Result<(), String>> + Send,
        ChFut: Future<Output = Result<(), String>> + Send,
    {
        let pg_offered = pg_op.is_some();
        let ch_offered = ch_op.is_some();
        if !pg_offered && !ch_offered {
            return Ok(());
        }

        let pg_op = self.gate(PG_NAME, context, self.pg_health.as_deref(), pg_op);
        let ch_op = self.gate(CH_NAME, context, self.ch_health.as_deref(), ch_op);

        if pg_op.is_none() && ch_op.is_none() {
            error!("All backend circuits are open — no ops dispatched for {}", context);
            return Err(format!(
                "All backend circuits are open — {} not written to any backend",
                context
            ));
        }

        let start = Instant::now();
        let pg_health = self.pg_health.clone();
        let ch_health = self.ch_health.clone();
        let (pg_outcome, ch_outcome) = tokio::join!(
            run_one(PG_NAME, pg_op, pg_health),
            run_one(CH_NAME, ch_op, ch_health),
        );

        let mut successes: Vec<&'static str> = Vec::with_capacity(2);
        let mut failures: Vec<(&'static str, String)> = Vec::with_capacity(2);
        for (name, result) in [(PG_NAME, pg_outcome), (CH_NAME, ch_outcome)] {
            match result {
                Some(Ok(())) => successes.push(name),
                Some(Err(e)) => failures.push((name, e)),
                None => {}
            }
        }

        if !failures.is_empty() && !successes.is_empty() {
            info!(
                "Partial write: {} succeeded ({:?}), {} failed ({:?}) in {:.1}ms",
                successes.len(),
                successes,
                failures.len(),
                failures.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
                start.elapsed().as_secs_f64() * 1000.0
            );
        }

        self.apply_write_policy(&successes, &failures)
    }

    /// Drop a future if its backend's circuit is open (logs once, returns `None`).
    fn gate<F>(
        &self,
        name: &'static str,
        context: &str,
        health: Option<&Mutex<BackendHealth>>,
        op: Option<F>,
    ) -> Option<F> {
        let op = op?;
        if self.circuit_breaker_enabled
            && health.is_some_and(|h| !h.lock().should_dispatch())
        {
            warn!("{} circuit open, skipping {}", name, context);
            None
        } else {
            Some(op)
        }
    }

    /// Apply the configured `WritePolicy` to a set of per-backend outcomes.
    fn apply_write_policy(
        &self,
        successes: &[&'static str],
        failures: &[(&'static str, String)],
    ) -> Result<(), String> {
        let format_failures = || {
            failures
                .iter()
                .map(|(n, e)| format!("{}: {}", n, e))
                .collect::<Vec<_>>()
                .join("; ")
        };
        match self.write_policy {
            WritePolicy::All => {
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(format_failures())
                }
            }
            WritePolicy::Any => {
                if successes.is_empty() {
                    Err(format_failures())
                } else {
                    Ok(())
                }
            }
            WritePolicy::PrimaryWithShadow => {
                // Primary = postgres if configured (else clickhouse). PrimaryWithShadow
                // is only accepted in `with_config` when both are configured, but we
                // re-derive here defensively.
                let primary = if self.postgres.is_some() {
                    PG_NAME
                } else if self.clickhouse.is_some() {
                    CH_NAME
                } else {
                    return Ok(());
                };
                if failures.iter().any(|(n, _)| *n == primary) {
                    Err(format_failures())
                } else {
                    Ok(())
                }
            }
        }
    }

    pub fn has_any_db(&self) -> bool {
        self.postgres.is_some() || self.clickhouse.is_some()
    }

    pub fn write_policy(&self) -> &WritePolicy {
        &self.write_policy
    }

    /// Get circuit state for a backend by name (for metrics/observability).
    pub fn circuit_state(&self, backend_name: &str) -> Option<CircuitState> {
        if !self.circuit_breaker_enabled {
            return None;
        }
        let health = match backend_name {
            PG_NAME => self.pg_health.as_deref()?,
            CH_NAME => self.ch_health.as_deref()?,
            _ => return None,
        };
        Some(health.lock().state)
    }
}

/// Run a single per-backend op, recording metrics and circuit-breaker outcome.
/// Returns `None` if no op was supplied.
async fn run_one<F>(
    name: &'static str,
    op: Option<F>,
    health: Option<Arc<Mutex<BackendHealth>>>,
) -> Option<Result<(), String>>
where
    F: Future<Output = Result<(), String>>,
{
    let op = op?;
    let start = Instant::now();
    let result = op.await;
    record_backend_outcome(name, start.elapsed().as_secs_f64(), health.as_deref(), &result);
    Some(result)
}
