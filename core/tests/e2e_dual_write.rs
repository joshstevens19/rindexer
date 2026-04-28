//! End-to-end integration tests for `DatabaseBackends` against real Postgres
//! and ClickHouse containers.
//!
//! These cover the multi-backend write path that the dual-write PR adds:
//!   * `insert_bulk` writing to both backends in parallel
//!   * `dispatch_paired` outcome handling under each `WritePolicy`
//!   * `max_batch_size` chunking
//!   * circuit-breaker trip + recovery
//!
//! Requires Docker. Run via nextest so each test gets its own process —
//! the tests mutate `DATABASE_URL`/`CLICKHOUSE_*` env, which would race
//! under `cargo test`:
//!   cargo nextest run -q -p rindexer --test e2e_dual_write
//!
//! Each test owns its own containers (no shared state); they may run in
//! parallel safely.

use std::sync::Arc;

use alloy::primitives::U256;
use rindexer::manifest::storage::{CircuitBreakerConfig, WritePolicy};
use rindexer::{ClickhouseClient, DatabaseBackends, EthereumSqlTypeWrapper, PostgresClient};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;
use testcontainers_modules::postgres::Postgres;

// ---------------------------------------------------------------------------
// Test environment
// ---------------------------------------------------------------------------

struct DualEnv {
    pg: Arc<PostgresClient>,
    ch: Arc<ClickhouseClient>,
    // Containers are kept alive for the test duration and removed on drop.
    _pg_container: ContainerAsync<Postgres>,
    _ch_container: ContainerAsync<ClickHouseImage>,
}

impl DualEnv {
    async fn new() -> Self {
        // rustls crypto provider is required by reqwest/testcontainers HTTP layers.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let pg_container =
            Postgres::default().start().await.expect("failed to start postgres container");
        let pg_port =
            pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        let ch_container =
            ClickHouseImage::default().start().await.expect("failed to start clickhouse container");
        let ch_port =
            ch_container.get_host_port_ipv4(8123).await.expect("failed to get clickhouse port");

        // SAFETY: nextest gives each test its own process, so this env mutation
        // races no other thread.
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"),
            );
            std::env::set_var("CLICKHOUSE_URL", format!("http://127.0.0.1:{ch_port}"));
            std::env::set_var("CLICKHOUSE_USER", "default");
            std::env::set_var("CLICKHOUSE_PASSWORD", "");
            std::env::set_var("CLICKHOUSE_DB", "default");
        }

        let pg = Arc::new(PostgresClient::new().await.expect("failed to create PostgresClient"));
        let ch =
            Arc::new(ClickhouseClient::new().await.expect("failed to create ClickhouseClient"));

        Self { pg, ch, _pg_container: pg_container, _ch_container: ch_container }
    }
}

/// Provision a small `events` table mirrored across PG and CH so we can
/// `insert_bulk` into the same logical schema and read it back from each side.
async fn create_events_table(env: &DualEnv, schema: &str, table: &str) {
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (
                 id BIGINT,
                 value NUMERIC,
                 label TEXT
             );"
        ))
        .await
        .expect("failed to create pg events table");

    env.ch
        .execute(&format!("CREATE DATABASE IF NOT EXISTS {schema}"))
        .await
        .expect("failed to create ch database");
    env.ch
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} (
                 id UInt64,
                 value UInt256,
                 label String
             ) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("failed to create ch events table");
}

fn columns() -> Vec<String> {
    vec!["id".to_string(), "value".to_string(), "label".to_string()]
}

fn row(id: u64, value: u64, label: &str) -> Vec<EthereumSqlTypeWrapper> {
    vec![
        EthereumSqlTypeWrapper::U64BigInt(id),
        EthereumSqlTypeWrapper::U256Numeric(U256::from(value)),
        EthereumSqlTypeWrapper::String(label.to_string()),
    ]
}

async fn pg_count(env: &DualEnv, schema: &str, table: &str) -> i64 {
    let r = env
        .pg
        .query_one(&format!("SELECT count(*)::BIGINT AS c FROM {schema}.{table}"), &[])
        .await
        .expect("pg count");
    r.get::<_, i64>("c")
}

async fn ch_count(env: &DualEnv, schema: &str, table: &str) -> u64 {
    use clickhouse::Row;
    use serde::Deserialize;

    #[derive(Row, Deserialize)]
    struct Count {
        c: u64,
    }

    let r = env
        .ch
        .query_one::<Count>(&format!("SELECT count() AS c FROM {schema}.{table}"))
        .await
        .expect("ch count");
    r.c
}

// ---------------------------------------------------------------------------
// insert_bulk
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dual_insert_bulk_writes_to_both_backends() {
    let env = DualEnv::new().await;
    let schema = "dual_insert";
    let table = "events";
    create_events_table(&env, schema, table).await;

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone()));
    let data = vec![row(1, 100, "a"), row(2, 200, "b"), row(3, 300, "c")];

    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &data)
        .await
        .expect("insert_bulk should succeed against both backends");

    assert_eq!(pg_count(&env, schema, table).await, 3);
    assert_eq!(ch_count(&env, schema, table).await, 3);
}

#[tokio::test]
async fn insert_bulk_chunks_data_by_max_batch_size() {
    let env = DualEnv::new().await;
    let schema = "dual_chunked";
    let table = "events";
    create_events_table(&env, schema, table).await;

    // 7 rows with max_batch_size=2 must produce 4 batches (2+2+2+1) — both
    // backends still see all 7 rows.
    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        None,
        None,
        Some(2),
    );

    let data: Vec<_> = (0..7).map(|i| row(i as u64, i as u64 * 10, "x")).collect();
    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &data)
        .await
        .expect("chunked insert_bulk should succeed");

    assert_eq!(pg_count(&env, schema, table).await, 7);
    assert_eq!(ch_count(&env, schema, table).await, 7);
}

#[tokio::test]
async fn insert_bulk_short_circuits_on_empty_data() {
    let env = DualEnv::new().await;
    let schema = "dual_empty";
    let table = "events";
    create_events_table(&env, schema, table).await;

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone()));
    backends.insert_bulk(&format!("{schema}.{table}"), &columns(), &[]).await.unwrap();

    assert_eq!(pg_count(&env, schema, table).await, 0);
    assert_eq!(ch_count(&env, schema, table).await, 0);
}

// ---------------------------------------------------------------------------
// WritePolicy semantics under partial failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_policy_all_errors_when_clickhouse_table_missing() {
    let env = DualEnv::new().await;
    let schema = "policy_all";
    let table = "events";
    // Create only the PG side — CH writes will hit a missing table and fail.
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (id BIGINT, value NUMERIC, label TEXT);"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        Some(WritePolicy::All),
        None,
        None,
    );

    let err = backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a")])
        .await
        .expect_err("WritePolicy::All must propagate the CH failure");
    assert!(err.contains("clickhouse"), "error must surface failing backend: {err}");
}

#[tokio::test]
async fn write_policy_any_succeeds_when_one_backend_writes() {
    let env = DualEnv::new().await;
    let schema = "policy_any";
    let table = "events";
    // Create only PG — CH write will fail.
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (id BIGINT, value NUMERIC, label TEXT);"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        Some(WritePolicy::Any),
        None,
        None,
    );

    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a")])
        .await
        .expect("WritePolicy::Any must succeed when at least one backend writes");

    assert_eq!(pg_count(&env, schema, table).await, 1);
}

#[tokio::test]
async fn write_policy_primary_with_shadow_tolerates_clickhouse_failure() {
    let env = DualEnv::new().await;
    let schema = "policy_pws_ch_fail";
    let table = "events";
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (id BIGINT, value NUMERIC, label TEXT);"
        ))
        .await
        .unwrap();
    // CH database/table intentionally absent — shadow failure should be tolerated.

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        Some(WritePolicy::PrimaryWithShadow),
        None,
        None,
    );

    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a")])
        .await
        .expect("PrimaryWithShadow must tolerate shadow (CH) failures");

    assert_eq!(pg_count(&env, schema, table).await, 1);
}

#[tokio::test]
async fn write_policy_primary_with_shadow_errors_on_postgres_failure() {
    let env = DualEnv::new().await;
    let schema = "policy_pws_pg_fail";
    let table = "events";
    // CH side ready, PG side absent — primary failure must propagate.
    env.ch.execute(&format!("CREATE DATABASE IF NOT EXISTS {schema}")).await.unwrap();
    env.ch
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} (
                 id UInt64, value UInt256, label String
             ) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        Some(WritePolicy::PrimaryWithShadow),
        None,
        None,
    );

    let err = backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a")])
        .await
        .expect_err("PrimaryWithShadow must error when primary (PG) fails");
    assert!(err.contains("postgres"), "error must surface failing backend: {err}");
}

// ---------------------------------------------------------------------------
// Single-backend regression
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_postgres_backend_still_works() {
    let env = DualEnv::new().await;
    let schema = "single_pg";
    let table = "events";
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (id BIGINT, value NUMERIC, label TEXT);"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(Some(env.pg.clone()), None);
    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a"), row(2, 2, "b")])
        .await
        .expect("single-backend insert_bulk should succeed");

    assert_eq!(pg_count(&env, schema, table).await, 2);
}

#[tokio::test]
async fn single_clickhouse_backend_still_works() {
    let env = DualEnv::new().await;
    let schema = "single_ch";
    let table = "events";
    env.ch.execute(&format!("CREATE DATABASE IF NOT EXISTS {schema}")).await.unwrap();
    env.ch
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} (
                 id UInt64, value UInt256, label String
             ) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(None, Some(env.ch.clone()));
    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(1, 1, "a"), row(2, 2, "b")])
        .await
        .expect("single-backend insert_bulk should succeed");

    assert_eq!(ch_count(&env, schema, table).await, 2);
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

#[tokio::test]
async fn circuit_breaker_trips_after_threshold_consecutive_failures() {
    use rindexer::CircuitState;

    let env = DualEnv::new().await;
    let schema = "cb_trip";
    let table = "events";
    // Only PG side exists — CH writes always fail. Threshold=2 means the CH
    // breaker should be Open after the second insert.
    env.pg
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             DROP TABLE IF EXISTS {schema}.{table};
             CREATE TABLE {schema}.{table} (id BIGINT, value NUMERIC, label TEXT);"
        ))
        .await
        .unwrap();

    let backends = DatabaseBackends::new(Some(env.pg.clone()), Some(env.ch.clone())).with_config(
        Some(WritePolicy::Any),
        Some(CircuitBreakerConfig { enabled: true, failure_threshold: 2, cooldown_seconds: 60 }),
        None,
    );

    for i in 0..2 {
        backends
            .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(i, 1, "x")])
            .await
            .expect("WritePolicy::Any: PG keeps the call green");
    }

    assert_eq!(
        backends.circuit_state("clickhouse"),
        Some(CircuitState::Open),
        "clickhouse circuit must be Open after threshold consecutive failures"
    );
    assert_eq!(
        backends.circuit_state("postgres"),
        Some(CircuitState::Closed),
        "postgres circuit must remain Closed (no PG failures)"
    );

    // While the CH circuit is open, the next call must still succeed via PG
    // (Any policy) without dispatching to the gated CH backend.
    backends
        .insert_bulk(&format!("{schema}.{table}"), &columns(), &[row(99, 99, "y")])
        .await
        .expect("subsequent writes succeed via PG while CH is gated open");
    assert_eq!(pg_count(&env, schema, table).await, 3);
}
