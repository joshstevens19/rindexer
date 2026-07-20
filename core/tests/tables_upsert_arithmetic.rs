//! Integration tests for upsert arithmetic semantics in the custom-tables
//! engine, against a real Postgres.
//!
//! Regression coverage for the subtract-on-insert fix: a row CREATED by a
//! subtract operation must start at `-value` (a subtraction from the implicit
//! starting balance of 0), not `+value`. Previously the fresh-insert path of
//! the upsert took the raw event value, so a holder first seen as a *sender*
//! got a positive balance and every aggregate built on subtract ops drifted.
//!
//! Uses one Postgres container per test, serialized by a global lock (safe
//! under plain `cargo test` and nextest — same pattern as
//! `sync_together_flush.rs`).

use std::collections::HashMap;

use rindexer::indexer::tables::{execute_postgres_operation_internal, TableRowData};
use rindexer::manifest::contract::{injected_columns, Table, TableOperation};
use rindexer::{EthereumSqlTypeWrapper, PostgresClient};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Ctx {
    db: PostgresClient,
    _guard: tokio::sync::MutexGuard<'static, ()>,
    _container: testcontainers::ContainerAsync<Postgres>,
}

async fn setup() -> Ctx {
    let guard = TEST_LOCK.lock().await;
    let _ = rustls::crypto::ring::default_provider().install_default();

    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("pg port");

    // SAFETY: serialized by TEST_LOCK; no concurrent reader in-process.
    unsafe {
        std::env::set_var(
            "DATABASE_URL",
            format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
        );
    }

    let db = PostgresClient::new().await.expect("connect");

    db.batch_execute(
        r#"
        CREATE SCHEMA data;
        CREATE TABLE data.balances (
            holder VARCHAR NOT NULL,
            network VARCHAR NOT NULL,
            balance NUMERIC DEFAULT 0,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        );
        "#,
    )
    .await
    .expect("schema setup");

    Ctx { db, _guard: guard, _container: container }
}

/// A `balances`-style table definition: upsert keyed on holder, with the
/// given action applied to `balance`.
fn table_def(action: &str) -> Table {
    serde_yaml::from_str(&format!(
        r#"
name: balances
columns:
  - name: holder
    type: string
  - name: balance
    type: uint256
    default: "0"
events:
  - event: Transfer
    operations:
      - type: upsert
        where:
          holder: $holder
        set:
          - column: balance
            action: {action}
            value: $value
"#
    ))
    .expect("table yaml")
}

fn operation(table: &Table) -> &TableOperation {
    &table.events[0].operations[0]
}

fn row(holder: &str, balance: u64, seq: u64) -> TableRowData {
    let mut columns = HashMap::new();
    columns.insert("holder".to_string(), EthereumSqlTypeWrapper::String(holder.to_string()));
    columns.insert(
        "balance".to_string(),
        EthereumSqlTypeWrapper::U256Numeric(alloy::primitives::U256::from(balance)),
    );
    columns.insert(
        injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
        EthereumSqlTypeWrapper::U128(seq as u128),
    );
    TableRowData { columns, network: "ethereum".to_string() }
}

async fn balance_of(db: &PostgresClient, holder: &str) -> i64 {
    db.query_one("SELECT balance::bigint AS b FROM data.balances WHERE holder = $1", &[&holder])
        .await
        .expect("balance query")
        .get::<_, i64>("b")
}

#[tokio::test]
async fn subtract_creates_negative_row_and_keeps_subtracting() {
    let ctx = setup().await;
    let table = table_def("subtract");

    // First ever event for this holder is an outgoing transfer: the created
    // row must be a subtraction from 0.
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("alice", 70, 1)],
        None,
    )
    .await
    .expect("first subtract");
    assert_eq!(balance_of(&ctx.db, "alice").await, -70, "fresh subtract must start at -value");

    // Subsequent subtracts keep accumulating downwards.
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("alice", 30, 2)],
        None,
    )
    .await
    .expect("second subtract");
    assert_eq!(balance_of(&ctx.db, "alice").await, -100);
}

#[tokio::test]
async fn add_then_subtract_round_trips_to_zero() {
    let ctx = setup().await;
    let add = table_def("add");
    let subtract = table_def("subtract");

    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &add,
        operation(&add),
        &[row("bob", 100, 1)],
        None,
    )
    .await
    .expect("add");
    assert_eq!(balance_of(&ctx.db, "bob").await, 100);

    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &subtract,
        operation(&subtract),
        &[row("bob", 100, 2)],
        None,
    )
    .await
    .expect("subtract");
    assert_eq!(balance_of(&ctx.db, "bob").await, 0, "add then equal subtract must net to zero");
}

#[tokio::test]
async fn batched_subtracts_for_one_holder_aggregate_correctly() {
    let ctx = setup().await;
    let table = table_def("subtract");

    // Same holder subtracted twice within ONE batch (GROUP BY aggregation
    // path) — a fresh row must still land at -(sum of values).
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("carol", 10, 1), row("carol", 25, 2)],
        None,
    )
    .await
    .expect("batched subtract");
    assert_eq!(balance_of(&ctx.db, "carol").await, -35);
}

#[tokio::test]
async fn custom_where_sees_positive_event_value_on_subtract() {
    let ctx = setup().await;
    let table = table_def("subtract");

    // `$value > 50` compiles to `EXCLUDED."balance" > 50`; the compensation
    // must compare the POSITIVE event value even though the inserted value is
    // negated. First seed a row so the conflict arm (where custom_where
    // applies) is exercised.
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("dave", 10, 1)],
        None,
    )
    .await
    .expect("seed");
    assert_eq!(balance_of(&ctx.db, "dave").await, -10);

    // value=60 passes `> 50` → subtracts.
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("dave", 60, 2)],
        Some("EXCLUDED.\"balance\" > 50"),
    )
    .await
    .expect("subtract above threshold");
    assert_eq!(balance_of(&ctx.db, "dave").await, -70);

    // value=40 fails `> 50` → no change.
    execute_postgres_operation_internal(
        &ctx.db,
        "data.balances",
        &table,
        operation(&table),
        &[row("dave", 40, 3)],
        Some("EXCLUDED.\"balance\" > 50"),
    )
    .await
    .expect("subtract below threshold");
    assert_eq!(balance_of(&ctx.db, "dave").await, -70, "condition on event value must gate the op");
}
