//! Integration tests for `PostgresClient::insert_bulk_with_cursor` — the
//! atomic [event batch + `rindexer_internal` last-synced cursor] commit that
//! closes the double-index race.
//!
//! Requires Docker (testcontainers). Mutates process-global state
//! (`DATABASE_URL`) — safe under `cargo nextest` (process per test), which is
//! what CI runs.

use rindexer::{BulkCursorUpdate, EthereumSqlTypeWrapper, PostgresClient};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

const NETWORK: &str = "ethereum";
const EVENT_TABLE: &str = "test_events";
const CURSOR_TABLE: &str = "test_indexer_events";

async fn setup() -> (testcontainers::ContainerAsync<Postgres>, PostgresClient) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let pg_container = Postgres::default().start().await.expect("failed to start postgres");
    let pg_port = pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");
    std::env::set_var(
        "DATABASE_URL",
        format!("postgresql://postgres:postgres@127.0.0.1:{pg_port}/postgres"),
    );

    let client = PostgresClient::new().await.expect("failed to build postgres client");
    client
        .batch_execute(&format!(
            r#"CREATE TABLE {EVENT_TABLE} ("value" NUMERIC NOT NULL);
               CREATE SCHEMA rindexer_internal;
               CREATE TABLE rindexer_internal.{CURSOR_TABLE} ("network" TEXT PRIMARY KEY, "last_synced_block" NUMERIC);
               INSERT INTO rindexer_internal.{CURSOR_TABLE} VALUES ('{NETWORK}', 0);"#
        ))
        .await
        .expect("failed to create test tables");

    (pg_container, client)
}

fn rows(n: u64) -> Vec<Vec<EthereumSqlTypeWrapper>> {
    (0..n).map(|i| vec![EthereumSqlTypeWrapper::U64(i)]).collect()
}

fn cursor(to_block: u64) -> BulkCursorUpdate {
    BulkCursorUpdate {
        internal_table_name: CURSOR_TABLE.to_string(),
        network: NETWORK.to_string(),
        to_block,
    }
}

async fn event_count(client: &PostgresClient) -> i64 {
    let row = client
        .query_one(&format!("SELECT count(*) FROM {EVENT_TABLE}"), &[])
        .await
        .expect("failed to count events");
    row.get(0)
}

async fn cursor_block(client: &PostgresClient) -> i64 {
    let row = client
        .query_one(
            &format!(
                "SELECT last_synced_block::bigint FROM rindexer_internal.{CURSOR_TABLE} WHERE network = $1"
            ),
            &[&NETWORK],
        )
        .await
        .expect("failed to read cursor");
    row.get(0)
}

#[tokio::test]
async fn rows_and_cursor_commit_together_via_both_insert_paths() {
    let (_pg, client) = setup().await;
    let columns = vec!["value".to_string()];

    // empty batch: early-return, nothing changes
    client
        .insert_bulk_with_cursor(EVENT_TABLE, &columns, &[], &cursor(10))
        .await
        .expect("empty batch must be a no-op");
    assert_eq!(event_count(&client).await, 0, "empty batch inserted rows");
    assert_eq!(cursor_block(&client).await, 0, "empty batch advanced cursor");

    // <= 100 rows: multi-row INSERT path
    client
        .insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(5), &cursor(100))
        .await
        .expect("insert path failed");
    assert_eq!(event_count(&client).await, 5);
    assert_eq!(cursor_block(&client).await, 100, "insert path must advance the cursor");

    // > 100 rows: binary COPY path, same transaction shape
    client
        .insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(150), &cursor(200))
        .await
        .expect("copy path failed");
    assert_eq!(event_count(&client).await, 155);
    assert_eq!(cursor_block(&client).await, 200, "copy path must advance the cursor");

    // monotonic guard: a lower to_block commits its rows but never rewinds the
    // cursor (the updated-0-rows warn arm)
    client
        .insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(3), &cursor(50))
        .await
        .expect("out-of-order batch failed");
    assert_eq!(event_count(&client).await, 158);
    assert_eq!(cursor_block(&client).await, 200, "cursor must never move backwards");
}

#[tokio::test]
async fn failed_cursor_update_rolls_back_the_event_batch() {
    let (_pg, client) = setup().await;
    let columns = vec!["value".to_string()];

    // Point the cursor at a missing internal table: the UPDATE errors AFTER the
    // batch insert succeeded inside the transaction — the whole commit must
    // roll back, leaving neither rows nor cursor (the atomicity contract).
    let broken = BulkCursorUpdate {
        internal_table_name: "does_not_exist".to_string(),
        network: NETWORK.to_string(),
        to_block: 300,
    };
    let insert_result =
        client.insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(150), &broken).await;
    assert!(insert_result.is_err(), "cursor update against a missing table must fail");
    assert_eq!(event_count(&client).await, 0, "failed cursor update must roll back the rows");
    assert_eq!(cursor_block(&client).await, 0, "cursor must be untouched");

    // same property on the INSERT (<100 rows) path
    let insert_result =
        client.insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(3), &broken).await;
    assert!(insert_result.is_err());
    assert_eq!(event_count(&client).await, 0);
}

#[tokio::test]
async fn missing_cursor_row_rolls_back_while_cursor_ahead_commits() {
    let (_pg, client) = setup().await;
    let columns = vec!["value".to_string()];

    // cursor TABLE exists but holds no row for this network: the batch must
    // roll back and error loudly — committing rows whose cursor can never
    // advance would re-index from the manifest start forever.
    let other_network = BulkCursorUpdate {
        internal_table_name: CURSOR_TABLE.to_string(),
        network: "base".to_string(),
        to_block: 100,
    };
    let result =
        client.insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(150), &other_network).await;
    assert!(result.is_err(), "missing cursor row must fail");
    assert!(
        result.unwrap_err().contains("seeded row missing"),
        "error must name the missing seeded row"
    );
    assert_eq!(event_count(&client).await, 0, "missing cursor row must roll back the rows");

    // cursor row present but already AHEAD (the live loop of the same event
    // committed at head while historic backfill is behind): rows MUST commit
    // and the cursor must stay put — erroring here would retry the historic
    // batch forever against a cursor that stays ahead.
    client
        .batch_execute(&format!(
            "UPDATE rindexer_internal.{CURSOR_TABLE} SET last_synced_block = 1000 WHERE network = '{NETWORK}'"
        ))
        .await
        .expect("failed to pre-advance cursor");
    client
        .insert_bulk_with_cursor(EVENT_TABLE, &columns, &rows(5), &cursor(100))
        .await
        .expect("cursor-ahead batch must still commit its rows");
    assert_eq!(event_count(&client).await, 5);
    assert_eq!(cursor_block(&client).await, 1000, "cursor must not rewind");
}
