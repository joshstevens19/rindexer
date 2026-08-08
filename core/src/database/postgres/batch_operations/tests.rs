//! Real-Postgres integration tests for batch upsert semantics, mirroring the
//! ERC20 running-balances shape from the no-code `tables:` feature (one upsert
//! operation crediting `$to` with `add`, one debiting `$from` with `subtract`).
//!
//! The key invariants under test:
//! - A row *created* by an arithmetic upsert starts from the column default
//!   (`default - value` for subtract), not the raw value. A sender-only
//!   address must end up negative.
//! - Duplicate keys within one statement accumulate (GROUP BY SUM) instead of
//!   dropping all but one row.
//! - Arithmetic deltas apply regardless of event sequence order (no sequence
//!   guard), while plain `set` upserts still honor the sequence guard.
//! - The `create_batch_postgres_operation!` macro path (custom Rust indexing)
//!   has the same semantics as the dynamic no-code path.
//!
//! Requires Docker:
//!   cargo test -p rindexer --lib database::postgres::batch_operations

use std::collections::HashMap;

use alloy::primitives::U256;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::types::ToSql;

use super::execute_dynamic_batch_operation;
use crate::database::batch_operations::{
    column, BatchOperationAction, BatchOperationColumnBehavior, BatchOperationSqlType,
    BatchOperationType, DynamicColumnDefinition,
};
use crate::database::postgres::client::PostgresClient;
use crate::{rindexer_error, EthereumSqlTypeWrapper};

const NETWORK: &str = "ethereum";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Builds one row for the balances table exactly as
/// `execute_postgres_operation` in `indexer/tables.rs` would for a
/// credit (`Add`) or debit (`Subtract`) operation.
fn balance_row(
    holder: &str,
    value: u64,
    seq: u128,
    action: BatchOperationAction,
    insert_default: Option<&str>,
) -> Vec<DynamicColumnDefinition> {
    vec![
        DynamicColumnDefinition::new(
            "network".to_string(),
            EthereumSqlTypeWrapper::String(NETWORK.to_string()),
            BatchOperationSqlType::Varchar,
            BatchOperationColumnBehavior::Distinct,
            BatchOperationAction::Where,
        ),
        DynamicColumnDefinition::new(
            "holder".to_string(),
            EthereumSqlTypeWrapper::String(holder.to_string()),
            BatchOperationSqlType::Text,
            BatchOperationColumnBehavior::Distinct,
            BatchOperationAction::Where,
        ),
        DynamicColumnDefinition::new(
            "balance".to_string(),
            EthereumSqlTypeWrapper::U256Numeric(U256::from(value)),
            BatchOperationSqlType::Numeric,
            BatchOperationColumnBehavior::Normal,
            action,
        )
        .with_insert_default(insert_default.map(str::to_string)),
        DynamicColumnDefinition::new(
            "rindexer_sequence_id".to_string(),
            EthereumSqlTypeWrapper::U128(seq),
            BatchOperationSqlType::Numeric,
            BatchOperationColumnBehavior::Sequence,
            BatchOperationAction::Set,
        ),
    ]
}

/// Applies one batch of transfers the way `process_table_operations` does:
/// first all credits as one upsert statement, then all debits as another.
async fn apply_transfer_batch(
    db: &PostgresClient,
    table: &str,
    transfers: &[(&str, &str, u64, u128)], // (from, to, value, sequence)
) {
    let credits: Vec<Vec<DynamicColumnDefinition>> = transfers
        .iter()
        .filter(|(_, to, _, _)| *to != ZERO_ADDRESS)
        .map(|(_, to, value, seq)| {
            balance_row(to, *value, *seq, BatchOperationAction::Add, Some("0"))
        })
        .collect();
    if !credits.is_empty() {
        execute_dynamic_batch_operation(
            db,
            table,
            BatchOperationType::Upsert,
            credits,
            "test-credit",
            None,
        )
        .await
        .expect("credit upsert failed");
    }

    let debits: Vec<Vec<DynamicColumnDefinition>> = transfers
        .iter()
        .filter(|(from, _, _, _)| *from != ZERO_ADDRESS)
        .map(|(from, _, value, seq)| {
            balance_row(from, *value, *seq, BatchOperationAction::Subtract, Some("0"))
        })
        .collect();
    if !debits.is_empty() {
        execute_dynamic_batch_operation(
            db,
            table,
            BatchOperationType::Upsert,
            debits,
            "test-debit",
            None,
        )
        .await
        .expect("debit upsert failed");
    }
}

async fn fetch_balances(db: &PostgresClient, table: &str) -> HashMap<String, i128> {
    db.query(&format!("SELECT holder, balance::TEXT AS balance FROM {table}"), &[])
        .await
        .expect("failed to query balances")
        .into_iter()
        .map(|row| {
            let holder: String = row.get("holder");
            let balance: String = row.get("balance");
            (holder, balance.parse::<i128>().expect("balance should be an integer"))
        })
        .collect()
}

async fn start_postgres() -> (testcontainers::ContainerAsync<Postgres>, PostgresClient) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let container = Postgres::default().start().await.expect("failed to start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

    let _guard = crate::database::postgres::client::TEST_DATABASE_URL_LOCK.lock().await;
    std::env::set_var(
        "DATABASE_URL",
        format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres?sslmode=disable"),
    );
    let client = PostgresClient::new().await.expect("failed to create postgres client");
    (container, client)
}

/// The envio open-indexer-benchmark `erc20-account-balances` shape: running
/// balances from Transfer events, including sender-only addresses that must
/// end up negative.
#[tokio::test]
async fn arithmetic_upsert_running_balances_match_in_memory_fold() {
    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE balances (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 0,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create balances table");

    // (from, to, value, sequence) — batches mirror how block ranges chunk
    // events. Covers: sender-only (a), duplicate sender in one batch (c),
    // self-transfer (d), debit-then-credit across batches (e), mint (zero
    // address from), receiver-then-sender (b).
    let batches: Vec<Vec<(&str, &str, u64, u128)>> = vec![
        vec![
            ("0xa", "0xb", 10, 1),
            ("0xc", "0xb", 3, 2),
            ("0xc", "0xb", 4, 3),
            ("0xd", "0xd", 5, 4),
            ("0xe", "0xf", 8, 5),
        ],
        vec![(ZERO_ADDRESS, "0xg", 100, 6), ("0xg", "0xe", 3, 7), ("0xb", "0xa", 2, 8)],
        vec![("0xe", "0xa", 1, 9), ("0xf", "0xc", 2, 10)],
    ];

    let mut expected: HashMap<String, i128> = HashMap::new();
    for batch in &batches {
        for (from, to, value, _) in batch {
            if *to != ZERO_ADDRESS {
                *expected.entry(to.to_string()).or_default() += *value as i128;
            }
            if *from != ZERO_ADDRESS {
                *expected.entry(from.to_string()).or_default() -= *value as i128;
            }
        }
    }

    for batch in &batches {
        apply_transfer_batch(&db, "balances", batch).await;
    }

    let actual = fetch_balances(&db, "balances").await;

    // Every touched address must have a row — no account may be absent.
    assert_eq!(actual.len(), expected.len(), "account count mismatch: {actual:?}");
    for (holder, expected_balance) in &expected {
        assert_eq!(
            actual.get(holder),
            Some(expected_balance),
            "balance mismatch for {holder}: {actual:?}"
        );
    }

    // The specific regression: a sender-only address must be negative.
    assert_eq!(actual.get("0xd"), Some(&0), "self-transfer must net to zero");
    assert!(actual.get("0xc").is_some_and(|b| *b < 0), "sender-first address must go negative");
}

/// A row created by an arithmetic upsert starts from the column default.
#[tokio::test]
async fn arithmetic_upsert_honors_nonzero_column_default() {
    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE scores (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 100,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create scores table");

    let upsert = |holder: &str, value: u64, seq: u128, action: BatchOperationAction| {
        vec![balance_row(holder, value, seq, action, Some("100"))]
    };

    // New row via subtract: 100 - 30 = 70
    execute_dynamic_batch_operation(
        &db,
        "scores",
        BatchOperationType::Upsert,
        upsert("0xx", 30, 1, BatchOperationAction::Subtract),
        "test",
        None,
    )
    .await
    .unwrap();
    // Existing row via add: 70 + 10 = 80
    execute_dynamic_batch_operation(
        &db,
        "scores",
        BatchOperationType::Upsert,
        upsert("0xx", 10, 2, BatchOperationAction::Add),
        "test",
        None,
    )
    .await
    .unwrap();
    // New row via add: 100 + 5 = 105
    execute_dynamic_batch_operation(
        &db,
        "scores",
        BatchOperationType::Upsert,
        upsert("0xy", 5, 3, BatchOperationAction::Add),
        "test",
        None,
    )
    .await
    .unwrap();

    let actual = fetch_balances(&db, "scores").await;
    assert_eq!(actual.get("0xx"), Some(&80));
    assert_eq!(actual.get("0xy"), Some(&105));
}

/// Plain `set` upserts (no arithmetic) must still honor the sequence guard:
/// an event with a lower sequence than the stored row is ignored.
#[tokio::test]
async fn set_upsert_still_honors_sequence_guard() {
    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE latest_names (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            name TEXT,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create latest_names table");

    let set_row = |name: &str, seq: u128| {
        vec![vec![
            DynamicColumnDefinition::new(
                "network".to_string(),
                EthereumSqlTypeWrapper::String(NETWORK.to_string()),
                BatchOperationSqlType::Varchar,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ),
            DynamicColumnDefinition::new(
                "holder".to_string(),
                EthereumSqlTypeWrapper::String("0xh".to_string()),
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ),
            DynamicColumnDefinition::new(
                "name".to_string(),
                EthereumSqlTypeWrapper::String(name.to_string()),
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ),
            DynamicColumnDefinition::new(
                "rindexer_sequence_id".to_string(),
                EthereumSqlTypeWrapper::U128(seq),
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
            ),
        ]]
    };

    // The stale write (seq 5) is applied LAST so the assertion can only pass
    // if the sequence guard blocks it — last-write-wins would leave "b".
    for (name, seq) in [("a", 10), ("c", 20), ("b", 5)] {
        execute_dynamic_batch_operation(
            &db,
            "latest_names",
            BatchOperationType::Upsert,
            set_row(name, seq),
            "test",
            None,
        )
        .await
        .unwrap();
    }

    let row =
        db.query_one("SELECT name FROM latest_names WHERE holder = '0xh'", &[]).await.unwrap();
    let name: String = row.get("name");
    assert_eq!(name, "c", "stale seq 5 applied last must be ignored; seq 20 must win");
}

/// A pushed-down `if:` condition ("$balance <= @balance" style) references the
/// arithmetic column via EXCLUDED — the rewrite must keep it meaning the raw
/// delta even though the INSERT branch now carries `default - delta`.
#[tokio::test]
async fn custom_where_on_subtract_column_keeps_raw_delta_semantics() {
    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE guarded_balances (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 0,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create guarded_balances table");

    // Exactly what Expression::to_sql_condition produces for
    // `if: "$balance <= @balance"` — only debit when funds are sufficient.
    let guard = "EXCLUDED.\"balance\" <= guarded_balances.\"balance\"";

    // Seed the account with 50.
    execute_dynamic_batch_operation(
        &db,
        "guarded_balances",
        BatchOperationType::Upsert,
        vec![balance_row("0xg1", 50, 1, BatchOperationAction::Add, Some("0"))],
        "test",
        None,
    )
    .await
    .unwrap();

    // Sufficient funds: 30 <= 50, debit applies: 50 - 30 = 20
    execute_dynamic_batch_operation(
        &db,
        "guarded_balances",
        BatchOperationType::Upsert,
        vec![balance_row("0xg1", 30, 2, BatchOperationAction::Subtract, Some("0"))],
        "test",
        Some(guard),
    )
    .await
    .unwrap();

    // Insufficient funds: 100 <= 20 is false, debit must be skipped
    execute_dynamic_batch_operation(
        &db,
        "guarded_balances",
        BatchOperationType::Upsert,
        vec![balance_row("0xg1", 100, 3, BatchOperationAction::Subtract, Some("0"))],
        "test",
        Some(guard),
    )
    .await
    .unwrap();

    let actual = fetch_balances(&db, "guarded_balances").await;
    assert_eq!(
        actual.get("0xg1"),
        Some(&20),
        "guard must compare the raw delta, not the negated insert value"
    );

    // Same guard with a NON-ZERO default: the rewrite must recover the delta
    // using the column's default (25), not 0. A wrong default source would
    // evaluate the second debit's guard as (0 - (25-30)) = 5 <= 20 and apply it.
    db.batch_execute(
        "CREATE TABLE guarded_scores (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 25,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create guarded_scores table");
    let score_guard = "EXCLUDED.\"balance\" <= guarded_scores.\"balance\"";

    // First touch via unguarded subtract: 25 - 5 = 20
    execute_dynamic_batch_operation(
        &db,
        "guarded_scores",
        BatchOperationType::Upsert,
        vec![balance_row("0xg2", 5, 1, BatchOperationAction::Subtract, Some("25"))],
        "test",
        None,
    )
    .await
    .unwrap();
    // Guarded debit of 30: raw delta 30 <= 20 is false, must be skipped
    execute_dynamic_batch_operation(
        &db,
        "guarded_scores",
        BatchOperationType::Upsert,
        vec![balance_row("0xg2", 30, 2, BatchOperationAction::Subtract, Some("25"))],
        "test",
        Some(score_guard),
    )
    .await
    .unwrap();

    let actual = fetch_balances(&db, "guarded_scores").await;
    assert_eq!(
        actual.get("0xg2"),
        Some(&20),
        "guard rewrite must use the column default (25) to recover the delta"
    );
}

/// Max upserts: new rows take the event value, existing rows keep the greater.
#[tokio::test]
async fn max_upsert_keeps_greater_value() {
    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE high_water (
            network VARCHAR(50) NOT NULL,
            holder TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 0,
            rindexer_sequence_id NUMERIC,
            PRIMARY KEY (network, holder)
        )",
    )
    .await
    .expect("failed to create high_water table");

    // The smaller value is applied LAST so the assertion can only pass with
    // Max semantics — last-write-wins (Set) would leave 5.
    for (value, seq) in [(10u64, 1u128), (20, 2), (5, 3)] {
        execute_dynamic_batch_operation(
            &db,
            "high_water",
            BatchOperationType::Upsert,
            vec![balance_row("0xw", value, seq, BatchOperationAction::Max, None)],
            "test",
            None,
        )
        .await
        .unwrap();
    }

    let actual = fetch_balances(&db, "high_water").await;
    assert_eq!(actual.get("0xw"), Some(&20), "smaller value applied last must not win");
}

/// The `create_batch_postgres_operation!` macro (custom Rust indexing) must
/// have the same arithmetic semantics as the dynamic no-code path: rows
/// created by subtract go negative, duplicate keys in a batch accumulate,
/// and lower-sequence deltas still apply.
#[tokio::test]
async fn macro_batch_upsert_matches_dynamic_semantics() {
    struct BalanceDelta {
        holder: &'static str,
        amount: U256,
        seq: u128,
        last_block: u64,
    }

    crate::create_batch_postgres_operation!(
        macro_debit,
        BalanceDelta,
        "macro_balances",
        BatchOperationType::Upsert,
        |delta: &BalanceDelta| {
            vec![
                column(
                    "holder",
                    EthereumSqlTypeWrapper::String(delta.holder.to_string()),
                    BatchOperationSqlType::Text,
                    BatchOperationColumnBehavior::Distinct,
                    BatchOperationAction::Where,
                ),
                column(
                    "balance",
                    EthereumSqlTypeWrapper::U256Numeric(delta.amount),
                    BatchOperationSqlType::Numeric,
                    BatchOperationColumnBehavior::Normal,
                    BatchOperationAction::Subtract,
                ),
                column(
                    "peak_debit",
                    EthereumSqlTypeWrapper::U256Numeric(delta.amount),
                    BatchOperationSqlType::Numeric,
                    BatchOperationColumnBehavior::Normal,
                    BatchOperationAction::Max,
                ),
                column(
                    "last_block",
                    EthereumSqlTypeWrapper::U64BigInt(delta.last_block),
                    BatchOperationSqlType::Bigint,
                    BatchOperationColumnBehavior::Normal,
                    BatchOperationAction::Set,
                ),
                column(
                    "rindexer_sequence_id",
                    EthereumSqlTypeWrapper::U128(delta.seq),
                    BatchOperationSqlType::Numeric,
                    BatchOperationColumnBehavior::Sequence,
                    BatchOperationAction::Set,
                ),
            ]
        },
        "test-macro-debit"
    );

    let (_container, db) = start_postgres().await;

    db.batch_execute(
        "CREATE TABLE macro_balances (
            holder TEXT PRIMARY KEY,
            balance NUMERIC NOT NULL DEFAULT 0,
            peak_debit NUMERIC,
            last_block BIGINT,
            rindexer_sequence_id NUMERIC
        )",
    )
    .await
    .expect("failed to create macro_balances table");

    // Duplicate key within one call must accumulate: -10 - 3 = -13.
    // The set column must keep the value from the highest sequence (600);
    // the max column must keep the greatest debit (10).
    macro_debit(
        &db,
        &[
            BalanceDelta { holder: "0xm1", amount: U256::from(10u64), seq: 5, last_block: 500 },
            BalanceDelta { holder: "0xm1", amount: U256::from(3u64), seq: 6, last_block: 600 },
            BalanceDelta { holder: "0xm2", amount: U256::from(7u64), seq: 7, last_block: 700 },
        ],
    )
    .await
    .expect("macro debit failed");

    // Lower sequence than the stored row: the arithmetic delta must still
    // apply (-13 - 2 = -15) but the stale set column must NOT overwrite, and
    // the smaller debit (2, applied last) must not shrink the max column.
    macro_debit(
        &db,
        &[BalanceDelta { holder: "0xm1", amount: U256::from(2u64), seq: 1, last_block: 100 }],
    )
    .await
    .expect("macro debit failed");

    let actual = fetch_balances(&db, "macro_balances").await;
    assert_eq!(actual.get("0xm1"), Some(&-15));
    assert_eq!(actual.get("0xm2"), Some(&-7));

    let row = db
        .query_one(
            "SELECT last_block, peak_debit::TEXT AS peak, rindexer_sequence_id::TEXT AS seq
             FROM macro_balances WHERE holder = '0xm1'",
            &[],
        )
        .await
        .unwrap();
    let last_block: i64 = row.get("last_block");
    let peak: String = row.get("peak");
    let seq: String = row.get("seq");
    assert_eq!(last_block, 600, "set column must keep the latest-by-sequence value");
    assert_eq!(peak, "10", "max column must keep the greatest value, not the last");
    assert_eq!(seq, "6", "sequence column must keep the max sequence");
}
