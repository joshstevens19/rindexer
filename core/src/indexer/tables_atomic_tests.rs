//! Docker-backed tests for the atomic write path of the no-code Postgres arm:
//! [`prepare_table_operations`] (no database access), [`apply_table_operations`] in
//! [`PgWriteMode::Tx`] (table operations plus their reorg-journal savepoints),
//! [`PostgresClient::insert_bulk_with_cursor_in`] (raw event rows plus the
//! `last_synced_block` cursor) and `COMMIT`, all on one pooled connection. That is the
//! sequence the atomic arm of `no_code_callback` runs; the tests drive it directly because
//! the callback's `trigger_event` retries on its own schedule.
//!
//! What is pinned here, in terms of the spec's failure table:
//!
//! - a failure in any operation before `COMMIT` leaves nothing behind (table effects, raw
//!   rows, cursor, journal) and the retry applies every effect exactly once, including
//!   `iterate` fan-out over overlapping keys and a downstream `BEFORE` trigger;
//! - a process killed between the last statement and `COMMIT` (the production incident
//!   shape) is a plain rollback, and the replay applies once;
//! - the shutdown flag stops `prepare`, never a transaction that already began;
//! - a journal INSERT failure is contained by its savepoint and the batch still commits;
//! - insert-only tables (binary COPY) and set-only tables (sequence guard) keep their
//!   semantics inside the transaction;
//! - a pool of one connection is enough (no nested checkout between BEGIN and COMMIT);
//! - the legacy eager path produces the same balances.
//!
//! Requires Docker (testcontainers). Every test starts its own Postgres container and
//! touches process-global state (`DATABASE_URL`, the shutdown flag, the
//! `rindexer_atomic_batches_total` counter), so run the module under nextest, which gives
//! each test its own process:
//!
//! ```text
//! cargo nextest run -p rindexer tables_atomic_tests
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, B256, U256};
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::Transaction as PgTransaction;

use super::tables::{
    apply_table_operations, prepare_table_operations, process_table_operations, PreparedTableOps,
    TableRuntime, TxMetadata,
};
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::client::{
    BulkCursorUpdate, CursorAdvance, PostgresClient, TEST_DATABASE_URL_LOCK,
};
use crate::database::postgres::generate::{
    generate_derived_op_log_table_sql, generate_internal_event_table_name,
};
use crate::database::postgres::write_mode::PgWriteMode;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::manifest::contract::{injected_columns, Table};
use crate::manifest::core::Constants;
use crate::metrics::database::record_atomic_batch;
use crate::metrics::definitions::ATOMIC_BATCHES_TOTAL;
use crate::provider::ChainProvider;
use crate::system_state::{_test_reset_shutdown_flag, initiate_shutdown, is_running};
use crate::types::core::LogParam;

const NETWORK: &str = "ethereum";
const INDEXER: &str = "atomic_tests";
const BASE_BLOCK: u64 = 1_000;

/// One decoded event as `prepare_table_operations` receives it.
type EventData = (Vec<LogParam>, String, TxMetadata);

/// `(from, to, value)`; addresses are given by their last byte, `0` is the zero address.
type Transfer = (u8, u8, u64);

/// `(from, to, ids, values)` with parallel arrays, the ERC1155 `TransferBatch` shape.
type BatchTransfer<'a> = (u8, u8, &'a [u64], &'a [u64]);

/// `(holder, token_id)` as read back from the database.
type Cell = (String, String);

/// `(credits, debits)` per cell after applying a batch exactly once.
type CellFold = HashMap<Cell, (i128, i128)>;

// -----------------------------------------------------------------------------
// Table fixtures (manifest YAML, parsed the way rindexer.yaml is)
// -----------------------------------------------------------------------------

fn parse_table(yaml: &str) -> Table {
    serde_yaml::from_str(yaml).expect("table yaml should parse")
}

/// Two additive operations per `Transfer`: credit the recipient, debit the sender, mints
/// and burns (zero address) excluded. Any ERC20 balance table looks like this.
fn two_op_balances_table(name: &str) -> Table {
    parse_table(&format!(
        r#"
name: {name}
columns:
  - name: holder
    type: address
  - name: balance
    type: uint256
    default: "0"
events:
  - event: Transfer
    operations:
      - type: upsert
        where:
          holder: $to
        if: "to != 0x0000000000000000000000000000000000000000"
        set:
          - column: balance
            action: add
            value: $value
      - type: upsert
        where:
          holder: $from
        if: "from != 0x0000000000000000000000000000000000000000"
        set:
          - column: balance
            action: subtract
            value: $value
"#
    ))
}

/// The same two operations fanned out over the parallel `ids` / `values` arrays of an
/// ERC1155 `TransferBatch`, keyed by `(holder, token_id)`.
fn iterate_token_balances_table() -> Table {
    parse_table(
        r#"
name: token_balances
columns:
  - name: holder
    type: address
  - name: token_id
    type: uint256
  - name: balance
    type: uint256
    default: "0"
events:
  - event: TransferBatch
    iterate:
      - "$ids as token_id"
      - "$values as amount"
    operations:
      - type: upsert
        where:
          holder: $to
          token_id: $token_id
        if: "to != 0x0000000000000000000000000000000000000000"
        set:
          - column: balance
            action: add
            value: $amount
      - type: upsert
        where:
          holder: $from
          token_id: $token_id
        if: "from != 0x0000000000000000000000000000000000000000"
        set:
          - column: balance
            action: subtract
            value: $amount
"#,
    )
}

/// A `set` operation on a numeric column keyed by an address: non-reversible, so every
/// row is journaled to `rindexer_internal.derived_op_log` (whose `value` column is NUMERIC).
fn set_last_block_table() -> Table {
    parse_table(
        r#"
name: last_seen
columns:
  - name: holder
    type: address
  - name: last_block
    type: uint64
events:
  - event: Transfer
    operations:
      - type: upsert
        where:
          holder: $to
        set:
          - column: last_block
            action: set
            value: $rindexer_block_number
"#,
    )
}

/// An insert-only (append) table, written through binary COPY.
fn insert_only_log_table() -> Table {
    parse_table(
        r#"
name: transfer_log
columns:
  - name: holder
    type: address
  - name: counterparty
    type: address
  - name: amount
    type: uint256
events:
  - event: Transfer
    operations:
      - type: insert
        set:
          - column: holder
            action: set
            value: $to
          - column: counterparty
            action: set
            value: $from
          - column: amount
            action: set
            value: $value
"#,
    )
}

/// A set-only upsert: last-write-wins by `rindexer_sequence_id`, never by arrival order.
fn set_only_latest_amount_table() -> Table {
    parse_table(
        r#"
name: latest_transfer
columns:
  - name: holder
    type: address
  - name: last_amount
    type: uint64
events:
  - event: Transfer
    operations:
      - type: upsert
        where:
          holder: $to
        set:
          - column: last_amount
            action: set
            value: $value
"#,
    )
}

/// The downstream-shaped fixture: credits go to `balance`, debits accumulate in
/// `debit_total` (`add`), `last_block` is `set` by both operations, and a downstream
/// `BEFORE` trigger (see [`Harness::install_debit_netting_trigger`]) nets the accumulator
/// into `balance` inside the same row write.
fn downstream_custody_table() -> Table {
    parse_table(
        r#"
name: custody
columns:
  - name: holder
    type: address
  - name: token_id
    type: uint256
  - name: balance
    type: uint256
    default: "0"
  - name: debit_total
    type: uint256
    default: "0"
  - name: last_block
    type: uint64
events:
  - event: TransferBatch
    iterate:
      - "$ids as token_id"
      - "$values as amount"
    operations:
      - type: upsert
        where:
          holder: $to
          token_id: $token_id
        if: "to != 0x0000000000000000000000000000000000000000"
        set:
          - column: balance
            action: add
            value: $amount
          - column: last_block
            action: set
            value: $rindexer_block_number
      - type: upsert
        where:
          holder: $from
          token_id: $token_id
        if: "from != 0x0000000000000000000000000000000000000000"
        set:
          - column: debit_total
            action: add
            value: $amount
          - column: last_block
            action: set
            value: $rindexer_block_number
"#,
    )
}

/// `CREATE TABLE` for a manifest table, mirroring the private `generate_tables_sql`:
/// `network`, the user columns (NOT NULL unless nullable, with defaults), the injected
/// metadata columns, and a primary key from the `where` columns (or `rindexer_id` for
/// insert-only tables).
fn create_table_sql(runtime: &TableRuntime) -> String {
    let table = &runtime.table;
    let mut columns = vec!["\"network\" VARCHAR(50) NOT NULL".to_string()];

    for column in &table.columns {
        let pg_type = column.resolved_type().to_postgres_type();
        let mut definition = format!("\"{}\" {}", column.name, pg_type);
        if !column.nullable {
            definition.push_str(" NOT NULL");
        }
        if let Some(default) = &column.default {
            let literal = if matches!(pg_type.as_str(), "NUMERIC" | "BIGINT" | "BOOLEAN") {
                default.clone()
            } else {
                format!("'{}'", default.replace('\'', "''"))
            };
            definition.push_str(&format!(" DEFAULT {literal}"));
        }
        columns.push(definition);
    }

    columns.push(format!("\"{}\" BIGINT NOT NULL", injected_columns::BLOCK_NUMBER));
    columns.push(format!("\"{}\" CHAR(66) NOT NULL", injected_columns::TX_HASH));
    columns.push(format!("\"{}\" CHAR(66) NOT NULL", injected_columns::BLOCK_HASH));
    columns.push(format!("\"{}\" CHAR(42) NOT NULL", injected_columns::CONTRACT_ADDRESS));
    columns.push(format!("\"{}\" NUMERIC NOT NULL", injected_columns::RINDEXER_SEQUENCE_ID));

    let mut primary_key = vec!["\"network\"".to_string()];
    if table.is_insert_only() {
        columns.push(format!("\"{}\" BIGSERIAL", injected_columns::RINDEXER_ID));
        primary_key.push(format!("\"{}\"", injected_columns::RINDEXER_ID));
    } else {
        primary_key.extend(table.primary_key_columns().into_iter().map(|c| format!("\"{c}\"")));
    }
    columns.push(format!("PRIMARY KEY ({})", primary_key.join(", ")));

    format!("CREATE TABLE {} ({})", runtime.full_table_name, columns.join(", "))
}

// -----------------------------------------------------------------------------
// Events and the in-memory folds the database must match
// -----------------------------------------------------------------------------

fn addr(last_byte: u8) -> Address {
    Address::with_last_byte(last_byte)
}

/// Lowercase `0x…` form, the shape used for every address comparison in this module.
fn hex(address: Address) -> String {
    format!("{address:#x}")
}

fn normalize_address(stored: &str) -> String {
    stored.trim().to_lowercase()
}

fn metadata(block_number: u64, index: u64) -> TxMetadata {
    TxMetadata {
        block_number,
        block_timestamp: None,
        tx_hash: B256::from(U256::from(block_number * 1_000 + index).to_be_bytes::<32>()),
        block_hash: B256::from(U256::from(block_number).to_be_bytes::<32>()),
        contract_address: addr(0xCC),
        log_index: U256::from(index),
        tx_index: index,
    }
}

fn uint_array(values: &[u64]) -> DynSolValue {
    DynSolValue::Array(values.iter().map(|v| DynSolValue::Uint(U256::from(*v), 256)).collect())
}

/// One `Transfer(from, to, value)` event per entry, at consecutive blocks from `first_block`.
fn transfer_events(first_block: u64, transfers: &[Transfer]) -> Vec<EventData> {
    transfers
        .iter()
        .enumerate()
        .map(|(i, (from, to, value))| {
            let params = vec![
                LogParam::new("from".to_string(), DynSolValue::Address(addr(*from))),
                LogParam::new("to".to_string(), DynSolValue::Address(addr(*to))),
                LogParam::new("value".to_string(), DynSolValue::Uint(U256::from(*value), 256)),
            ];
            (params, NETWORK.to_string(), metadata(first_block + i as u64, i as u64))
        })
        .collect()
}

/// One `TransferBatch(operator, from, to, ids, values)` event per entry.
fn transfer_batch_events(first_block: u64, transfers: &[BatchTransfer<'_>]) -> Vec<EventData> {
    transfers
        .iter()
        .enumerate()
        .map(|(i, (from, to, ids, values))| {
            let params = vec![
                LogParam::new("operator".to_string(), DynSolValue::Address(addr(*from))),
                LogParam::new("from".to_string(), DynSolValue::Address(addr(*from))),
                LogParam::new("to".to_string(), DynSolValue::Address(addr(*to))),
                LogParam::new("ids".to_string(), uint_array(ids)),
                LogParam::new("values".to_string(), uint_array(values)),
            ];
            (params, NETWORK.to_string(), metadata(first_block + i as u64, i as u64))
        })
        .collect()
}

fn to_block(events: &[EventData]) -> u64 {
    events.iter().map(|(_, _, meta)| meta.block_number).max().expect("a batch has events")
}

/// Balance per holder after applying every transfer once: `+value` to the recipient,
/// `-value` from the sender, the zero address on either side skipped.
fn fold_transfers(transfers: &[Transfer]) -> HashMap<String, i128> {
    let mut balances: HashMap<String, i128> = HashMap::new();
    for (from, to, value) in transfers {
        if *to != 0 {
            *balances.entry(hex(addr(*to))).or_default() += i128::from(*value);
        }
        if *from != 0 {
            *balances.entry(hex(addr(*from))).or_default() -= i128::from(*value);
        }
    }
    balances
}

/// `(credits, debits)` per `(holder, token_id)` after applying every batch transfer once.
fn fold_batch_transfers(transfers: &[BatchTransfer<'_>]) -> CellFold {
    let mut fold: CellFold = HashMap::new();
    for (from, to, ids, values) in transfers {
        for (id, amount) in ids.iter().zip(values.iter()) {
            let amount = i128::from(*amount);
            if *to != 0 {
                fold.entry((hex(addr(*to)), id.to_string())).or_default().0 += amount;
            }
            if *from != 0 {
                fold.entry((hex(addr(*from)), id.to_string())).or_default().1 += amount;
            }
        }
    }
    fold
}

fn net_balances(fold: &CellFold) -> HashMap<Cell, i128> {
    fold.iter().map(|(cell, (credits, debits))| (cell.clone(), credits - debits)).collect()
}

fn debits_only(fold: &CellFold) -> HashMap<Cell, i128> {
    fold.iter().map(|(cell, (_, debits))| (cell.clone(), *debits)).collect()
}

fn parse_numeric(text: &str) -> i128 {
    text.parse().unwrap_or_else(|e| panic!("{text:?} is not an integer: {e}"))
}

// -----------------------------------------------------------------------------
// Driving the atomic arm without the callback
// -----------------------------------------------------------------------------

fn raw_columns() -> Vec<String> {
    ["network", "block_number", "tx_hash"].iter().map(|c| c.to_string()).collect()
}

/// One raw event row per event, in `raw_columns()` order.
fn raw_rows(events: &[EventData]) -> Vec<Vec<EthereumSqlTypeWrapper>> {
    events
        .iter()
        .map(|(_, network, meta)| {
            vec![
                EthereumSqlTypeWrapper::String(network.clone()),
                EthereumSqlTypeWrapper::U64(meta.block_number),
                EthereumSqlTypeWrapper::String(format!("{:?}", meta.tx_hash)),
            ]
        })
        .collect()
}

/// The DB-free phase, with no RPC providers, constants or Multicall overrides.
async fn prepare<'t>(
    tables: &'t [TableRuntime],
    event_name: &str,
    events: &[EventData],
) -> Result<PreparedTableOps<'t>, String> {
    let providers: Arc<HashMap<String, Arc<dyn ChainProvider>>> = Arc::new(HashMap::new());
    prepare_table_operations(
        tables,
        event_name,
        events,
        providers,
        &Constants::new(),
        &HashMap::new(),
    )
    .await
}

/// Everything the arm stages between BEGIN and COMMIT: table operations with their
/// journal savepoints, then the raw rows and the cursor.
async fn stage_in_tx(
    tx: &PgTransaction<'_>,
    prepared: &PreparedTableOps<'_>,
    events: &[EventData],
    raw_table: &str,
    cursor: &BulkCursorUpdate,
) -> Result<CursorAdvance, String> {
    apply_table_operations(prepared, Some(PgWriteMode::Tx(tx)), None, None).await?;
    PostgresClient::insert_bulk_with_cursor_in(
        tx,
        raw_table,
        &raw_columns(),
        &raw_rows(events),
        cursor,
    )
    .await
}

/// Runs one batch the way the atomic arm of `no_code_callback` does: prepare, BEGIN,
/// stage, COMMIT on success or drop (ROLLBACK) on failure, and records the outcome in
/// `rindexer_atomic_batches_total` exactly once per batch.
async fn run_atomic_batch(
    client: &PostgresClient,
    tables: &[TableRuntime],
    event_name: &str,
    events: &[EventData],
    raw_table: &str,
    cursor: &BulkCursorUpdate,
) -> Result<CursorAdvance, String> {
    let prepared = prepare(tables, event_name, events).await?;

    let mut conn = client.raw_connection().await.map_err(|e| e.to_string())?;
    let tx = conn.transaction().await.map_err(|e| e.to_string())?;

    let outcome = match stage_in_tx(&tx, &prepared, events, raw_table, cursor).await {
        Ok(advance) => tx.commit().await.map(|()| advance).map_err(|e| e.to_string()),
        Err(e) => {
            drop(tx);
            Err(e)
        }
    };
    record_atomic_batch(&outcome.as_ref().map(|_| ()).map_err(Clone::clone));
    outcome
}

/// Snapshot of the four `rindexer_atomic_batches_total` series this path can emit.
/// The counter is process-global, so tests assert deltas, never absolute values.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct AtomicBatchCounters {
    committed: u64,
    deadlock: u64,
    cursor_missing: u64,
    error: u64,
}

impl AtomicBatchCounters {
    fn snapshot() -> Self {
        let read = |status: &str, reason: &str| {
            ATOMIC_BATCHES_TOTAL.with_label_values(&[status, reason]).get() as u64
        };
        Self {
            committed: read("committed", "ok"),
            deadlock: read("rolled_back", "deadlock"),
            cursor_missing: read("rolled_back", "cursor_missing"),
            error: read("rolled_back", "error"),
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            committed: self.committed - before.committed,
            deadlock: self.deadlock - before.deadlock,
            cursor_missing: self.cursor_missing - before.cursor_missing,
            error: self.error - before.error,
        }
    }

    /// Every `rolled_back` series, whatever the reason label.
    fn rolled_back(self) -> u64 {
        self.deadlock + self.cursor_missing + self.error
    }
}

/// Restores the process-wide shutdown flag on every exit path of a test that lowers it.
struct ShutdownFlagReset;

impl Drop for ShutdownFlagReset {
    fn drop(&mut self) {
        _test_reset_shutdown_flag();
    }
}

// -----------------------------------------------------------------------------
// Per-test Postgres harness
// -----------------------------------------------------------------------------

/// One Postgres container with the schema the arm expects: the raw event table, the
/// seeded cursor row in `rindexer_internal` and the reorg journal.
struct Harness {
    _container: ContainerAsync<Postgres>,
    client: Arc<PostgresClient>,
    contract: String,
    schema: String,
    raw_table: String,
    cursor_table: String,
}

impl Harness {
    /// `pool_size` is passed through `DATABASE_POOL_SIZE` for the client's construction only.
    async fn start(contract: &str, event_name: &str, pool_size: Option<&str>) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let container = Postgres::default().start().await.expect("failed to start postgres");
        let port = container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        let client = {
            let _guard = TEST_DATABASE_URL_LOCK.lock().await;
            std::env::set_var(
                "DATABASE_URL",
                format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres?sslmode=disable"),
            );
            if let Some(size) = pool_size {
                std::env::set_var("DATABASE_POOL_SIZE", size);
            }
            let client = PostgresClient::new().await;
            if pool_size.is_some() {
                std::env::remove_var("DATABASE_POOL_SIZE");
            }
            client.expect("failed to create postgres client")
        };

        let schema = generate_indexer_contract_schema_name(INDEXER, contract);
        let raw_table = format!("{schema}.raw_{}", event_name.to_lowercase());
        let cursor_table = generate_internal_event_table_name(&schema, event_name);

        client
            .batch_execute(&format!(
                r#"CREATE SCHEMA rindexer_internal;
                   {journal}
                   CREATE SCHEMA {schema};
                   CREATE TABLE {raw_table} (
                       "network" VARCHAR(50) NOT NULL,
                       "block_number" NUMERIC NOT NULL,
                       "tx_hash" TEXT NOT NULL
                   );
                   CREATE TABLE rindexer_internal.{cursor_table} (
                       "network" TEXT PRIMARY KEY,
                       "last_synced_block" NUMERIC NOT NULL
                   );
                   INSERT INTO rindexer_internal.{cursor_table} VALUES ('{NETWORK}', 0);"#,
                journal = generate_derived_op_log_table_sql(),
            ))
            .await
            .expect("failed to bootstrap the test schema");

        Self {
            _container: container,
            client: Arc::new(client),
            contract: contract.to_string(),
            schema,
            raw_table,
            cursor_table,
        }
    }

    fn runtime(&self, table: Table) -> TableRuntime {
        TableRuntime::new(table, INDEXER, &self.contract)
    }

    async fn create_table(&self, runtime: &TableRuntime) {
        self.client
            .batch_execute(&create_table_sql(runtime))
            .await
            .unwrap_or_else(|e| panic!("failed to create {}: {e}", runtime.full_table_name));
    }

    fn cursor(&self, to_block: u64) -> BulkCursorUpdate {
        BulkCursorUpdate {
            internal_table_name: self.cursor_table.clone(),
            network: NETWORK.to_string(),
            to_block,
        }
    }

    /// One atomic batch over `events`, cursor target = the batch's highest block.
    async fn run_batch(
        &self,
        tables: &[TableRuntime],
        event_name: &str,
        events: &[EventData],
    ) -> Result<CursorAdvance, String> {
        run_atomic_batch(
            &self.client,
            tables,
            event_name,
            events,
            &self.raw_table,
            &self.cursor(to_block(events)),
        )
        .await
    }

    /// Stages a full batch, then drops the transaction without COMMIT: what a process
    /// killed after its last statement was sent looks like to Postgres.
    async fn stage_then_drop(
        &self,
        tables: &[TableRuntime],
        event_name: &str,
        events: &[EventData],
    ) {
        let prepared = prepare(tables, event_name, events).await.expect("prepare must succeed");
        let mut conn = self.client.raw_connection().await.expect("pool checkout failed");
        let tx = conn.transaction().await.expect("BEGIN failed");
        let advance =
            stage_in_tx(&tx, &prepared, events, &self.raw_table, &self.cursor(to_block(events)))
                .await
                .expect("every statement must succeed before the simulated crash");
        assert!(
            matches!(advance, CursorAdvance::Advanced { .. }),
            "the cursor UPDATE must have matched before the crash, got {advance:?}"
        );
        drop(tx);
    }

    async fn count(&self, table: &str) -> i64 {
        let row = self
            .client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap_or_else(|e| panic!("failed to count {table}: {e}"));
        row.get(0)
    }

    async fn raw_count(&self) -> i64 {
        self.count(&self.raw_table).await
    }

    async fn journal_count(&self) -> i64 {
        self.count("rindexer_internal.derived_op_log").await
    }

    /// `None` when the seeded cursor row is gone.
    async fn cursor_block(&self) -> Option<i64> {
        let row = self
            .client
            .query_one_or_none(
                &format!(
                    "SELECT last_synced_block::BIGINT FROM rindexer_internal.{} WHERE network = $1",
                    self.cursor_table
                ),
                &[&NETWORK],
            )
            .await
            .expect("failed to read the cursor row");
        row.map(|r| r.get(0))
    }

    async fn delete_cursor_row(&self) {
        self.client
            .batch_execute(&format!("DELETE FROM rindexer_internal.{}", self.cursor_table))
            .await
            .expect("failed to delete the cursor row");
    }

    async fn reseed_cursor_row(&self) {
        self.client
            .batch_execute(&format!(
                "INSERT INTO rindexer_internal.{} VALUES ('{NETWORK}', 0)",
                self.cursor_table
            ))
            .await
            .expect("failed to reseed the cursor row");
    }

    /// `holder -> column` for a table keyed by `holder`.
    async fn holder_values(&self, table: &str, column: &str) -> HashMap<String, i128> {
        self.client
            .query(&format!("SELECT \"holder\", \"{column}\"::TEXT AS value FROM {table}"), &[])
            .await
            .unwrap_or_else(|e| panic!("failed to read {table}: {e}"))
            .into_iter()
            .map(|row| {
                let holder: String = row.get("holder");
                let value: String = row.get("value");
                (normalize_address(&holder), parse_numeric(&value))
            })
            .collect()
    }

    async fn holder_balances(&self, table: &str) -> HashMap<String, i128> {
        self.holder_values(table, "balance").await
    }

    /// `(holder, token_id) -> column` for a table keyed by `(holder, token_id)`.
    async fn cell_values(&self, table: &str, column: &str) -> HashMap<Cell, i128> {
        self.client
            .query(
                &format!(
                    "SELECT \"holder\", \"token_id\"::TEXT AS token_id, \"{column}\"::TEXT AS value FROM {table}"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("failed to read {table}: {e}"))
            .into_iter()
            .map(|row| {
                let holder: String = row.get("holder");
                let token_id: String = row.get("token_id");
                let value: String = row.get("value");
                ((normalize_address(&holder), token_id), parse_numeric(&value))
            })
            .collect()
    }

    async fn sum_column(&self, table: &str, column: &str) -> i128 {
        let row = self
            .client
            .query_one(&format!("SELECT COALESCE(SUM(\"{column}\"), 0)::TEXT FROM {table}"), &[])
            .await
            .unwrap_or_else(|e| panic!("failed to sum {table}.{column}: {e}"));
        let total: String = row.get(0);
        parse_numeric(&total)
    }

    /// A `BEFORE UPDATE` trigger that raises `40P01` (deadlock detected) on the first row
    /// update it ever sees and lets every later one through. `nextval` is not
    /// transactional, so the failure survives the rollback and the retry passes with no
    /// test-side timing.
    async fn install_fail_once_trigger(&self, runtime: &TableRuntime) {
        let schema = &self.schema;
        let table = &runtime.full_table_name;
        self.client
            .batch_execute(&format!(
                r#"CREATE SEQUENCE {schema}.fail_once;
                   CREATE FUNCTION {schema}.fail_once_on_update() RETURNS trigger
                   LANGUAGE plpgsql AS $$
                   BEGIN
                       IF nextval('{schema}.fail_once') = 1 THEN
                           RAISE EXCEPTION 'deadlock detected' USING ERRCODE = '40P01';
                       END IF;
                       RETURN NEW;
                   END $$;
                   CREATE TRIGGER fail_once BEFORE UPDATE ON {table}
                       FOR EACH ROW EXECUTE FUNCTION {schema}.fail_once_on_update();"#
            ))
            .await
            .expect("failed to install the one-shot failure trigger");
    }

    /// How many times the one-shot trigger has consumed `nextval` so far.
    async fn fail_once_fires(&self) -> i64 {
        let row = self
            .client
            .query_one(
                &format!(
                    "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {}.fail_once",
                    self.schema
                ),
                &[],
            )
            .await
            .expect("failed to read the fail_once sequence");
        row.get(0)
    }

    /// The first attempt must have died on the injected trigger, which fires exactly once
    /// (the RAISE aborts the statement at its first updated row). Callers also assert that
    /// the error text carries the `40P01` SQLSTATE, which `pg_error_to_string` preserves.
    async fn assert_injected_failure_fired_once(&self, error: &str) {
        assert_eq!(
            self.fail_once_fires().await,
            1,
            "the injected trigger must have fired exactly once; the batch error was: {error}"
        );
    }

    /// A downstream `BEFORE` trigger of the kind users add to a balance table so that a
    /// debit accumulator (`debit_total`, fed by an `add` operation) is netted into
    /// `balance` in the same row write: the full accumulator on INSERT, its delta on UPDATE.
    async fn install_debit_netting_trigger(&self, runtime: &TableRuntime) {
        let schema = &self.schema;
        let table = &runtime.full_table_name;
        self.client
            .batch_execute(&format!(
                r#"CREATE FUNCTION {schema}.apply_debit_total() RETURNS trigger
                   LANGUAGE plpgsql AS $$
                   BEGIN
                       IF TG_OP = 'INSERT' THEN
                           NEW.balance := NEW.balance - NEW.debit_total;
                       ELSE
                           NEW.balance := NEW.balance - (NEW.debit_total - OLD.debit_total);
                       END IF;
                       RETURN NEW;
                   END $$;
                   CREATE TRIGGER debit_total_insert BEFORE INSERT ON {table}
                       FOR EACH ROW WHEN (NEW.debit_total <> 0)
                       EXECUTE FUNCTION {schema}.apply_debit_total();
                   CREATE TRIGGER debit_total_update BEFORE UPDATE OF debit_total ON {table}
                       FOR EACH ROW WHEN (NEW.debit_total IS DISTINCT FROM OLD.debit_total)
                       EXECUTE FUNCTION {schema}.apply_debit_total();"#
            ))
            .await
            .expect("failed to install the debit netting trigger");
    }

    /// Nothing from a batch may be visible: no table rows, no raw rows, cursor at `0`,
    /// empty journal.
    async fn assert_nothing_written(&self, table: &str, context: &str) {
        assert_eq!(self.count(table).await, 0, "{context}: table effects leaked");
        assert_eq!(self.raw_count().await, 0, "{context}: raw rows leaked");
        assert_eq!(self.cursor_block().await, Some(0), "{context}: cursor moved");
        assert_eq!(self.journal_count().await, 0, "{context}: journal rows leaked");
    }
}

// Recipients B and C are debited later in the same batch, so the debit operation UPDATEs
// rows the credit operation just INSERTed; D is a mint (no debit row for the zero address).
const TRANSFERS: [Transfer; 4] =
    [(0xA1, 0xB1, 10), (0xB1, 0xC1, 3), (0, 0xD1, 100), (0xC1, 0xA1, 1)];

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

/// A1: a failure in the second operation of a two-operation additive table (credit `add`
/// to the recipient, debit `subtract` from the sender) rolls the first operation back
/// together with the raw rows and the cursor; the retry applies every effect exactly once.
#[tokio::test]
async fn op2_failure_rolls_back_op1_then_retry_applies_once() {
    let harness = Harness::start("deadlock", "Transfer", None).await;
    let balances = harness.runtime(two_op_balances_table("balances"));
    harness.create_table(&balances).await;
    harness.install_fail_once_trigger(&balances).await;

    let events = transfer_events(BASE_BLOCK, &TRANSFERS);
    let table = balances.full_table_name.clone();
    let tables = [balances];

    let before = AtomicBatchCounters::snapshot();
    let error = harness
        .run_batch(&tables, "Transfer", &events)
        .await
        .expect_err("the injected 40P01 on the first debit UPDATE must fail the batch");
    harness.assert_injected_failure_fired_once(&error).await;
    assert!(error.contains("40P01"), "the batch error must carry the SQLSTATE, got: {error}");
    harness.assert_nothing_written(&table, "after the failed first attempt").await;
    let delta = AtomicBatchCounters::snapshot().since(before);
    assert_eq!(
        (delta.rolled_back(), delta.deadlock, delta.committed),
        (1, 1, 0),
        "the failed batch must be counted exactly once as rolled_back/deadlock, got {delta:?}"
    );

    let before = AtomicBatchCounters::snapshot();
    let advance =
        harness.run_batch(&tables, "Transfer", &events).await.expect("the retry must commit");
    assert!(
        matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }),
        "the retry must advance the cursor, got {advance:?}"
    );
    assert!(
        harness.fail_once_fires().await > 1,
        "the retry must have run the debit updates the trigger let through"
    );
    assert_eq!(
        harness.holder_balances(&table).await,
        fold_transfers(&TRANSFERS),
        "the retry must apply every credit and debit exactly once"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64, "raw rows must land once");
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
    assert_eq!(
        AtomicBatchCounters::snapshot().since(before),
        AtomicBatchCounters { committed: 1, ..Default::default() },
        "the retry must be counted once as committed/ok"
    );
}

/// A2: `iterate` fan-out over parallel arrays, two events with overlapping token ids in
/// opposite directions plus a self-transfer, through the failure-then-retry path. Every
/// cell equals the fold and the balances sum to total credits minus total debits.
#[tokio::test]
async fn iterate_fanout_overlapping_keys() {
    let harness = Harness::start("fanout", "TransferBatch", None).await;
    let token_balances = harness.runtime(iterate_token_balances_table());
    harness.create_table(&token_balances).await;
    harness.install_fail_once_trigger(&token_balances).await;

    let transfers: [BatchTransfer<'_>; 3] = [
        (0x11, 0x22, &[1, 2, 3], &[10, 20, 30]),
        (0x22, 0x11, &[2, 3, 4], &[5, 6, 7]),
        (0x33, 0x33, &[1], &[9]),
    ];
    let events = transfer_batch_events(BASE_BLOCK, &transfers);
    let fold = fold_batch_transfers(&transfers);
    let table = token_balances.full_table_name.clone();
    let tables = [token_balances];

    let before = AtomicBatchCounters::snapshot();
    let error = harness
        .run_batch(&tables, "TransferBatch", &events)
        .await
        .expect_err("the injected 40P01 on the first debit UPDATE must fail the batch");
    harness.assert_injected_failure_fired_once(&error).await;
    assert!(error.contains("40P01"), "the batch error must carry the SQLSTATE, got: {error}");
    harness.assert_nothing_written(&table, "after the failed first attempt").await;
    let delta = AtomicBatchCounters::snapshot().since(before);
    assert_eq!(
        (delta.rolled_back(), delta.deadlock, delta.committed),
        (1, 1, 0),
        "the failed batch must be counted exactly once as rolled_back/deadlock, got {delta:?}"
    );

    harness.run_batch(&tables, "TransferBatch", &events).await.expect("the retry must commit");

    let cells = harness.cell_values(&table, "balance").await;
    assert_eq!(cells, net_balances(&fold), "every (holder, token_id) cell must equal the fold");
    let total_credits: i128 = fold.values().map(|(credits, _)| credits).sum();
    let total_debits: i128 = fold.values().map(|(_, debits)| debits).sum();
    assert_eq!(
        cells.values().sum::<i128>(),
        total_credits - total_debits,
        "balances must sum to credits minus debits"
    );
    assert_eq!(
        cells.get(&(hex(addr(0x33)), "1".to_string())),
        Some(&0),
        "a self-transfer cell is credited and debited in the same batch and nets to zero"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
}

/// The restart seam behind the production incident: the process dies (pod kill, OOM)
/// after every statement of the batch was sent but before COMMIT. To Postgres that is an
/// abandoned transaction: nothing is visible, and the replay applies everything once.
#[tokio::test]
async fn process_death_between_apply_and_commit_replays_exactly_once() {
    let harness = Harness::start("restart", "Transfer", None).await;
    let balances = harness.runtime(two_op_balances_table("balances"));
    harness.create_table(&balances).await;

    let events = transfer_events(BASE_BLOCK, &TRANSFERS);
    let table = balances.full_table_name.clone();
    let tables = [balances];

    harness.stage_then_drop(&tables, "Transfer", &events).await;
    harness.assert_nothing_written(&table, "after the abandoned transaction").await;

    let advance =
        harness.run_batch(&tables, "Transfer", &events).await.expect("the replay must commit");
    assert!(matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }), "got {advance:?}");
    assert_eq!(
        harness.holder_balances(&table).await,
        fold_transfers(&TRANSFERS),
        "the replay must apply every effect exactly once"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
}

/// The shutdown flag is checked in `prepare` (nothing written yet) and once more by the
/// arm between prepare and BEGIN (in `no_code.rs`, not reachable here). It is never
/// checked after BEGIN: a transaction that began either commits or rolls back, so a batch
/// whose flag drops after prepare still commits in full.
#[tokio::test]
async fn shutdown_flag_blocks_prepare_and_apply_tx_has_no_check() {
    let harness = Harness::start("shutdown", "Transfer", None).await;
    let balances = harness.runtime(two_op_balances_table("balances"));
    harness.create_table(&balances).await;

    let events = transfer_events(BASE_BLOCK, &TRANSFERS);
    let table = balances.full_table_name.clone();
    let tables = [balances];

    let _guard = TEST_DATABASE_URL_LOCK.lock().await;
    let _reset = ShutdownFlagReset;

    initiate_shutdown().await;
    assert!(!is_running(), "initiate_shutdown must lower the flag");
    let error = match prepare(&tables, "Transfer", &events).await {
        Err(error) => error,
        Ok(_) => panic!("prepare must refuse to run once shutdown began"),
    };
    assert!(error.contains("Shutdown"), "unexpected error text: {error}");
    harness.assert_nothing_written(&table, "after prepare refused").await;

    _test_reset_shutdown_flag();
    let prepared = prepare(&tables, "Transfer", &events)
        .await
        .expect("prepare must run while the indexer is running");
    // The flag drops between prepare and BEGIN; the arm's single check lives in
    // no_code.rs, so from here on nothing must look at it.
    initiate_shutdown().await;

    let mut conn = harness.client.raw_connection().await.expect("pool checkout failed");
    let tx = conn.transaction().await.expect("BEGIN failed");
    let advance = stage_in_tx(
        &tx,
        &prepared,
        &events,
        &harness.raw_table,
        &harness.cursor(to_block(&events)),
    )
    .await
    .expect("apply(Tx) and the cursor write have no shutdown check after BEGIN");
    tx.commit().await.expect("COMMIT failed");
    drop(conn);

    assert!(!is_running(), "the flag stays down until the guard resets it");
    assert!(matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }), "got {advance:?}");
    assert_eq!(
        harness.holder_balances(&table).await,
        fold_transfers(&TRANSFERS),
        "a batch that began must commit in full despite the flag"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
}

/// A3: table rows, raw rows, cursor and reorg journal are one unit. With the seeded
/// cursor row missing the cursor write fails after everything else was staged and all
/// four are absent; with the row back all four land in one commit.
#[tokio::test]
async fn rows_cursor_table_ops_and_journal_commit_together() {
    let harness = Harness::start("journal", "Transfer", None).await;
    let last_seen = harness.runtime(set_last_block_table());
    harness.create_table(&last_seen).await;

    let transfers: [Transfer; 3] = [(0xA1, 0xB1, 10), (0xB1, 0xC1, 3), (0xC1, 0xA1, 1)];
    let events = transfer_events(BASE_BLOCK, &transfers);
    let table = last_seen.full_table_name.clone();
    let tables = [last_seen];

    harness.delete_cursor_row().await;
    let before = AtomicBatchCounters::snapshot();
    let error = harness
        .run_batch(&tables, "Transfer", &events)
        .await
        .expect_err("a missing cursor row must fail the batch");
    assert!(error.contains("seeded row missing"), "unexpected error text: {error}");
    assert_eq!(harness.count(&table).await, 0, "table effects must roll back");
    assert_eq!(harness.raw_count().await, 0, "raw rows must roll back");
    assert_eq!(harness.cursor_block().await, None, "no cursor row exists to advance");
    assert_eq!(harness.journal_count().await, 0, "journal rows must roll back");
    assert_eq!(
        AtomicBatchCounters::snapshot().since(before),
        AtomicBatchCounters { cursor_missing: 1, ..Default::default() },
        "the failed batch must be counted once as rolled_back/cursor_missing"
    );

    harness.reseed_cursor_row().await;
    let before = AtomicBatchCounters::snapshot();
    harness.run_batch(&tables, "Transfer", &events).await.expect("the batch must commit");

    let expected: HashMap<String, i128> = transfers
        .iter()
        .enumerate()
        .map(|(i, (_, to, _))| (hex(addr(*to)), i128::from(BASE_BLOCK + i as u64)))
        .collect();
    assert_eq!(
        harness.holder_values(&table, "last_block").await,
        expected,
        "every recipient carries the block of its transfer"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
    assert_eq!(
        harness.journal_count().await,
        events.len() as i64,
        "one derived_op_log row per journaled `set` row"
    );
    assert_eq!(
        AtomicBatchCounters::snapshot().since(before),
        AtomicBatchCounters { committed: 1, ..Default::default() }
    );
}

/// The journal INSERT runs under a savepoint: when it fails the savepoint is rolled back,
/// the failure is logged, and the batch (table rows, raw rows, cursor) still commits.
#[tokio::test]
async fn journal_failure_under_savepoint_does_not_abort_batch() {
    let harness = Harness::start("savepoint", "Transfer", None).await;
    let last_seen = harness.runtime(set_last_block_table());
    harness.create_table(&last_seen).await;
    // Every journal INSERT now fails deterministically, whatever the row carries.
    harness
        .client
        .batch_execute(
            "ALTER TABLE rindexer_internal.derived_op_log \
             ADD CONSTRAINT reject_every_row CHECK (block_number < 0)",
        )
        .await
        .expect("failed to make the journal reject inserts");

    let transfers: [Transfer; 3] = [(0xA1, 0xB1, 10), (0xB1, 0xC1, 3), (0xC1, 0xA1, 1)];
    let events = transfer_events(BASE_BLOCK, &transfers);
    let table = last_seen.full_table_name.clone();
    let tables = [last_seen];

    let advance = harness
        .run_batch(&tables, "Transfer", &events)
        .await
        .expect("a failing journal INSERT must not fail the batch");
    assert!(matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }), "got {advance:?}");
    assert_eq!(harness.count(&table).await, 3, "table rows must commit");
    assert_eq!(harness.raw_count().await, events.len() as i64, "raw rows must commit");
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64), "cursor must advance");
    assert_eq!(harness.journal_count().await, 0, "the rejected journal rows must not exist");
}

/// An insert-only table is written with binary COPY inside the transaction. A failure
/// after the COPY (the cursor write) leaves zero rows in both the insert-only table and
/// the raw table; the rerun populates both once.
#[tokio::test]
async fn insert_only_table_copyin_is_atomic() {
    let harness = Harness::start("activity", "Transfer", None).await;
    let transfer_log = harness.runtime(insert_only_log_table());
    harness.create_table(&transfer_log).await;

    let transfers: [Transfer; 3] = [(0xA1, 0xB1, 10), (0xB1, 0xC1, 3), (0xC1, 0xA1, 1)];
    let events = transfer_events(BASE_BLOCK, &transfers);
    let table = transfer_log.full_table_name.clone();
    let tables = [transfer_log];

    harness.delete_cursor_row().await;
    let before = AtomicBatchCounters::snapshot();
    let error = harness
        .run_batch(&tables, "Transfer", &events)
        .await
        .expect_err("a missing cursor row must fail the batch after the COPY");
    assert!(error.contains("seeded row missing"), "unexpected error text: {error}");
    assert_eq!(harness.count(&table).await, 0, "COPY rows must roll back");
    assert_eq!(harness.raw_count().await, 0, "raw rows must roll back");
    assert_eq!(
        AtomicBatchCounters::snapshot().since(before),
        AtomicBatchCounters { cursor_missing: 1, ..Default::default() }
    );

    harness.reseed_cursor_row().await;
    harness.run_batch(&tables, "Transfer", &events).await.expect("the rerun must commit");
    assert_eq!(harness.count(&table).await, 3, "each transfer appends exactly one row");
    assert_eq!(
        harness.sum_column(&table, "amount").await,
        transfers.iter().map(|(_, _, value)| i128::from(*value)).sum::<i128>(),
        "amounts must land once each"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
}

/// A set-only upsert keeps `rindexer_sequence_id` semantics inside the transaction: the
/// newest sequence wins within a batch, and a later batch carrying an older sequence (a
/// historic range landing after live) neither overwrites the value nor rewinds the cursor.
#[tokio::test]
async fn set_only_table_keeps_sequence_guard_in_tx() {
    let harness = Harness::start("latest", "Transfer", None).await;
    let latest = harness.runtime(set_only_latest_amount_table());
    harness.create_table(&latest).await;

    let holder = hex(addr(0xAA));
    let table = latest.full_table_name.clone();
    let tables = [latest];

    // Two transfers to the same holder in one batch: the higher sequence (block 1001) wins.
    let newer = transfer_events(BASE_BLOCK, &[(0x11, 0xAA, 10), (0x11, 0xAA, 30)]);
    let advance = harness.run_batch(&tables, "Transfer", &newer).await.expect("first batch");
    assert!(matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }), "got {advance:?}");
    assert_eq!(harness.holder_values(&table, "last_amount").await.get(&holder), Some(&30));
    assert_eq!(harness.cursor_block().await, Some((BASE_BLOCK + 1) as i64));

    // An older block arriving later must not overwrite; its rows still commit and the
    // cursor stays where it was.
    let stale = transfer_events(BASE_BLOCK - 500, &[(0x11, 0xAA, 99)]);
    let advance = harness.run_batch(&tables, "Transfer", &stale).await.expect("stale batch");
    match advance {
        CursorAdvance::AlreadyAhead { current } => {
            assert_eq!(current, (BASE_BLOCK + 1).to_string(), "cursor must report where it is")
        }
        other => panic!("an older batch must see the cursor already ahead, got {other:?}"),
    }
    assert_eq!(
        harness.holder_values(&table, "last_amount").await.get(&holder),
        Some(&30),
        "an older sequence must not overwrite a newer value"
    );
    assert_eq!(harness.raw_count().await, 3, "the stale batch's raw rows still commit");
    assert_eq!(harness.cursor_block().await, Some((BASE_BLOCK + 1) as i64), "no rewind");

    // A newer block overwrites and advances the cursor.
    let newest = transfer_events(BASE_BLOCK + 100, &[(0x11, 0xAA, 7)]);
    harness.run_batch(&tables, "Transfer", &newest).await.expect("newest batch");
    assert_eq!(harness.holder_values(&table, "last_amount").await.get(&holder), Some(&7));
    assert_eq!(harness.cursor_block().await, Some((BASE_BLOCK + 100) as i64));
}

/// `DATABASE_POOL_SIZE=1`: a full batch (two-operation table, a journaled `set` table,
/// raw rows, cursor) commits on the single connection. Any nested pool checkout between
/// BEGIN and COMMIT would wait on the connection the transaction holds and time out.
#[tokio::test]
async fn single_connection_pool_suffices() {
    let harness = Harness::start("pool", "Transfer", Some("1")).await;
    let balances = harness.runtime(two_op_balances_table("balances"));
    let last_seen = harness.runtime(set_last_block_table());
    harness.create_table(&balances).await;
    harness.create_table(&last_seen).await;

    let events = transfer_events(BASE_BLOCK, &TRANSFERS);
    let balances_table = balances.full_table_name.clone();
    let last_seen_table = last_seen.full_table_name.clone();
    let tables = [balances, last_seen];

    let advance = tokio::time::timeout(
        Duration::from_secs(20),
        harness.run_batch(&tables, "Transfer", &events),
    )
    .await
    .expect("the batch hung: something checked out a second connection inside the transaction")
    .expect("the batch must commit on a single connection");
    assert!(matches!(advance, CursorAdvance::Advanced { updated_rows: 1 }), "got {advance:?}");
    assert_eq!(harness.holder_balances(&balances_table).await, fold_transfers(&TRANSFERS));
    assert_eq!(harness.count(&last_seen_table).await, 4, "one last_seen row per recipient");
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));
    assert!(harness.journal_count().await > 0, "the journaled `set` rows must have landed");
}

/// The legacy arm (`process_table_operations`, per-operation commits) and the atomic path
/// produce identical balances from the same events.
#[tokio::test]
async fn legacy_eager_path_matches_atomic_balances() {
    let harness = Harness::start("parity", "Transfer", None).await;
    let atomic = harness.runtime(two_op_balances_table("balances_atomic"));
    let eager = harness.runtime(two_op_balances_table("balances_eager"));
    harness.create_table(&atomic).await;
    harness.create_table(&eager).await;

    let events = transfer_events(BASE_BLOCK, &TRANSFERS);

    harness
        .run_batch(std::slice::from_ref(&atomic), "Transfer", &events)
        .await
        .expect("the atomic batch must commit");

    let providers: Arc<HashMap<String, Arc<dyn ChainProvider>>> = Arc::new(HashMap::new());
    process_table_operations(
        std::slice::from_ref(&eager),
        "Transfer",
        &events,
        Some(harness.client.clone()),
        None,
        providers,
        &Constants::new(),
        &HashMap::new(),
        None,
    )
    .await
    .expect("the legacy eager path must succeed");

    let atomic_balances = harness.holder_balances(&atomic.full_table_name).await;
    let eager_balances = harness.holder_balances(&eager.full_table_name).await;
    assert_eq!(atomic_balances, eager_balances, "both arms must agree on every balance");
    assert_eq!(atomic_balances, fold_transfers(&TRANSFERS), "and both must equal the fold");
}

/// The downstream-shaped fixture: a debit accumulator netted into `balance` by a
/// `BEFORE INSERT` / `BEFORE UPDATE OF debit_total` trigger that now fires inside the
/// batch transaction. Through the failure-then-retry sequence and the abandoned
/// transaction sequence, `balance` equals credits minus debits per cell after exactly one
/// application.
#[tokio::test]
async fn downstream_before_trigger_nets_debits_inside_the_transaction() {
    let harness = Harness::start("custody", "TransferBatch", None).await;
    let custody = harness.runtime(downstream_custody_table());
    harness.create_table(&custody).await;
    harness.install_debit_netting_trigger(&custody).await;
    harness.install_fail_once_trigger(&custody).await;

    // (0x22, 2) is credited by the first event and debited by the second, so the debit
    // operation UPDATEs a row the credit operation INSERTed in the same statement batch.
    let first_batch: [BatchTransfer<'_>; 3] =
        [(0x11, 0x22, &[1, 2], &[10, 20]), (0x22, 0x11, &[2], &[5]), (0, 0x33, &[1], &[100])];
    let events = transfer_batch_events(BASE_BLOCK, &first_batch);
    let table = custody.full_table_name.clone();
    let tables = [custody];

    // Failure then retry (the A1 sequence).
    let before = AtomicBatchCounters::snapshot();
    let error = harness
        .run_batch(&tables, "TransferBatch", &events)
        .await
        .expect_err("the injected 40P01 on the first debit UPDATE must fail the batch");
    harness.assert_injected_failure_fired_once(&error).await;
    assert!(error.contains("40P01"), "the batch error must carry the SQLSTATE, got: {error}");
    harness.assert_nothing_written(&table, "after the failed first attempt").await;
    let delta = AtomicBatchCounters::snapshot().since(before);
    assert_eq!(
        (delta.rolled_back(), delta.deadlock, delta.committed),
        (1, 1, 0),
        "the failed batch must be counted exactly once as rolled_back/deadlock, got {delta:?}"
    );

    harness.run_batch(&tables, "TransferBatch", &events).await.expect("the retry must commit");
    let fold = fold_batch_transfers(&first_batch);
    assert_eq!(
        harness.cell_values(&table, "balance").await,
        net_balances(&fold),
        "balance must equal credits minus debits per cell after one application"
    );
    assert_eq!(
        harness.cell_values(&table, "debit_total").await,
        debits_only(&fold),
        "the accumulator must hold exactly the debits"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));

    // Abandoned transaction then replay (the restart seam sequence) on top of that state.
    let second_batch: [BatchTransfer<'_>; 1] = [(0x33, 0x11, &[1], &[40])];
    let later = transfer_batch_events(BASE_BLOCK + 10, &second_batch);

    harness.stage_then_drop(&tables, "TransferBatch", &later).await;
    assert_eq!(
        harness.cell_values(&table, "balance").await,
        net_balances(&fold),
        "an abandoned transaction must leave the committed state untouched"
    );
    assert_eq!(harness.raw_count().await, events.len() as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&events) as i64));

    harness.run_batch(&tables, "TransferBatch", &later).await.expect("the replay must commit");
    let all: Vec<BatchTransfer<'_>> =
        first_batch.iter().chain(second_batch.iter()).copied().collect();
    let fold = fold_batch_transfers(&all);
    assert_eq!(
        harness.cell_values(&table, "balance").await,
        net_balances(&fold),
        "the replay must net the second batch exactly once"
    );
    assert_eq!(harness.cell_values(&table, "debit_total").await, debits_only(&fold));
    assert_eq!(harness.raw_count().await, (events.len() + later.len()) as i64);
    assert_eq!(harness.cursor_block().await, Some(to_block(&later) as i64));
}
