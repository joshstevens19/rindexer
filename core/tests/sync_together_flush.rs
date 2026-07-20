//! Integration tests for `PostgresClient::flush_sync_together_block` — the
//! per-block atomic commit at the heart of `sync_together` — plus the
//! `LEAST()` checkpoint-rewind guard in `reorg_rollback_transaction`.
//!
//! Each test gets its own Postgres container, serialized by a global lock so
//! the `DATABASE_URL` mutation never races when tests share a process (plain
//! `cargo test`). Safe under nextest's process-per-test as well.

use std::sync::atomic::{AtomicU64, Ordering};

use rindexer::{
    BufferedPgOp, EthereumSqlTypeWrapper, MemberBlockOps, MemberCheckpoint, PostgresClient,
    SyncTogetherFlushOutcome,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Serializes tests: each takes its own container and sets DATABASE_URL, so
/// two tests running concurrently in one process (plain `cargo test`) would
/// race the env var. tokio's Mutex is runtime-independent, so holding the
/// guard across `#[tokio::test]` runtimes is fine.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Per-test namespace: unique member checkpoint tables, data tables, and
/// network name.
struct Ctx {
    db: PostgresClient,
    /// e.g. "t3" — prefixes table names.
    p: String,
    network: String,
    _guard: tokio::sync::MutexGuard<'static, ()>,
    _container: testcontainers::ContainerAsync<Postgres>,
}

impl Ctx {
    async fn new() -> Self {
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

        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let p = format!("t{id}");
        let network = format!("net_{id}");

        db
            .batch_execute(&format!(
                r#"
                CREATE SCHEMA IF NOT EXISTS rindexer_internal;
                CREATE SCHEMA IF NOT EXISTS data;
                CREATE TABLE IF NOT EXISTS rindexer_internal.latest_block (network VARCHAR NOT NULL, block NUMERIC NOT NULL);
                CREATE TABLE IF NOT EXISTS rindexer_internal.reorg_block_hashes (
                    network VARCHAR NOT NULL,
                    block_number BIGINT NOT NULL,
                    block_hash VARCHAR NOT NULL,
                    parent_hash VARCHAR NOT NULL
                );
                CREATE TABLE rindexer_internal.{p}_member_a (network VARCHAR NOT NULL, last_synced_block NUMERIC NOT NULL);
                CREATE TABLE rindexer_internal.{p}_member_b (network VARCHAR NOT NULL, last_synced_block NUMERIC NOT NULL);
                INSERT INTO rindexer_internal.{p}_member_a VALUES ('{network}', 0);
                INSERT INTO rindexer_internal.{p}_member_b VALUES ('{network}', 0);
                INSERT INTO rindexer_internal.latest_block VALUES ('{network}', 0);
                CREATE TABLE data.{p}_events_a (id BIGINT NOT NULL, block BIGINT NOT NULL);
                CREATE TABLE data.{p}_events_b (id BIGINT NOT NULL, block BIGINT NOT NULL);
                "#
            ))
            .await
            .expect("per-test schema setup");

        Ctx { db, p, network, _guard: guard, _container: container }
    }

    fn member_table(&self, suffix: &str) -> String {
        format!("{}_member_{suffix}", self.p)
    }

    fn data_table(&self, suffix: &str) -> String {
        format!("data.{}_events_{suffix}", self.p)
    }

    async fn set_checkpoint(&self, suffix: &str, block: u64) {
        self.db
            .batch_execute(&format!(
                "UPDATE rindexer_internal.{} SET last_synced_block = {block} WHERE network = '{}'",
                self.member_table(suffix),
                self.network
            ))
            .await
            .unwrap();
    }

    async fn checkpoint(&self, suffix: &str) -> i64 {
        self.db
            .query_one(
                &format!(
                    "SELECT last_synced_block::BIGINT AS b FROM rindexer_internal.{} WHERE network = $1",
                    self.member_table(suffix)
                ),
                &[&self.network],
            )
            .await
            .expect("checkpoint query")
            .get::<_, i64>("b")
    }

    async fn row_count(&self, suffix: &str) -> i64 {
        self.db
            .query_one(
                &format!("SELECT COUNT(*)::BIGINT AS c FROM {}", self.data_table(suffix)),
                &[],
            )
            .await
            .expect("count query")
            .get::<_, i64>("c")
    }

    fn insert_op(&self, suffix: &str, id: u64, block: u64) -> BufferedPgOp {
        BufferedPgOp::Query {
            sql: format!("INSERT INTO {} (id, block) VALUES ($1, $2)", self.data_table(suffix)),
            params: vec![
                EthereumSqlTypeWrapper::U64BigInt(id),
                EthereumSqlTypeWrapper::U64BigInt(block),
            ],
        }
    }

    fn member(&self, suffix: &str, to_block: u64, ops: Vec<BufferedPgOp>) -> MemberBlockOps {
        MemberBlockOps {
            checkpoint: MemberCheckpoint {
                internal_table_name: self.member_table(suffix),
                to_block,
                manifest_start_block: 1,
            },
            ops,
        }
    }

    async fn flush(
        &self,
        members: Vec<MemberBlockOps>,
        group_cursor: u64,
        latest: u64,
    ) -> Result<SyncTogetherFlushOutcome, impl std::fmt::Debug> {
        self.db.flush_sync_together_block(members, &self.network, group_cursor, latest).await
    }
}

#[tokio::test]
async fn flush_commits_ops_and_checkpoints_atomically() {
    let ctx = Ctx::new().await;
    ctx.set_checkpoint("a", 10).await;
    ctx.set_checkpoint("b", 10).await;

    let members = vec![
        ctx.member("a", 11, vec![ctx.insert_op("a", 1, 11)]),
        ctx.member("b", 11, vec![ctx.insert_op("b", 1, 11)]),
    ];

    let outcome = ctx.flush(members, 10, 15).await.expect("flush");

    assert!(matches!(outcome, SyncTogetherFlushOutcome::Committed { .. }), "outcome: {outcome:?}");
    assert_eq!(ctx.row_count("a").await, 1);
    assert_eq!(ctx.row_count("b").await, 1);
    assert_eq!(ctx.checkpoint("a").await, 11);
    assert_eq!(ctx.checkpoint("b").await, 11);
    let latest = ctx
        .db
        .query_one(
            "SELECT block::BIGINT AS b FROM rindexer_internal.latest_block WHERE network = $1",
            &[&ctx.network],
        )
        .await
        .unwrap()
        .get::<_, i64>("b");
    assert_eq!(latest, 15);
}

#[tokio::test]
async fn flush_detects_reorg_rewind_and_writes_nothing() {
    let ctx = Ctx::new().await;
    // member_a was rewound to 5 by a reorg rollback while the loop (cursor 10)
    // was mid-window.
    ctx.set_checkpoint("a", 5).await;
    ctx.set_checkpoint("b", 10).await;

    let members = vec![
        ctx.member("a", 11, vec![ctx.insert_op("a", 1, 11)]),
        ctx.member("b", 11, vec![ctx.insert_op("b", 1, 11)]),
    ];

    let outcome = ctx.flush(members, 10, 15).await.expect("flush");

    assert_eq!(outcome, SyncTogetherFlushOutcome::RewindTo(5));
    assert_eq!(ctx.row_count("a").await, 0, "aborted flush must write nothing");
    assert_eq!(ctx.row_count("b").await, 0);
    assert_eq!(ctx.checkpoint("a").await, 5);
    assert_eq!(ctx.checkpoint("b").await, 10);
}

#[tokio::test]
async fn flush_skips_members_already_committed_past_block() {
    let ctx = Ctx::new().await;
    // member_b already committed through 20 during backfill (hist→live seam);
    // re-applying its ops for block 11 would double-count aggregations.
    ctx.set_checkpoint("a", 10).await;
    ctx.set_checkpoint("b", 20).await;

    let members = vec![
        ctx.member("a", 11, vec![ctx.insert_op("a", 1, 11)]),
        ctx.member("b", 11, vec![ctx.insert_op("b", 1, 11)]),
    ];

    let outcome = ctx.flush(members, 10, 15).await.expect("flush");

    // The applied mask tells the loop to drop the skipped member's deferred
    // side effects (they already fired during its eager backfill).
    assert_eq!(outcome, SyncTogetherFlushOutcome::Committed { applied_members: vec![true, false] });
    assert_eq!(ctx.row_count("a").await, 1);
    assert_eq!(ctx.row_count("b").await, 0, "ahead member's ops skipped");
    assert_eq!(ctx.checkpoint("a").await, 11);
    assert_eq!(ctx.checkpoint("b").await, 20, "ahead checkpoint untouched");
}

#[tokio::test]
async fn never_synced_member_is_not_misread_as_reorg_rewind() {
    let ctx = Ctx::new().await;
    // member_a is live at 10; member_b has never indexed (seeded 0) because
    // its manifest start_block (500) is above the group cursor. Its effective
    // position is start-1 = 499 >= cursor, so no rewind may be detected and
    // the flush must commit normally.
    ctx.set_checkpoint("a", 10).await;

    let never_synced = MemberBlockOps {
        checkpoint: MemberCheckpoint {
            internal_table_name: ctx.member_table("b"),
            to_block: 11,
            manifest_start_block: 500,
        },
        ops: Vec::new(),
    };
    let members = vec![ctx.member("a", 11, vec![ctx.insert_op("a", 1, 11)]), never_synced];

    let outcome = ctx.flush(members, 10, 15).await.expect("flush");

    assert!(
        matches!(outcome, SyncTogetherFlushOutcome::Committed { .. }),
        "seeded-0 checkpoint must not trigger RewindTo(0): {outcome:?}"
    );
    assert_eq!(ctx.row_count("a").await, 1);
    assert_eq!(ctx.checkpoint("a").await, 11);
    // The never-synced member's checkpoint advances too (monotonic guard, 11 > 0).
    assert_eq!(ctx.checkpoint("b").await, 11);
}

#[tokio::test]
async fn failing_op_rolls_back_the_whole_block() {
    let ctx = Ctx::new().await;
    ctx.set_checkpoint("a", 10).await;
    ctx.set_checkpoint("b", 10).await;

    let bad_op = BufferedPgOp::BatchSql {
        sql: format!("INSERT INTO {} (id, block) VALUES ('not-a-number', 11)", ctx.data_table("b")),
    };
    let members = vec![
        ctx.member("a", 11, vec![ctx.insert_op("a", 1, 11)]),
        ctx.member("b", 11, vec![bad_op]),
    ];

    let result = ctx.flush(members, 10, 15).await;

    assert!(result.is_err(), "type error must fail the flush");
    assert_eq!(ctx.row_count("a").await, 0, "member_a's rows rolled back too");
    assert_eq!(ctx.checkpoint("a").await, 10);
    assert_eq!(ctx.checkpoint("b").await, 10);
}

#[tokio::test]
async fn checkpoint_only_flush_advances_members_together() {
    let ctx = Ctx::new().await;
    ctx.set_checkpoint("a", 10).await;
    ctx.set_checkpoint("b", 10).await;

    let members = vec![ctx.member("a", 42, vec![]), ctx.member("b", 42, vec![])];

    let outcome = ctx.flush(members, 10, 50).await.expect("flush");

    assert!(matches!(outcome, SyncTogetherFlushOutcome::Committed { .. }), "outcome: {outcome:?}");
    assert_eq!(ctx.checkpoint("a").await, 42);
    assert_eq!(ctx.checkpoint("b").await, 42);
}

#[tokio::test]
async fn copy_in_op_commits_atomically_with_checkpoints() {
    let ctx = Ctx::new().await;
    ctx.set_checkpoint("a", 10).await;
    ctx.set_checkpoint("b", 10).await;

    let copy_op = BufferedPgOp::CopyIn {
        table_name: ctx.data_table("a"),
        column_names: vec!["id".to_string(), "block".to_string()],
        column_types: vec![tokio_postgres::types::Type::INT8, tokio_postgres::types::Type::INT8],
        rows: vec![
            vec![EthereumSqlTypeWrapper::U64BigInt(1), EthereumSqlTypeWrapper::U64BigInt(11)],
            vec![EthereumSqlTypeWrapper::U64BigInt(2), EthereumSqlTypeWrapper::U64BigInt(11)],
        ],
    };
    let members = vec![ctx.member("a", 11, vec![copy_op]), ctx.member("b", 11, vec![])];

    let outcome = ctx.flush(members, 10, 15).await.expect("flush");

    assert!(matches!(outcome, SyncTogetherFlushOutcome::Committed { .. }), "outcome: {outcome:?}");
    assert_eq!(ctx.row_count("a").await, 2);
    assert_eq!(ctx.checkpoint("a").await, 11);
    assert_eq!(ctx.checkpoint("b").await, 11);
}

#[tokio::test]
async fn reorg_rollback_never_moves_checkpoints_forward() {
    let ctx = Ctx::new().await;
    // member_a is still backfilling far below the fork; member_b is at tip.
    ctx.set_checkpoint("a", 50).await;
    ctx.set_checkpoint("b", 200).await;

    // Rollback with fork_point 100 → rewind target 99.
    let member_a = ctx.member_table("a");
    let member_b = ctx.member_table("b");
    ctx.db
        .reorg_rollback_transaction(
            &[],
            &ctx.network,
            100,
            150,
            &[],
            &[member_a.as_str(), member_b.as_str()],
        )
        .await
        .expect("rollback");

    assert_eq!(
        ctx.checkpoint("a").await,
        50,
        "LEAST() guard: a lagging checkpoint must never jump forward"
    );
    assert_eq!(ctx.checkpoint("b").await, 99, "ahead checkpoint rewound");
}
