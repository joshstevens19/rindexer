//! Per-block buffer-and-flush sink for `sync_together` groups.
//!
//! During live indexing of a `sync_together` group, member event callbacks do
//! not write to Postgres directly: every write is buffered here as a
//! [`BufferedPgOp`] and side effects (CSV/streams/chat) as deferred futures.
//! After all members of a block have been invoked, the group loop replays the
//! buffered ops in one transaction per block via
//! [`PostgresClient::flush_sync_together_block`], commits the members'
//! `last_synced_block` checkpoints atomically with the data, and only then
//! dispatches the deferred side effects.
//!
//! Buffering is semantically equivalent to today's eager execution because the
//! custom-table engine has no Rust-side read-your-own-writes: all conditional
//! logic lives inside the generated SQL and value resolution reads only RPC
//! calls and in-memory caches.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use rust_decimal::Decimal;
use tracing::{debug, error};

use crate::database::postgres::batch_operations::DynamicBatchStatement;
use crate::database::postgres::client::{
    build_multi_row_insert_sql, copy_in_via_transaction, PgType, PostgresClient, PostgresError,
};
use crate::EthereumSqlTypeWrapper;

/// A single Postgres write buffered during a `sync_together` member callback,
/// replayed in buffered order inside the per-block transaction.
pub enum BufferedPgOp {
    /// Parameterized single-statement SQL (raw-event multi-row INSERT,
    /// custom-table upsert/update/delete).
    Query { sql: String, params: Vec<EthereumSqlTypeWrapper> },
    /// Binary COPY (custom-table insert ops, large raw-event batches).
    CopyIn {
        table_name: String,
        column_names: Vec<String>,
        column_types: Vec<PgType>,
        rows: Vec<Vec<EthereumSqlTypeWrapper>>,
    },
    /// Literal SQL executed via `batch_execute` (reorg journal inserts).
    BatchSql { sql: String },
}

/// A side effect (CSV append, stream publish, chat message) captured during a
/// member callback and dispatched only after the block transaction commits.
/// Dispatch failures are logged, never propagated — the block is already
/// durable.
pub type DeferredSideEffect = BoxFuture<'static, ()>;

/// Identifies one group member's checkpoint row for the atomic per-block
/// checkpoint update. The checkpoint row's network is the `network` argument
/// of [`PostgresClient::flush_sync_together_block`] — a flush is strictly
/// per-network, so members cannot carry their own.
#[derive(Debug, Clone)]
pub struct MemberCheckpoint {
    /// Table name inside `rindexer_internal` (see
    /// `internal_event_checkpoint_table_name`).
    pub internal_table_name: String,
    /// The block this flush checkpoints the member to.
    pub to_block: u64,
    /// The member's rewind-detection floor: its MANIFEST-declared start_block
    /// on this network, or — when the manifest omits one — the boot-time
    /// checkpoint-aware resume point (never the live-leg config start_block,
    /// which reapply_after_historic mutates). A stored checkpoint below
    /// `floor - 1` means either "never indexed yet" (seeded 0, member starts
    /// above the group cursor) or "reorg rolled back below the member's
    /// entire range" — in both cases the member's effective position is
    /// `floor - 1`, which is what the rewind detection must compare against.
    /// Without this floor, a never-synced member's seeded 0 reads as a rewind
    /// to block 0 and triggers a full-chain re-scan.
    pub manifest_start_block: u64,
}

impl MemberCheckpoint {
    /// The member's effective sync position given its stored checkpoint.
    fn effective_position(&self, stored: Decimal) -> Decimal {
        stored.max(Decimal::from(self.manifest_start_block.saturating_sub(1)))
    }
}

/// One member's buffered writes for the block being flushed.
pub struct MemberBlockOps {
    pub checkpoint: MemberCheckpoint,
    pub ops: Vec<BufferedPgOp>,
}

#[derive(Default)]
struct NetworkBuffer {
    ops: Vec<BufferedPgOp>,
    side_effects: Vec<DeferredSideEffect>,
}

/// Write buffer for one `sync_together` group.
///
/// One instance is owned by each group (never shared across groups — a shared
/// sink would let one group's flush drain or clear another group's pending
/// ops). It is keyed by network internally because a group runs one loop per
/// network; within a network, member callbacks run sequentially, so the loop
/// can attribute buffered ops to a member by calling [`Self::take`] after each
/// member's callback returns.
#[derive(Default)]
pub struct SyncTogetherSink {
    buffers: Mutex<HashMap<String, NetworkBuffer>>,
}

impl SyncTogetherSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_op(&self, network: &str, op: BufferedPgOp) {
        let mut buffers = self.buffers.lock().expect("sync sink poisoned");
        buffers.entry(network.to_string()).or_default().ops.push(op);
    }

    pub fn push_side_effect(&self, network: &str, effect: DeferredSideEffect) {
        let mut buffers = self.buffers.lock().expect("sync sink poisoned");
        buffers.entry(network.to_string()).or_default().side_effects.push(effect);
    }

    /// Drains and returns everything buffered for `network`.
    pub fn take(&self, network: &str) -> (Vec<BufferedPgOp>, Vec<DeferredSideEffect>) {
        let mut buffers = self.buffers.lock().expect("sync sink poisoned");
        match buffers.remove(network) {
            Some(buffer) => (buffer.ops, buffer.side_effects),
            None => (Vec::new(), Vec::new()),
        }
    }

    /// Discards everything buffered for `network` (block retry path).
    pub fn clear(&self, network: &str) {
        let mut buffers = self.buffers.lock().expect("sync sink poisoned");
        buffers.remove(network);
    }
}

impl From<DynamicBatchStatement> for BufferedPgOp {
    fn from(statement: DynamicBatchStatement) -> Self {
        match statement {
            DynamicBatchStatement::Query { sql, params } => BufferedPgOp::Query { sql, params },
            DynamicBatchStatement::CopyIn { table_name, column_names, column_types, rows } => {
                BufferedPgOp::CopyIn { table_name, column_names, column_types, rows }
            }
        }
    }
}

/// How the custom-table engine should perform Postgres writes.
#[derive(Clone, Copy)]
pub enum PgWriteMode<'a> {
    /// Execute immediately (autocommit) — the normal hot path.
    Eager(&'a PostgresClient),
    /// Buffer into a `sync_together` sink under `network`, to be committed by
    /// the group loop's per-block transaction.
    Buffered { sink: &'a SyncTogetherSink, network: &'a str },
}

/// Buffers a raw-event bulk insert, mirroring `PostgresClient::insert_bulk`'s
/// COPY-vs-INSERT threshold so the replayed statement shape matches what the
/// eager path would have executed.
pub fn push_raw_event_insert(
    sink: &SyncTogetherSink,
    network: &str,
    table_name: &str,
    column_names: &[String],
    column_types: &[PgType],
    bulk_data: &[Vec<EthereumSqlTypeWrapper>],
) {
    if bulk_data.is_empty() {
        return;
    }

    let total_params = bulk_data.len() * column_names.len();
    if bulk_data.len() > 100 || total_params > 65535 {
        sink.push_op(
            network,
            BufferedPgOp::CopyIn {
                table_name: table_name.to_string(),
                column_names: column_names.to_vec(),
                column_types: column_types.to_vec(),
                rows: bulk_data.to_vec(),
            },
        );
    } else {
        let sql = build_multi_row_insert_sql(table_name, column_names, bulk_data.len());
        let params = bulk_data.iter().flatten().cloned().collect();
        sink.push_op(network, BufferedPgOp::Query { sql, params });
    }
}

tokio::task_local! {
    /// The sink of the `sync_together` group loop currently invoking a member
    /// callback. Scoped (not baked into callback params) so the same callback
    /// writes eagerly during historical backfill and buffers only when the
    /// lockstep loop drives it.
    static ACTIVE_SYNC_SINK: Arc<SyncTogetherSink>;
}

/// Runs `future` with `sink` visible to any `sync_together`-aware write path
/// it awaits inline (task-locals do not cross `tokio::spawn`).
pub async fn with_sync_sink<F: Future>(sink: Arc<SyncTogetherSink>, future: F) -> F::Output {
    ACTIVE_SYNC_SINK.scope(sink, future).await
}

/// The sink installed by [`with_sync_sink`], if this invocation is being
/// driven by a `sync_together` group loop.
pub fn active_sync_sink() -> Option<Arc<SyncTogetherSink>> {
    ACTIVE_SYNC_SINK.try_with(|sink| sink.clone()).ok()
}

/// Outcome of a per-block flush.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncTogetherFlushOutcome {
    Committed {
        /// Per member (input order): whether its ops were applied. `false`
        /// means the member was already checkpointed at or past the block and
        /// was skipped — its deferred side effects must NOT be dispatched.
        /// Two causes: hist→live overshoot (the effects already fired during
        /// eager backfill) or a retry of a block whose earlier flush committed
        /// server-side but errored client-side (lost COMMIT ack) — in the
        /// latter case the effects are lost, the documented at-most-once
        /// behavior (same fault class as crashing between commit and
        /// dispatch).
        applied_members: Vec<bool>,
    },
    /// A reorg rollback rewound member checkpoints below the group cursor
    /// while this block was being processed. Nothing was written; the loop
    /// must rewind its cursor to this block and refetch from there.
    RewindTo(u64),
}

impl PostgresClient {
    /// Commits one `sync_together` block atomically: every member's buffered
    /// ops, their `last_synced_block` checkpoints, and the network's
    /// `latest_block`, in a single transaction.
    ///
    /// The caller must hold the network's `ReorgCoordinator` mutex for the
    /// duration of this call (acquired BEFORE the pool connection — the
    /// codebase-wide lock order) so a reorg rollback cannot interleave with
    /// the commit.
    ///
    /// In-transaction checkpoint checks:
    /// - If any member's stored checkpoint is below `group_cursor` (the last
    ///   block the loop committed), a reorg rollback rewound it: the flush
    ///   aborts untouched and returns [`SyncTogetherFlushOutcome::RewindTo`]
    ///   with the lowest member checkpoint.
    /// - If a member's stored checkpoint is already at or past its target
    ///   block (backfill overshoot at the hist→live seam), that member's ops
    ///   and checkpoint update are skipped — re-applying them would
    ///   double-count non-idempotent custom-table aggregations.
    ///
    /// `members` must be in group spec order; ops replay in that order.
    /// An empty-ops member list still updates checkpoints (checkpoint-only
    /// flush for empty blocks/windows — lockstep cursor advance is the point).
    pub async fn flush_sync_together_block(
        &self,
        members: Vec<MemberBlockOps>,
        network: &str,
        group_cursor: u64,
        latest_block: u64,
    ) -> Result<SyncTogetherFlushOutcome, PostgresError> {
        if members.is_empty() {
            return Ok(SyncTogetherFlushOutcome::Committed { applied_members: Vec::new() });
        }

        let mut conn = self.raw_connection().await?;
        let transaction = conn.transaction().await?;

        // 1. Read every member's stored checkpoint inside the tx.
        let mut stored_checkpoints: Vec<Decimal> = Vec::with_capacity(members.len());
        for member in &members {
            let query = format!(
                "SELECT last_synced_block FROM rindexer_internal.{} WHERE network = $1",
                member.checkpoint.internal_table_name
            );
            let row = transaction.query_one(&query, &[&network]).await?;
            stored_checkpoints.push(row.get("last_synced_block"));
        }

        let group_cursor_decimal = Decimal::from(group_cursor);

        // 2. Reorg rewind detection: a rollback only moves checkpoints DOWN
        // (LEAST() guard), so any member whose EFFECTIVE position (stored
        // checkpoint floored at its manifest start - 1, see MemberCheckpoint)
        // is below the loop's last committed block means a rollback happened
        // since. Abort before writing. The floor keeps a never-synced member's
        // seeded 0 from reading as a rewind to block 0.
        let min_effective = members
            .iter()
            .zip(&stored_checkpoints)
            .map(|(member, stored)| member.checkpoint.effective_position(*stored))
            .min()
            .unwrap_or_default();
        if min_effective < group_cursor_decimal {
            transaction.rollback().await?;
            let rewind_to = u64::try_from(min_effective).unwrap_or(0);
            debug!(
                network,
                group_cursor, rewind_to, "sync_together flush detected checkpoint rewind (reorg)"
            );
            return Ok(SyncTogetherFlushOutcome::RewindTo(rewind_to));
        }

        // 3. Replay each member's ops, skipping members already checkpointed
        // at or past their target block (hist→live overshoot idempotency).
        let mut applied_members: Vec<bool> = Vec::with_capacity(members.len());
        for (member, stored) in members.iter().zip(&stored_checkpoints) {
            let target = Decimal::from(member.checkpoint.to_block);
            if *stored >= target {
                applied_members.push(false);
                if !member.ops.is_empty() {
                    debug!(
                        network,
                        table = %member.checkpoint.internal_table_name,
                        to_block = member.checkpoint.to_block,
                        "sync_together member already committed past block; skipping ops"
                    );
                }
                continue;
            }
            applied_members.push(true);

            for op in &member.ops {
                match op {
                    BufferedPgOp::Query { sql, params } => {
                        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                            .iter()
                            .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
                            .collect();
                        transaction.execute(sql.as_str(), &param_refs).await?;
                    }
                    BufferedPgOp::CopyIn { table_name, column_names, column_types, rows } => {
                        copy_in_via_transaction(
                            &transaction,
                            table_name,
                            column_names,
                            column_types,
                            rows,
                        )
                        .await
                        .map_err(|e| PostgresError::Custom(e.to_string()))?;
                    }
                    BufferedPgOp::BatchSql { sql } => {
                        transaction.batch_execute(sql).await?;
                    }
                }
            }
        }

        // 4. Advance checkpoints (monotonic guard) + the network's latest_block.
        for (member, stored) in members.iter().zip(&stored_checkpoints) {
            let target = Decimal::from(member.checkpoint.to_block);
            if *stored >= target {
                continue;
            }
            let query = format!(
                "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                member.checkpoint.internal_table_name
            );
            transaction.execute(&query, &[&target, &network]).await?;
        }

        if latest_block > 0 {
            let latest_decimal = Decimal::from(latest_block);
            transaction
                .execute(
                    "UPDATE rindexer_internal.latest_block SET block = $1 WHERE network = $2 AND $1 > block",
                    &[&latest_decimal, &network],
                )
                .await?;
        }

        transaction.commit().await?;

        Ok(SyncTogetherFlushOutcome::Committed { applied_members })
    }
}

/// Dispatches deferred side effects after a successful commit. Failures are
/// logged and swallowed — the block is already durable, and re-running the
/// block would duplicate its Postgres writes.
pub async fn dispatch_deferred_side_effects(effects: Vec<DeferredSideEffect>) {
    for effect in effects {
        // Each deferred future does its own error logging; this wrapper exists
        // so a panic in one effect doesn't take down the group loop.
        if let Err(panic) =
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(effect)).await
        {
            error!("sync_together deferred side effect panicked: {:?}", panic);
        }
    }
}
