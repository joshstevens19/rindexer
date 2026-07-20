//! `sync_together` lockstep live indexing.
//!
//! One loop per group per network drives all member events block-by-block:
//! for each block N, every member's callback runs with block-N logs (writes
//! buffered into the group's [`SyncTogetherSink`]), then the buffered ops and
//! all member checkpoints commit in ONE Postgres transaction via
//! [`PostgresClient::flush_sync_together_block`]. Readers never observe a
//! block partially applied across the group, and checkpoints can never drift
//! from data.
//!
//! Transactions are strictly per network — block heights are not comparable
//! across chains, so an ethereum commit never waits on an arbitrum commit,
//! even for `cross_chain: true` tables.
//!
//! Modeled on `live_indexing_for_contract_event_dependencies`
//! (`indexer/process.rs`), with these differences: single-block granularity
//! with an atomic flush; callbacks fired via `trigger_event_once` (no registry
//! retry — the loop owns retry at block granularity: any failure clears the
//! sink and refetches from the cursor); in-transaction checkpoint checks that
//! both detect reorg rewinds performed by other loops and skip members already
//! committed past a block (hist→live seam idempotency); and a coordinator
//! rollback-epoch check that invalidates an in-flight window when a rollback
//! interleaves between its per-block commits.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U64;
use alloy::rpc::types::Log;
use futures::future::join_all;
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::database::postgres::block_sink::{
    dispatch_deferred_side_effects, with_sync_sink, DeferredSideEffect, MemberBlockOps,
    MemberCheckpoint, SyncTogetherFlushOutcome, SyncTogetherSink,
};
use crate::event::callback_registry::EventResult;
use crate::event::config::EventProcessingConfig;
use crate::event::RindexerEventFilter;
use crate::indexer::heartbeat::{HeartbeatAction, HeartbeatTracker};
use crate::indexer::last_synced::{
    internal_event_checkpoint_table_name, update_in_memory_progress_and_metrics,
};
use crate::indexer::process::process_blocking_event_historical_data;
use crate::indexer::progress::IndexingEventProgressStatus;
use crate::indexer::reorg::{detect_and_handle_reorg, ReorgContext, ReorgCoordinator};
use crate::indexer::task_tracker::{indexing_event_processed, indexing_event_processing};
use crate::is_running;
use crate::metrics::indexing as metrics;
use crate::provider::ProviderError;
use crate::PostgresClient;

#[derive(thiserror::Error, Debug)]
pub enum ProcessSyncTogetherError {
    #[error("sync_together group '{0}': member fast-forward failed: {1}")]
    FastForwardFailed(String, Box<ProviderError>),

    #[error("sync_together group '{0}': could not build event filter: {1}")]
    BuildFilterError(String, String),

    #[error("sync_together group '{0}': postgres client missing — sync_together requires postgres storage")]
    PostgresMissing(String),

    #[error("sync_together group '{0}': could not read the factory discovery checkpoint: {1}")]
    FactoryClampFailed(String, String),

    #[error("Could not run sync_together group tasks: {0}")]
    JoinError(#[from] JoinError),
}

/// One group member (per network) with routing-time metadata the live-leg
/// `EventProcessingConfig` no longer carries.
pub struct SyncTogetherMember {
    pub config: Arc<EventProcessingConfig>,
    /// Rewind-detection floor: the MANIFEST-declared start_block for this
    /// member on this network, or — when the manifest omits one — this boot's
    /// checkpoint-aware resume point. With a manifest start the live-leg
    /// config's `start_block` (mutated by reapply_after_historic) is useless
    /// as the floor — see `MemberCheckpoint::manifest_start_block`.
    pub manifest_start_block: u64,
    /// For factory-DEPLOYED members: the `rindexer_internal` checkpoint table
    /// of the factory-discovery event (which runs on the ordinary eager
    /// pipeline). The group loop never processes a block past this checkpoint,
    /// so the member's `FactoryFilter` address set — resolved fresh on every
    /// window fetch — is complete for any block the group commits: a vault
    /// deployed at block N is discovered and committed by the factory pipeline
    /// before the group fetches block N.
    pub factory_checkpoint_table: Option<String>,
}

/// Runtime configuration for one `sync_together` group, collected during
/// routing in `start_indexing_contract_events` (live leg only).
pub struct SyncTogetherGroupConfig {
    pub group_name: String,
    /// One entry per member × network, sorted into group spec order (contract
    /// order × event order from the manifest).
    pub members: Vec<SyncTogetherMember>,
    /// The per-network shared reorg coordinators (the SAME instances every
    /// other live loop on the network uses).
    pub reorg_coordinators: HashMap<String, Arc<Mutex<ReorgCoordinator>>>,
}

/// Spawns one processing task per group and drives them to completion.
pub async fn process_sync_together_groups(
    groups: Vec<SyncTogetherGroupConfig>,
) -> Result<(), ProcessSyncTogetherError> {
    if groups.is_empty() {
        return Ok(());
    }

    let handles: Vec<JoinHandle<Result<(), ProcessSyncTogetherError>>> =
        groups.into_iter().map(|group| tokio::spawn(process_sync_together_group(group))).collect();

    for result in join_all(handles).await {
        result??;
    }

    Ok(())
}

async fn process_sync_together_group(
    group: SyncTogetherGroupConfig,
) -> Result<(), ProcessSyncTogetherError> {
    // One sink per GROUP (keyed by network internally): sharing a sink across
    // groups would let one group's flush drain or clear another's buffers.
    let sink = Arc::new(SyncTogetherSink::new());

    // Split members by network, preserving spec order within each network.
    let mut by_network: HashMap<String, Vec<Arc<SyncTogetherMember>>> = HashMap::new();
    for member in group.members {
        let network = member.config.network_contract().network.clone();
        by_network.entry(network).or_default().push(Arc::new(member));
    }

    let group_name = Arc::new(group.group_name);

    let handles: Vec<JoinHandle<Result<(), ProcessSyncTogetherError>>> = by_network
        .into_iter()
        .map(|(network, members)| {
            let coordinator = group.reorg_coordinators.get(&network).cloned();
            let sink = Arc::clone(&sink);
            let group_name = Arc::clone(&group_name);
            tokio::spawn(async move {
                sync_together_network_task(group_name, network, members, coordinator, sink).await
            })
        })
        .collect();

    for result in join_all(handles).await {
        result??;
    }

    Ok(())
}

/// Backfills stragglers to a common block (Phase A), then runs the lockstep
/// loop (Phase B) for one group on one network.
async fn sync_together_network_task(
    group_name: Arc<String>,
    network: String,
    members: Vec<Arc<SyncTogetherMember>>,
    reorg_coordinator: Option<Arc<Mutex<ReorgCoordinator>>>,
    sink: Arc<SyncTogetherSink>,
) -> Result<(), ProcessSyncTogetherError> {
    let postgres = members
        .first()
        .and_then(|m| m.config.postgres())
        .ok_or_else(|| ProcessSyncTogetherError::PostgresMissing((*group_name).clone()))?;

    info!(
        "sync_together '{}' on {}: members in lockstep order: {}",
        group_name,
        network,
        members
            .iter()
            .map(|m| format!("{}::{}", m.config.contract_name(), m.config.event_name()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // ---- Phase A: fast-forward stragglers to a common cursor ----
    //
    // Members resume from different checkpoints (config.start_block is each
    // member's first unindexed block; config.end_block is the reorg-safe head
    // snapshotted at startup). Run stragglers forward NON-atomically to
    //   target = min( max(member start_block) - 1, min(member end_block) )
    // using the ordinary historical pipeline — those blocks were already
    // committed non-atomically for the front-runner during backfill, so the
    // per-block guarantee only ever holds for blocks > target. Members ahead
    // of target are protected by the flush's skip-already-committed check.
    let max_resume = members.iter().map(|m| m.config.start_block()).max().unwrap_or_default();
    let safe_head_snapshot = members.iter().map(|m| m.config.end_block()).min().unwrap_or_default();
    let mut target = std::cmp::min(max_resume.saturating_sub(U64::from(1)), safe_head_snapshot);

    // Factory-deployed members: never process (even in Phase A) past the
    // factory-discovery checkpoint — blocks beyond it may contain events from
    // children the factory pipeline hasn't discovered yet, and a fetch with an
    // incomplete address set that advances the checkpoint is a permanent gap.
    // The lockstep loop re-reads this clamp every iteration, so the group
    // simply trails discovery (usually by nothing).
    let factory_clamp_sql = build_factory_clamp_sql(
        members.iter().filter_map(|m| m.factory_checkpoint_table.as_deref()).collect(),
    );
    if factory_clamp_sql.is_some() {
        let clamp = read_factory_clamp(
            &postgres,
            factory_clamp_sql.as_deref().expect("checked above"),
            &network,
        )
        .await
        .map_err(|e| {
            ProcessSyncTogetherError::FactoryClampFailed((*group_name).clone(), e.to_string())
        })?;
        target = std::cmp::min(target, U64::from(clamp));
    }

    let checkpoints_meta: Vec<(String, u64)> = members
        .iter()
        .map(|m| {
            (
                internal_event_checkpoint_table_name(
                    &m.config.indexer_name(),
                    &m.config.contract_name(),
                    &m.config.event_name(),
                ),
                m.manifest_start_block,
            )
        })
        .collect();

    // Epoch before any fast-forward write: the post-Phase-A checkpoint heal
    // below must know whether a reorg rollback interleaved with Phase A.
    let pre_fast_forward_epoch: u64 = match reorg_coordinator.as_ref() {
        Some(coordinator) => coordinator.lock().await.rollback_epoch(),
        None => 0,
    };

    let mut fast_forward_tasks = Vec::new();
    for member in &members {
        let from = member.config.start_block();
        if from > target {
            continue;
        }
        info!(
            "sync_together '{}' on {}: fast-forwarding {} from {} to {}",
            group_name,
            network,
            member.config.info_log_name(),
            from,
            target
        );
        // force_no_live_indexing inside: the historical pipeline writes
        // eagerly and checkpoints as it goes (crash-resumable), and never
        // attaches its own live loop.
        let range_config = Arc::new(member.config.clone_for_range(from, target, false));
        fast_forward_tasks
            .push(async move { process_blocking_event_historical_data(range_config).await });
    }

    for result in join_all(fast_forward_tasks).await {
        if let Err(e) = result {
            return Err(ProcessSyncTogetherError::FastForwardFailed((*group_name).clone(), e));
        }
    }

    // All members share one generation cancel token (hot reload). Also
    // checked before the checkpoint heal below: a cancelled generation is the
    // one path where the historical pipeline can return Ok without having
    // durably processed its whole range (a panicked fetch worker cancels the
    // shared token and the stream ends early) — healing checkpoints past
    // non-durable blocks would turn that partial progress into a permanent
    // gap, so bail out instead.
    let generation_cancel = members.first().map(|m| m.config.cancel_token().clone());

    if !is_running() || generation_cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
        return Ok(());
    }

    // Heal member checkpoints. `update_progress_and_last_synced_task` (used by
    // both the pre-group historical leg and Phase A) logs-and-continues when a
    // checkpoint UPDATE fails, so a member's stored checkpoint can sit below
    // `target` even though its data through `target` is durable. Left alone,
    // the first lockstep flush would misread that as a reorg rewind, RewindTo,
    // and replay blocks the member already committed — duplicate raw rows and
    // double-counted aggregations. Re-issue the monotonic checkpoint write for
    // every member whose manifest start_block is at or below `target`:
    // fast-forwarded members are durable through `target` (Phase A returned
    // Ok), and members that resumed above `target` are durable through
    // resume - 1 >= target (DB checkpoint or same-run historical handoff).
    // Members whose manifest start sits ABOVE `target` are excluded — they
    // have nothing durable below their start, the flush's effective-position
    // floor (start - 1) already shields them from false rewind detection, and
    // healing them would move their restart resume below their declared
    // start_block. Skipped entirely if a rollback ran since the pre-Phase-A
    // epoch snapshot (checked under the coordinator mutex so another rollback
    // can't interleave with the heal): then a low checkpoint may be a genuine
    // rewind whose rows were deleted, and the flush's RewindTo replay is the
    // correct way to re-index them.
    let heal_members: Vec<usize> = checkpoints_meta
        .iter()
        .enumerate()
        .filter(|(_, (_, manifest_start))| *manifest_start <= target.to::<u64>())
        .map(|(member_idx, _)| member_idx)
        .collect();

    while !heal_members.is_empty() {
        let guard = match reorg_coordinator.as_ref() {
            Some(coordinator) => Some(coordinator.lock().await),
            None => None,
        };
        if guard.as_ref().is_some_and(|g| g.rollback_epoch() != pre_fast_forward_epoch) {
            info!(
                "sync_together '{}' on {}: reorg rollback interleaved with fast-forward — skipping checkpoint heal, flush rewind detection will reprocess",
                group_name, network
            );
            break;
        }

        let target_decimal = Decimal::from(target.to::<u64>());
        let mut heal_error = None;
        for member_idx in &heal_members {
            let query = format!(
                "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                checkpoints_meta[*member_idx].0
            );
            if let Err(e) = postgres.execute(&query, &[&target_decimal, &network]).await {
                heal_error = Some(e);
                break;
            }
        }
        drop(guard);

        match heal_error {
            None => break,
            Some(e) => {
                error!(
                    "sync_together '{}' on {}: fast-forward checkpoint heal failed, retrying in 1s: {}",
                    group_name, network, e
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                if !is_running() || generation_cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
                    return Ok(());
                }
            }
        }
    }

    // ---- Phase B: lockstep live loop ----
    let mut filters: Vec<RindexerEventFilter> = Vec::with_capacity(members.len());
    for member in &members {
        filters.push(member.config.to_event_filter().map_err(|e| {
            ProcessSyncTogetherError::BuildFilterError((*group_name).clone(), e.to_string())
        })?);
    }

    let cached_provider = members
        .first()
        .map(|m| m.config.network_contract().cached_provider.clone())
        .expect("sync_together network task requires at least one member");

    let block_clock = members.first().map(|m| m.config.network_contract().block_clock.clone());

    let (pg_for_reorg, ch_for_reorg, event_registry) = members
        .first()
        .map(|m| (m.config.postgres(), m.config.clickhouse(), m.config.registry()))
        .expect("at least one member");

    // Congruence validation enforces one reorg distance across members; take
    // the max defensively.
    let reorg_distance =
        members.iter().map(|m| m.config.indexing_distance_from_head()).max().unwrap_or_default();

    let mut heartbeat = HeartbeatTracker::new(Duration::from_secs(300));
    let target_iteration_duration = Duration::from_millis(200);

    let mut cursor = target;
    // Fetch window cap; halved on fetch errors (providers reject over-large
    // responses), grown gently (+25%) only after a window that used the full
    // cap — doubling would jump straight back to the size that just failed
    // and oscillate on every other window.
    const MAX_WINDOW: u64 = 1000;
    let mut window_cap: u64 = MAX_WINDOW;

    info!("sync_together '{}' on {}: entering lockstep at block {}", group_name, network, cursor);

    loop {
        if !is_running() {
            break;
        }
        if generation_cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
            info!(
                "Hot-reload: generation cancelled, stopping sync_together '{}' on {}",
                group_name, network
            );
            break;
        }

        let iteration_start = Instant::now();

        // One shared tip for the whole group.
        let latest_block = match cached_provider.get_latest_block().await {
            Ok(Some(block)) => block,
            Ok(None) => {
                error!(
                    "sync_together '{}' on {}: empty latest block from provider, retrying in 200ms",
                    group_name, network
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            Err(e) => {
                error!(
                    "sync_together '{}' on {}: failed to get latest block, retrying in 1s: {}",
                    group_name, network, e
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // Reorg detection via the shared per-network coordinator. Also
        // snapshot the rollback epoch: if ANOTHER loop executes a rollback
        // between our per-block commits, the logs we fetched for this window
        // are from the old chain — the epoch check before each commit aborts
        // the window in that case.
        let mut window_epoch: u64 = 0;
        if let Some(coordinator) = reorg_coordinator.as_ref() {
            let log_prefix = format!("{} - {}", network, IndexingEventProgressStatus::live_log());
            let reorg_ctx = ReorgContext {
                postgres: pg_for_reorg.as_deref(),
                clickhouse: ch_for_reorg.as_ref(),
                registry: Some(&event_registry),
                trace_registry: None,
            };
            let mut guard = coordinator.lock().await;
            match detect_and_handle_reorg(
                &mut guard,
                latest_block.header.number,
                latest_block.header.hash,
                latest_block.header.parent_hash,
                &log_prefix,
                &reorg_ctx,
            )
            .await
            {
                Ok(Some(fork_point)) => {
                    // The coordinator already rolled back all tables
                    // network-wide and rewound checkpoints (LEAST-guarded).
                    sink.clear(&network);
                    cursor = std::cmp::min(cursor, U64::from(fork_point.saturating_sub(1)));
                    info!(
                        "sync_together '{}' on {}: reorg detected, cursor rewound to {}",
                        group_name, network, cursor
                    );
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    error!(
                        "sync_together '{}' on {}: reorg handling failed, pausing before retry: {:?}",
                        group_name, network, e
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
            window_epoch = guard.rollback_epoch();
        }

        let latest_block_number = U64::from(latest_block.header.number);

        match heartbeat.tick(latest_block_number) {
            HeartbeatAction::Silent => {}
            HeartbeatAction::Alive => {
                info!(
                    "sync_together '{}' on {} - {} - lockstep alive - cursor {} chain tip {}",
                    group_name,
                    network,
                    IndexingEventProgressStatus::live_log(),
                    cursor,
                    latest_block_number
                );
            }
            HeartbeatAction::Stalled => {
                warn!(
                    "sync_together '{}' on {} - RPC tip has not advanced past block {} in the last 5 minutes",
                    group_name, network, latest_block_number
                );
            }
        }

        let safe_head = latest_block_number.saturating_sub(reorg_distance);

        // Factory-deployed members: cap the window at the factory-discovery
        // checkpoint (re-read every iteration — the eager factory pipeline
        // advances it continuously) so member address sets are complete for
        // every block this window can commit.
        let mut window_limit = safe_head;
        if let Some(sql) = factory_clamp_sql.as_deref() {
            match read_factory_clamp(&postgres, sql, &network).await {
                Ok(clamp) => window_limit = std::cmp::min(window_limit, U64::from(clamp)),
                Err(e) => {
                    warn!(
                        "sync_together '{}' on {}: could not read factory discovery checkpoint, retrying: {}",
                        group_name, network, e
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }
        }

        if cursor >= window_limit {
            let elapsed = iteration_start.elapsed();
            if elapsed < target_iteration_duration {
                tokio::time::sleep(target_iteration_duration - elapsed).await;
            }
            continue;
        }

        let from = cursor + U64::from(1);
        let to = std::cmp::min(window_limit, cursor + U64::from(window_cap));
        let catching_up = to < window_limit;

        // Fetch ALL members' logs for the window before processing anything —
        // never process a partial view of the window. Timestamp enrichment
        // matches the ungrouped stream path (`fetch_logs`): attach via the
        // network's block clock when the member has timestamps enabled, and
        // treat enrichment failure as a fetch failure (retry the window).
        let mut member_logs: Vec<Vec<Log>> = Vec::with_capacity(members.len());
        let mut fetch_failed = false;
        for (member, filter) in members.iter().zip(&filters) {
            let window_filter = filter.clone().set_from_block(from).set_to_block(to);
            match cached_provider.get_logs(&window_filter).await {
                Ok(logs) => {
                    let logs: Vec<Log> = logs.into_iter().filter(|l| !l.removed).collect();
                    let logs = if member.config.timestamps() {
                        match block_clock.as_ref() {
                            Some(clock) => match clock.attach_log_timestamps(logs).await {
                                Ok(logs) => logs,
                                Err(e) => {
                                    error!(
                                        "sync_together '{}' on {}: timestamp enrichment failed for {}: {} — retrying window",
                                        group_name,
                                        network,
                                        member.config.info_log_name(),
                                        e
                                    );
                                    fetch_failed = true;
                                    break;
                                }
                            },
                            None => logs,
                        }
                    } else {
                        logs
                    };
                    member_logs.push(logs);
                }
                Err(e) => {
                    error!(
                        "sync_together '{}' on {}: log fetch failed for {} ({} - {}): {} — shrinking window and retrying",
                        group_name,
                        network,
                        member.config.info_log_name(),
                        from,
                        to,
                        e
                    );
                    fetch_failed = true;
                    break;
                }
            }
        }

        if fetch_failed {
            window_cap = (window_cap / 2).max(1);
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let fetched_span = (to - from + U64::from(1)).to::<u64>();
        if fetched_span == window_cap {
            window_cap = (window_cap + (window_cap / 4).max(1)).min(MAX_WINDOW);
        }

        let Some(buckets) = plan_block_buckets(member_logs, from, to) else {
            error!(
                "sync_together '{}' on {}: provider returned logs without block numbers or outside {} - {}, retrying window",
                group_name, network, from, to
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };

        // Make shutdown wait for in-flight block commits (the group loop
        // bypasses the registry path that normally maintains this counter).
        indexing_event_processing();

        let mut window_failed = false;

        'blocks: for (block_number, mut per_member_logs) in buckets {
            if !is_running() || generation_cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
                window_failed = true;
                break 'blocks;
            }

            let block = U64::from(block_number);
            let mut member_ops: Vec<MemberBlockOps> = Vec::with_capacity(members.len());
            // Per member, so B4-skipped members' effects can be dropped.
            let mut member_side_effects: Vec<Vec<DeferredSideEffect>> =
                Vec::with_capacity(members.len());
            let mut member_event_counts: Vec<u64> = Vec::with_capacity(members.len());

            for (member_idx, member) in members.iter().enumerate() {
                let logs = per_member_logs.remove(&member_idx).unwrap_or_default();
                let event_count = logs.len() as u64;

                if !logs.is_empty() {
                    let fn_data: Vec<EventResult> = logs
                        .into_iter()
                        .map(|log| {
                            EventResult::new(member.config.network_contract(), log, block, block)
                        })
                        .collect();

                    // Exactly-once invocation: the registry retry wrapper
                    // would double-buffer a partially-buffered member.
                    let callback_result = with_sync_sink(
                        Arc::clone(&sink),
                        member
                            .config
                            .registry()
                            .trigger_event_once(&member.config.id().to_string(), fn_data),
                    )
                    .await;

                    if let Err(e) = callback_result {
                        error!(
                            "sync_together '{}' on {}: {} callback failed at block {}: {} — retrying block",
                            group_name,
                            network,
                            member.config.info_log_name(),
                            block_number,
                            e
                        );
                        sink.clear(&network);
                        window_failed = true;
                        break 'blocks;
                    }
                }

                // Drain this member's buffered writes (callbacks run
                // sequentially, so everything since the last take belongs to
                // this member).
                let (ops, effects) = sink.take(&network);
                member_side_effects.push(effects);
                member_event_counts.push(event_count);
                member_ops.push(MemberBlockOps {
                    checkpoint: MemberCheckpoint {
                        internal_table_name: checkpoints_meta[member_idx].0.clone(),
                        to_block: block_number,
                        manifest_start_block: checkpoints_meta[member_idx].1,
                    },
                    ops,
                });
            }

            // Commit the block: hold the coordinator mutex for the duration
            // so a reorg rollback cannot interleave with the transaction
            // (mutex acquired BEFORE the pool connection — codebase order).
            // Released between retries so a failing DB can't pin the
            // network's reorg-detection path. The epoch check catches
            // rollbacks that ran between our commits at a fork ABOVE the
            // cursor (which leave checkpoints untouched but invalidate this
            // window's fetched logs).
            let flush_result = match reorg_coordinator.as_ref() {
                Some(coordinator) => {
                    let guard = coordinator.lock().await;
                    if guard.rollback_epoch() != window_epoch {
                        warn!(
                            "sync_together '{}' on {}: reorg rollback interleaved mid-window, dropping window at block {}",
                            group_name, network, block_number
                        );
                        window_failed = true;
                        break 'blocks;
                    }
                    flush_block(&postgres, member_ops, &network, cursor, latest_block_number).await
                }
                None => {
                    flush_block(&postgres, member_ops, &network, cursor, latest_block_number).await
                }
            };

            match flush_result {
                Ok(SyncTogetherFlushOutcome::Committed { applied_members }) => {
                    cursor = block;
                    let mut effects_to_dispatch = Vec::new();
                    for (member_idx, effects) in member_side_effects.into_iter().enumerate() {
                        let applied = applied_members.get(member_idx).copied().unwrap_or(true);
                        if applied {
                            effects_to_dispatch.extend(effects);
                            let count = member_event_counts[member_idx];
                            if count > 0 {
                                let member = &members[member_idx];
                                metrics::record_events_indexed(
                                    &network,
                                    &member.config.contract_name(),
                                    &member.config.event_name(),
                                    count,
                                    block_number,
                                    None,
                                );
                            }
                        }
                        // Skipped members' effects are dropped. At the
                        // hist→live seam they already fired during eager
                        // backfill (dropping prevents double publishes); on a
                        // retry of a block whose earlier flush committed
                        // server-side but lost the COMMIT ack, they are lost —
                        // the documented at-most-once behavior, same fault
                        // class as crashing between commit and dispatch.
                    }
                    dispatch_deferred_side_effects(effects_to_dispatch).await;
                }
                Ok(SyncTogetherFlushOutcome::RewindTo(rewind_block)) => {
                    // Another loop's reorg rollback rewound our checkpoints
                    // while this window was in flight. Nothing was written.
                    warn!(
                        "sync_together '{}' on {}: checkpoint rewind detected mid-window, cursor {} -> {}",
                        group_name, network, cursor, rewind_block
                    );
                    cursor = std::cmp::min(cursor, U64::from(rewind_block));
                    window_failed = true;
                    break 'blocks;
                }
                Err(e) => {
                    error!(
                        "sync_together '{}' on {}: block {} flush failed: {} — retrying block",
                        group_name, network, block_number, e
                    );
                    window_failed = true;
                    break 'blocks;
                }
            }
        }

        // Tail: advance all member checkpoints together through the empty
        // remainder of the window (also runs on fully-empty windows). One
        // atomic checkpoint-only tx keeps the "checkpoints never drift"
        // invariant even across crashes.
        if !window_failed && cursor < to {
            let checkpoint_only: Vec<MemberBlockOps> = checkpoints_meta
                .iter()
                .map(|(table, manifest_start)| MemberBlockOps {
                    checkpoint: MemberCheckpoint {
                        internal_table_name: table.clone(),
                        to_block: to.to::<u64>(),
                        manifest_start_block: *manifest_start,
                    },
                    ops: Vec::new(),
                })
                .collect();

            let flush_result = match reorg_coordinator.as_ref() {
                Some(coordinator) => {
                    let guard = coordinator.lock().await;
                    if guard.rollback_epoch() != window_epoch {
                        warn!(
                            "sync_together '{}' on {}: reorg rollback interleaved before tail checkpoint, dropping window",
                            group_name, network
                        );
                        window_failed = true;
                        Ok(SyncTogetherFlushOutcome::RewindTo(cursor.to::<u64>()))
                    } else {
                        flush_block(
                            &postgres,
                            checkpoint_only,
                            &network,
                            cursor,
                            latest_block_number,
                        )
                        .await
                    }
                }
                None => {
                    flush_block(&postgres, checkpoint_only, &network, cursor, latest_block_number)
                        .await
                }
            };

            match flush_result {
                Ok(SyncTogetherFlushOutcome::Committed { .. }) => {
                    cursor = to;
                }
                Ok(SyncTogetherFlushOutcome::RewindTo(rewind_block)) => {
                    if U64::from(rewind_block) < cursor {
                        warn!(
                            "sync_together '{}' on {}: checkpoint rewind detected at window tail, cursor {} -> {}",
                            group_name, network, cursor, rewind_block
                        );
                        cursor = U64::from(rewind_block);
                    }
                }
                Err(e) => {
                    error!(
                        "sync_together '{}' on {}: checkpoint flush failed: {} — retrying window",
                        group_name, network, e
                    );
                }
            }
        }

        indexing_event_processed();

        // Refresh in-memory progress + metrics once per window (all members
        // sit at the same cursor by construction).
        if cursor >= from {
            for member in &members {
                update_in_memory_progress_and_metrics(
                    &member.config,
                    cursor,
                    latest_block_number.to::<u64>(),
                )
                .await;
            }
        }

        // Skip the throttle while catching up below the safe head.
        if !catching_up || window_failed {
            let elapsed = iteration_start.elapsed();
            if elapsed < target_iteration_duration {
                tokio::time::sleep(target_iteration_duration - elapsed).await;
            }
        }
    }

    Ok(())
}

async fn flush_block(
    postgres: &Arc<PostgresClient>,
    members: Vec<MemberBlockOps>,
    network: &str,
    cursor: U64,
    latest_block: U64,
) -> Result<SyncTogetherFlushOutcome, crate::database::postgres::client::PostgresError> {
    postgres
        .flush_sync_together_block(members, network, cursor.to::<u64>(), latest_block.to::<u64>())
        .await
}

/// Builds the query reading the LOWEST factory-discovery checkpoint across the
/// group's factory-deployed members on one network, or `None` when the group
/// has no factory-deployed members (no clamp needed).
fn build_factory_clamp_sql(mut tables: Vec<&str>) -> Option<String> {
    tables.sort_unstable();
    tables.dedup();

    if tables.is_empty() {
        return None;
    }

    let selects = tables
        .iter()
        .map(|table| {
            format!(
                "SELECT last_synced_block AS cp FROM rindexer_internal.{table} WHERE network = $1"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    Some(format!("SELECT COALESCE(MIN(cp), 0) AS clamp FROM ({selects}) AS factory_checkpoints"))
}

async fn read_factory_clamp(
    postgres: &Arc<PostgresClient>,
    sql: &str,
    network: &str,
) -> Result<u64, crate::database::postgres::client::PostgresError> {
    let row = postgres.query_one(sql, &[&network]).await?;
    let clamp: Decimal = row.get("clamp");
    Ok(u64::try_from(clamp).unwrap_or(0))
}

/// Buckets each member's window logs by block number, ascending.
///
/// Returns `None` if any log is missing a block number or falls outside
/// `[from, to]` — either means the fetched view of the window can't be
/// trusted, so the caller drops the whole window and refetches.
#[allow(clippy::type_complexity)]
fn plan_block_buckets(
    member_logs: Vec<Vec<Log>>,
    from: U64,
    to: U64,
) -> Option<BTreeMap<u64, HashMap<usize, Vec<Log>>>> {
    let mut buckets: BTreeMap<u64, HashMap<usize, Vec<Log>>> = BTreeMap::new();

    for (member_idx, logs) in member_logs.into_iter().enumerate() {
        for log in logs {
            let block_number = log.block_number?;
            if U64::from(block_number) < from || U64::from(block_number) > to {
                return None;
            }
            buckets.entry(block_number).or_default().entry(member_idx).or_default().push(log);
        }
    }

    Some(buckets)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_at(block: u64) -> Log {
        Log { block_number: Some(block), ..Default::default() }
    }

    #[test]
    fn buckets_by_block_ascending_with_member_attribution() {
        let member_a = vec![log_at(10), log_at(12)];
        let member_b = vec![log_at(12), log_at(11)];

        let buckets =
            plan_block_buckets(vec![member_a, member_b], U64::from(10), U64::from(20)).unwrap();

        let blocks: Vec<u64> = buckets.keys().copied().collect();
        assert_eq!(blocks, vec![10, 11, 12]);

        assert_eq!(buckets[&10].get(&0).map(Vec::len), Some(1));
        assert!(!buckets[&10].contains_key(&1));
        assert_eq!(buckets[&11].get(&1).map(Vec::len), Some(1));
        assert_eq!(buckets[&12].get(&0).map(Vec::len), Some(1));
        assert_eq!(buckets[&12].get(&1).map(Vec::len), Some(1));
    }

    #[test]
    fn rejects_logs_missing_block_number() {
        let log = Log::default(); // no block number
        assert!(plan_block_buckets(vec![vec![log]], U64::from(1), U64::from(2)).is_none());
    }

    #[test]
    fn rejects_logs_outside_window() {
        assert!(plan_block_buckets(vec![vec![log_at(30)]], U64::from(10), U64::from(20)).is_none());
    }

    #[test]
    fn empty_members_produce_empty_buckets() {
        let buckets = plan_block_buckets(vec![vec![], vec![]], U64::from(1), U64::from(5)).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn factory_clamp_sql_none_without_factory_members() {
        assert!(build_factory_clamp_sql(vec![]).is_none());
    }

    #[test]
    fn factory_clamp_sql_dedups_and_unions_tables() {
        let sql = build_factory_clamp_sql(vec![
            "idx_factory_b_created",
            "idx_factory_a_created",
            "idx_factory_a_created",
        ])
        .expect("sql");

        assert_eq!(sql.matches("SELECT last_synced_block").count(), 2, "deduped: {sql}");
        assert!(sql.contains("MIN(cp)"), "takes the LOWEST checkpoint: {sql}");
        assert!(
            sql.contains("rindexer_internal.idx_factory_a_created")
                && sql.contains("rindexer_internal.idx_factory_b_created"),
            "both factories included: {sql}"
        );
        assert!(sql.contains("WHERE network = $1"), "network scoped: {sql}");
    }
}
