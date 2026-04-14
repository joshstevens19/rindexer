use std::sync::Arc;

use alloy::primitives::{B256, U64};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::manifest::contract::SetAction;
use crate::metrics::indexing as metrics;
use crate::provider::JsonRpcCachedProvider;

use super::persistence::ReorgBlockHashPersistence;
use super::window::BlockChainWindow;

#[derive(Clone)]
pub struct EventTableInfo {
    pub schema: String,
    pub table_name: String,
    /// Full table path: schema.table_name
    pub full_name: String,
    /// Checkpoint table name in rindexer_internal (without schema prefix)
    pub checkpoint_table: String,
}

impl EventTableInfo {
    pub fn new(schema: String, table_name: String, checkpoint_table: String) -> Self {
        let full_name = format!("{}.{}", schema, table_name);
        Self { schema, table_name, full_name, checkpoint_table }
    }
}

/// Describes how to reverse one column's accumulation during reorg.
#[derive(Clone, Debug)]
pub struct DerivedColumnRollback {
    /// Column in the derived table (e.g., "balance")
    pub derived_column: String,
    /// Column in the source event table (e.g., "value")
    pub event_column: String,
    /// The forward action that was applied (Add, Subtract, Increment, Decrement)
    pub action: SetAction,
}

/// Links a source event table to the derived table for reversal.
#[derive(Clone, Debug)]
pub struct DerivedTableRollbackOp {
    /// Source event table (e.g., "myindexer_mycontract.transfer")
    pub event_table: String,
    /// WHERE clause: (derived_table_col, event_table_col) pairs
    pub where_columns: Vec<(String, String)>,
    /// Columns to reverse
    pub columns: Vec<DerivedColumnRollback>,
    /// Optional SQL condition re-evaluated against event data
    pub condition: Option<String>,
}

/// Describes a non-reversible column (Set/Max/Min) that uses the operation journal
/// for recalculation during reorg.
#[derive(Clone, Debug)]
pub struct DerivedColumnJournal {
    /// Column in the derived table (e.g., "max_trade")
    pub derived_column: String,
    /// The action: Set, Max, or Min — determines the recalculation aggregate
    pub action: SetAction,
    /// WHERE clause columns in the derived table (for matching journal where_key)
    pub where_columns: Vec<String>,
}

/// Metadata for a derived/custom table needed during reorg rollback.
#[derive(Clone)]
pub struct DerivedTableInfo {
    pub full_table_name: String,
    pub cross_chain: bool,
    /// Reversible operations (Add/Subtract/Increment/Decrement) — snapshot + reverse.
    pub rollback_ops: Vec<DerivedTableRollbackOp>,
    /// Non-reversible columns (Set/Max/Min) — recalculated from operation journal.
    pub journal_columns: Vec<DerivedColumnJournal>,
}

pub struct ReorgTask {
    pub network: String,
    pub fork_point: u64,
    pub detection_point: u64,
    pub event_tables: Vec<EventTableInfo>,
    pub derived_tables: Vec<DerivedTableInfo>,
    /// Pre-fetched canonical blocks `(block_number, block_hash, parent_hash)` from
    /// `find_fork_point`, so `execute()` can skip a redundant RPC round-trip.
    pub canonical_blocks: Vec<(u64, B256, B256)>,
}

pub struct ReorgTaskResult {
    pub events_deleted: u64,
    pub duration_secs: f64,
    pub affected_tx_hashes: Vec<String>,
}

#[derive(Clone, Copy, PartialEq)]
enum SnapshotBackend {
    Postgres,
    Clickhouse,
}

/// A snapshot temp table name and the info needed to apply the reversal later.
struct ReversalSnapshot {
    backend: SnapshotBackend,
    temp_table: String,
    derived_table: String,
    cross_chain: bool,
    network: String,
    where_columns: Vec<(String, String)>,
    set_clauses: Vec<String>,
}

impl ReorgTask {
    /// Phase 1: Before event deletion, snapshot aggregated event data into temp tables.
    /// Returns the snapshots needed for phase 2.
    async fn snapshot_for_reversal(
        &self,
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> Vec<ReversalSnapshot> {
        let mut snapshots = Vec::new();
        let mut snap_idx = 0usize;

        for dt in &self.derived_tables {
            for op in &dt.rollback_ops {
                let mut agg_columns: Vec<String> = Vec::new();
                let mut set_clauses: Vec<String> = Vec::new();

                for col in &op.columns {
                    let Some(reversed) = col.action.reverse() else {
                        tracing::warn!(
                            table = %dt.full_table_name,
                            column = %col.derived_column,
                            action = ?col.action,
                            "Non-reversible action — skipping column during reorg rollback"
                        );
                        continue;
                    };

                    let agg_expr = if col.action.is_counter_action() {
                        format!("COUNT(*) AS {}_agg", col.event_column)
                    } else {
                        format!("SUM({}::NUMERIC) AS {}_agg", col.event_column, col.event_column)
                    };
                    agg_columns.push(agg_expr);

                    let op_symbol = match reversed {
                        SetAction::Add | SetAction::Increment => "+",
                        SetAction::Subtract | SetAction::Decrement => "-",
                        _ => unreachable!(),
                    };
                    set_clauses.push(format!(
                        "{} = dt.{} {} snap.{}_agg",
                        col.derived_column, col.derived_column, op_symbol, col.event_column
                    ));
                }

                if set_clauses.is_empty() || agg_columns.is_empty() {
                    continue;
                }

                let group_cols: Vec<&str> =
                    op.where_columns.iter().map(|(_, ev_col)| ev_col.as_str()).collect();

                let network_filter = if dt.cross_chain {
                    String::new()
                } else {
                    format!(" AND network = '{}'", self.network)
                };

                let condition_filter = match &op.condition {
                    Some(cond) => format!(" AND ({})", cond),
                    None => String::new(),
                };

                let temp_name = format!("_rindexer_reorg_snap_{}", snap_idx);
                snap_idx += 1;

                let create_sql = format!(
                    "CREATE TEMP TABLE {} AS SELECT {}, {} FROM {} WHERE block_number >= {} AND block_number <= {}{}{} GROUP BY {}",
                    temp_name,
                    group_cols.join(", "),
                    agg_columns.join(", "),
                    op.event_table,
                    self.fork_point,
                    self.detection_point,
                    network_filter,
                    condition_filter,
                    group_cols.join(", "),
                );

                if let Some(pg) = pg {
                    let pg_temp = format!("{}_pg", temp_name);
                    match pg.batch_execute(&create_sql.replace(&temp_name, &pg_temp)).await {
                        Ok(_) => {
                            tracing::debug!(temp_table = %pg_temp, "Created PG reorg reversal snapshot");
                            snapshots.push(ReversalSnapshot {
                                backend: SnapshotBackend::Postgres,
                                temp_table: pg_temp,
                                derived_table: dt.full_table_name.clone(),
                                cross_chain: dt.cross_chain,
                                network: self.network.clone(),
                                where_columns: op.where_columns.clone(),
                                set_clauses: set_clauses.clone(),
                            });
                        }
                        Err(e) => tracing::error!(
                            table = %dt.full_table_name,
                            "Failed to create PG reorg reversal snapshot: {:?}", e
                        ),
                    }
                }

                if let Some(ch) = ch {
                    let ch_temp = format!("rindexer_internal.{}_ch", temp_name);
                    // ClickHouse: CREATE TABLE ... ENGINE = Memory AS SELECT ...
                    let ch_create = format!(
                        "CREATE TABLE IF NOT EXISTS {} ENGINE = Memory AS {}",
                        ch_temp,
                        create_sql
                            .trim_start_matches(&format!("CREATE TEMP TABLE {} AS ", temp_name)),
                    );
                    match ch.execute(&ch_create).await {
                        Ok(_) => {
                            tracing::debug!(temp_table = %ch_temp, "Created CH reorg reversal snapshot");
                            snapshots.push(ReversalSnapshot {
                                backend: SnapshotBackend::Clickhouse,
                                temp_table: ch_temp,
                                derived_table: dt.full_table_name.clone(),
                                cross_chain: dt.cross_chain,
                                network: self.network.clone(),
                                where_columns: op.where_columns.clone(),
                                set_clauses,
                            });
                        }
                        Err(e) => tracing::error!(
                            table = %dt.full_table_name,
                            "Failed to create CH reorg reversal snapshot: {:?}", e
                        ),
                    }
                }
            }
        }

        snapshots
    }

    /// Phase 2: After event deletion, apply reverse UPDATEs from snapshots and drop temp tables.
    async fn apply_reversal_from_snapshots(
        snapshots: &[ReversalSnapshot],
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) {
        for snap in snapshots {
            let where_join: Vec<String> = snap
                .where_columns
                .iter()
                .map(|(dt_col, ev_col)| format!("dt.{} = snap.{}", dt_col, ev_col))
                .collect();

            let network_join = if snap.cross_chain {
                String::new()
            } else {
                format!(" AND dt.network = '{}'", snap.network)
            };

            match snap.backend {
                SnapshotBackend::Postgres => {
                    let Some(pg) = pg else { continue };
                    let update_sql = format!(
                        "UPDATE {} AS dt SET {} FROM {} AS snap WHERE {}{}",
                        snap.derived_table,
                        snap.set_clauses.join(", "),
                        snap.temp_table,
                        where_join.join(" AND "),
                        network_join,
                    );
                    match pg.batch_execute(&update_sql).await {
                        Ok(_) => tracing::info!(
                            table = %snap.derived_table,
                            "PostgreSQL: reversed accumulative ops"
                        ),
                        Err(e) => tracing::error!(
                            table = %snap.derived_table,
                            "PostgreSQL: failed to reverse accumulative ops: {:?}", e
                        ),
                    }
                    let _ = pg
                        .batch_execute(&format!("DROP TABLE IF EXISTS {}", snap.temp_table))
                        .await;
                }
                SnapshotBackend::Clickhouse => {
                    let Some(ch) = ch else { continue };

                    // ClickHouse ALTER TABLE ... UPDATE with scalar subqueries from snapshot.
                    let ch_where_join: Vec<String> = snap
                        .where_columns
                        .iter()
                        .map(|(dt_col, ev_col)| {
                            format!("{}.{} = {}", snap.temp_table, ev_col, dt_col)
                        })
                        .collect();
                    let ch_join_predicate = ch_where_join.join(" AND ");

                    // Build CH set clauses by replacing known "snap.X_agg" tokens with scalar subqueries
                    let ch_set_clauses: Vec<String> = snap
                        .set_clauses
                        .iter()
                        .map(|clause| {
                            let result = clause.replace("dt.", "");
                            let snap_prefix = "snap.";
                            let mut offset = 0;
                            let mut new_result = String::with_capacity(result.len());
                            while let Some(pos) = result[offset..].find(snap_prefix) {
                                let abs_pos = offset + pos;
                                new_result.push_str(&result[offset..abs_pos]);
                                let rest = &result[abs_pos + snap_prefix.len()..];
                                let end = rest
                                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                                    .unwrap_or(rest.len());
                                let col_name = &rest[..end];
                                new_result.push_str(&format!(
                                    "(SELECT {} FROM {} WHERE {} LIMIT 1)",
                                    col_name, snap.temp_table, ch_join_predicate
                                ));
                                offset = abs_pos + snap_prefix.len() + end;
                            }
                            new_result.push_str(&result[offset..]);
                            new_result
                        })
                        .collect();

                    let ch_network = if snap.cross_chain {
                        "1 = 1".to_string()
                    } else {
                        format!("network = '{}'", snap.network)
                    };

                    // Only update rows that have a matching entry in the snapshot
                    let ch_exists_filter: Vec<String> = snap
                        .where_columns
                        .iter()
                        .map(|(dt_col, ev_col)| {
                            format!("{} IN (SELECT {} FROM {})", dt_col, ev_col, snap.temp_table)
                        })
                        .collect();
                    let ch_scope = if ch_exists_filter.is_empty() {
                        ch_network.clone()
                    } else {
                        format!("{} AND {}", ch_network, ch_exists_filter.join(" AND "))
                    };

                    let ch_update = format!(
                        "ALTER TABLE {} UPDATE {} WHERE {} SETTINGS mutations_sync = 1",
                        snap.derived_table,
                        ch_set_clauses.join(", "),
                        ch_scope,
                    );

                    match ch.execute(&ch_update).await {
                        Ok(_) => tracing::info!(
                            table = %snap.derived_table,
                            "ClickHouse: reversed accumulative ops"
                        ),
                        Err(e) => tracing::error!(
                            table = %snap.derived_table,
                            "ClickHouse: failed to reverse accumulative ops: {:?}", e
                        ),
                    }

                    let _ = ch.execute(&format!("DROP TABLE IF EXISTS {}", snap.temp_table)).await;
                }
            }
        }
    }

    /// Recalculate non-reversible columns (Set/Max/Min) from the operation journal.
    /// Deletes journal entries in the reorg range, then recalculates from remaining entries.
    async fn recalculate_from_journal(
        &self,
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) {
        for dt in &self.derived_tables {
            if dt.journal_columns.is_empty() {
                continue;
            }

            let network_filter = if dt.cross_chain {
                String::new()
            } else {
                format!(" AND network = '{}'", self.network)
            };

            // Delete journal entries in the reorg range
            let pg_delete = format!(
                "DELETE FROM rindexer_internal.derived_op_log \
                 WHERE derived_table = '{}' AND block_number >= {}{}",
                dt.full_table_name, self.fork_point, network_filter,
            );
            let ch_delete = format!(
                "ALTER TABLE rindexer_internal.derived_op_log DELETE \
                 WHERE derived_table = '{}' AND block_number >= {}{} SETTINGS mutations_sync = 1",
                dt.full_table_name, self.fork_point, network_filter,
            );

            if let Some(pg) = pg {
                if let Err(e) = pg.batch_execute(&pg_delete).await {
                    tracing::error!(
                        table = %dt.full_table_name,
                        "PG: failed to delete journal entries for reorg range: {:?}", e
                    );
                }
            }
            if let Some(ch) = ch {
                if let Err(e) = ch.execute(&ch_delete).await {
                    tracing::error!(
                        table = %dt.full_table_name,
                        "CH: failed to delete journal entries for reorg range: {:?}", e
                    );
                }
            }

            // Recalculate each non-reversible column from remaining journal entries
            for jc in &dt.journal_columns {
                let network_join = if dt.cross_chain {
                    String::new()
                } else {
                    format!(" AND dt.network = '{}'", self.network)
                };

                // --- Postgres recalculation ---
                if let Some(pg) = pg {
                    let update_sql = if matches!(jc.action, SetAction::Set) {
                        format!(
                            "UPDATE {} AS dt SET {} = sub.value \
                             FROM ( \
                                 SELECT DISTINCT ON (where_key) where_key, value \
                                 FROM rindexer_internal.derived_op_log \
                                 WHERE derived_table = '{}' AND column_name = '{}'{} \
                                 ORDER BY where_key, block_number DESC, tx_index DESC, log_index DESC \
                             ) sub \
                             WHERE {} {}",
                            dt.full_table_name, jc.derived_column,
                            dt.full_table_name, jc.derived_column, network_filter,
                            Self::journal_where_key_join(&jc.where_columns, "dt", "sub"),
                            network_join,
                        )
                    } else {
                        let agg_fn = match jc.action {
                            SetAction::Max => "MAX(value)",
                            SetAction::Min => "MIN(value)",
                            _ => continue,
                        };
                        format!(
                            "UPDATE {} AS dt SET {} = sub.recalc \
                             FROM ( \
                                 SELECT where_key, {}::NUMERIC AS recalc \
                                 FROM rindexer_internal.derived_op_log \
                                 WHERE derived_table = '{}' AND column_name = '{}'{} \
                                 GROUP BY where_key \
                             ) sub \
                             WHERE {} {}",
                            dt.full_table_name,
                            jc.derived_column,
                            agg_fn,
                            dt.full_table_name,
                            jc.derived_column,
                            network_filter,
                            Self::journal_where_key_join(&jc.where_columns, "dt", "sub"),
                            network_join,
                        )
                    };

                    match pg.batch_execute(&update_sql).await {
                        Ok(_) => tracing::info!(
                            table = %dt.full_table_name,
                            column = %jc.derived_column,
                            "PG: recalculated non-reversible column from journal"
                        ),
                        Err(e) => tracing::error!(
                            table = %dt.full_table_name,
                            column = %jc.derived_column,
                            "PG: failed to recalculate from journal: {:?}", e
                        ),
                    }
                }

                // --- ClickHouse recalculation ---
                if let Some(ch) = ch {
                    let ch_network = if dt.cross_chain {
                        "1 = 1".to_string()
                    } else {
                        format!("network = '{}'", self.network)
                    };

                    let ch_where_key_expr = Self::journal_where_key_concat_ch(&jc.where_columns);

                    let ch_subquery = if matches!(jc.action, SetAction::Set) {
                        format!(
                            "(SELECT value FROM rindexer_internal.derived_op_log \
                             WHERE derived_table = '{}' AND column_name = '{}' \
                             AND where_key = {} {} \
                             ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT 1)",
                            dt.full_table_name,
                            jc.derived_column,
                            ch_where_key_expr,
                            network_filter,
                        )
                    } else {
                        let agg_fn = match jc.action {
                            SetAction::Max => "max(value)",
                            SetAction::Min => "min(value)",
                            _ => continue,
                        };
                        format!(
                            "(SELECT {} FROM rindexer_internal.derived_op_log \
                             WHERE derived_table = '{}' AND column_name = '{}' \
                             AND where_key = {}{})",
                            agg_fn,
                            dt.full_table_name,
                            jc.derived_column,
                            ch_where_key_expr,
                            network_filter,
                        )
                    };

                    let ch_update = format!(
                        "ALTER TABLE {} UPDATE {} = {} WHERE {} SETTINGS mutations_sync = 1",
                        dt.full_table_name, jc.derived_column, ch_subquery, ch_network,
                    );

                    match ch.execute(&ch_update).await {
                        Ok(_) => tracing::info!(
                            table = %dt.full_table_name,
                            column = %jc.derived_column,
                            "CH: recalculated non-reversible column from journal"
                        ),
                        Err(e) => tracing::error!(
                            table = %dt.full_table_name,
                            column = %jc.derived_column,
                            "CH: failed to recalculate from journal: {:?}", e
                        ),
                    }
                }
            }
        }
    }

    /// Build a WHERE clause joining derived table rows to journal where_key.
    /// The journal stores where_key as "col1=val1,col2=val2", so we match using
    /// string concatenation on the derived table side.
    fn journal_where_key_join(where_columns: &[String], dt_alias: &str, sub_alias: &str) -> String {
        if where_columns.is_empty() {
            return format!("{}.network = {}.where_key", dt_alias, sub_alias);
        }

        let concat_parts: Vec<String> = where_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == 0 {
                    format!("'{}=' || {}.{}::TEXT", col, dt_alias, col)
                } else {
                    format!("',{}=' || {}.{}::TEXT", col, dt_alias, col)
                }
            })
            .collect();

        format!("{}.where_key = {}", sub_alias, concat_parts.join(" || "))
    }

    /// Build a ClickHouse expression that reconstructs the where_key string
    /// from table columns using `concat()`.
    fn journal_where_key_concat_ch(where_columns: &[String]) -> String {
        if where_columns.is_empty() {
            return "network".to_string();
        }
        let parts: Vec<String> = where_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == 0 {
                    format!("'{}=', toString({})", col, col)
                } else {
                    format!("',{}=', toString({})", col, col)
                }
            })
            .collect();
        format!("concat({})", parts.join(", "))
    }

    pub async fn execute(
        &self,
        window: &mut BlockChainWindow,
        persistence: &ReorgBlockHashPersistence,
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
        provider: Option<&Arc<JsonRpcCachedProvider>>,
    ) -> Result<ReorgTaskResult, String> {
        let _ = persistence;
        let start = std::time::Instant::now();

        tracing::info!(
            network = %self.network,
            fork_point = self.fork_point,
            detection_point = self.detection_point,
            depth = self.detection_point - self.fork_point + 1,
            "Starting reorg task"
        );

        // Use pre-fetched canonical blocks if available; otherwise fall back to RPC.
        let canonical: Vec<(u64, B256, B256)> = if !self.canonical_blocks.is_empty() {
            self.canonical_blocks
                .iter()
                .filter(|(n, _, _)| *n >= self.fork_point && *n <= self.detection_point)
                .copied()
                .collect()
        } else if let Some(provider) = provider {
            let block_numbers: Vec<U64> =
                (self.fork_point..=self.detection_point).map(|n| U64::from(n)).collect();
            match provider.get_block_by_number_batch(&block_numbers, false).await {
                Ok(blocks) => blocks
                    .iter()
                    .map(|b| (b.header.number, b.header.hash, b.header.parent_hash))
                    .collect(),
                Err(e) => {
                    tracing::error!("Failed to fetch corrected blocks for reorg range: {}", e);
                    vec![]
                }
            }
        } else {
            vec![]
        };

        let corrected_blocks_owned: Vec<(u64, String, String)> = canonical
            .iter()
            .map(|(n, h, p)| (*n, format!("{:#x}", h), format!("{:#x}", p)))
            .collect();

        let corrected_blocks: Vec<(u64, &str, &str)> =
            corrected_blocks_owned.iter().map(|(n, h, p)| (*n, h.as_str(), p.as_str())).collect();

        // Phase 1: snapshot event data for accumulative reversal (before deletion)
        let reversal_snapshots = self.snapshot_for_reversal(postgres, clickhouse).await;

        let mut affected_tx_hashes: Vec<String> = Vec::new();
        let mut total_deleted = 0u64;

        if let Some(pg) = postgres {
            let table_names: Vec<&str> =
                self.event_tables.iter().map(|t| t.full_name.as_str()).collect();
            let checkpoint_tables: Vec<&str> =
                self.event_tables.iter().map(|t| t.checkpoint_table.as_str()).collect();

            let (deleted, tx_hashes) = pg
                .reorg_rollback_transaction(
                    &table_names,
                    &self.network,
                    self.fork_point,
                    self.detection_point,
                    &corrected_blocks,
                    &checkpoint_tables,
                )
                .await
                .map_err(|e| {
                    let mut msg = e.to_string();
                    let mut source: Option<&dyn std::error::Error> = std::error::Error::source(&e);
                    while let Some(s) = source {
                        msg.push_str(&format!(": {}", s));
                        source = s.source();
                    }
                    msg
                })?;
            total_deleted = deleted;
            affected_tx_hashes = tx_hashes;
        }

        if let Some(ch) = clickhouse {
            let tables: Vec<(String, String)> = self
                .event_tables
                .iter()
                .map(|t| (t.schema.clone(), t.table_name.clone()))
                .collect();
            let checkpoint_tables: Vec<String> =
                self.event_tables.iter().map(|t| t.checkpoint_table.clone()).collect();

            let (ch_deleted, ch_tx_hashes) = ch
                .reorg_rollback(
                    &tables,
                    &self.network,
                    self.fork_point,
                    self.detection_point,
                    &checkpoint_tables,
                    &corrected_blocks,
                )
                .await
                .map_err(|e| e.to_string())?;

            if postgres.is_none() {
                total_deleted = ch_deleted;
                affected_tx_hashes = ch_tx_hashes;
            } else if ch_deleted != total_deleted {
                tracing::warn!(
                    network = %self.network,
                    postgres_deleted = total_deleted,
                    clickhouse_deleted = ch_deleted,
                    "Reorg rollback: postgres and clickhouse deleted counts differ"
                );
            }
        }

        // Phase 2: apply accumulative reversals from snapshots (after event deletion)
        Self::apply_reversal_from_snapshots(&reversal_snapshots, postgres, clickhouse).await;

        // Phase 3: recalculate non-reversible columns (Set/Max/Min) from operation journal
        self.recalculate_from_journal(postgres, clickhouse).await;

        // Phase 4: DELETE insert-only derived tables (no rollback_ops and no journal_columns)
        for dt in &self.derived_tables {
            if !dt.rollback_ops.is_empty() || !dt.journal_columns.is_empty() {
                continue; // handled by reversal and/or journal recalculation
            }
            if let Some(pg) = postgres {
                let query = if dt.cross_chain {
                    format!(
                        "DELETE FROM {} WHERE rindexer_block_number >= {}",
                        dt.full_table_name, self.fork_point
                    )
                } else {
                    format!(
                        "DELETE FROM {} WHERE rindexer_block_number >= {} AND network = '{}'",
                        dt.full_table_name, self.fork_point, self.network
                    )
                };

                match pg.batch_execute(&query).await {
                    Ok(_) => tracing::info!(
                        "PostgreSQL: deleted derived table rows from block >= {} in {}",
                        self.fork_point,
                        dt.full_table_name
                    ),
                    Err(e) => tracing::error!(
                        "PostgreSQL: failed to delete derived table rows in {}: {:?}",
                        dt.full_table_name,
                        e
                    ),
                }
            }

            if let Some(ch) = clickhouse {
                let query = if dt.cross_chain {
                    format!(
                        "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} SETTINGS mutations_sync = 1",
                        dt.full_table_name, self.fork_point
                    )
                } else {
                    format!(
                        "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {} AND network = '{}' SETTINGS mutations_sync = 1",
                        dt.full_table_name, self.fork_point, self.network
                    )
                };

                match ch.execute(&query).await {
                    Ok(_) => tracing::info!(
                        "ClickHouse: deleted derived table rows from block >= {} in {}",
                        self.fork_point,
                        dt.full_table_name
                    ),
                    Err(e) => tracing::error!(
                        "ClickHouse: failed to delete derived table rows in {}: {:?}",
                        dt.full_table_name,
                        e
                    ),
                }
            }
        }

        // Update the in-memory window with canonical blocks only after all DB changes succeed
        if !canonical.is_empty() {
            window.update_range(&canonical);
        }

        let duration = start.elapsed().as_secs_f64();
        metrics::record_reorg_handling_duration(&self.network, duration);
        metrics::record_reorg_events_deleted(&self.network, total_deleted);

        tracing::info!(
            network = %self.network,
            events_deleted = total_deleted,
            duration_secs = duration,
            "Reorg task completed"
        );

        Ok(ReorgTaskResult {
            events_deleted: total_deleted,
            duration_secs: duration,
            affected_tx_hashes,
        })
    }
}
