use std::sync::Arc;

use alloy::primitives::{B256, U64};
use anyhow::Context;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::manifest::contract::SetAction;
use crate::metrics::indexing as metrics;
use crate::provider::ChainProvider;

use super::validate_sql_condition;
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
    pub fn try_new(
        schema: String,
        table_name: String,
        checkpoint_table: String,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&schema, "event table schema")?;
        super::validate_sql_identifier(&table_name, "event table name")?;
        super::validate_sql_identifier(&checkpoint_table, "checkpoint table name")?;
        let full_name = format!("{}.{}", schema, table_name);
        Ok(Self { schema, table_name, full_name, checkpoint_table })
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

impl DerivedColumnRollback {
    pub fn try_new(
        derived_column: String,
        event_column: String,
        action: SetAction,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&derived_column, "derived column")?;
        super::validate_sql_identifier(&event_column, "event column")?;
        Ok(Self { derived_column, event_column, action })
    }
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
    /// Optional SQL condition re-evaluated against event data.
    pub condition: Option<String>,
}

impl DerivedTableRollbackOp {
    pub fn try_new(
        event_table: String,
        where_columns: Vec<(String, String)>,
        columns: Vec<DerivedColumnRollback>,
        condition: Option<String>,
    ) -> anyhow::Result<Self> {
        // event_table is "schema.table" — validate both parts
        if let Some((schema, table)) = event_table.split_once('.') {
            super::validate_sql_identifier(schema, "rollback op event table schema")?;
            super::validate_sql_identifier(table, "rollback op event table name")?;
        } else {
            super::validate_sql_identifier(&event_table, "rollback op event table")?;
        }
        for (dt_col, ev_col) in &where_columns {
            super::validate_sql_identifier(dt_col, "rollback op WHERE derived column")?;
            super::validate_sql_identifier(ev_col, "rollback op WHERE event column")?;
        }
        if let Some(cond) = &condition {
            validate_sql_condition(cond)?;
        }
        Ok(Self { event_table, where_columns, columns, condition })
    }
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

impl DerivedColumnJournal {
    pub fn try_new(
        derived_column: String,
        action: SetAction,
        where_columns: Vec<String>,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&derived_column, "journal derived column")?;
        for col in &where_columns {
            super::validate_sql_identifier(col, "journal WHERE column")?;
        }
        Ok(Self { derived_column, action, where_columns })
    }
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

impl DerivedTableInfo {
    pub fn try_new(
        full_table_name: String,
        cross_chain: bool,
        rollback_ops: Vec<DerivedTableRollbackOp>,
        journal_columns: Vec<DerivedColumnJournal>,
    ) -> anyhow::Result<Self> {
        // full_table_name is "schema.table" — validate both parts
        if let Some((schema, table)) = full_table_name.split_once('.') {
            super::validate_sql_identifier(schema, "derived table schema")?;
            super::validate_sql_identifier(table, "derived table name")?;
        } else {
            super::validate_sql_identifier(&full_table_name, "derived table name")?;
        }
        Ok(Self { full_table_name, cross_chain, rollback_ops, journal_columns })
    }
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
    /// Returns ` AND network = '<network>'` when not cross-chain, empty string otherwise.
    fn network_filter(&self, cross_chain: bool) -> String {
        if cross_chain {
            String::new()
        } else {
            format!(" AND network = '{}'", self.network)
        }
    }

    /// Phase 1: Before event deletion, snapshot aggregated event data into temp tables.
    /// Returns the snapshots needed for phase 2.
    /// Fails the entire reorg task if any snapshot cannot be created — this prevents
    /// event deletion from proceeding without proper reversal data.
    async fn snapshot_for_reversal(
        &self,
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> anyhow::Result<Vec<ReversalSnapshot>> {
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
                        other => anyhow::bail!(
                            "unexpected reversed action {:?} for column {}",
                            other,
                            col.derived_column,
                        ),
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

                let network_filter = self.network_filter(dt.cross_chain);

                let condition_filter = match &op.condition {
                    Some(cond) => format!(" AND ({})", cond),
                    None => String::new(),
                };

                // Include network + fork_point in temp table name to avoid collisions
                // across concurrent reorg tasks on different networks.
                // Replace hyphens with underscores so the name is a valid SQL identifier.
                let safe_network = self.network.replace('-', "_");
                let temp_base = format!(
                    "_rindexer_reorg_snap_{}_{}_{}",
                    safe_network, self.fork_point, snap_idx
                );
                snap_idx += 1;

                // Build the SELECT portion independently so PG and CH can wrap it
                // in their own CREATE TABLE syntax without fragile string stripping.
                let select_sql = format!(
                    "SELECT {}, {} FROM {} WHERE block_number >= {} AND block_number <= {}{}{} GROUP BY {}",
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
                    let pg_temp = format!("{}_pg", temp_base);
                    let pg_create = format!("CREATE TEMP TABLE {} AS {}", pg_temp, select_sql);
                    pg.batch_execute(&pg_create).await.with_context(|| {
                        format!(
                            "Failed to create PG reorg reversal snapshot for {}",
                            dt.full_table_name
                        )
                    })?;
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

                if let Some(ch) = ch {
                    let ch_temp = format!("rindexer_internal.{}_ch", temp_base);
                    let ch_create = format!(
                        "CREATE TABLE IF NOT EXISTS {} ENGINE = Memory AS {}",
                        ch_temp, select_sql,
                    );
                    ch.execute(&ch_create).await.with_context(|| {
                        format!(
                            "Failed to create CH reorg reversal snapshot for {}",
                            dt.full_table_name
                        )
                    })?;
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
            }
        }

        Ok(snapshots)
    }

    /// Phase 2: After event deletion, apply reverse UPDATEs from snapshots and drop temp tables.
    async fn apply_reversal_from_snapshots(
        snapshots: &[ReversalSnapshot],
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> anyhow::Result<()> {
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
                    pg.batch_execute(&update_sql).await.with_context(|| {
                        format!(
                            "PostgreSQL: failed to reverse accumulative ops for {}",
                            snap.derived_table
                        )
                    })?;
                    tracing::info!(
                        table = %snap.derived_table,
                        "PostgreSQL: reversed accumulative ops"
                    );
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

                    ch.execute(&ch_update).await.with_context(|| {
                        format!(
                            "ClickHouse: failed to reverse accumulative ops for {}",
                            snap.derived_table
                        )
                    })?;
                    tracing::info!(
                        table = %snap.derived_table,
                        "ClickHouse: reversed accumulative ops"
                    );

                    let _ = ch.execute(&format!("DROP TABLE IF EXISTS {}", snap.temp_table)).await;
                }
            }
        }
        Ok(())
    }

    /// Recalculate non-reversible columns (Set/Max/Min) from the operation journal.
    /// Deletes journal entries in the reorg range, then recalculates from remaining entries.
    async fn recalculate_from_journal(
        &self,
        pg: Option<&PostgresClient>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> anyhow::Result<()> {
        for dt in &self.derived_tables {
            if dt.journal_columns.is_empty() {
                continue;
            }

            let network_filter = self.network_filter(dt.cross_chain);

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
                pg.batch_execute(&pg_delete).await.with_context(|| {
                    format!(
                        "PG: failed to delete journal entries for reorg range in {}",
                        dt.full_table_name
                    )
                })?;
            }
            if let Some(ch) = ch {
                ch.execute(&ch_delete).await.with_context(|| {
                    format!(
                        "CH: failed to delete journal entries for reorg range in {}",
                        dt.full_table_name
                    )
                })?;
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

                    pg.batch_execute(&update_sql).await.with_context(|| {
                        format!(
                            "PG: failed to recalculate journal column {} in {}",
                            jc.derived_column, dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        table = %dt.full_table_name,
                        column = %jc.derived_column,
                        "PG: recalculated non-reversible column from journal"
                    );
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

                    ch.execute(&ch_update).await.with_context(|| {
                        format!(
                            "CH: failed to recalculate journal column {} in {}",
                            jc.derived_column, dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        table = %dt.full_table_name,
                        column = %jc.derived_column,
                        "CH: recalculated non-reversible column from journal"
                    );
                }
            }
        }
        Ok(())
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
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
        provider: Option<&Arc<dyn ChainProvider>>,
    ) -> anyhow::Result<ReorgTaskResult> {
        // Validate network before any SQL interpolation
        super::validate_sql_value(&self.network, "reorg task network")?;

        let start = std::time::Instant::now();

        tracing::info!(
            network = %self.network,
            fork_point = self.fork_point,
            detection_point = self.detection_point,
            depth = self.detection_point.saturating_sub(self.fork_point) + 1,
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
        let reversal_snapshots = self.snapshot_for_reversal(postgres, clickhouse).await?;

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
                .context("PostgreSQL reorg rollback transaction failed")?;
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
                .context("ClickHouse reorg rollback failed")?;

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
        Self::apply_reversal_from_snapshots(&reversal_snapshots, postgres, clickhouse)
            .await
            .context("Accumulative reversal from snapshots failed")?;

        // Phase 3: recalculate non-reversible columns (Set/Max/Min) from operation journal
        self.recalculate_from_journal(postgres, clickhouse)
            .await
            .context("Journal recalculation failed")?;

        // Phase 4: DELETE insert-only derived tables (no rollback_ops and no journal_columns)
        for dt in &self.derived_tables {
            if !dt.rollback_ops.is_empty() || !dt.journal_columns.is_empty() {
                continue; // handled by reversal and/or journal recalculation
            }
            let network_filter = self.network_filter(dt.cross_chain);

            if let Some(pg) = postgres {
                let query = format!(
                    "DELETE FROM {} WHERE rindexer_block_number >= {}{}",
                    dt.full_table_name, self.fork_point, network_filter
                );
                pg.batch_execute(&query).await.with_context(|| {
                    format!(
                        "PostgreSQL: failed to delete derived table rows in {}",
                        dt.full_table_name
                    )
                })?;
                tracing::info!(
                    "PostgreSQL: deleted derived table rows from block >= {} in {}",
                    self.fork_point,
                    dt.full_table_name
                );
            }

            if let Some(ch) = clickhouse {
                let query = format!(
                    "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {}{} SETTINGS mutations_sync = 1",
                    dt.full_table_name, self.fork_point, network_filter
                );
                ch.execute(&query).await.with_context(|| {
                    format!(
                        "ClickHouse: failed to delete derived table rows in {}",
                        dt.full_table_name
                    )
                })?;
                tracing::info!(
                    "ClickHouse: deleted derived table rows from block >= {} in {}",
                    self.fork_point,
                    dt.full_table_name
                );
            }
        }

        // Update the in-memory window after all DB changes succeed.
        // When canonical blocks are available (parent-hash detection), overwrite with corrected hashes.
        // When canonical blocks are empty (removed-logs / ExEx detection), remove stale entries
        // so the next parent-hash check doesn't immediately re-trigger.
        if !canonical.is_empty() {
            window.update_range(&canonical);
        } else {
            window.remove_from(self.fork_point);
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
