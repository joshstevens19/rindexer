//! Dynamic batch operations for runtime-defined columns (used by custom indexing).

use tokio_postgres::types::ToSql;

use super::query_builder::{
    build_arithmetic_insert_expr, build_cte_header, build_delete_body, build_sequence_condition,
    build_set_clause, build_to_process_cte, build_to_process_cte_aggregated, build_update_body,
    build_upsert_body, build_upsert_set_clause, build_upsert_set_clause_arithmetic,
    build_upsert_set_clause_latest_by_sequence, build_where_clause, build_where_condition,
    format_table_name, rewrite_custom_where_for_arithmetic, ColumnAggregate, ColumnInfo,
    SetClauseType, UpsertClauseType,
};
use crate::database::batch_operations::{
    BatchOperationAction, BatchOperationColumnBehavior, BatchOperationType, DynamicColumnDefinition,
};
use crate::database::postgres::client::{copy_in_via_transaction, pg_error_to_string};
use crate::database::postgres::write_mode::PgWriteMode;
use crate::EthereumSqlTypeWrapper;

/// One chunk of a dynamic batch operation, built but not yet sent to Postgres.
#[derive(Debug, Clone)]
pub enum DynamicBatchStatement {
    /// Parameterized single statement (Upsert, Update, Delete).
    Query { sql: String, params: Vec<EthereumSqlTypeWrapper> },
    /// Binary COPY payload (Insert).
    CopyIn {
        table_name: String,
        column_names: Vec<String>,
        column_types: Vec<tokio_postgres::types::Type>,
        rows: Vec<Vec<EthereumSqlTypeWrapper>>,
    },
}

/// Executes a dynamic batch operation with runtime-defined columns.
///
/// This mirrors the `create_batch_postgres_operation!` macro but works with
/// dynamically defined columns at runtime (used by custom indexing).
///
/// # Arguments
/// * `mode` - `Eager` commits every chunk in its own transaction; `Tx` stages the
///   chunks inside the caller's transaction, which commits or rolls back all of them.
/// * `custom_where` - Optional SQL WHERE condition for upsert operations.
///   Used to push conditions with `@table` references to SQL level.
///   E.g., `"EXCLUDED.value > token_balances.balance"` to only update if new value is greater.
pub async fn execute_dynamic_batch_operation(
    mode: PgWriteMode<'_>,
    table_name: &str,
    op_type: BatchOperationType,
    rows: Vec<Vec<DynamicColumnDefinition>>,
    event_name: &str,
    custom_where: Option<&str>,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    // PostgreSQL wire protocol limits parameters to i16::MAX (32767) per statement.
    // Cap batch size to stay under this limit based on column count.
    let num_columns = rows.first().map_or(1, |r| r.len().max(1));
    let max_rows_per_batch = (32767 / num_columns).max(1);

    for batch in rows.chunks(max_rows_per_batch) {
        let Some(statement) = build_batch_statement(table_name, op_type, batch, custom_where)
        else {
            continue;
        };
        execute_batch_statement(mode, statement).await.map_err(|e| {
            tracing::error!("{} - Batch operation failed: {}", event_name, e);
            e
        })?;
    }

    Ok(())
}

/// Sends one built statement to Postgres in the given write mode.
pub async fn execute_batch_statement(
    mode: PgWriteMode<'_>,
    statement: DynamicBatchStatement,
) -> Result<(), String> {
    match statement {
        DynamicBatchStatement::Query { sql, params } => {
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

            tracing::debug!("Custom indexing query: {}", sql);

            match mode {
                PgWriteMode::Eager(database) => database
                    .with_transaction(&sql, &param_refs, |_| async move { Ok(()) })
                    .await
                    .map_err(|e| query_failure(&e, e.to_string(), &sql)),
                PgWriteMode::Tx(transaction) => transaction
                    .execute(sql.as_str(), &param_refs)
                    .await
                    .map(|_| ())
                    .map_err(|e| query_failure(&e, pg_error_to_string(&e), &sql)),
            }
        }
        DynamicBatchStatement::CopyIn { table_name, column_names, column_types, rows } => {
            tracing::debug!(
                "Custom indexing INSERT via binary COPY: {} rows into {}",
                rows.len(),
                table_name
            );

            let result = match mode {
                PgWriteMode::Eager(database) => {
                    database
                        .bulk_insert_via_copy(&table_name, &column_names, &column_types, &rows)
                        .await
                }
                PgWriteMode::Tx(transaction) => {
                    copy_in_via_transaction(
                        transaction,
                        &table_name,
                        &column_names,
                        &column_types,
                        &rows,
                    )
                    .await
                }
            };

            result.map_err(|e| {
                tracing::error!("PostgreSQL COPY error: {:?}", e);
                e.to_string()
            })
        }
    }
}

/// Logs a failed statement with its SQL and hands the caller the rendered error.
///
/// `rendered` is the caller's `String` form: `Display` for a `PostgresError` (whose
/// `PgError` variant already carries the server message and SQLSTATE) and
/// `pg_error_to_string` for a bare `tokio_postgres::Error`, whose `Display` is the kind only.
fn query_failure(error: &impl std::fmt::Debug, rendered: String, sql: &str) -> String {
    tracing::error!("PostgreSQL error: {:?}", error);
    tracing::error!("Failed query:\n{}", sql);
    rendered
}

/// Builds the statement for one chunk without touching the database.
///
/// Returns `None` for an empty chunk. The same statement runs eagerly or inside a
/// caller-owned transaction with identical SQL.
pub fn build_batch_statement(
    table_name: &str,
    op_type: BatchOperationType,
    batch: &[Vec<DynamicColumnDefinition>],
    custom_where: Option<&str>,
) -> Option<DynamicBatchStatement> {
    let columns = batch.first()?;

    // Extract column metadata
    let column_names: Vec<&str> = columns.iter().map(|col| col.name.as_str()).collect();

    let distinct_cols: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.behavior {
            BatchOperationColumnBehavior::Distinct => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let sequence_col = columns.iter().find_map(|col| match col.behavior {
        BatchOperationColumnBehavior::Sequence => Some(col.name.as_str()),
        _ => None,
    });

    let set_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Set => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let add_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Add => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let subtract_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Subtract => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let max_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Max => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let min_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Min => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let where_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Where => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    // Build CTE header
    let mut query = build_cte_header(&column_names);

    // Build placeholders and collect parameters
    let mut placeholders = Vec::new();
    let mut owned_params: Vec<EthereumSqlTypeWrapper> = Vec::new();

    for (i, row_columns) in batch.iter().enumerate() {
        let base = i * row_columns.len() + 1;
        let placeholder = row_columns
            .iter()
            .enumerate()
            .map(|(j, col)| format!("${}::{}", base + j, col.sql_type.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        placeholders.push(format!("({})", placeholder));

        for col in row_columns {
            owned_params.push(col.value.clone());
        }
    }

    query.push_str(&placeholders.join(", "));
    query.push(')');

    // Add to_process CTE
    // When arithmetic columns exist (add/subtract/max/min), use GROUP BY with
    // aggregations instead of DISTINCT ON. This fixes duplicate-key accumulation
    // within a single batch (GitHub #383).
    let has_arithmetic = !add_columns.is_empty()
        || !subtract_columns.is_empty()
        || !max_columns.is_empty()
        || !min_columns.is_empty();

    if has_arithmetic && !distinct_cols.is_empty() {
        let agg_columns: Vec<(&str, ColumnAggregate)> = columns
            .iter()
            .map(|col| {
                let name = col.name.as_str();
                let agg = if distinct_cols.contains(&name) || where_columns.contains(&name) {
                    ColumnAggregate::GroupKey
                } else if sequence_col == Some(name) {
                    ColumnAggregate::Max
                } else if add_columns.contains(&name) || subtract_columns.contains(&name) {
                    ColumnAggregate::Sum
                } else if max_columns.contains(&name) {
                    ColumnAggregate::Max
                } else if min_columns.contains(&name) {
                    ColumnAggregate::Min
                } else if col.sql_type.is_array() {
                    // array_agg over arrays adds a dimension ([1] yields NULL);
                    // max(anyarray) is a valid deterministic pick
                    ColumnAggregate::Max
                } else if set_columns.contains(&name) {
                    ColumnAggregate::LastBySeq
                } else {
                    // Unknown columns (shouldn't happen): take last by sequence
                    ColumnAggregate::LastBySeq
                };
                (name, agg)
            })
            .collect();
        query.push_str(&build_to_process_cte_aggregated(&agg_columns, sequence_col));
    } else {
        query.push_str(&build_to_process_cte(&distinct_cols, sequence_col));
    }

    let formatted_table_name = format_table_name(table_name);

    match op_type {
        BatchOperationType::Update => {
            let mut all_set_clauses: Vec<String> = Vec::new();

            for col_name in &set_columns {
                let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                let col_info =
                    ColumnInfo { name: col_name, table_column: column_def.table_column.as_deref() };
                all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Set));
            }

            for col_name in &add_columns {
                let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                let col_info =
                    ColumnInfo { name: col_name, table_column: column_def.table_column.as_deref() };
                all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Add));
            }

            for col_name in &subtract_columns {
                let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                let col_info =
                    ColumnInfo { name: col_name, table_column: column_def.table_column.as_deref() };
                all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Subtract));
            }

            for col_name in &max_columns {
                let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                let col_info =
                    ColumnInfo { name: col_name, table_column: column_def.table_column.as_deref() };
                all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Max));
            }

            for col_name in &min_columns {
                let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                let col_info =
                    ColumnInfo { name: col_name, table_column: column_def.table_column.as_deref() };
                all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Min));
            }

            query.push_str(&build_update_body(&formatted_table_name, all_set_clauses));
        }
        BatchOperationType::Delete => {
            query.push_str(&build_delete_body(&formatted_table_name));
        }
        BatchOperationType::Insert => {
            // Use binary COPY for INSERT operations - much faster than SQL INSERT
            let column_names_owned: Vec<String> =
                columns.iter().map(|col| col.name.clone()).collect();

            // Get column types from the schema definition (not the values)
            // This ensures correct types even when values are null
            let column_types: Vec<tokio_postgres::types::Type> =
                columns.iter().map(|col| col.sql_type.to_pg_type()).collect();

            // Collect data rows
            let data: Vec<Vec<EthereumSqlTypeWrapper>> =
                batch.iter().map(|row| row.iter().map(|col| col.value.clone()).collect()).collect();

            return Some(DynamicBatchStatement::CopyIn {
                table_name: table_name.to_string(),
                column_names: column_names_owned,
                column_types,
                rows: data,
            });
        }
        BatchOperationType::Upsert => {
            let conflict_columns: Vec<&str> = if !where_columns.is_empty() {
                where_columns.clone()
            } else {
                distinct_cols.clone()
            };

            let mut update_clauses: Vec<String> = Vec::new();
            // INSERT-branch SELECT expression overrides for arithmetic columns:
            // a row created by add/subtract must start from the column default
            // (default ± delta), not the raw delta.
            let mut insert_exprs: Vec<(&str, String)> = Vec::new();
            // Overriding the INSERT branch changes what EXCLUDED.<col> carries,
            // so EXCLUDED references to arithmetic columns inside the pushed-down
            // condition must be rewritten back to the raw delta.
            let mut rewritten_where = custom_where.map(str::to_string);
            let arithmetic_without_sequence_guard = has_arithmetic && sequence_col.is_some();

            for col in &set_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    if arithmetic_without_sequence_guard {
                        if Some(*col) == sequence_col {
                            update_clauses.push(build_upsert_set_clause(
                                col,
                                &formatted_table_name,
                                UpsertClauseType::Max,
                            ));
                        } else {
                            update_clauses.push(build_upsert_set_clause_latest_by_sequence(
                                col,
                                &formatted_table_name,
                                sequence_col.expect("sequence column exists"),
                            ));
                        }
                    } else {
                        update_clauses.push(build_upsert_set_clause(
                            col,
                            &formatted_table_name,
                            UpsertClauseType::Set,
                        ));
                    }
                }
            }

            for (cols, clause_type) in [
                (&add_columns, UpsertClauseType::Add),
                (&subtract_columns, UpsertClauseType::Subtract),
            ] {
                for col in cols {
                    if !where_columns.contains(col) && !distinct_cols.contains(col) {
                        let insert_default = columns
                            .iter()
                            .find(|c| c.name == *col)
                            .and_then(|c| c.insert_default.as_deref());
                        update_clauses.push(build_upsert_set_clause_arithmetic(
                            col,
                            &formatted_table_name,
                            insert_default,
                        ));
                        insert_exprs.push((
                            col,
                            build_arithmetic_insert_expr(col, clause_type, insert_default),
                        ));
                        if let Some(w) = rewritten_where.as_mut() {
                            *w = rewrite_custom_where_for_arithmetic(
                                w,
                                col,
                                clause_type,
                                insert_default,
                            );
                        }
                    }
                }
            }

            for col in &max_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    update_clauses.push(build_upsert_set_clause(
                        col,
                        &formatted_table_name,
                        UpsertClauseType::Max,
                    ));
                }
            }

            for col in &min_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    update_clauses.push(build_upsert_set_clause(
                        col,
                        &formatted_table_name,
                        UpsertClauseType::Min,
                    ));
                }
            }

            query.push_str(&build_upsert_body(
                &formatted_table_name,
                &column_names,
                &conflict_columns,
                update_clauses,
                if arithmetic_without_sequence_guard { None } else { sequence_col },
                rewritten_where.as_deref(),
                &insert_exprs,
            ));

            return Some(DynamicBatchStatement::Query { sql: query, params: owned_params });
        }
    }

    // Build WHERE conditions for UPDATE/DELETE
    let mut where_conditions = Vec::new();

    for col in &where_columns {
        let column_def = columns.iter().find(|c| c.name == *col).unwrap();
        let col_info = ColumnInfo { name: col, table_column: column_def.table_column.as_deref() };
        where_conditions.push(build_where_condition(&col_info));
    }

    for col in &distinct_cols {
        if !where_columns.contains(col) {
            let column_def = columns.iter().find(|c| c.name == *col).unwrap();
            let col_info =
                ColumnInfo { name: col, table_column: column_def.table_column.as_deref() };
            where_conditions.push(build_where_condition(&col_info));
        }
    }

    if let Some(seq_col) = sequence_col {
        if let Some(condition) = build_sequence_condition(seq_col, op_type) {
            where_conditions.push(condition);
        }
    }

    query.push_str(&build_where_clause(&where_conditions));

    Some(DynamicBatchStatement::Query { sql: query, params: owned_params })
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;
    use crate::database::batch_operations::BatchOperationSqlType;

    // The four MASTER_* constants are the SQL master's `execute_batch` produced at
    // dd22e676 for the fixtures below (captured before the refactor). The only
    // intended difference after the refactor is the deterministic conflict-key
    // ORDER BY on upserts, applied through `with_order_by`.
    const MASTER_UPSERT_ARITHMETIC: &str = r#"
        WITH raw_data (network, holder, balance, last_block, rindexer_sequence_id) AS (
            VALUES
        ($1::VARCHAR, $2::TEXT, $3::NUMERIC, $4::BIGINT, $5::NUMERIC), ($6::VARCHAR, $7::TEXT, $8::NUMERIC, $9::BIGINT, $10::NUMERIC)),
        to_process AS (
            SELECT network, holder, SUM(balance) AS balance, (array_agg(last_block ORDER BY rindexer_sequence_id DESC))[1] AS last_block, MAX(rindexer_sequence_id) AS rindexer_sequence_id
            FROM raw_data
            GROUP BY network, holder
        )
INSERT INTO "test"."balances" (network, holder, balance, last_block, rindexer_sequence_id)
SELECT tp.network, tp.holder, (0 - tp.balance), tp.last_block, tp.rindexer_sequence_id
FROM to_process tp
ON CONFLICT (network, holder)
DO UPDATE SET last_block = CASE WHEN EXCLUDED.rindexer_sequence_id > COALESCE("test"."balances".rindexer_sequence_id, 0) THEN EXCLUDED.last_block ELSE "test"."balances".last_block END, rindexer_sequence_id = GREATEST(COALESCE("test"."balances".rindexer_sequence_id, EXCLUDED.rindexer_sequence_id), EXCLUDED.rindexer_sequence_id), balance = COALESCE("test"."balances".balance, 0) + EXCLUDED.balance - 0
WHERE (0 - EXCLUDED."balance") <= "test"."balances"."balance""#;

    const MASTER_UPSERT_SET_ONLY: &str = r#"
        WITH raw_data (network, holder, name, rindexer_sequence_id) AS (
            VALUES
        ($1::VARCHAR, $2::TEXT, $3::TEXT, $4::NUMERIC), ($5::VARCHAR, $6::TEXT, $7::TEXT, $8::NUMERIC)),
        to_process AS (
            SELECT DISTINCT ON (network, holder) *
            FROM raw_data
            ORDER BY network, holder, rindexer_sequence_id DESC
        )
INSERT INTO "test"."balances" (network, holder, name, rindexer_sequence_id)
SELECT tp.network, tp.holder, tp.name, tp.rindexer_sequence_id
FROM to_process tp
ON CONFLICT (network, holder)
DO UPDATE SET name = EXCLUDED.name, rindexer_sequence_id = EXCLUDED.rindexer_sequence_id
WHERE EXCLUDED.rindexer_sequence_id > COALESCE("test"."balances".rindexer_sequence_id, 0)"#;

    const MASTER_UPDATE: &str = r#"
        WITH raw_data (holder, balance, rindexer_sequence_id) AS (
            VALUES
        ($1::TEXT, $2::NUMERIC, $3::NUMERIC)),
        to_process AS (
            SELECT DISTINCT ON (holder) *
            FROM raw_data
            ORDER BY holder, rindexer_sequence_id DESC
        )
UPDATE "test"."balances" am
SET balance = tp.balance, rindexer_sequence_id = tp.rindexer_sequence_id
FROM to_process tp
WHERE am.holder = tp.holder
  AND tp.rindexer_sequence_id > am.rindexer_sequence_id"#;

    const MASTER_DELETE: &str = r#"
        WITH raw_data (holder, balance, rindexer_sequence_id) AS (
            VALUES
        ($1::TEXT, $2::NUMERIC, $3::NUMERIC)),
        to_process AS (
            SELECT DISTINCT ON (holder) *
            FROM raw_data
            ORDER BY holder, rindexer_sequence_id DESC
        )
DELETE FROM "test"."balances" am
USING to_process tp
WHERE am.holder = tp.holder
  AND tp.rindexer_sequence_id >= am.rindexer_sequence_id"#;

    const TABLE: &str = "test.balances";
    const GUARD: &str = "EXCLUDED.\"balance\" <= \"test\".\"balances\".\"balance\"";

    fn col(
        name: &str,
        sql_type: BatchOperationSqlType,
        behavior: BatchOperationColumnBehavior,
        action: BatchOperationAction,
        insert_default: Option<&str>,
    ) -> DynamicColumnDefinition {
        DynamicColumnDefinition::new(
            name.to_string(),
            EthereumSqlTypeWrapper::U64(1),
            sql_type,
            behavior,
            action,
        )
        .with_insert_default(insert_default.map(str::to_string))
    }

    /// Custody-shaped row: keyed debit with a `set` column and a sequence.
    fn arithmetic_row() -> Vec<DynamicColumnDefinition> {
        vec![
            col(
                "network",
                BatchOperationSqlType::Varchar,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
                None,
            ),
            col(
                "holder",
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
                None,
            ),
            col(
                "balance",
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Subtract,
                Some("0"),
            ),
            col(
                "last_block",
                BatchOperationSqlType::Bigint,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
                None,
            ),
            col(
                "rindexer_sequence_id",
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
                None,
            ),
        ]
    }

    fn set_only_row() -> Vec<DynamicColumnDefinition> {
        vec![
            col(
                "network",
                BatchOperationSqlType::Varchar,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
                None,
            ),
            col(
                "holder",
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
                None,
            ),
            col(
                "name",
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
                None,
            ),
            col(
                "rindexer_sequence_id",
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
                None,
            ),
        ]
    }

    fn keyed_row() -> Vec<DynamicColumnDefinition> {
        vec![
            col(
                "holder",
                BatchOperationSqlType::Text,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
                None,
            ),
            col(
                "balance",
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
                None,
            ),
            col(
                "rindexer_sequence_id",
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
                None,
            ),
        ]
    }

    fn query_parts(statement: DynamicBatchStatement) -> (String, usize) {
        match statement {
            DynamicBatchStatement::Query { sql, params } => (sql, params.len()),
            DynamicBatchStatement::CopyIn { .. } => panic!("expected a Query statement"),
        }
    }

    /// Master's upsert text plus the deterministic conflict-key ORDER BY.
    fn with_order_by(master: &str, order_by: &str) -> String {
        master.replacen("\nON CONFLICT", &format!("\nORDER BY {order_by}\nON CONFLICT"), 1)
    }

    #[test]
    fn empty_chunk_builds_nothing() {
        for op in [
            BatchOperationType::Upsert,
            BatchOperationType::Insert,
            BatchOperationType::Update,
            BatchOperationType::Delete,
        ] {
            assert!(build_batch_statement(TABLE, op, &[], None).is_none());
        }
    }

    #[test]
    fn arithmetic_upsert_matches_master_plus_order_by() {
        let statement = build_batch_statement(
            TABLE,
            BatchOperationType::Upsert,
            &[arithmetic_row(), arithmetic_row()],
            Some(GUARD),
        )
        .expect("statement");

        let (sql, params) = query_parts(statement);
        assert_eq!(params, 10, "two rows x five columns");
        assert_eq!(sql, with_order_by(MASTER_UPSERT_ARITHMETIC, "tp.network, tp.holder"));
    }

    #[test]
    fn set_only_upsert_matches_master_plus_order_by() {
        let statement = build_batch_statement(
            TABLE,
            BatchOperationType::Upsert,
            &[set_only_row(), set_only_row()],
            None,
        )
        .expect("statement");

        let (sql, params) = query_parts(statement);
        assert_eq!(params, 8, "two rows x four columns");
        assert_eq!(sql, with_order_by(MASTER_UPSERT_SET_ONLY, "tp.network, tp.holder"));
    }

    #[test]
    fn update_matches_master_and_carries_where_clause() {
        let statement =
            build_batch_statement(TABLE, BatchOperationType::Update, &[keyed_row()], None)
                .expect("statement");

        let (sql, params) = query_parts(statement);
        assert_eq!(params, 3);
        assert_eq!(sql, MASTER_UPDATE);
        assert!(
            sql.ends_with(
                "WHERE am.holder = tp.holder\n  AND tp.rindexer_sequence_id > am.rindexer_sequence_id"
            ),
            "{sql}"
        );
    }

    #[test]
    fn delete_matches_master_and_carries_where_clause() {
        let statement =
            build_batch_statement(TABLE, BatchOperationType::Delete, &[keyed_row()], None)
                .expect("statement");

        let (sql, params) = query_parts(statement);
        assert_eq!(params, 3);
        assert_eq!(sql, MASTER_DELETE);
        assert!(
            sql.ends_with(
                "WHERE am.holder = tp.holder\n  AND tp.rindexer_sequence_id >= am.rindexer_sequence_id"
            ),
            "{sql}"
        );
    }

    #[test]
    fn insert_builds_copy_in_from_schema_types() {
        let statement = build_batch_statement(
            TABLE,
            BatchOperationType::Insert,
            &[set_only_row(), set_only_row()],
            None,
        )
        .expect("statement");

        let DynamicBatchStatement::CopyIn { table_name, column_names, column_types, rows } =
            statement
        else {
            panic!("expected a CopyIn statement");
        };

        assert_eq!(table_name, TABLE, "COPY targets the unformatted table name, as before");
        assert_eq!(column_names, ["network", "holder", "name", "rindexer_sequence_id"]);
        assert_eq!(column_types, [Type::VARCHAR, Type::TEXT, Type::TEXT, Type::NUMERIC]);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 4));
    }
}
