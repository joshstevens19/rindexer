//! Dynamic batch operations for runtime-defined columns (used by custom indexing).

use tokio_postgres::types::ToSql;

use super::query_builder::{
    build_cte_header, build_delete_body, build_sequence_condition, build_set_clause,
    build_to_process_cte, build_to_process_cte_aggregated, build_update_body, build_upsert_body,
    build_upsert_set_clause, build_upsert_set_clause_latest_by_sequence, build_where_clause,
    build_where_condition, format_table_name, ColumnAggregate, ColumnInfo, SetClauseType,
    UpsertClauseType,
};
use crate::database::batch_operations::{
    BatchOperationAction, BatchOperationColumnBehavior, BatchOperationType, DynamicColumnDefinition,
};
use crate::database::postgres::block_sink::PgWriteMode;
use crate::database::postgres::client::PostgresClient;
use crate::EthereumSqlTypeWrapper;

/// A fully-built dynamic batch statement, ready to execute eagerly or to be
/// buffered into a `sync_together` per-block transaction and replayed there.
#[derive(Debug, Clone)]
pub enum DynamicBatchStatement {
    /// Parameterized single-statement SQL (Upsert/Update/Delete).
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
    // Buffered mode keeps the same chunking so a replayed block produces the
    // exact statement shapes the eager path would have.
    let num_columns = rows.first().map_or(1, |r| r.len().max(1));
    let max_rows_per_batch = (32767 / num_columns).max(1);

    for batch in rows.chunks(max_rows_per_batch) {
        let Some(statement) = build_batch_statement(table_name, op_type, batch, custom_where)
        else {
            continue;
        };

        match mode {
            PgWriteMode::Eager(database) => {
                execute_batch_statement(database, statement).await.map_err(|e| {
                    tracing::error!("{} - Batch operation failed: {}", event_name, e);
                    e
                })?;
            }
            PgWriteMode::Buffered { sink, network } => {
                sink.push_op(network, statement.into());
            }
        }
    }

    Ok(())
}

/// Eagerly executes a previously built [`DynamicBatchStatement`].
pub async fn execute_batch_statement(
    database: &PostgresClient,
    statement: DynamicBatchStatement,
) -> Result<(), String> {
    match statement {
        DynamicBatchStatement::Query { sql, params } => {
            let param_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

            tracing::debug!("Custom indexing query: {}", sql);

            database.with_transaction(&sql, &param_refs, |_| async move { Ok(()) }).await.map_err(
                |e| {
                    tracing::error!("PostgreSQL error: {:?}", e);
                    tracing::error!("Failed query:\n{}", sql);
                    e.to_string()
                },
            )?;

            Ok(())
        }
        DynamicBatchStatement::CopyIn { table_name, column_names, column_types, rows } => {
            tracing::debug!(
                "Custom indexing INSERT via binary COPY: {} rows into {}",
                rows.len(),
                table_name
            );

            database
                .bulk_insert_via_copy(&table_name, &column_names, &column_types, &rows)
                .await
                .map_err(|e| {
                tracing::error!("PostgreSQL COPY error: {:?}", e);
                e.to_string()
            })?;

            Ok(())
        }
    }
}

/// Builds the statement for one dynamic batch without executing it.
///
/// Pure with respect to the database: the returned [`DynamicBatchStatement`]
/// can be executed eagerly ([`execute_batch_statement`]) or replayed inside a
/// `sync_together` block transaction with identical semantics.
///
/// Returns `None` for an empty batch.
pub fn build_batch_statement(
    table_name: &str,
    op_type: BatchOperationType,
    batch: &[Vec<DynamicColumnDefinition>],
    custom_where: Option<&str>,
) -> Option<DynamicBatchStatement> {
    if batch.is_empty() {
        return None;
    }

    let columns = &batch[0];

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

    if op_type == BatchOperationType::Insert {
        // INSERT operations use binary COPY - much faster than SQL INSERT
        let column_names_owned: Vec<String> = columns.iter().map(|col| col.name.clone()).collect();

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
                } else if set_columns.contains(&name) {
                    ColumnAggregate::LastBySeq
                } else {
                    // Unknown columns (shouldn't happen) — take last by sequence
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
            unreachable!("Insert handled above via binary COPY");
        }
        BatchOperationType::Upsert => {
            let conflict_columns: Vec<&str> = if !where_columns.is_empty() {
                where_columns.clone()
            } else {
                distinct_cols.clone()
            };

            let mut update_clauses: Vec<String> = Vec::new();
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

            for col in &add_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    update_clauses.push(build_upsert_set_clause(
                        col,
                        &formatted_table_name,
                        UpsertClauseType::Add,
                    ));
                }
            }

            for col in &subtract_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    update_clauses.push(build_upsert_set_clause(
                        col,
                        &formatted_table_name,
                        UpsertClauseType::Subtract,
                    ));
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
                custom_where,
                &subtract_columns,
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
    use super::*;

    fn column(
        name: &str,
        value: u64,
        behavior: BatchOperationColumnBehavior,
        action: BatchOperationAction,
    ) -> DynamicColumnDefinition {
        DynamicColumnDefinition::new(
            name.to_string(),
            EthereumSqlTypeWrapper::U64BigInt(value),
            crate::database::batch_operations::BatchOperationSqlType::Bigint,
            behavior,
            action,
        )
    }

    fn upsert_row(holder: u64, balance: u64, seq: u64) -> Vec<DynamicColumnDefinition> {
        vec![
            column(
                "holder",
                holder,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ),
            column(
                "balance",
                balance,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Add,
            ),
            column(
                "rindexer_sequence_id",
                seq,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
            ),
        ]
    }

    #[test]
    fn upsert_builds_parameterized_query_with_deterministic_order_by() {
        let batch = vec![upsert_row(2, 100, 1), upsert_row(1, 50, 2)];

        let statement =
            build_batch_statement("schema.balances", BatchOperationType::Upsert, &batch, None)
                .expect("statement");

        let DynamicBatchStatement::Query { sql, params } = statement else {
            panic!("upsert must build a Query statement");
        };

        assert_eq!(params.len(), 6, "two rows x three columns");
        assert!(sql.contains("INSERT INTO \"schema\".\"balances\""), "sql: {sql}");
        assert!(sql.contains("ON CONFLICT (holder)"), "sql: {sql}");
        // Deterministic conflict-key ordering (ABBA deadlock avoidance for
        // concurrent per-network sync_together flushes).
        let order_by = sql.find("ORDER BY tp.holder").expect("ORDER BY on conflict key");
        let on_conflict = sql.find("ON CONFLICT").expect("ON CONFLICT clause");
        assert!(order_by < on_conflict, "ORDER BY must precede ON CONFLICT: {sql}");
    }

    #[test]
    fn insert_builds_copy_in_payload() {
        let batch = vec![vec![
            column("holder", 1, BatchOperationColumnBehavior::Normal, BatchOperationAction::Set),
            column("balance", 5, BatchOperationColumnBehavior::Normal, BatchOperationAction::Set),
        ]];

        let statement =
            build_batch_statement("schema.history", BatchOperationType::Insert, &batch, None)
                .expect("statement");

        let DynamicBatchStatement::CopyIn { table_name, column_names, column_types, rows } =
            statement
        else {
            panic!("insert must build a CopyIn statement (binary COPY parity with eager path)");
        };

        assert_eq!(table_name, "schema.history");
        assert_eq!(column_names, vec!["holder", "balance"]);
        assert_eq!(column_types.len(), 2);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn update_and_delete_build_queries_with_where_conditions() {
        let batch = vec![vec![
            column(
                "holder",
                1,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ),
            column("balance", 9, BatchOperationColumnBehavior::Normal, BatchOperationAction::Set),
        ]];

        for op in [BatchOperationType::Update, BatchOperationType::Delete] {
            let statement = build_batch_statement("schema.balances", op, &batch, None)
                .unwrap_or_else(|| panic!("{op:?} statement"));
            let DynamicBatchStatement::Query { sql, params } = statement else {
                panic!("{op:?} must build a Query statement");
            };
            assert_eq!(params.len(), 2);
            assert!(sql.contains("WHERE"), "{op:?} sql must include WHERE: {sql}");
        }
    }

    #[test]
    fn empty_batch_builds_nothing() {
        assert!(build_batch_statement("t", BatchOperationType::Upsert, &[], None).is_none());
    }

    #[test]
    fn subtract_upsert_negates_insert_value_and_adds_on_conflict() {
        let batch = vec![vec![
            column(
                "holder",
                1,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ),
            column(
                "balance",
                50,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Subtract,
            ),
            column(
                "rindexer_sequence_id",
                1,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
            ),
        ]];

        let statement = build_batch_statement(
            "schema.balances",
            BatchOperationType::Upsert,
            &batch,
            Some("EXCLUDED.\"balance\" > 10"),
        )
        .expect("statement");

        let DynamicBatchStatement::Query { sql, .. } = statement else {
            panic!("upsert must build a Query statement");
        };

        // A row created by a subtract must start at -value (subtract from the
        // implicit 0), not +value...
        assert!(sql.contains("-tp.balance"), "insert select must negate subtract column: {sql}");
        // ...and the conflict arm ADDS the already-negated excluded value.
        assert!(
            sql.contains(
                "balance = COALESCE(\"schema\".\"balances\".balance, 0) + EXCLUDED.balance"
            ),
            "conflict arm must add the negated value: {sql}"
        );
        assert!(!sql.contains("- EXCLUDED.balance"), "no double negation: {sql}");
        // User conditions keep seeing the positive event value.
        assert!(
            sql.contains("(-EXCLUDED.\"balance\") > 10"),
            "custom_where must be compensated for the negation: {sql}"
        );
    }

    #[test]
    fn custom_where_is_appended_to_upsert() {
        let batch = vec![upsert_row(1, 10, 1)];
        let statement = build_batch_statement(
            "schema.balances",
            BatchOperationType::Upsert,
            &batch,
            Some("EXCLUDED.balance > \"schema\".\"balances\".balance"),
        )
        .expect("statement");

        let DynamicBatchStatement::Query { sql, .. } = statement else {
            panic!("upsert must build a Query statement");
        };
        assert!(sql.contains("EXCLUDED.balance > "), "custom where must survive: {sql}");
    }
}
