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
use crate::database::postgres::client::PostgresClient;
use crate::EthereumSqlTypeWrapper;

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
    database: &PostgresClient,
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
        execute_batch(database, table_name, op_type, batch, custom_where).await.map_err(|e| {
            tracing::error!("{} - Batch operation failed: {}", event_name, e);
            e
        })?;
    }

    Ok(())
}

async fn execute_batch(
    database: &PostgresClient,
    table_name: &str,
    op_type: BatchOperationType,
    batch: &[Vec<DynamicColumnDefinition>],
    custom_where: Option<&str>,
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
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

            tracing::debug!(
                "Custom indexing INSERT via binary COPY: {} rows into {}",
                data.len(),
                table_name
            );

            database
                .bulk_insert_via_copy(table_name, &column_names_owned, &column_types, &data)
                .await
                .map_err(|e| {
                    tracing::error!("PostgreSQL COPY error: {:?}", e);
                    e.to_string()
                })?;

            return Ok(());
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

            let params: Vec<&(dyn ToSql + Sync)> =
                owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

            tracing::debug!("Custom indexing query: {}", query);

            database.with_transaction(&query, &params, |_| async move { Ok(()) }).await.map_err(
                |e| {
                    tracing::error!("PostgreSQL error: {:?}", e);
                    tracing::error!("Failed query:");
                    e.to_string()
                },
            )?;

            return Ok(());
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

    let params: Vec<&(dyn ToSql + Sync)> =
        owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

    tracing::debug!("Custom indexing query: {}", query);

    database.with_transaction(&query, &params, |_| async move { Ok(()) }).await.map_err(|e| {
        tracing::error!("PostgreSQL error: {:?}", e);
        tracing::error!("Failed query:\n{}", query);
        e.to_string()
    })?;

    Ok(())
}
