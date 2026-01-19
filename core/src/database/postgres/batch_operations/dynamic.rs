//! Dynamic batch operations for runtime-defined columns (used by custom indexing).

use tokio_postgres::types::ToSql;

use super::query_builder::{
    build_cte_header, build_delete_body, build_sequence_condition, build_set_clause,
    build_to_process_cte, build_update_body, build_upsert_body, build_upsert_set_clause,
    build_where_clause, build_where_condition, format_table_name, ColumnInfo, SetClauseType,
    UpsertClauseType,
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

    for batch in rows.chunks(1000) {
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
    query.push_str(&build_to_process_cte(&distinct_cols, sequence_col));

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
        BatchOperationType::Upsert => {
            let conflict_columns: Vec<&str> = if !where_columns.is_empty() {
                where_columns.clone()
            } else {
                distinct_cols.clone()
            };

            let mut update_clauses: Vec<String> = Vec::new();

            for col in &set_columns {
                if !where_columns.contains(col) && !distinct_cols.contains(col) {
                    update_clauses.push(build_upsert_set_clause(
                        col,
                        &formatted_table_name,
                        UpsertClauseType::Set,
                    ));
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
                sequence_col,
                custom_where,
            ));

            let params: Vec<&(dyn ToSql + Sync)> =
                owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

            tracing::debug!("Custom indexing query: {}", query);

            database.with_transaction(&query, &params, |_| async move { Ok(()) }).await.map_err(
                |e| {
                    tracing::error!("PostgreSQL error: {:?}", e);
                    tracing::error!("Failed query:\n{}", query);
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
