//! Dynamic batch operations for runtime-defined columns (used by custom indexing).

use crate::database::batch_operations::{
    BatchOperationAction, BatchOperationColumnBehavior, BatchOperationType,
    DynamicColumnDefinition,
};
use crate::database::clickhouse::client::ClickhouseClient;

use super::query_builder::{format_table_name, quote_identifier};

/// Executes a dynamic batch operation with runtime-defined columns.
///
/// This mirrors the `create_batch_clickhouse_operation!` macro but works with
/// dynamically defined columns at runtime (used by custom indexing).
pub async fn execute_dynamic_batch_operation(
    database: &ClickhouseClient,
    table_name: &str,
    op_type: BatchOperationType,
    rows: Vec<Vec<DynamicColumnDefinition>>,
    event_name: &str,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    for batch in rows.chunks(1000) {
        execute_batch(database, table_name, op_type, batch).await.map_err(|e| {
            tracing::error!("{} - Batch operation failed: {}", event_name, e);
            e
        })?;
    }

    Ok(())
}

async fn execute_batch(
    database: &ClickhouseClient,
    table_name: &str,
    op_type: BatchOperationType,
    batch: &[Vec<DynamicColumnDefinition>],
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }

    let columns = &batch[0];
    let formatted_table_name = format_table_name(table_name);

    // Extract column metadata
    let column_names: Vec<&str> = columns.iter().map(|col| col.name.as_str()).collect();

    let where_columns: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.action {
            BatchOperationAction::Where => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    let distinct_cols: Vec<&str> = columns
        .iter()
        .filter_map(|col| match col.behavior {
            BatchOperationColumnBehavior::Distinct => Some(col.name.as_str()),
            _ => None,
        })
        .collect();

    match op_type {
        BatchOperationType::Update | BatchOperationType::Upsert => {
            // In ClickHouse, both Update and Upsert map to INSERT
            // ReplacingMergeTree automatically keeps the latest version
            let formatted_columns = column_names
                .iter()
                .map(|col| quote_identifier(col))
                .collect::<Vec<_>>()
                .join(", ");

            let mut values_parts: Vec<String> = Vec::new();

            for row_columns in batch.iter() {
                let row_values: Vec<String> = row_columns
                    .iter()
                    .map(|col| col.value.to_clickhouse_value())
                    .collect();
                values_parts.push(format!("({})", row_values.join(", ")));
            }

            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                formatted_table_name,
                formatted_columns,
                values_parts.join(", ")
            );

            tracing::debug!("Custom indexing ClickHouse query: {}", query);

            database.execute(&query).await.map_err(|e| {
                tracing::error!("ClickHouse error: {:?}", e);
                tracing::error!("Failed query:\n{}", query);
                e.to_string()
            })?;
        }
        BatchOperationType::Delete => {
            // Determine which columns to use for matching
            let match_columns: Vec<&str> = if !where_columns.is_empty() {
                where_columns
            } else {
                distinct_cols
            };

            if match_columns.is_empty() {
                return Err("Delete operation requires WHERE or DISTINCT columns".to_string());
            }

            let mut or_conditions: Vec<String> = Vec::new();

            for row_columns in batch.iter() {
                let mut and_conditions: Vec<String> = Vec::new();

                for match_col in &match_columns {
                    if let Some(col_def) = row_columns.iter().find(|c| c.name == *match_col) {
                        let quoted_col = quote_identifier(match_col);
                        let value = col_def.value.to_clickhouse_value();
                        and_conditions.push(format!("{} = {}", quoted_col, value));
                    }
                }

                if !and_conditions.is_empty() {
                    or_conditions.push(format!("({})", and_conditions.join(" AND ")));
                }
            }

            if or_conditions.is_empty() {
                return Ok(());
            }

            let query = format!(
                "ALTER TABLE {} DELETE WHERE {}",
                formatted_table_name,
                or_conditions.join(" OR ")
            );

            tracing::debug!("Custom indexing ClickHouse query: {}", query);

            database.execute(&query).await.map_err(|e| {
                tracing::error!("ClickHouse error: {:?}", e);
                tracing::error!("Failed query:\n{}", query);
                e.to_string()
            })?;
        }
    }

    Ok(())
}
