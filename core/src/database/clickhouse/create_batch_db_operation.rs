/// Creates a batch operation function for ClickHouse database.
///
/// # Example
///
/// ```ignore
/// // Define the batch operation
/// create_batch_clickhouse_operation!(
///     update_reserve_supplied_shares,
///     UpdateReserveInfo,
///     "spoke.reserve",
///     BatchOperationType::Update,
///     |result: &UpdateReserveInfo| {
///         vec![
///             column(
///                 "spoke",
///                 EthereumSqlTypeWrapper::AddressBytes(result.withdraw.tx_information.address),
///                 BatchOperationSqlType::Bytea,
///                 BatchOperationColumnBehavior::Distinct,
///                 BatchOperationAction::Where,
///             ),
///             column(
///                 "reserve_id",
///                 EthereumSqlTypeWrapper::U256Numeric(result.withdraw.event_data.reserveId),
///                 BatchOperationSqlType::Numeric,
///                 BatchOperationColumnBehavior::Distinct,
///                 BatchOperationAction::Where,
///             ),
///             column(
///                 "chain_id",
///                 EthereumSqlTypeWrapper::U64BigInt(result.withdraw.tx_information.chain_id),
///                 BatchOperationSqlType::Bigint,
///                 BatchOperationColumnBehavior::Distinct,
///                 BatchOperationAction::Where,
///             ),
///             column(
///                 "supplied_shares",
///                 EthereumSqlTypeWrapper::U256Numeric(result.withdrawn_shares),
///                 BatchOperationSqlType::Numeric,
///                 BatchOperationColumnBehavior::Normal,
///                 BatchOperationAction::Subtract,
///             ),
///         ]
///     },
///     "Spoke::Withdraw - Update spoke.reserve"
/// );
///
/// // Call the generated function
/// update_reserve_supplied_shares(&context.database, &reserve_update_data).await?;
/// ```
#[macro_export]
macro_rules! create_batch_clickhouse_operation {
    (
        $func_name:ident,
        $result_type:ty,
        $table_name:expr,
        $op_type:expr,
        $columns_def:expr,
        $event_name:expr
    ) => {
        async fn $func_name(
            database: &ClickhouseClient,
            filtered_results: &[$result_type],
        ) -> Result<(), String> {
            use $crate::database::batch_operations::{
                BatchOperationAction, BatchOperationColumnBehavior, BatchOperationType,
                RESERVED_KEYWORDS,
            };

            async fn execute_batch(
                database: &ClickhouseClient,
                batch: &[$result_type],
            ) -> Result<(), String> {
                if batch.is_empty() {
                    return Ok(());
                }

                let columns = $columns_def(&batch[0]);

                // Get column names for INSERT
                let column_names: Vec<&str> = columns.iter().map(|col| col.name).collect();

                // Get WHERE columns for DELETE operations
                let where_columns: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.action {
                        BatchOperationAction::Where => Some(col.name),
                        _ => None,
                    })
                    .collect();

                // Get DISTINCT columns (also used for DELETE WHERE if no explicit WHERE columns)
                let distinct_cols: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.behavior {
                        BatchOperationColumnBehavior::Distinct => Some(col.name),
                        _ => None,
                    })
                    .collect();

                // Helper to quote column names if reserved
                let quote_col = |col: &str| -> String {
                    if RESERVED_KEYWORDS.contains(&col) {
                        format!("`{}`", col)
                    } else {
                        col.to_string()
                    }
                };

                // Handle table name (ClickHouse uses backticks for quoting)
                let formatted_table_name = if $table_name.contains('.') {
                    let parts: Vec<&str> = $table_name.split('.').collect();
                    if parts.len() == 2 {
                        let schema = parts[0].trim_matches('"').trim_matches('`');
                        let table = parts[1].trim_matches('"').trim_matches('`');
                        format!("`{}`.`{}`", schema, table)
                    } else {
                        $table_name.to_string()
                    }
                } else {
                    $table_name.to_string()
                };

                match $op_type {
                    BatchOperationType::Update | BatchOperationType::Upsert => {
                        // In ClickHouse, both Update and Upsert map to INSERT
                        // ReplacingMergeTree automatically keeps the latest version
                        // based on ORDER BY columns

                        let formatted_columns = column_names
                            .iter()
                            .map(|col| quote_col(col))
                            .collect::<Vec<_>>()
                            .join(", ");

                        let mut values_parts: Vec<String> = Vec::new();

                        for result in batch.iter() {
                            let columns_for_result = $columns_def(result);
                            let row_values: Vec<String> = columns_for_result
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

                        database.execute(&query).await.map_err(|e| e.to_string())?;
                    }
                    BatchOperationType::Delete => {
                        // In ClickHouse, DELETE is performed via ALTER TABLE mutation
                        // Build WHERE clause: (col1 = val1 AND col2 = val2) OR (col1 = val3 AND col2 = val4)

                        // Determine which columns to use for matching
                        let match_columns: Vec<&str> = if !where_columns.is_empty() {
                            where_columns.clone()
                        } else {
                            distinct_cols.clone()
                        };

                        if match_columns.is_empty() {
                            return Err(
                                "Delete operation requires WHERE or DISTINCT columns".to_string()
                            );
                        }

                        let mut or_conditions: Vec<String> = Vec::new();

                        for result in batch.iter() {
                            let columns_for_result = $columns_def(result);
                            let mut and_conditions: Vec<String> = Vec::new();

                            for match_col in &match_columns {
                                if let Some(col_def) =
                                    columns_for_result.iter().find(|c| c.name == *match_col)
                                {
                                    let quoted_col = quote_col(match_col);
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

                        database.execute(&query).await.map_err(|e| e.to_string())?;
                    }
                }

                Ok(())
            }

            for batch in filtered_results.chunks(1000) {
                if let Err(e) = execute_batch(database, batch).await {
                    rindexer_error!("{} - Batch operation failed: {}", $event_name, e);
                    return Err(e);
                }
            }

            Ok(())
        }
    };
}
