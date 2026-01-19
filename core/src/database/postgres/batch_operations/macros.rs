/// Creates a batch operation function for PostgreSQL database.
///
/// # Example
///
/// ```ignore
/// // Define the batch operation
/// create_batch_postgres_operation!(
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
macro_rules! create_batch_postgres_operation {
    (
        $func_name:ident,
        $result_type:ty,
        $table_name:expr,
        $op_type:expr,
        $columns_def:expr,
        $event_name:expr
    ) => {
        async fn $func_name(
            database: &PostgresClient,
            filtered_results: &[$result_type],
        ) -> Result<(), String> {
            use $crate::database::batch_operations::{
                BatchOperationAction, BatchOperationColumnBehavior, BatchOperationType,
            };
            use $crate::database::postgres::batch_operations::{
                build_cte_header, build_delete_body, build_insert_body, build_sequence_condition,
                build_set_clause, build_to_process_cte, build_update_body, build_upsert_body,
                build_upsert_set_clause, build_where_clause, build_where_condition,
                format_table_name, ColumnInfo, SetClauseType, UpsertClauseType,
            };

            async fn execute_batch(
                database: &PostgresClient,
                batch: &[$result_type],
            ) -> Result<(), String> {
                if batch.is_empty() {
                    return Ok(());
                }

                let columns = $columns_def(&batch[0]);

                // Extract column metadata
                let column_names: Vec<&str> = columns.iter().map(|col| col.name).collect();

                let distinct_cols: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.behavior {
                        BatchOperationColumnBehavior::Distinct => Some(col.name),
                        _ => None,
                    })
                    .collect();

                let sequence_col = columns.iter().find_map(|col| match col.behavior {
                    BatchOperationColumnBehavior::Sequence => Some(col.name),
                    _ => None,
                });

                let set_columns: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.action {
                        BatchOperationAction::Set => Some(col.name),
                        _ => None,
                    })
                    .collect();

                let add_columns: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.action {
                        BatchOperationAction::Add => Some(col.name),
                        _ => None,
                    })
                    .collect();

                let subtract_columns: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.action {
                        BatchOperationAction::Subtract => Some(col.name),
                        _ => None,
                    })
                    .collect();

                let where_columns: Vec<&str> = columns
                    .iter()
                    .filter_map(|col| match col.action {
                        BatchOperationAction::Where => Some(col.name),
                        _ => None,
                    })
                    .collect();

                // Build CTE header
                let mut query = build_cte_header(&column_names);

                // Build placeholders and collect parameters
                let mut placeholders = Vec::new();
                let mut owned_params: Vec<EthereumSqlTypeWrapper> = Vec::new();

                for (i, result) in batch.iter().enumerate() {
                    let columns_for_result = $columns_def(result);
                    let base = i * columns_for_result.len() + 1;
                    let placeholder = columns_for_result
                        .iter()
                        .enumerate()
                        .map(|(j, col)| format!("${}::{}", base + j, col.sql_type.as_str()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    placeholders.push(format!("({})", placeholder));

                    for col in &columns_for_result {
                        owned_params.push(col.value.clone());
                    }
                }

                query.push_str(&placeholders.join(", "));
                query.push_str(")");

                // Add to_process CTE
                query.push_str(&build_to_process_cte(&distinct_cols, sequence_col));

                let formatted_table_name = format_table_name($table_name);

                match $op_type {
                    BatchOperationType::Update => {
                        let mut all_set_clauses: Vec<String> = Vec::new();

                        for col_name in &set_columns {
                            let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                            let col_info = ColumnInfo {
                                name: col_name,
                                table_column: column_def.table_column,
                            };
                            all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Set));
                        }

                        for col_name in &add_columns {
                            let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                            let col_info = ColumnInfo {
                                name: col_name,
                                table_column: column_def.table_column,
                            };
                            all_set_clauses.push(build_set_clause(&col_info, SetClauseType::Add));
                        }

                        for col_name in &subtract_columns {
                            let column_def = columns.iter().find(|c| c.name == *col_name).unwrap();
                            let col_info = ColumnInfo {
                                name: col_name,
                                table_column: column_def.table_column,
                            };
                            all_set_clauses
                                .push(build_set_clause(&col_info, SetClauseType::Subtract));
                        }

                        query.push_str(&build_update_body(&formatted_table_name, all_set_clauses));
                    }
                    BatchOperationType::Delete => {
                        query.push_str(&build_delete_body(&formatted_table_name));
                    }
                    BatchOperationType::Insert => {
                        // Plain INSERT - no conflict handling, just insert all rows
                        query.push_str(&build_insert_body(&formatted_table_name, &column_names));

                        let params: Vec<&(dyn ToSql + Sync)> =
                            owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

                        database
                            .with_transaction(&query, &params, |_| async move { Ok(()) })
                            .await
                            .map_err(|e| e.to_string())?;

                        return Ok(());
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

                        query.push_str(&build_upsert_body(
                            &formatted_table_name,
                            &column_names,
                            &conflict_columns,
                            update_clauses,
                            sequence_col,
                            None, // custom_where - not used in macro-generated operations
                        ));

                        let params: Vec<&(dyn ToSql + Sync)> =
                            owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

                        database
                            .with_transaction(&query, &params, |_| async move { Ok(()) })
                            .await
                            .map_err(|e| e.to_string())?;

                        return Ok(());
                    }
                }

                // Build WHERE conditions for UPDATE/DELETE
                let mut where_conditions = Vec::new();

                for col in &where_columns {
                    let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                    let col_info = ColumnInfo { name: col, table_column: column_def.table_column };
                    where_conditions.push(build_where_condition(&col_info));
                }

                for col in &distinct_cols {
                    if !where_columns.contains(col) {
                        let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                        let col_info =
                            ColumnInfo { name: col, table_column: column_def.table_column };
                        where_conditions.push(build_where_condition(&col_info));
                    }
                }

                if let Some(seq_col) = sequence_col {
                    if let Some(condition) = build_sequence_condition(seq_col, $op_type) {
                        where_conditions.push(condition);
                    }
                }

                query.push_str(&build_where_clause(&where_conditions));

                let params: Vec<&(dyn ToSql + Sync)> =
                    owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

                database
                    .with_transaction(&query, &params, |_| async move { Ok(()) })
                    .await
                    .map_err(|e| e.to_string())?;

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
