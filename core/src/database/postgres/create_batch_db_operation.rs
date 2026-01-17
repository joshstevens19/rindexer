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
                RESERVED_KEYWORDS,
            };

            async fn execute_batch(
                database: &PostgresClient,
                batch: &[$result_type],
            ) -> Result<(), String> {
                if batch.is_empty() {
                    return Ok(());
                }

                let columns = $columns_def(&batch[0]);

                let cte_cols_and_types: Vec<(&str, &str)> =
                    columns.iter().map(|col| (col.name, col.sql_type.as_str())).collect();

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

                let formatted_cte_cols = cte_cols_and_types
                    .iter()
                    .map(|(col, _)| {
                        if RESERVED_KEYWORDS.contains(col) {
                            format!("\"{}\"", col)
                        } else {
                            col.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let mut query = format!(
                    "
                    WITH raw_data ({}) AS (
                        VALUES
                    ",
                    formatted_cte_cols
                );

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

                if !distinct_cols.is_empty() && sequence_col.is_some() {
                    let seq_col = sequence_col.unwrap();

                    let quoted_distinct_cols = distinct_cols
                        .iter()
                        .map(|col| {
                            if RESERVED_KEYWORDS.contains(col) {
                                format!("\"{}\"", col)
                            } else {
                                col.to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    let quoted_order_cols = quoted_distinct_cols.clone();

                    let quoted_seq_col = if RESERVED_KEYWORDS.contains(&seq_col) {
                        format!("\"{}\"", seq_col)
                    } else {
                        seq_col.to_string()
                    };

                    query.push_str(&format!(
                        ",
                    to_process AS (
                        SELECT DISTINCT ON ({}) *
                        FROM raw_data
                        ORDER BY {}, {} DESC
                    )",
                        quoted_distinct_cols, quoted_order_cols, quoted_seq_col
                    ));
                } else {
                    query.push_str(
                        ",
                    to_process AS (
                        SELECT * FROM raw_data
                    )",
                    );
                }

                // Handle table name escaping for schema.table format
                let formatted_table_name = if $table_name.contains('.') {
                    let parts: Vec<&str> = $table_name.split('.').collect();
                    if parts.len() == 2 {
                        let schema = parts[0].trim_matches('"');
                        let table = parts[1].trim_matches('"');
                        format!("\"{}\".\"{}\"", schema, table)
                    } else {
                        $table_name.to_string()
                    }
                } else {
                    $table_name.to_string()
                };

                match $op_type {
                    BatchOperationType::Update => {
                        query.push_str(&format!("\nUPDATE {} am\nSET ", formatted_table_name));

                        let mut all_set_clauses: Vec<String> = Vec::new();

                        let set_clauses: Vec<String> = set_columns
                            .iter()
                            .map(|col| {
                                let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                                let table_col_name =
                                    column_def.table_column.unwrap_or(column_def.name);
                                let cte_col_name = column_def.name;

                                let column_name = if RESERVED_KEYWORDS.contains(&table_col_name) {
                                    format!("\"{}\"", table_col_name)
                                } else {
                                    table_col_name.to_string()
                                };

                                let tp_col = if RESERVED_KEYWORDS.contains(&cte_col_name) {
                                    format!("tp.\"{}\"", cte_col_name)
                                } else {
                                    format!("tp.{}", cte_col_name)
                                };

                                format!("{} = {}", column_name, tp_col)
                            })
                            .collect();

                        all_set_clauses.extend(set_clauses);

                        let add_clauses: Vec<String> = add_columns
                            .iter()
                            .map(|col| {
                                let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                                let table_col_name =
                                    column_def.table_column.unwrap_or(column_def.name);
                                let cte_col_name = column_def.name;

                                let column_name = if RESERVED_KEYWORDS.contains(&table_col_name) {
                                    format!("\"{}\"", table_col_name)
                                } else {
                                    table_col_name.to_string()
                                };

                                let tp_col = if RESERVED_KEYWORDS.contains(&cte_col_name) {
                                    format!("tp.\"{}\"", cte_col_name)
                                } else {
                                    format!("tp.{}", cte_col_name)
                                };

                                format!("{} = am.{} + {}", column_name, column_name, tp_col)
                            })
                            .collect();

                        all_set_clauses.extend(add_clauses);

                        let subtract_clauses: Vec<String> = subtract_columns
                            .iter()
                            .map(|col| {
                                let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                                let table_col_name =
                                    column_def.table_column.unwrap_or(column_def.name);
                                let cte_col_name = column_def.name;

                                let column_name = if RESERVED_KEYWORDS.contains(&table_col_name) {
                                    format!("\"{}\"", table_col_name)
                                } else {
                                    table_col_name.to_string()
                                };

                                let tp_col = if RESERVED_KEYWORDS.contains(&cte_col_name) {
                                    format!("tp.\"{}\"", cte_col_name)
                                } else {
                                    format!("tp.{}", cte_col_name)
                                };

                                format!("{} = am.{} - {}", column_name, column_name, tp_col)
                            })
                            .collect();

                        all_set_clauses.extend(subtract_clauses);

                        query.push_str(&all_set_clauses.join(", "));

                        query.push_str("\nFROM to_process tp");
                    }
                    BatchOperationType::Delete => {
                        query.push_str(&format!("\nDELETE FROM {} am", formatted_table_name));
                        query.push_str("\nUSING to_process tp");
                    }
                    BatchOperationType::Upsert => {
                        let all_columns: Vec<&str> = columns.iter().map(|col| col.name).collect();

                        let formatted_columns = all_columns
                            .iter()
                            .map(|col| {
                                if RESERVED_KEYWORDS.contains(col) {
                                    format!("\"{}\"", col)
                                } else {
                                    col.to_string()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        let tp_columns = all_columns
                            .iter()
                            .map(|col| {
                                if RESERVED_KEYWORDS.contains(col) {
                                    format!("tp.\"{}\"", col)
                                } else {
                                    format!("tp.{}", col)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");

                        query.push_str(&format!(
                            "\nINSERT INTO {} ({})\nSELECT {}\nFROM to_process tp",
                            formatted_table_name, formatted_columns, tp_columns
                        ));

                        let conflict_columns = if !where_columns.is_empty() {
                            where_columns
                                .iter()
                                .map(|col| {
                                    if RESERVED_KEYWORDS.contains(col) {
                                        format!("\"{}\"", col)
                                    } else {
                                        col.to_string()
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ")
                        } else {
                            distinct_cols
                                .iter()
                                .map(|col| {
                                    if RESERVED_KEYWORDS.contains(col) {
                                        format!("\"{}\"", col)
                                    } else {
                                        col.to_string()
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join(", ")
                        };

                        if !conflict_columns.is_empty() {
                            query.push_str(&format!("\nON CONFLICT ({})", conflict_columns));

                            let mut update_clauses: Vec<String> = Vec::new();

                            let set_clauses: Vec<String> = set_columns
                                .iter()
                                .filter(|col| {
                                    !where_columns.contains(col) && !distinct_cols.contains(col)
                                })
                                .map(|col| {
                                    let column_name = if RESERVED_KEYWORDS.contains(col) {
                                        format!("\"{}\"", col)
                                    } else {
                                        col.to_string()
                                    };
                                    format!("{} = EXCLUDED.{}", column_name, column_name)
                                })
                                .collect();
                            update_clauses.extend(set_clauses);

                            let add_clauses: Vec<String> = add_columns
                                .iter()
                                .filter(|col| {
                                    !where_columns.contains(col) && !distinct_cols.contains(col)
                                })
                                .map(|col| {
                                    let column_name = if RESERVED_KEYWORDS.contains(col) {
                                        format!("\"{}\"", col)
                                    } else {
                                        col.to_string()
                                    };
                                    format!(
                                        "{} = {}.{} + EXCLUDED.{}",
                                        column_name, formatted_table_name, column_name, column_name
                                    )
                                })
                                .collect();
                            update_clauses.extend(add_clauses);

                            let subtract_clauses: Vec<String> = subtract_columns
                                .iter()
                                .filter(|col| {
                                    !where_columns.contains(col) && !distinct_cols.contains(col)
                                })
                                .map(|col| {
                                    let column_name = if RESERVED_KEYWORDS.contains(col) {
                                        format!("\"{}\"", col)
                                    } else {
                                        col.to_string()
                                    };
                                    format!(
                                        "{} = {}.{} - EXCLUDED.{}",
                                        column_name, formatted_table_name, column_name, column_name
                                    )
                                })
                                .collect();
                            update_clauses.extend(subtract_clauses);

                            if !update_clauses.is_empty() {
                                query.push_str(&format!(
                                    "\nDO UPDATE SET {}",
                                    update_clauses.join(", ")
                                ));
                            } else {
                                query.push_str("\nDO NOTHING");
                            }
                        } else {
                            query.push_str("\nON CONFLICT DO NOTHING");
                        }

                        let params: Vec<&(dyn ToSql + Sync)> =
                            owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

                        database
                            .with_transaction(&query, &params, |_| async move { Ok(()) })
                            .await
                            .map_err(|e| e.to_string())?;

                        return Ok(());
                    }
                }

                let mut where_conditions = Vec::new();

                for col in &where_columns {
                    let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                    let table_col = column_def.table_column.unwrap_or(column_def.name);

                    let am_col = if RESERVED_KEYWORDS.contains(&table_col) {
                        format!("am.\"{}\"", table_col)
                    } else {
                        format!("am.{}", table_col)
                    };

                    let tp_col = if RESERVED_KEYWORDS.contains(col) {
                        format!("tp.\"{}\"", col)
                    } else {
                        format!("tp.{}", col)
                    };

                    where_conditions.push(format!("{} = {}", am_col, tp_col));
                }

                for col in &distinct_cols {
                    if !where_columns.contains(col) {
                        let column_def = columns.iter().find(|c| c.name == *col).unwrap();
                        let table_col = column_def.table_column.unwrap_or(column_def.name);

                        let am_col = if RESERVED_KEYWORDS.contains(&table_col) {
                            format!("am.\"{}\"", table_col)
                        } else {
                            format!("am.{}", table_col)
                        };

                        let tp_col = if RESERVED_KEYWORDS.contains(col) {
                            format!("tp.\"{}\"", col)
                        } else {
                            format!("tp.{}", col)
                        };

                        where_conditions.push(format!("{} = {}", am_col, tp_col));
                    }
                }

                if let Some(seq_col) = sequence_col {
                    let seq_col_name = if RESERVED_KEYWORDS.contains(&seq_col) {
                        format!("\"{}\"", seq_col)
                    } else {
                        seq_col.to_string()
                    };

                    match $op_type {
                        BatchOperationType::Update => {
                            where_conditions
                                .push(format!("tp.{} > am.{}", seq_col_name, seq_col_name));
                        }
                        BatchOperationType::Delete => {
                            where_conditions
                                .push(format!("tp.{} >= am.{}", seq_col_name, seq_col_name));
                        }
                        BatchOperationType::Upsert => {
                            // Sequence handling already done above
                        }
                    }
                }

                if !where_conditions.is_empty() {
                    query.push_str("\nWHERE ");
                    query.push_str(&where_conditions.join("\n  AND "));
                }

                let params: Vec<&(dyn ToSql + Sync)> =
                    owned_params.iter().map(|param| param as &(dyn ToSql + Sync)).collect();

                // println!("{}", query);
                // println!("{:?}", params);

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
