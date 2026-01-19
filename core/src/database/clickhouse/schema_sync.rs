//! Schema synchronization for custom tables in ClickHouse.
//!
//! Detects differences between YAML-defined table schemas and actual database schemas,
//! and provides mechanisms to apply migrations.

use std::collections::{HashMap, HashSet};

use clickhouse::Row;
use serde::Deserialize;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::helpers::camel_to_snake;
use crate::manifest::contract::{injected_columns, Table};
use crate::manifest::core::Manifest;

/// Represents a detected schema change that may need to be applied.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// A new column needs to be added to the table.
    AddColumn {
        table_full_name: String,
        column_name: String,
        column_type: String,
        default_value: Option<String>,
    },
    /// A column exists in DB but not in YAML (user removed it).
    RemoveColumn { table_full_name: String, column_name: String },
    /// ORDER BY columns have changed (equivalent to PK change in ClickHouse).
    ChangeOrderBy {
        table_full_name: String,
        current_order_by: Vec<String>,
        new_order_by: Vec<String>,
    },
    /// Column type has changed (requires manual migration).
    ColumnTypeChanged {
        table_full_name: String,
        column_name: String,
        current_type: String,
        new_type: String,
    },
}

impl SchemaChange {
    /// Returns true if this change is safe to auto-apply without user confirmation.
    pub fn is_safe(&self) -> bool {
        matches!(self, SchemaChange::AddColumn { .. })
    }

    /// Returns a human-readable description of the change.
    pub fn description(&self) -> String {
        match self {
            SchemaChange::AddColumn {
                table_full_name,
                column_name,
                column_type,
                default_value,
            } => {
                let default_str = match default_value {
                    Some(v) => format!(" DEFAULT {}", v),
                    None => String::new(),
                };
                format!(
                    "Add column '{}' ({}){} to table '{}'",
                    column_name, column_type, default_str, table_full_name
                )
            }
            SchemaChange::RemoveColumn { table_full_name, column_name } => {
                format!(
                    "Column '{}' exists in database but not in YAML for table '{}'",
                    column_name, table_full_name
                )
            }
            SchemaChange::ChangeOrderBy { table_full_name, current_order_by, new_order_by } => {
                format!(
                    "ORDER BY change for table '{}': ({}) -> ({})",
                    table_full_name,
                    current_order_by.join(", "),
                    new_order_by.join(", ")
                )
            }
            SchemaChange::ColumnTypeChanged {
                table_full_name,
                column_name,
                current_type,
                new_type,
            } => {
                format!(
                    "Column '{}' type changed in table '{}': {} -> {}",
                    column_name, table_full_name, current_type, new_type
                )
            }
        }
    }
}

/// Row type for querying column information from system.columns.
#[derive(Debug, Clone, Row, Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

/// Row type for querying table information from system.tables.
#[derive(Debug, Clone, Row, Deserialize)]
struct TableInfo {
    sorting_key: String,
}

/// Row type for checking table existence.
#[derive(Debug, Clone, Row, Deserialize)]
struct ExistsResult {
    exists: u8,
}

/// Queries the existing columns for a table from ClickHouse.
async fn get_existing_columns(
    client: &ClickhouseClient,
    database: &str,
    table_name: &str,
) -> Result<Vec<ColumnInfo>, String> {
    let query = format!(
        "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
        database, table_name
    );

    client
        .query_all::<ColumnInfo>(&query)
        .await
        .map_err(|e| format!("Failed to query columns: {}", e))
}

/// Queries the ORDER BY (sorting key) columns for a table from ClickHouse.
async fn get_order_by_columns(
    client: &ClickhouseClient,
    database: &str,
    table_name: &str,
) -> Result<Option<Vec<String>>, String> {
    let query = format!(
        "SELECT sorting_key FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, table_name
    );

    let result = client
        .query_optional::<TableInfo>(&query)
        .await
        .map_err(|e| format!("Failed to query sorting key: {}", e))?;

    match result {
        Some(info) if !info.sorting_key.is_empty() => {
            // Parse sorting_key which is a comma-separated list of columns
            // It may include backticks, so we need to clean them
            let columns: Vec<String> = info
                .sorting_key
                .split(',')
                .map(|s| s.trim().trim_matches('`').to_string())
                .collect();
            Ok(Some(columns))
        }
        _ => Ok(None),
    }
}

/// Checks if a table exists in the database.
async fn table_exists(
    client: &ClickhouseClient,
    database: &str,
    table_name: &str,
) -> Result<bool, String> {
    let query = format!(
        "SELECT count() > 0 as exists FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, table_name
    );

    let result = client
        .query_optional::<ExistsResult>(&query)
        .await
        .map_err(|e| format!("Failed to check table existence: {}", e))?;

    Ok(result.map(|r| r.exists > 0).unwrap_or(false))
}

/// Get all column names that should exist for a table based on YAML definition.
fn get_expected_columns(table: &Table) -> HashMap<String, String> {
    let mut columns: HashMap<String, String> = HashMap::new();

    // Network column (unless cross_chain)
    if !table.cross_chain {
        columns.insert("network".to_string(), "String".to_string());
    }

    // User-defined columns
    for column in &table.columns {
        let ch_type = if let Some(col_type) = &column.column_type {
            col_type.to_clickhouse_type()
        } else {
            // Type not yet resolved - use a placeholder
            "unknown".to_string()
        };
        columns.insert(column.name.clone(), ch_type);
    }

    // Injected columns
    columns.insert(injected_columns::LAST_UPDATED_BLOCK.to_string(), "UInt64".to_string());
    columns.insert(
        injected_columns::LAST_UPDATED_AT.to_string(),
        "Nullable(DateTime('UTC'))".to_string(),
    );
    columns.insert(injected_columns::TX_HASH.to_string(), "FixedString(66)".to_string());
    columns.insert(injected_columns::BLOCK_HASH.to_string(), "FixedString(66)".to_string());
    columns.insert(injected_columns::CONTRACT_ADDRESS.to_string(), "FixedString(42)".to_string());
    columns.insert(injected_columns::RINDEXER_SEQUENCE_ID.to_string(), "UInt128".to_string());

    columns
}

/// Get expected ORDER BY columns for a table.
fn get_expected_order_by_columns(table: &Table) -> Vec<String> {
    let mut order_by: Vec<String> = vec![];

    if !table.cross_chain {
        order_by.push("network".to_string());
    }

    for pk_col in table.primary_key_columns() {
        order_by.push(pk_col.to_string());
    }

    order_by
}

/// Detects schema changes for all custom tables in the manifest.
pub async fn detect_schema_changes(
    client: &ClickhouseClient,
    manifest: &Manifest,
) -> Result<Vec<SchemaChange>, String> {
    let mut changes: Vec<SchemaChange> = vec![];

    for contract in &manifest.contracts {
        let Some(tables) = &contract.tables else {
            continue;
        };

        let database = generate_indexer_contract_schema_name(&manifest.name, &contract.name);

        for table in tables {
            let table_name = camel_to_snake(&table.name);
            let table_full_name = format!("{}.{}", database, table_name);

            // Check if table exists
            if !table_exists(client, &database, &table_name).await? {
                // Table doesn't exist yet, will be created normally
                continue;
            }

            // Get existing columns
            let existing_columns = get_existing_columns(client, &database, &table_name).await?;
            let existing_col_names: HashSet<String> =
                existing_columns.iter().map(|c| c.name.clone()).collect();
            let existing_col_types: HashMap<String, String> =
                existing_columns.iter().map(|c| (c.name.clone(), c.data_type.clone())).collect();

            // Get expected columns from YAML
            let expected_columns = get_expected_columns(table);
            let expected_col_names: HashSet<String> = expected_columns.keys().cloned().collect();

            // Find new columns (in YAML but not in DB)
            for col_name in expected_col_names.difference(&existing_col_names) {
                let col_type = expected_columns.get(col_name).unwrap();

                // Skip columns with unknown types (not yet resolved from ABI)
                if col_type == "unknown" {
                    continue;
                }

                // Look up the default value from the column definition
                let default_value = table
                    .columns
                    .iter()
                    .find(|c| &c.name == col_name)
                    .and_then(|c| c.default.clone())
                    .map(|d| {
                        // ClickHouse needs proper quoting for string defaults
                        if col_type == "String" || col_type.starts_with("FixedString") {
                            format!("'{}'", d.replace('\'', "\\'"))
                        } else {
                            d
                        }
                    });

                changes.push(SchemaChange::AddColumn {
                    table_full_name: table_full_name.clone(),
                    column_name: col_name.clone(),
                    column_type: col_type.clone(),
                    default_value,
                });
            }

            // Find removed columns (in DB but not in YAML)
            // Exclude internal columns that might have been added by migrations
            let internal_columns: HashSet<&str> =
                ["rindexer_id", "block_timestamp"].into_iter().collect();

            for col_name in existing_col_names.difference(&expected_col_names) {
                if !internal_columns.contains(col_name.as_str()) {
                    changes.push(SchemaChange::RemoveColumn {
                        table_full_name: table_full_name.clone(),
                        column_name: col_name.clone(),
                    });
                }
            }

            // Check for ORDER BY changes
            if let Some(current_order_by) =
                get_order_by_columns(client, &database, &table_name).await?
            {
                let expected_order_by = get_expected_order_by_columns(table);

                if current_order_by != expected_order_by {
                    changes.push(SchemaChange::ChangeOrderBy {
                        table_full_name: table_full_name.clone(),
                        current_order_by,
                        new_order_by: expected_order_by,
                    });
                }
            }

            // Check for type changes (only for user-defined columns with resolved types)
            for column in &table.columns {
                if column.column_type.is_none() {
                    continue;
                }

                if let Some(existing_type) = existing_col_types.get(&column.name) {
                    let expected_type = expected_columns.get(&column.name).unwrap();
                    if expected_type == "unknown" {
                        continue;
                    }

                    // Normalize types for comparison
                    let normalized_existing = normalize_ch_type(existing_type);
                    let normalized_expected = normalize_ch_type(expected_type);

                    if normalized_existing != normalized_expected {
                        changes.push(SchemaChange::ColumnTypeChanged {
                            table_full_name: table_full_name.clone(),
                            column_name: column.name.clone(),
                            current_type: existing_type.clone(),
                            new_type: expected_type.clone(),
                        });
                    }
                }
            }
        }
    }

    Ok(changes)
}

/// Normalize ClickHouse type names for comparison.
fn normalize_ch_type(ch_type: &str) -> String {
    let t = ch_type.to_lowercase();
    // Handle nullable wrapper
    let inner =
        if t.starts_with("nullable(") && t.ends_with(')') { &t[9..t.len() - 1] } else { &t };

    match inner {
        "string" => "string".to_string(),
        "int64" => "int64".to_string(),
        "uint64" => "uint64".to_string(),
        "int128" => "int128".to_string(),
        "uint128" => "uint128".to_string(),
        "int256" => "int256".to_string(),
        "uint256" => "uint256".to_string(),
        "bool" => "bool".to_string(),
        _ => inner.to_string(),
    }
}

/// Applies a schema change to the database.
pub async fn apply_schema_change(
    client: &ClickhouseClient,
    change: &SchemaChange,
) -> Result<(), String> {
    match change {
        SchemaChange::AddColumn { table_full_name, column_name, column_type, default_value } => {
            let default_clause = match default_value {
                Some(val) => format!(" DEFAULT {}", val),
                None => String::new(),
            };
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN `{}` {}{}",
                table_full_name, column_name, column_type, default_clause
            );

            client.execute(&sql).await.map_err(|e| format!("Failed to add column: {}", e))?;
        }
        SchemaChange::RemoveColumn { table_full_name, column_name } => {
            let sql = format!("ALTER TABLE {} DROP COLUMN `{}`", table_full_name, column_name);

            client.execute(&sql).await.map_err(|e| format!("Failed to remove column: {}", e))?;
        }
        SchemaChange::ChangeOrderBy { table_full_name, current_order_by: _, new_order_by } => {
            // ClickHouse supports MODIFY ORDER BY for ReplacingMergeTree
            let order_by_cols =
                new_order_by.iter().map(|c| format!("`{}`", c)).collect::<Vec<_>>().join(", ");

            let sql =
                format!("ALTER TABLE {} MODIFY ORDER BY ({})", table_full_name, order_by_cols);

            client.execute(&sql).await.map_err(|e| format!("Failed to change ORDER BY: {}", e))?;
        }
        SchemaChange::ColumnTypeChanged { .. } => {
            // Type changes require manual migration in ClickHouse
            return Err(
                "Column type changes require manual migration. Please backup your data and recreate the table.".to_string(),
            );
        }
    }

    Ok(())
}
