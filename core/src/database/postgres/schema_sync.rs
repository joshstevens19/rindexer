//! Schema synchronization for custom tables.
//!
//! Detects differences between YAML-defined table schemas and actual database schemas,
//! and provides mechanisms to apply migrations.

use std::collections::{HashMap, HashSet};

use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::client::PostgresClient;
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
    /// Primary key columns have changed.
    ChangePrimaryKey {
        table_full_name: String,
        current_pk_columns: Vec<String>,
        new_pk_columns: Vec<String>,
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
                    None => " DEFAULT NULL".to_string(),
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
            SchemaChange::ChangePrimaryKey {
                table_full_name,
                current_pk_columns,
                new_pk_columns,
            } => {
                format!(
                    "Primary key change for table '{}': ({}) -> ({})",
                    table_full_name,
                    current_pk_columns.join(", "),
                    new_pk_columns.join(", ")
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

/// Information about a column in the database.
#[derive(Debug, Clone)]
struct DbColumn {
    name: String,
    data_type: String,
}

/// Information about a table's primary key.
#[derive(Debug, Clone)]
struct DbPrimaryKey {
    columns: Vec<String>,
}

/// Queries the existing columns for a table from PostgreSQL.
async fn get_existing_columns(
    client: &PostgresClient,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<DbColumn>, String> {
    let query = r#"
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    "#;

    let rows = client
        .query(query, &[&schema_name, &table_name])
        .await
        .map_err(|e| format!("Failed to query columns: {}", e))?;

    let columns = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            DbColumn { name, data_type }
        })
        .collect();

    Ok(columns)
}

/// Queries the primary key columns for a table from PostgreSQL.
async fn get_primary_key_columns(
    client: &PostgresClient,
    schema_name: &str,
    table_name: &str,
) -> Result<Option<DbPrimaryKey>, String> {
    let query = r#"
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = $1
            AND tc.table_name = $2
        ORDER BY kcu.ordinal_position
    "#;

    let rows = client
        .query(query, &[&schema_name, &table_name])
        .await
        .map_err(|e| format!("Failed to query primary key: {}", e))?;

    if rows.is_empty() {
        return Ok(None);
    }

    let columns = rows.iter().map(|row| row.get::<_, String>(0)).collect();

    Ok(Some(DbPrimaryKey { columns }))
}

/// Checks if a table exists in the database.
async fn table_exists(
    client: &PostgresClient,
    schema_name: &str,
    table_name: &str,
) -> Result<bool, String> {
    let query = r#"
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
    "#;

    let rows = client
        .query(query, &[&schema_name, &table_name])
        .await
        .map_err(|e| format!("Failed to check table existence: {}", e))?;

    let exists: bool = rows.first().map(|row| row.get(0)).unwrap_or(false);
    Ok(exists)
}

/// Get all column names that should exist for a table based on YAML definition.
fn get_expected_columns(table: &Table) -> HashMap<String, String> {
    let mut columns: HashMap<String, String> = HashMap::new();

    // Network column (unless cross_chain)
    if !table.cross_chain {
        columns.insert("network".to_string(), "character varying".to_string());
    }

    // User-defined columns
    for column in &table.columns {
        // Use column_type if available, otherwise skip type comparison for this column
        // Types may not be resolved yet at schema sync time
        let info_schema_type = if let Some(col_type) = &column.column_type {
            let pg_type = col_type.to_postgres_type().to_lowercase();
            // Map to information_schema types
            match pg_type.as_str() {
                "numeric" => "numeric",
                "bigint" => "bigint",
                "boolean" => "boolean",
                "text" => "text",
                "char(42)" => "character",
                "char(66)" => "character",
                "bytea" => "bytea",
                _ => &pg_type,
            }
            .to_string()
        } else {
            // Type not yet resolved - use a placeholder that won't trigger type mismatch
            // The column will still be detected for add/remove checks
            "unknown".to_string()
        };
        columns.insert(column.name.clone(), info_schema_type);
    }

    // Injected columns
    columns.insert(injected_columns::LAST_UPDATED_BLOCK.to_string(), "bigint".to_string());
    columns.insert(
        injected_columns::LAST_UPDATED_AT.to_string(),
        "timestamp with time zone".to_string(),
    );
    columns.insert(injected_columns::TX_HASH.to_string(), "character".to_string());
    columns.insert(injected_columns::BLOCK_HASH.to_string(), "character".to_string());
    columns.insert(injected_columns::CONTRACT_ADDRESS.to_string(), "character".to_string());
    columns.insert(injected_columns::RINDEXER_SEQUENCE_ID.to_string(), "numeric".to_string());

    columns
}

/// Get expected primary key columns for a table.
fn get_expected_pk_columns(table: &Table) -> Vec<String> {
    use crate::manifest::contract::injected_columns;

    let mut pk_columns: Vec<String> = vec![];

    if !table.cross_chain {
        pk_columns.push("network".to_string());
    }

    // For insert-only tables, use auto-incrementing rindexer_id as PK
    // For other tables, use the where clause columns as PK
    if table.is_insert_only() {
        pk_columns.push(injected_columns::RINDEXER_ID.to_string());
    } else {
        for pk_col in table.primary_key_columns() {
            pk_columns.push(pk_col.to_string());
        }
    }

    pk_columns
}

/// Detects schema changes for all custom tables in the manifest.
pub async fn detect_schema_changes(
    client: &PostgresClient,
    manifest: &Manifest,
) -> Result<Vec<SchemaChange>, String> {
    let mut changes: Vec<SchemaChange> = vec![];

    for contract in &manifest.contracts {
        let Some(tables) = &contract.tables else {
            continue;
        };

        let schema_name = generate_indexer_contract_schema_name(&manifest.name, &contract.name);

        for table in tables {
            let table_name = camel_to_snake(&table.name);
            let table_full_name = format!("{}.{}", schema_name, table_name);

            // Check if table exists
            if !table_exists(client, &schema_name, &table_name).await? {
                // Table doesn't exist yet, will be created normally
                continue;
            }

            // Get existing columns
            let existing_columns = get_existing_columns(client, &schema_name, &table_name).await?;
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
                // These will be created when the table is set up with resolved types
                if col_type == "unknown" {
                    continue;
                }

                // Convert back to PostgreSQL CREATE type format
                let pg_type = match col_type.as_str() {
                    "character varying" => "VARCHAR(50)",
                    "character" => {
                        // Determine length based on column name
                        if col_name == injected_columns::TX_HASH
                            || col_name == injected_columns::BLOCK_HASH
                        {
                            "CHAR(66)"
                        } else if col_name == injected_columns::CONTRACT_ADDRESS {
                            "CHAR(42)"
                        } else {
                            "TEXT"
                        }
                    }
                    "timestamp with time zone" => "TIMESTAMPTZ",
                    other => other,
                };

                // Look up the default value from the column definition
                let default_value = table
                    .columns
                    .iter()
                    .find(|c| &c.name == col_name)
                    .and_then(|c| c.default.clone());

                changes.push(SchemaChange::AddColumn {
                    table_full_name: table_full_name.clone(),
                    column_name: col_name.clone(),
                    column_type: pg_type.to_uppercase(),
                    default_value,
                });
            }

            // Find removed columns (in DB but not in YAML)
            // Exclude internal columns that might have been added by migrations
            let internal_columns: HashSet<&str> = [
                "rindexer_id",
                "block_timestamp", // Migration V1
            ]
            .into_iter()
            .collect();

            for col_name in existing_col_names.difference(&expected_col_names) {
                if !internal_columns.contains(col_name.as_str()) {
                    changes.push(SchemaChange::RemoveColumn {
                        table_full_name: table_full_name.clone(),
                        column_name: col_name.clone(),
                    });
                }
            }

            // Check for primary key changes
            if let Some(current_pk) =
                get_primary_key_columns(client, &schema_name, &table_name).await?
            {
                let expected_pk = get_expected_pk_columns(table);

                if current_pk.columns != expected_pk {
                    changes.push(SchemaChange::ChangePrimaryKey {
                        table_full_name: table_full_name.clone(),
                        current_pk_columns: current_pk.columns,
                        new_pk_columns: expected_pk,
                    });
                }
            }

            // Check for type changes (only for user-defined columns with resolved types)
            for column in &table.columns {
                // Skip type comparison if column type isn't resolved yet
                if column.column_type.is_none() {
                    continue;
                }

                if let Some(existing_type) = existing_col_types.get(&column.name) {
                    let expected_type = expected_columns.get(&column.name).unwrap();
                    // Skip if expected type is unknown (not resolved)
                    if expected_type == "unknown" {
                        continue;
                    }

                    // Normalize types for comparison
                    let normalized_existing = normalize_pg_type(existing_type);
                    let normalized_expected = normalize_pg_type(expected_type);

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

/// Normalize PostgreSQL type names for comparison.
fn normalize_pg_type(pg_type: &str) -> String {
    let t = pg_type.to_lowercase();
    match t.as_str() {
        "character varying" | "varchar" => "varchar".to_string(),
        "character" | "char" | "bpchar" => "char".to_string(),
        "timestamp with time zone" | "timestamptz" => "timestamptz".to_string(),
        "integer" | "int" | "int4" => "integer".to_string(),
        "bigint" | "int8" => "bigint".to_string(),
        "numeric" | "decimal" => "numeric".to_string(),
        "boolean" | "bool" => "boolean".to_string(),
        _ => t,
    }
}

/// Applies a schema change to the database.
pub async fn apply_schema_change(
    client: &PostgresClient,
    change: &SchemaChange,
) -> Result<(), String> {
    match change {
        SchemaChange::AddColumn { table_full_name, column_name, column_type, default_value } => {
            let default_clause = match default_value {
                Some(val) => format!("DEFAULT {}", val),
                None => "DEFAULT NULL".to_string(),
            };
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN \"{}\" {} {}",
                table_full_name, column_name, column_type, default_clause
            );

            client.batch_execute(&sql).await.map_err(|e| format!("Failed to add column: {}", e))?;
        }
        SchemaChange::RemoveColumn { table_full_name, column_name } => {
            let sql = format!("ALTER TABLE {} DROP COLUMN \"{}\"", table_full_name, column_name);

            client
                .batch_execute(&sql)
                .await
                .map_err(|e| format!("Failed to remove column: {}", e))?;
        }
        SchemaChange::ChangePrimaryKey {
            table_full_name,
            current_pk_columns: _,
            new_pk_columns,
        } => {
            // Get the constraint name
            let parts: Vec<&str> = table_full_name.split('.').collect();
            let (schema_name, table_name) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                return Err("Invalid table name format".to_string());
            };

            // Find the constraint name
            let query = r#"
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = $1 AND table_name = $2 AND constraint_type = 'PRIMARY KEY'
            "#;

            let rows = client
                .query(query, &[&schema_name, &table_name])
                .await
                .map_err(|e| format!("Failed to find PK constraint: {}", e))?;

            if let Some(row) = rows.first() {
                let constraint_name: String = row.get(0);

                // Drop old constraint
                let drop_sql = format!(
                    "ALTER TABLE {} DROP CONSTRAINT \"{}\"",
                    table_full_name, constraint_name
                );
                client
                    .batch_execute(&drop_sql)
                    .await
                    .map_err(|e| format!("Failed to drop old primary key: {}", e))?;
            }

            // Add new constraint
            let pk_cols =
                new_pk_columns.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");

            let add_sql = format!("ALTER TABLE {} ADD PRIMARY KEY ({})", table_full_name, pk_cols);

            client
                .batch_execute(&add_sql)
                .await
                .map_err(|e| format!("Failed to add new primary key: {}", e))?;
        }
        SchemaChange::ColumnTypeChanged { .. } => {
            // Type changes require manual migration
            return Err(
                "Column type changes require manual migration. Please backup your data and recreate the table.".to_string(),
            );
        }
    }

    Ok(())
}
