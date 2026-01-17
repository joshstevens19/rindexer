// Types are used by exported macros, not internally
#![allow(dead_code)]
use crate::EthereumSqlTypeWrapper;

/// The type of batch operation to perform.
pub enum BatchOperationType {
    Update,
    Delete,
    Upsert,
}

/// Column behavior determines how columns are used in deduplication and ordering.
pub enum BatchOperationColumnBehavior {
    /// Normal column with no special behavior
    Normal,
    /// Column used for deduplication (DISTINCT ON in Postgres, part of ORDER BY in ClickHouse)
    Distinct,
    /// Column used for ordering/sequencing (determines which row to keep when deduplicating)
    Sequence,
}

/// SQL type mapping for batch operations.
pub enum BatchOperationSqlType {
    Bytea,
    Numeric,
    Bool,
    Jsonb,
    Varchar,
    Bigint,
    DateTime,
    Custom(&'static str),
}

impl BatchOperationSqlType {
    /// Returns the PostgreSQL type string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            BatchOperationSqlType::Bytea => "BYTEA",
            BatchOperationSqlType::Numeric => "NUMERIC",
            BatchOperationSqlType::Bool => "BOOL",
            BatchOperationSqlType::Jsonb => "JSONB",
            BatchOperationSqlType::Varchar => "VARCHAR",
            BatchOperationSqlType::Bigint => "BIGINT",
            BatchOperationSqlType::DateTime => "TIMESTAMPTZ",
            BatchOperationSqlType::Custom(type_name) => type_name,
        }
    }

    /// Returns the ClickHouse type string representation.
    pub fn as_clickhouse_str(&self) -> &'static str {
        match self {
            BatchOperationSqlType::Bytea => "String",
            BatchOperationSqlType::Numeric => "String",
            BatchOperationSqlType::Bool => "Bool",
            BatchOperationSqlType::Jsonb => "String",
            BatchOperationSqlType::Varchar => "String",
            BatchOperationSqlType::Bigint => "Int64",
            BatchOperationSqlType::DateTime => "DateTime('UTC')",
            BatchOperationSqlType::Custom(type_name) => type_name,
        }
    }
}

/// Action to perform on a column during batch operations.
pub enum BatchOperationAction {
    /// No action (column is included but not updated)
    Nothing,
    /// Set the column to the new value
    Set,
    /// Use column in WHERE clause for matching
    Where,
    /// Add the new value to the existing value
    Add,
    /// Subtract the new value from the existing value
    Subtract,
}

/// Definition of a column for batch operations.
pub struct ColumnDefinition {
    pub name: &'static str,
    pub table_column: Option<&'static str>,
    pub value: EthereumSqlTypeWrapper,
    pub sql_type: BatchOperationSqlType,
    pub behavior: BatchOperationColumnBehavior,
    pub action: BatchOperationAction,
}

/// Creates a column definition for batch operations.
pub fn column(
    name: &'static str,
    value: EthereumSqlTypeWrapper,
    sql_type: BatchOperationSqlType,
    behavior: BatchOperationColumnBehavior,
    action: BatchOperationAction,
) -> ColumnDefinition {
    ColumnDefinition { name, table_column: None, value, sql_type, behavior, action }
}

/// Creates a column definition with a separate table column name.
#[allow(clippy::too_many_arguments)]
pub fn column_with_table_name(
    name: &'static str,
    table_column: &'static str,
    value: EthereumSqlTypeWrapper,
    sql_type: BatchOperationSqlType,
    behavior: BatchOperationColumnBehavior,
    action: BatchOperationAction,
) -> ColumnDefinition {
    ColumnDefinition { name, table_column: Some(table_column), value, sql_type, behavior, action }
}

/// Reserved SQL keywords that need quoting.
pub const RESERVED_KEYWORDS: &[&str] =
    &["group", "user", "order", "table", "index", "primary", "key"];
