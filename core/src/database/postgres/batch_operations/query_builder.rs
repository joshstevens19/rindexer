//! Shared SQL query building logic for PostgreSQL batch operations.
//!
//! This module contains the common SQL generation functions used by both
//! the `create_batch_postgres_operation!` macro and the dynamic batch operation function.

use crate::database::batch_operations::{BatchOperationType, RESERVED_KEYWORDS};

/// Quotes an identifier if it's a reserved keyword.
#[inline]
pub fn quote_identifier(name: &str) -> String {
    if RESERVED_KEYWORDS.contains(&name) {
        format!("\"{}\"", name)
    } else {
        name.to_string()
    }
}

/// Formats a table name, handling schema.table format.
pub fn format_table_name(table_name: &str) -> String {
    if table_name.contains('.') {
        let parts: Vec<&str> = table_name.split('.').collect();
        if parts.len() == 2 {
            let schema = parts[0].trim_matches('"');
            let table = parts[1].trim_matches('"');
            format!("\"{}\".\"{}\"", schema, table)
        } else {
            table_name.to_string()
        }
    } else {
        table_name.to_string()
    }
}

/// Builds the CTE header: `WITH raw_data (col1, col2, ...) AS (VALUES`
pub fn build_cte_header(column_names: &[&str]) -> String {
    let formatted_cols =
        column_names.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

    format!(
        "
        WITH raw_data ({}) AS (
            VALUES
        ",
        formatted_cols
    )
}

/// Builds the `to_process` CTE with optional DISTINCT ON for deduplication.
pub fn build_to_process_cte(distinct_cols: &[&str], sequence_col: Option<&str>) -> String {
    if let (false, Some(seq_col)) = (distinct_cols.is_empty(), sequence_col) {
        let quoted_distinct_cols =
            distinct_cols.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

        let quoted_seq_col = quote_identifier(seq_col);

        format!(
            ",
        to_process AS (
            SELECT DISTINCT ON ({}) *
            FROM raw_data
            ORDER BY {}, {} DESC
        )",
            quoted_distinct_cols, quoted_distinct_cols, quoted_seq_col
        )
    } else {
        ",
        to_process AS (
            SELECT * FROM raw_data
        )"
        .to_string()
    }
}

/// Column info needed for building SET clauses.
pub struct ColumnInfo<'a> {
    pub name: &'a str,
    pub table_column: Option<&'a str>,
}

/// Builds SET clauses for UPDATE operations.
pub fn build_set_clause(column: &ColumnInfo, clause_type: SetClauseType) -> String {
    let table_col_name = column.table_column.unwrap_or(column.name);
    let cte_col_name = column.name;

    let column_name = quote_identifier(table_col_name);
    let tp_col = format!("tp.{}", quote_identifier(cte_col_name));

    match clause_type {
        SetClauseType::Set => format!("{} = {}", column_name, tp_col),
        SetClauseType::Add => format!("{} = am.{} + {}", column_name, column_name, tp_col),
        SetClauseType::Subtract => format!("{} = am.{} - {}", column_name, column_name, tp_col),
        SetClauseType::Max => format!("{} = GREATEST(am.{}, {})", column_name, column_name, tp_col),
        SetClauseType::Min => format!("{} = LEAST(am.{}, {})", column_name, column_name, tp_col),
    }
}

/// Type of SET clause to generate.
pub enum SetClauseType {
    Set,
    Add,
    Subtract,
    Max,
    Min,
}

/// Builds the UPDATE statement body.
pub fn build_update_body(formatted_table_name: &str, set_clauses: Vec<String>) -> String {
    let mut query = format!("\nUPDATE {} am\nSET ", formatted_table_name);
    query.push_str(&set_clauses.join(", "));
    query.push_str("\nFROM to_process tp");
    query
}

/// Builds the DELETE statement body.
pub fn build_delete_body(formatted_table_name: &str) -> String {
    format!("\nDELETE FROM {} am\nUSING to_process tp", formatted_table_name)
}

/// Builds the INSERT ... ON CONFLICT statement for upserts.
///
/// # Arguments
/// * `formatted_table_name` - The fully qualified table name
/// * `all_columns` - All columns to insert/update
/// * `conflict_columns` - Columns for ON CONFLICT detection (primary key)
/// * `update_clauses` - SET clauses for the update
/// * `sequence_col` - Optional sequence column for ordering (adds WHERE EXCLUDED.seq > table.seq)
/// * `custom_where` - Optional custom WHERE condition (e.g., for @table references)
pub fn build_upsert_body(
    formatted_table_name: &str,
    all_columns: &[&str],
    conflict_columns: &[&str],
    update_clauses: Vec<String>,
    sequence_col: Option<&str>,
    custom_where: Option<&str>,
) -> String {
    let formatted_columns =
        all_columns.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

    let tp_columns = all_columns
        .iter()
        .map(|col| format!("tp.{}", quote_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut query = format!(
        "\nINSERT INTO {} ({})\nSELECT {}\nFROM to_process tp",
        formatted_table_name, formatted_columns, tp_columns
    );

    if !conflict_columns.is_empty() {
        let conflict_cols_str =
            conflict_columns.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

        query.push_str(&format!("\nON CONFLICT ({})", conflict_cols_str));

        if !update_clauses.is_empty() {
            query.push_str(&format!("\nDO UPDATE SET {}", update_clauses.join(", ")));

            // Build WHERE conditions
            let mut where_conditions: Vec<String> = Vec::new();

            // Add sequence comparison if we have a sequence column
            if let Some(seq_col) = sequence_col {
                let seq_col_name = quote_identifier(seq_col);
                where_conditions.push(format!(
                    "EXCLUDED.{} > COALESCE({}.{}, 0)",
                    seq_col_name, formatted_table_name, seq_col_name
                ));
            }

            // Add custom WHERE condition (for @table references)
            if let Some(custom) = custom_where {
                where_conditions.push(custom.to_string());
            }

            if !where_conditions.is_empty() {
                query.push_str(&format!("\nWHERE {}", where_conditions.join(" AND ")));
            }
        } else {
            query.push_str("\nDO NOTHING");
        }
    } else {
        query.push_str("\nON CONFLICT DO NOTHING");
    }

    query
}

/// Builds a plain INSERT body (no conflict handling).
/// Used for time-series/history data where we always want to insert new rows.
///
/// # Arguments
/// * `formatted_table_name` - The fully qualified table name
/// * `all_columns` - All columns to insert
pub fn build_insert_body(formatted_table_name: &str, all_columns: &[&str]) -> String {
    let formatted_columns =
        all_columns.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

    let tp_columns = all_columns
        .iter()
        .map(|col| format!("tp.{}", quote_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "\nINSERT INTO {} ({})\nSELECT {}\nFROM to_process tp",
        formatted_table_name, formatted_columns, tp_columns
    )
}

/// Builds an upsert SET clause (uses EXCLUDED instead of tp).
pub fn build_upsert_set_clause(
    col: &str,
    formatted_table_name: &str,
    clause_type: UpsertClauseType,
) -> String {
    let column_name = quote_identifier(col);

    match clause_type {
        UpsertClauseType::Set => {
            format!("{} = EXCLUDED.{}", column_name, column_name)
        }
        UpsertClauseType::Add => {
            format!(
                "{} = COALESCE({}.{}, 0) + EXCLUDED.{}",
                column_name, formatted_table_name, column_name, column_name
            )
        }
        UpsertClauseType::Subtract => {
            format!(
                "{} = COALESCE({}.{}, 0) - EXCLUDED.{}",
                column_name, formatted_table_name, column_name, column_name
            )
        }
        UpsertClauseType::Max => {
            format!(
                "{} = GREATEST(COALESCE({}.{}, EXCLUDED.{}), EXCLUDED.{})",
                column_name, formatted_table_name, column_name, column_name, column_name
            )
        }
        UpsertClauseType::Min => {
            format!(
                "{} = LEAST(COALESCE({}.{}, EXCLUDED.{}), EXCLUDED.{})",
                column_name, formatted_table_name, column_name, column_name, column_name
            )
        }
    }
}

/// Type of upsert SET clause to generate.
pub enum UpsertClauseType {
    Set,
    Add,
    Subtract,
    Max,
    Min,
}

/// Builds WHERE conditions for UPDATE/DELETE operations.
pub fn build_where_condition(column: &ColumnInfo) -> String {
    let table_col = column.table_column.unwrap_or(column.name);

    let am_col = format!("am.{}", quote_identifier(table_col));
    let tp_col = format!("tp.{}", quote_identifier(column.name));

    format!("{} = {}", am_col, tp_col)
}

/// Builds the sequence comparison condition.
pub fn build_sequence_condition(seq_col: &str, op_type: BatchOperationType) -> Option<String> {
    let seq_col_name = quote_identifier(seq_col);

    match op_type {
        BatchOperationType::Update => Some(format!("tp.{} > am.{}", seq_col_name, seq_col_name)),
        BatchOperationType::Delete => Some(format!("tp.{} >= am.{}", seq_col_name, seq_col_name)),
        BatchOperationType::Upsert | BatchOperationType::Insert => None,
    }
}

/// Builds the complete WHERE clause from conditions.
pub fn build_where_clause(conditions: &[String]) -> String {
    if conditions.is_empty() {
        String::new()
    } else {
        format!("\nWHERE {}", conditions.join("\n  AND "))
    }
}
