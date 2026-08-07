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

/// Describes how a column should be aggregated in GROUP BY.
pub enum ColumnAggregate {
    /// Column is a GROUP BY key (distinct/where columns).
    GroupKey,
    /// SUM the values (for add/subtract actions).
    Sum,
    /// Take the MAX value (for max action or sequence column).
    Max,
    /// Take the MIN value (for min action).
    Min,
    /// Take the value from the row with the highest sequence (for set action).
    LastBySeq,
}

/// Builds the `to_process` CTE with GROUP BY and aggregations for arithmetic columns.
///
/// When a batch contains duplicate keys with arithmetic actions (add/subtract/max/min),
/// `DISTINCT ON` would discard all but one row per key. Instead, this function generates
/// a `GROUP BY` with appropriate aggregations so all values contribute:
/// - `add`/`subtract` columns → `SUM()` (accumulated in ON CONFLICT SET clause)
/// - `max` columns → `MAX()`
/// - `min` columns → `MIN()`
/// - `set` columns → last value by sequence (`(array_agg(col ORDER BY seq DESC))[1]`)
/// - sequence column → `MAX()` (latest sequence per group)
pub fn build_to_process_cte_aggregated(
    columns: &[(&str, ColumnAggregate)],
    sequence_col: Option<&str>,
) -> String {
    let group_keys: Vec<String> = columns
        .iter()
        .filter_map(|(name, agg)| match agg {
            ColumnAggregate::GroupKey => Some(quote_identifier(name)),
            _ => None,
        })
        .collect();

    if group_keys.is_empty() {
        return ",
        to_process AS (
            SELECT * FROM raw_data
        )"
        .to_string();
    }

    let quoted_seq = sequence_col.map(quote_identifier);

    let select_exprs: Vec<String> = columns
        .iter()
        .map(|(name, agg)| {
            let qname = quote_identifier(name);
            match agg {
                ColumnAggregate::GroupKey => qname,
                ColumnAggregate::Sum => format!("SUM({}) AS {}", qname, qname),
                ColumnAggregate::Max => format!("MAX({}) AS {}", qname, qname),
                ColumnAggregate::Min => format!("MIN({}) AS {}", qname, qname),
                ColumnAggregate::LastBySeq => {
                    if let Some(ref seq) = quoted_seq {
                        format!("(array_agg({} ORDER BY {} DESC))[1] AS {}", qname, seq, qname)
                    } else {
                        // No sequence column — take an arbitrary value. array_agg is
                        // type-agnostic; MAX would fail for BOOLEAN/BYTEA columns.
                        format!("(array_agg({}))[1] AS {}", qname, qname)
                    }
                }
            }
        })
        .collect();

    format!(
        ",
        to_process AS (
            SELECT {}
            FROM raw_data
            GROUP BY {}
        )",
        select_exprs.join(", "),
        group_keys.join(", ")
    )
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
/// * `insert_exprs` - Per-column SQL expression overrides for the INSERT branch's
///   SELECT list (e.g., `(0 - tp."balance")` so a `subtract` that creates the row
///   starts from the column default instead of inserting the raw value). Note that
///   `EXCLUDED.<col>` in update clauses and WHERE conditions then refers to the
///   overridden expression's value, not the raw batch value.
pub fn build_upsert_body(
    formatted_table_name: &str,
    all_columns: &[&str],
    conflict_columns: &[&str],
    update_clauses: Vec<String>,
    sequence_col: Option<&str>,
    custom_where: Option<&str>,
    insert_exprs: &[(&str, String)],
) -> String {
    let formatted_columns =
        all_columns.iter().map(|col| quote_identifier(col)).collect::<Vec<_>>().join(", ");

    let tp_columns = all_columns
        .iter()
        .map(|col| {
            insert_exprs
                .iter()
                .find(|(name, _)| name == col)
                .map(|(_, expr)| expr.clone())
                .unwrap_or_else(|| format!("tp.{}", quote_identifier(col)))
        })
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
/// Note: Dynamic batch operations now use binary COPY for inserts, but this
/// function is kept for potential use by macros or future SQL-based inserts.
///
/// # Arguments
/// * `formatted_table_name` - The fully qualified table name
/// * `all_columns` - All columns to insert
#[allow(dead_code)]
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

/// Builds the SELECT expression for the INSERT branch of an arithmetic
/// (add/subtract) upsert column.
///
/// A row created by an arithmetic upsert must start from the column default,
/// not the raw batch value — otherwise a first-touch `subtract` inserts
/// `+value` instead of `default - value` (lost debits in running balances).
///
/// `insert_default` must be a validated numeric literal (see
/// `DynamicColumnDefinition::insert_default`); when absent, 0 is used, which
/// matches the `COALESCE(table.col, 0)` semantics of the update branch.
pub fn build_arithmetic_insert_expr(
    col: &str,
    clause_type: UpsertClauseType,
    insert_default: Option<&str>,
) -> String {
    let tp_col = format!("tp.{}", quote_identifier(col));
    let default = insert_default.unwrap_or("0");

    match clause_type {
        UpsertClauseType::Add => format!("({} + {})", default, tp_col),
        UpsertClauseType::Subtract => format!("({} - {})", default, tp_col),
        // Set/Max/Min don't rewrite the insert value
        _ => tp_col,
    }
}

/// Builds the ON CONFLICT SET clause for an arithmetic (add/subtract) upsert
/// column whose INSERT branch was built with `build_arithmetic_insert_expr`.
///
/// `EXCLUDED.col` carries `default ± delta` (the value the INSERT branch would
/// have written), so the update branch recovers the delta by removing the
/// default again:
///   col = COALESCE(table.col, 0) + EXCLUDED.col - default
///
/// This same clause serves both add and subtract — the sign already lives in
/// the EXCLUDED value.
pub fn build_upsert_set_clause_arithmetic(
    col: &str,
    formatted_table_name: &str,
    insert_default: Option<&str>,
) -> String {
    let column_name = quote_identifier(col);
    match insert_default {
        Some(default) => format!(
            "{} = COALESCE({}.{}, 0) + EXCLUDED.{} - {}",
            column_name, formatted_table_name, column_name, column_name, default
        ),
        None => format!(
            "{} = COALESCE({}.{}, 0) + EXCLUDED.{}",
            column_name, formatted_table_name, column_name, column_name
        ),
    }
}

/// Rewrites `EXCLUDED."<col>"` references in a custom WHERE condition so they
/// keep meaning the raw batch delta for an arithmetic upsert column.
///
/// SQL-pushed YAML conditions render event variables as `EXCLUDED."<col>"`
/// (see `Expression::to_sql_condition`). Once the INSERT branch is overridden
/// with `build_arithmetic_insert_expr`, `EXCLUDED.<col>` carries
/// `default ± delta` — sign-inverted for subtract — so conditions like
/// `"$value <= @balance"` would silently invert. This recovers the delta:
/// - subtract: `(default - EXCLUDED."col")`
/// - add: `(EXCLUDED."col" - default)` (no-op when there is no default)
pub fn rewrite_custom_where_for_arithmetic(
    custom_where: &str,
    col: &str,
    clause_type: UpsertClauseType,
    insert_default: Option<&str>,
) -> String {
    // Always-quoted form produced by Expression::to_sql_condition
    let excluded_ref = format!("EXCLUDED.\"{}\"", col);
    let replacement = match clause_type {
        UpsertClauseType::Subtract => {
            format!("({} - {})", insert_default.unwrap_or("0"), excluded_ref)
        }
        UpsertClauseType::Add => match insert_default {
            Some(default) => format!("({} - {})", excluded_ref, default),
            // No default: EXCLUDED already carries the raw delta
            None => return custom_where.to_string(),
        },
        _ => return custom_where.to_string(),
    };
    custom_where.replace(&excluded_ref, &replacement)
}

/// Builds an upsert SET clause that keeps the latest value by sequence while
/// allowing arithmetic columns in the same upsert to accumulate regardless of
/// processing order.
pub fn build_upsert_set_clause_latest_by_sequence(
    col: &str,
    formatted_table_name: &str,
    sequence_col: &str,
) -> String {
    let column_name = quote_identifier(col);
    let sequence_name = quote_identifier(sequence_col);

    format!(
        "{} = CASE WHEN EXCLUDED.{} > COALESCE({}.{}, 0) THEN EXCLUDED.{} ELSE {}.{} END",
        column_name,
        sequence_name,
        formatted_table_name,
        sequence_name,
        column_name,
        formatted_table_name,
        column_name
    )
}

/// Type of upsert SET clause to generate.
#[derive(Clone, Copy)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_insert_expr_negates_subtract() {
        assert_eq!(
            build_arithmetic_insert_expr("balance", UpsertClauseType::Subtract, None),
            "(0 - tp.balance)"
        );
        assert_eq!(
            build_arithmetic_insert_expr("balance", UpsertClauseType::Subtract, Some("100")),
            "(100 - tp.balance)"
        );
    }

    #[test]
    fn arithmetic_insert_expr_offsets_add_by_default() {
        assert_eq!(
            build_arithmetic_insert_expr("balance", UpsertClauseType::Add, None),
            "(0 + tp.balance)"
        );
        assert_eq!(
            build_arithmetic_insert_expr("balance", UpsertClauseType::Add, Some("100")),
            "(100 + tp.balance)"
        );
    }

    #[test]
    fn arithmetic_insert_expr_leaves_set_max_min_untouched() {
        for clause_type in [UpsertClauseType::Set, UpsertClauseType::Max, UpsertClauseType::Min] {
            assert_eq!(
                build_arithmetic_insert_expr("balance", clause_type, Some("100")),
                "tp.balance"
            );
        }
    }

    #[test]
    fn arithmetic_upsert_set_clause_recovers_delta_from_excluded() {
        assert_eq!(
            build_upsert_set_clause_arithmetic("balance", "t", None),
            "balance = COALESCE(t.balance, 0) + EXCLUDED.balance"
        );
        assert_eq!(
            build_upsert_set_clause_arithmetic("balance", "t", Some("100")),
            "balance = COALESCE(t.balance, 0) + EXCLUDED.balance - 100"
        );
    }

    #[test]
    fn upsert_body_applies_insert_expr_overrides() {
        let insert_exprs = vec![(
            "balance",
            build_arithmetic_insert_expr("balance", UpsertClauseType::Subtract, None),
        )];
        let body = build_upsert_body(
            "t",
            &["network", "holder", "balance"],
            &["network", "holder"],
            vec!["balance = COALESCE(t.balance, 0) + EXCLUDED.balance".to_string()],
            None,
            None,
            &insert_exprs,
        );

        assert!(
            body.contains("SELECT tp.network, tp.holder, (0 - tp.balance)"),
            "insert branch must start subtract rows from the default: {body}"
        );
        assert!(
            !body.contains("WHERE EXCLUDED."),
            "arithmetic upserts must not carry the sequence guard: {body}"
        );
    }

    #[test]
    fn custom_where_rewrite_recovers_raw_delta() {
        let guard = "EXCLUDED.\"balance\" <= t.\"balance\"";

        assert_eq!(
            rewrite_custom_where_for_arithmetic(guard, "balance", UpsertClauseType::Subtract, None),
            "(0 - EXCLUDED.\"balance\") <= t.\"balance\""
        );
        assert_eq!(
            rewrite_custom_where_for_arithmetic(
                guard,
                "balance",
                UpsertClauseType::Subtract,
                Some("100")
            ),
            "(100 - EXCLUDED.\"balance\") <= t.\"balance\""
        );
        assert_eq!(
            rewrite_custom_where_for_arithmetic(
                guard,
                "balance",
                UpsertClauseType::Add,
                Some("100")
            ),
            "(EXCLUDED.\"balance\" - 100) <= t.\"balance\""
        );
        // Add with no default: EXCLUDED already carries the raw delta
        assert_eq!(
            rewrite_custom_where_for_arithmetic(guard, "balance", UpsertClauseType::Add, None),
            guard
        );
        // Other columns' EXCLUDED references are untouched
        assert_eq!(
            rewrite_custom_where_for_arithmetic(guard, "amount", UpsertClauseType::Subtract, None),
            guard
        );
    }

    #[test]
    fn aggregated_cte_without_sequence_uses_array_agg_for_set_columns() {
        let cte = build_to_process_cte_aggregated(
            &[("holder", ColumnAggregate::GroupKey), ("flag", ColumnAggregate::LastBySeq)],
            None,
        );
        assert!(
            cte.contains("(array_agg(flag))[1] AS flag"),
            "must not use MAX() which fails for BOOLEAN/BYTEA: {cte}"
        );
    }

    #[test]
    fn upsert_body_without_overrides_keeps_sequence_guard() {
        let body = build_upsert_body(
            "t",
            &["holder", "name", "seq"],
            &["holder"],
            vec!["name = EXCLUDED.name".to_string()],
            Some("seq"),
            None,
            &[],
        );

        assert!(body.contains("SELECT tp.holder, tp.name, tp.seq"), "{body}");
        assert!(body.contains("WHERE EXCLUDED.seq > COALESCE(t.seq, 0)"), "{body}");
    }
}
