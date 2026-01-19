//! Shared SQL query building logic for ClickHouse batch operations.

use crate::database::batch_operations::RESERVED_KEYWORDS;

/// Quotes an identifier if it's a reserved keyword (uses backticks for ClickHouse).
#[inline]
pub fn quote_identifier(name: &str) -> String {
    if RESERVED_KEYWORDS.contains(&name) {
        format!("`{}`", name)
    } else {
        name.to_string()
    }
}

/// Formats a table name, handling schema.table format (uses backticks for ClickHouse).
pub fn format_table_name(table_name: &str) -> String {
    if table_name.contains('.') {
        let parts: Vec<&str> = table_name.split('.').collect();
        if parts.len() == 2 {
            let schema = parts[0].trim_matches('"').trim_matches('`');
            let table = parts[1].trim_matches('"').trim_matches('`');
            format!("`{}`.`{}`", schema, table)
        } else {
            table_name.to_string()
        }
    } else {
        table_name.to_string()
    }
}
