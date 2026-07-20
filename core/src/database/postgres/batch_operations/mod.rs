//! PostgreSQL batch operations for efficient bulk database updates.
//!
//! This module provides infrastructure for batch INSERT, UPDATE, DELETE,
//! and UPSERT operations with support for deduplication, sequencing, and
//! arithmetic operations (add/subtract) on columns.

mod dynamic;
mod macros;
mod query_builder;

// build_batch_statement/execute_batch_statement are consumed by unit tests
// (SQL parity assertions) and kept public for future callers.
#[allow(unused_imports)]
pub use dynamic::{
    build_batch_statement, execute_batch_statement, execute_dynamic_batch_operation,
    DynamicBatchStatement,
};

// Re-exported for use by the create_batch_postgres_operation! macro
#[allow(unused_imports)]
pub use query_builder::{
    build_cte_header, build_delete_body, build_insert_body, build_sequence_condition,
    build_set_clause, build_to_process_cte, build_to_process_cte_aggregated, build_update_body,
    build_upsert_body, build_upsert_set_clause, build_upsert_set_clause_latest_by_sequence,
    build_where_clause, build_where_condition, format_table_name, quote_identifier,
    ColumnAggregate, ColumnInfo, SetClauseType, UpsertClauseType,
};
