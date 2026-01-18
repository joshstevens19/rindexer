//! ClickHouse batch operations for efficient bulk database updates.
//!
//! This module provides infrastructure for batch INSERT, UPDATE, DELETE,
//! and UPSERT operations. Note that ClickHouse handles upserts via
//! ReplacingMergeTree which automatically keeps the latest version.

mod dynamic;
mod macros;
mod query_builder;

pub use dynamic::execute_dynamic_batch_operation;

// Re-exported for use by the create_batch_clickhouse_operation! macro
#[allow(unused_imports)]
pub use query_builder::{format_table_name, quote_identifier};
