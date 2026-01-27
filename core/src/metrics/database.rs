//! Database-specific metrics helpers.

use super::definitions::{DB_OPERATIONS_TOTAL, DB_OPERATION_DURATION, DB_POOL_CONNECTIONS};
use super::timer::TimerGuard;

/// Database operation types for labeling.
pub mod ops {
    pub const INSERT: &str = "insert";
    pub const UPDATE: &str = "update";
    pub const DELETE: &str = "delete";
    pub const QUERY: &str = "query";
    pub const BATCH_INSERT: &str = "batch_insert";
    pub const BATCH_UPDATE: &str = "batch_update";
    pub const BATCH_EXECUTE: &str = "batch_execute";
}

/// Record a completed database operation.
pub fn record_db_operation(operation: &str, success: bool, duration_secs: f64) {
    let status = if success { "success" } else { "error" };

    DB_OPERATIONS_TOTAL
        .with_label_values(&[operation, status])
        .inc();

    DB_OPERATION_DURATION
        .with_label_values(&[operation])
        .observe(duration_secs);
}

/// Record a successful database operation.
pub fn record_db_success(operation: &str, duration_secs: f64) {
    record_db_operation(operation, true, duration_secs);
}

/// Record a failed database operation.
pub fn record_db_error(operation: &str, duration_secs: f64) {
    record_db_operation(operation, false, duration_secs);
}

/// Create a timer for a database operation. Records duration on drop.
pub fn time_db_operation<'a>(operation: &str) -> TimerGuard<'a> {
    TimerGuard::new(&DB_OPERATION_DURATION, &[operation])
}

/// Update connection pool metrics.
pub fn set_pool_connections(database: &str, active: usize, idle: usize) {
    DB_POOL_CONNECTIONS
        .with_label_values(&[database, "active"])
        .set(active as f64);
    DB_POOL_CONNECTIONS
        .with_label_values(&[database, "idle"])
        .set(idle as f64);
}
