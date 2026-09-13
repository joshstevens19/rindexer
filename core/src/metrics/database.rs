//! Database-specific metrics helpers.

use super::definitions::{
    ATOMIC_BATCHES_TOTAL, DB_OPERATIONS_TOTAL, DB_OPERATION_DURATION, DB_POOL_CONNECTIONS,
};
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

    DB_OPERATIONS_TOTAL.with_label_values(&[operation, status]).inc();

    DB_OPERATION_DURATION.with_label_values(&[operation]).observe(duration_secs);
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
    DB_POOL_CONNECTIONS.with_label_values(&[database, "active"]).set(active as f64);
    DB_POOL_CONNECTIONS.with_label_values(&[database, "idle"]).set(idle as f64);
}

/// Record the outcome of one atomic no-code batch.
///
/// `status` is `committed` or `rolled_back`; `reason` is `ok` on success and otherwise
/// `deadlock`, `cursor_missing` or `error`. Classification is text-based because this
/// write path returns `String` errors (pre-existing); the label set is bounded to
/// those four values whatever the error text says. The SQLSTATE the classifier keys on
/// is present because the write path renders Postgres errors through
/// `database::postgres::client::pg_error_to_string`, which appends the server message
/// and code that `tokio_postgres::Error`'s `Display` omits.
pub fn record_atomic_batch(result: &Result<(), String>) {
    let (status, reason) = match result {
        Ok(()) => ("committed", "ok"),
        Err(error) => ("rolled_back", classify_atomic_batch_error(error)),
    };

    ATOMIC_BATCHES_TOTAL.with_label_values(&[status, reason]).inc();
}

/// Maps an atomic-batch error text onto the bounded `reason` label set. Expects the
/// `pg_error_to_string` shape (`db error: ERROR: <message> (<SQLSTATE>)`).
fn classify_atomic_batch_error(error: &str) -> &'static str {
    if error.contains("40P01") || error.contains("deadlock detected") {
        "deadlock"
    } else if error.contains("seeded row missing") {
        "cursor_missing"
    } else {
        "error"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_batch_error_reasons_are_bounded() {
        assert_eq!(classify_atomic_batch_error("db error: ERROR: deadlock detected"), "deadlock");
        assert_eq!(classify_atomic_batch_error("SqlState(E40P01)"), "deadlock");
        assert_eq!(
            classify_atomic_batch_error(
                "ATOMIC-CURSOR: no cursor row for network=x in rindexer_internal.y, seeded row missing, rolling back"
            ),
            "cursor_missing"
        );
        assert_eq!(classify_atomic_batch_error("connection closed"), "error");
    }

    #[test]
    fn deadlock_reason_matches_pg_error_to_string_shape() {
        // Exactly what `pg_error_to_string` renders for a 40P01 raised by the server.
        let rendered = "db error: ERROR: deadlock detected (40P01)";
        assert_eq!(classify_atomic_batch_error(rendered), "deadlock");
        // A custom RAISE message still classifies by its SQLSTATE.
        assert_eq!(classify_atomic_batch_error("db error: ERROR: boom (40P01)"), "deadlock");
        // The bare Display text tokio-postgres produces on its own never did.
        assert_eq!(classify_atomic_batch_error("db error"), "error");
    }

    #[test]
    fn record_atomic_batch_labels_outcomes() {
        record_atomic_batch(&Ok(()));
        record_atomic_batch(&Err("deadlock detected".to_string()));

        let committed = ATOMIC_BATCHES_TOTAL.with_label_values(&["committed", "ok"]).get();
        let deadlocked = ATOMIC_BATCHES_TOTAL.with_label_values(&["rolled_back", "deadlock"]).get();
        assert!(committed >= 1.0);
        assert!(deadlocked >= 1.0);
    }
}
