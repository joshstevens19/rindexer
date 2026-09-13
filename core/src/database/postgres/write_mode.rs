//! Chooses whether a custom-table statement commits on its own or joins a
//! caller-owned transaction.

use tokio_postgres::Transaction as PgTransaction;

use crate::database::postgres::client::PostgresClient;

/// How a custom-table statement reaches Postgres.
///
/// `Tx` needs only a shared borrow: every `Transaction` method used on this path
/// (`execute`, `query_opt`, `batch_execute`, `copy_in`) takes `&self`. Savepoints are
/// issued as SQL through `batch_execute`; `Transaction::savepoint` (`&mut self`) is not used.
#[derive(Clone, Copy)]
pub enum PgWriteMode<'a> {
    /// Own transaction per statement (legacy arm, cron).
    Eager(&'a PostgresClient),
    /// Execute inside the caller's open transaction (atomic arm).
    Tx(&'a PgTransaction<'a>),
}
