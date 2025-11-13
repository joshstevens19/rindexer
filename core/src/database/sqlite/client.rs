use std::{env, path::PathBuf};

use dotenv::dotenv;
use rusqlite::{Connection, ToSql};
use tracing::{error, info};

use crate::database::generate::generate_event_table_columns_names_sql;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;

pub fn connection_string() -> Result<String, env::VarError> {
    dotenv().ok();
    // Default to ./rindexer.db if DATABASE_URL is not set
    let connection = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "./rindexer.db".to_string());
    Ok(connection)
}

#[derive(thiserror::Error, Debug)]
pub enum SqliteConnectionError {
    #[error("The database connection string is wrong please check your environment: {0}")]
    DatabaseConnectionConfigWrong(#[from] env::VarError),

    #[error("SQLite error: {0}")]
    SqliteError(#[from] SqliteError),

    #[error("Can not connect to the database please make sure your connection string is correct")]
    CanNotConnectToDatabase,

    #[error("Could not parse connection string make sure it is correctly formatted")]
    CouldNotParseConnectionString,
}

#[derive(thiserror::Error, Debug)]
pub enum SqliteError {
    #[error("SQLite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("Failed to acquire connection lock")]
    ConnectionLockError,
}

#[allow(dead_code)]
#[derive(thiserror::Error, Debug)]
pub enum BulkInsertSqliteError {
    #[error("{0}")]
    SqliteError(#[from] SqliteError),

    #[error("Could not write data to SQLite: {0}")]
    CouldNotWriteDataToSqlite(#[from] rusqlite::Error),
}

pub struct SqliteClient {
    db_path: String,
}

impl SqliteClient {
    pub async fn new() -> Result<Self, SqliteConnectionError> {
        let connection_str = connection_string()?;
        
        info!("Connecting to SQLite database at: {}", connection_str);
        
        // Create parent directories if they don't exist
        let db_path_clone = connection_str.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = PathBuf::from(&db_path_clone).parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| {
                            error!("Failed to create parent directories for SQLite database: {}", e);
                            SqliteConnectionError::CanNotConnectToDatabase
                        })?;
                }
            }

            let conn = Connection::open(&db_path_clone)
                .map_err(|e| {
                    error!("Error connecting to SQLite database: {}", e);
                    SqliteConnectionError::CanNotConnectToDatabase
                })?;

            // Enable WAL mode for better concurrent performance
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
                .map_err(|_e| {
                    error!("Error setting SQLite pragmas");
                    SqliteConnectionError::CanNotConnectToDatabase
                })?;

            info!("Successfully connected to SQLite database");
            Ok::<(), SqliteConnectionError>(())
        })
        .await
        .map_err(|_| SqliteConnectionError::CanNotConnectToDatabase)??;

        Ok(SqliteClient {
            db_path: connection_str,
        })
    }

    pub async fn batch_execute(&self, sql: &str) -> Result<(), SqliteError> {
        let db_path = self.db_path.clone();
        let sql = sql.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            conn.execute_batch(&sql)?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|_| SqliteError::ConnectionLockError)?
        .map_err(SqliteError::SqliteError)
    }

    // Helper method for single execute - currently unused but kept for potential future use
    #[allow(dead_code)]
    pub async fn execute(&self, query: &str, params: Vec<String>) -> Result<usize, SqliteError> {
        let db_path = self.db_path.clone();
        let query = query.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            // Use dynamic dispatch with explicit type annotation to avoid trait object issues
            let params_dynamic: Vec<Box<dyn ToSql>> = params
                .into_iter()
                .map(|p| Box::new(p) as Box<dyn ToSql>)
                .collect();
            let params_refs: Vec<&dyn ToSql> = params_dynamic.iter().map(|p| p.as_ref()).collect();
            let result = conn.execute(&query, params_refs.as_slice())?;
            Ok::<usize, rusqlite::Error>(result)
        })
        .await
        .map_err(|_| SqliteError::ConnectionLockError)?
        .map_err(SqliteError::SqliteError)
    }

    /// Bulk insert method using SQLite transactions for better performance
    /// Note: SQLite doesn't have COPY like PostgreSQL, but transactions significantly improve bulk insert performance
    pub async fn insert_bulk(
        &self,
        table_name: &str,
        columns: &[String],
        bulk_data: &[Vec<EthereumSqlTypeWrapper>],
    ) -> Result<(), String> {
        if bulk_data.is_empty() {
            return Ok(());
        }

        let db_path = self.db_path.clone();
        let table_name = table_name.to_string();
        let columns = columns.to_vec();
        
        // Convert all data to strings for SQLite (simplest approach)
        let string_data: Vec<Vec<String>> = bulk_data.iter()
            .map(|row| {
                row.iter().map(|wrapper| wrapper.to_sqlite_string_value()).collect()
            })
            .collect();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| format!("Failed to open connection: {}", e))?;

            // Start a transaction for bulk insert
            conn.execute("BEGIN TRANSACTION", [])
                .map_err(|e| format!("Failed to begin transaction: {}", e))?;

            let placeholders = (1..=columns.len())
                .map(|i| format!("?{}", i))
                .collect::<Vec<_>>()
                .join(", ");

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                generate_event_table_columns_names_sql(&columns),
                placeholders
            );

            for row in &string_data {
                let params: Vec<&dyn ToSql> = row.iter().map(|s| s as &dyn ToSql).collect();
                
                conn.execute(&query, params.as_slice())
                    .map_err(|e| {
                        // Try to rollback on error
                        let _ = conn.execute("ROLLBACK", []);
                        format!("Failed to insert row: {}", e)
                    })?;
            }

            conn.execute("COMMIT", [])
                .map_err(|e| format!("Failed to commit transaction: {}", e))?;

            Ok::<(), String>(())
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))??;

        Ok(())
    }
}

