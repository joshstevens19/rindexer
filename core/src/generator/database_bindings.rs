use crate::types::code::Code;

pub fn generate_postgres_code() -> Code {
    Code::new(
        r#"
    use std::sync::Arc;
    use rindexer::PostgresClient;
    use tokio::sync::OnceCell;

    static POSTGRES_CLIENT: OnceCell<Arc<PostgresClient>> = OnceCell::const_new();

    pub async fn get_or_init_postgres_client() -> Arc<PostgresClient> {
        POSTGRES_CLIENT
            .get_or_init(|| async {
                Arc::new(PostgresClient::new().await.expect("Failed to connect to Postgres"))
            })
            .await
            .clone()
    }
    "#
        .to_string(),
    )
}

pub fn generate_clickhouse_code() -> Code {
    Code::new(
        r#"
    use std::sync::Arc;
    use rindexer::ClickhouseClient;
    use tokio::sync::OnceCell;

    static CLICKHOUSE_CLIENT: OnceCell<Arc<ClickhouseClient>> = OnceCell::const_new();

    pub async fn get_or_init_clickhouse_client() -> Arc<ClickhouseClient> {
        CLICKHOUSE_CLIENT
            .get_or_init(|| async {
                Arc::new(ClickhouseClient::new().await.expect("Failed to connect to Clickhouse"))
            })
            .await
            .clone()
    }
    "#
        .to_string(),
    )
}

pub fn generate_sqlite_code() -> Code {
    Code::new(
        r#"
    use std::sync::Arc;
    use rindexer::SqliteClient;
    use tokio::sync::OnceCell;

    static SQLITE_CLIENT: OnceCell<Arc<SqliteClient>> = OnceCell::const_new();

    pub async fn get_or_init_sqlite_client() -> Arc<SqliteClient> {
        SQLITE_CLIENT
            .get_or_init(|| async {
                Arc::new(SqliteClient::new().await.expect("Failed to connect to SQLite"))
            })
            .await
            .clone()
    }
    "#
        .to_string(),
    )
}
