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

pub fn generate_database_backends_code() -> Code {
    Code::new(
        r#"
    use std::sync::Arc;
    use rindexer::{PostgresClient, ClickhouseClient, DatabaseBackends};
    use tokio::sync::OnceCell;

    static POSTGRES_CLIENT: OnceCell<Arc<PostgresClient>> = OnceCell::const_new();
    static CLICKHOUSE_CLIENT: OnceCell<Arc<ClickhouseClient>> = OnceCell::const_new();
    static DATABASE_BACKENDS: OnceCell<Arc<DatabaseBackends>> = OnceCell::const_new();

    pub async fn get_or_init_postgres_client() -> Arc<PostgresClient> {
        POSTGRES_CLIENT
            .get_or_init(|| async {
                Arc::new(PostgresClient::new().await.expect("Failed to connect to Postgres"))
            })
            .await
            .clone()
    }

    pub async fn get_or_init_clickhouse_client() -> Arc<ClickhouseClient> {
        CLICKHOUSE_CLIENT
            .get_or_init(|| async {
                Arc::new(ClickhouseClient::new().await.expect("Failed to connect to Clickhouse"))
            })
            .await
            .clone()
    }

    pub async fn get_or_init_database_backends() -> Arc<DatabaseBackends> {
        DATABASE_BACKENDS
            .get_or_init(|| async {
                let pg = get_or_init_postgres_client().await;
                let ch = get_or_init_clickhouse_client().await;
                Arc::new(DatabaseBackends::new(Some(pg), Some(ch)))
            })
            .await
            .clone()
    }
    "#
        .to_string(),
    )
}
