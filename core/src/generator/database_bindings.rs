use crate::types::code::Code;

pub fn generate_database_code() -> Code {
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
