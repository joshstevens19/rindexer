use rindexer::ClickhouseClient;
use std::sync::Arc;
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
