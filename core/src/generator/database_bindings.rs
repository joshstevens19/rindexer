use crate::manifest::storage::{CircuitBreakerConfig, Storage, WritePolicy};
use crate::types::code::Code;

/// Render an `Option<T>` as a Rust literal for baking into generated code.
fn render_option<T>(value: Option<&T>, render: impl FnOnce(&T) -> String) -> String {
    match value {
        Some(v) => format!("Some({})", render(v)),
        None => "None".to_string(),
    }
}

fn render_write_policy(policy: &WritePolicy) -> String {
    let variant = match policy {
        WritePolicy::All => "All",
        WritePolicy::Any => "Any",
        WritePolicy::PrimaryWithShadow => "PrimaryWithShadow",
    };
    format!("rindexer::WritePolicy::{variant}")
}

fn render_circuit_breaker(cfg: &CircuitBreakerConfig) -> String {
    format!(
        "rindexer::CircuitBreakerConfig {{ enabled: {}, failure_threshold: {}, cooldown_seconds: {} }}",
        cfg.enabled, cfg.failure_threshold, cfg.cooldown_seconds,
    )
}

pub fn generate_postgres_code() -> Code {
    Code::new(
        r#"use std::sync::Arc;
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
        r#"use std::sync::Arc;
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

pub fn generate_database_backends_code(storage: &Storage) -> Code {
    let write_policy = render_option(storage.write_policy.as_ref(), render_write_policy);
    let circuit_breaker = render_option(storage.circuit_breaker.as_ref(), render_circuit_breaker);
    let max_batch_size = render_option(storage.max_batch_size.as_ref(), |v| v.to_string());

    Code::new(format!(
        r#"
    use std::sync::Arc;
    use rindexer::{{PostgresClient, ClickhouseClient, DatabaseBackends}};
    use tokio::sync::OnceCell;

    static POSTGRES_CLIENT: OnceCell<Arc<PostgresClient>> = OnceCell::const_new();
    static CLICKHOUSE_CLIENT: OnceCell<Arc<ClickhouseClient>> = OnceCell::const_new();
    static DATABASE_BACKENDS: OnceCell<Arc<DatabaseBackends>> = OnceCell::const_new();

    pub async fn get_or_init_postgres_client() -> Arc<PostgresClient> {{
        POSTGRES_CLIENT
            .get_or_init(|| async {{
                Arc::new(PostgresClient::new().await.expect("Failed to connect to Postgres"))
            }})
            .await
            .clone()
    }}

    pub async fn get_or_init_clickhouse_client() -> Arc<ClickhouseClient> {{
        CLICKHOUSE_CLIENT
            .get_or_init(|| async {{
                Arc::new(ClickhouseClient::new().await.expect("Failed to connect to Clickhouse"))
            }})
            .await
            .clone()
    }}

    pub async fn get_or_init_database_backends() -> Arc<DatabaseBackends> {{
        DATABASE_BACKENDS
            .get_or_init(|| async {{
                let pg = get_or_init_postgres_client().await;
                let ch = get_or_init_clickhouse_client().await;
                let write_policy: Option<rindexer::WritePolicy> = {write_policy};
                let circuit_breaker: Option<rindexer::CircuitBreakerConfig> = {circuit_breaker};
                let max_batch_size: Option<usize> = {max_batch_size};
                Arc::new(
                    DatabaseBackends::new(Some(pg), Some(ch))
                        .with_config(write_policy, circuit_breaker, max_batch_size),
                )
            }})
            .await
            .clone()
    }}
    "#,
    ))
}
