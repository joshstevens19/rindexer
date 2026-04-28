use crate::manifest::storage::{CircuitBreakerConfig, Storage, WritePolicy};
use crate::types::code::Code;

/// Storage-aware tokens that the events/trace bindings paste into the same
/// four spots: rindexer-crate imports, typings imports, the `EventContext`
/// database field type, and its initializer. Centralising the 3-way conditional
/// keeps events_bindings and trace_bindings in lockstep.
pub(super) struct DatabaseTokens {
    /// Items added to the `use rindexer::{...}` import list.
    pub rindexer_import: &'static str,
    /// `use super::...::typings::database::...` lines.
    pub typings_imports: &'static str,
    /// Field declaration in `EventContext` / `TraceContext`, e.g.
    /// `pub database: Arc<PostgresClient>,`. Empty when no DB is configured.
    pub context_field: &'static str,
    /// Initializer line in the `EventContext` / `TraceContext` struct literal,
    /// e.g. `database: get_or_init_postgres_client().await,`. Empty when no DB.
    pub context_initializer: &'static str,
}

pub(super) fn database_tokens(storage: &Storage) -> DatabaseTokens {
    match (storage.postgres_enabled(), storage.clickhouse_enabled()) {
        (true, true) => DatabaseTokens {
            rindexer_import: "PostgresClient, ClickhouseClient, DatabaseBackends,",
            typings_imports: "use super::super::super::super::typings::database::get_or_init_postgres_client;\nuse super::super::super::super::typings::database::get_or_init_clickhouse_client;\nuse super::super::super::super::typings::database::get_or_init_database_backends;",
            context_field: "pub database: Arc<rindexer::DatabaseBackends>,",
            context_initializer: "database: get_or_init_database_backends().await,",
        },
        (true, false) => DatabaseTokens {
            rindexer_import: "PostgresClient,",
            typings_imports: "use super::super::super::super::typings::database::get_or_init_postgres_client;",
            context_field: "pub database: Arc<PostgresClient>,",
            context_initializer: "database: get_or_init_postgres_client().await,",
        },
        (false, true) => DatabaseTokens {
            rindexer_import: "ClickhouseClient,",
            typings_imports: "use super::super::super::super::typings::database::get_or_init_clickhouse_client;",
            context_field: "pub database: Arc<ClickhouseClient>,",
            context_initializer: "database: get_or_init_clickhouse_client().await,",
        },
        (false, false) => DatabaseTokens {
            rindexer_import: "",
            typings_imports: "",
            context_field: "",
            context_initializer: "",
        },
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_option_none_emits_none() {
        assert_eq!(render_option::<u32>(None, |v| v.to_string()), "None");
    }

    #[test]
    fn render_option_some_wraps_value() {
        assert_eq!(render_option(Some(&42u32), |v| v.to_string()), "Some(42)");
    }

    #[test]
    fn render_write_policy_emits_qualified_variant() {
        assert_eq!(render_write_policy(&WritePolicy::All), "rindexer::WritePolicy::All");
        assert_eq!(render_write_policy(&WritePolicy::Any), "rindexer::WritePolicy::Any");
        assert_eq!(
            render_write_policy(&WritePolicy::PrimaryWithShadow),
            "rindexer::WritePolicy::PrimaryWithShadow"
        );
    }

    #[test]
    fn render_circuit_breaker_emits_struct_literal_with_all_fields() {
        let cfg =
            CircuitBreakerConfig { enabled: true, failure_threshold: 7, cooldown_seconds: 30 };
        let rendered = render_circuit_breaker(&cfg);
        assert!(rendered.contains("rindexer::CircuitBreakerConfig"));
        assert!(rendered.contains("enabled: true"));
        assert!(rendered.contains("failure_threshold: 7"));
        assert!(rendered.contains("cooldown_seconds: 30"));
    }

    #[test]
    fn postgres_and_clickhouse_static_codegen_compile_helpers() {
        let pg = generate_postgres_code().to_string();
        assert!(pg.contains("static POSTGRES_CLIENT"));
        assert!(pg.contains("get_or_init_postgres_client"));
        assert!(pg.contains("PostgresClient::new"));

        let ch = generate_clickhouse_code().to_string();
        assert!(ch.contains("static CLICKHOUSE_CLIENT"));
        assert!(ch.contains("get_or_init_clickhouse_client"));
        assert!(ch.contains("ClickhouseClient::new"));
    }

    #[test]
    fn database_backends_codegen_defaults_to_none_for_optional_config() {
        let storage = Storage::default();
        let code = generate_database_backends_code(&storage).to_string();

        assert!(code.contains("static POSTGRES_CLIENT"));
        assert!(code.contains("static CLICKHOUSE_CLIENT"));
        assert!(code.contains("static DATABASE_BACKENDS"));
        assert!(code.contains("get_or_init_database_backends"));
        // All three optional knobs left as None when unset on storage.
        assert!(code.contains("let write_policy: Option<rindexer::WritePolicy> = None;"));
        assert!(
            code.contains("let circuit_breaker: Option<rindexer::CircuitBreakerConfig> = None;")
        );
        assert!(code.contains("let max_batch_size: Option<usize> = None;"));
    }

    #[test]
    fn database_backends_codegen_inlines_storage_config() {
        let storage = Storage {
            write_policy: Some(WritePolicy::PrimaryWithShadow),
            circuit_breaker: Some(CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 3,
                cooldown_seconds: 15,
            }),
            max_batch_size: Some(2_000),
            ..Storage::default()
        };
        let code = generate_database_backends_code(&storage).to_string();

        assert!(code.contains(
            "let write_policy: Option<rindexer::WritePolicy> = Some(rindexer::WritePolicy::PrimaryWithShadow);"
        ));
        assert!(code.contains("failure_threshold: 3"));
        assert!(code.contains("cooldown_seconds: 15"));
        assert!(code.contains("let max_batch_size: Option<usize> = Some(2000);"));
        // Make sure the with_config call is wired up to the local bindings.
        assert!(code.contains(".with_config(write_policy, circuit_breaker, max_batch_size)"));
    }

    #[test]
    fn database_backends_codegen_each_write_policy_variant_renders() {
        for policy in [WritePolicy::All, WritePolicy::Any, WritePolicy::PrimaryWithShadow] {
            let storage = Storage { write_policy: Some(policy.clone()), ..Storage::default() };
            let code = generate_database_backends_code(&storage).to_string();
            let expected = render_write_policy(&policy);
            assert!(
                code.contains(&format!("Some({expected})")),
                "expected {expected} in generated code"
            );
        }
    }

    use crate::manifest::storage::{ClickhouseDetails, PostgresDetails};

    fn pg_storage() -> Storage {
        Storage {
            postgres: Some(PostgresDetails {
                enabled: true,
                drop_each_run: None,
                relationships: None,
                indexes: None,
                disable_create_tables: None,
            }),
            ..Storage::default()
        }
    }

    fn ch_storage() -> Storage {
        Storage {
            clickhouse: Some(ClickhouseDetails {
                enabled: true,
                drop_each_run: None,
                disable_create_tables: None,
            }),
            ..Storage::default()
        }
    }

    fn dual_storage() -> Storage {
        Storage {
            postgres: pg_storage().postgres,
            clickhouse: ch_storage().clickhouse,
            ..Storage::default()
        }
    }

    #[test]
    fn database_tokens_neither_backend_is_all_empty() {
        let tokens = database_tokens(&Storage::default());
        assert_eq!(tokens.rindexer_import, "");
        assert_eq!(tokens.typings_imports, "");
        assert_eq!(tokens.context_field, "");
        assert_eq!(tokens.context_initializer, "");
    }

    #[test]
    fn database_tokens_postgres_only_emits_postgres_helpers() {
        let tokens = database_tokens(&pg_storage());
        assert_eq!(tokens.rindexer_import, "PostgresClient,");
        assert!(tokens.typings_imports.contains("get_or_init_postgres_client"));
        assert!(!tokens.typings_imports.contains("clickhouse"));
        assert!(!tokens.typings_imports.contains("database_backends"));
        assert_eq!(tokens.context_field, "pub database: Arc<PostgresClient>,");
        assert_eq!(tokens.context_initializer, "database: get_or_init_postgres_client().await,");
    }

    #[test]
    fn database_tokens_clickhouse_only_emits_clickhouse_helpers() {
        let tokens = database_tokens(&ch_storage());
        assert_eq!(tokens.rindexer_import, "ClickhouseClient,");
        assert!(tokens.typings_imports.contains("get_or_init_clickhouse_client"));
        assert!(!tokens.typings_imports.contains("postgres"));
        assert_eq!(tokens.context_field, "pub database: Arc<ClickhouseClient>,");
        assert_eq!(tokens.context_initializer, "database: get_or_init_clickhouse_client().await,");
    }

    #[test]
    fn database_tokens_dual_emits_database_backends() {
        let tokens = database_tokens(&dual_storage());
        assert_eq!(tokens.rindexer_import, "PostgresClient, ClickhouseClient, DatabaseBackends,");
        assert!(tokens.typings_imports.contains("get_or_init_postgres_client"));
        assert!(tokens.typings_imports.contains("get_or_init_clickhouse_client"));
        assert!(tokens.typings_imports.contains("get_or_init_database_backends"));
        assert_eq!(tokens.context_field, "pub database: Arc<rindexer::DatabaseBackends>,");
        assert_eq!(tokens.context_initializer, "database: get_or_init_database_backends().await,");
    }
}
