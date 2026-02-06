// public
pub mod adaptive_concurrency;
pub mod generator;
pub mod hot_reload;
pub use hot_reload::RELOAD_EXIT_CODE;
pub mod indexer;
pub mod layer_extensions;
pub mod manifest;
pub mod metrics;
pub mod reth;

mod system_state;
pub use system_state::{
    get_reload_state, initiate_shutdown, is_running, set_reload_state, ReloadState,
};

mod health;
pub use health::{start_health_server, HealthServer, HealthServerState, HealthStatus};

mod database;
pub use database::{
    clickhouse::{
        client::ClickhouseClient,
        schema_sync::{
            apply_schema_change as apply_clickhouse_schema_change,
            detect_schema_changes as detect_clickhouse_schema_changes,
            SchemaChange as ClickhouseSchemaChange,
        },
        setup::setup_clickhouse,
    },
    generate::drop_tables_for_indexer_sql,
    postgres::{
        client::{PostgresClient, ToSql},
        schema_sync::{apply_schema_change, detect_schema_changes, SchemaChange},
        setup::setup_postgres,
    },
};

mod simple_file_formatters;
pub use simple_file_formatters::csv::AsyncCsvAppender;

mod helpers;
pub use helpers::{
    format_all_files_for_project, generate_random_id, load_env_from_project_path,
    public_read_env_value, write_file, WriteFileError,
};
mod api;
pub use api::{generate_graphql_queries, GraphqlOverrideSettings};

mod logger;
pub use logger::setup_info_logger;
mod abi;
pub use abi::ABIItem;
mod chat;
pub mod event;
pub mod notifications;
pub use notifications::ChainStateNotification;
pub mod blockclock;
pub mod phantom;
pub mod provider;
mod start;
mod streams;
mod types;

mod events;
pub use events::{RindexerEvent, RindexerEventStream};

// export 3rd party dependencies
pub use async_trait::async_trait;
pub use colored::Colorize as RindexerColorize;
pub use database::sql_type_wrapper::EthereumSqlTypeWrapper;
pub use futures::FutureExt;
pub use indexer::no_code::resolve_table_column_types;
pub use lazy_static::lazy_static;
pub use reqwest::header::HeaderMap;
pub use start::{
    start_rindexer, start_rindexer_no_code, IndexerNoCodeDetails, IndexingDetails, StartDetails,
    StartNoCodeDetails,
};
pub use tokio::main as rindexer_main;
pub use tokio_postgres::types::Type as PgType;
pub use tracing::{error as rindexer_error, info as rindexer_info};
pub use types::single_or_array::StringOrArray;
