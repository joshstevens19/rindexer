// public
pub mod generator;
pub mod indexer;
pub mod manifest;

mod database;
pub use database::postgres::{
    drop_tables_for_indexer_sql, setup_postgres, EthereumSqlTypeWrapper, PostgresClient,
};

mod simple_file_formatters;
pub use simple_file_formatters::csv::AsyncCsvAppender;

mod helpers;
pub use helpers::{format_all_files_for_project, generate_random_id, write_file, WriteFileError};
mod api;
pub use api::{generate_graphql_queries, GraphQLServerDetails, GraphQLServerSettings};

mod logger;
pub use logger::setup_info_logger;
pub mod provider;
mod start;
mod types;

pub use start::{
    start_rindexer, start_rindexer_no_code, GraphqlNoCodeDetails, IndexerNoCodeDetails,
    IndexingDetails, StartDetails, StartNoCodeDetails,
};

// export 3rd party dependencies
pub use async_trait::async_trait;
pub use colored::Colorize as RindexerColorize;
pub use futures::FutureExt;
pub use lazy_static::lazy_static;
pub use tokio::main as rindexer_main;
pub use tokio_postgres::types::Type as PgType;
pub use tracing::{error as rindexer_error, info as rindexer_info};
