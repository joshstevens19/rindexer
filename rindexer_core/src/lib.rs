// public
pub mod generator;
pub mod indexer;
pub mod manifest;

mod database;
pub use database::postgres::{setup_postgres, EthereumSqlTypeWrapper, PostgresClient};

mod simple_file_formatters;
pub use simple_file_formatters::csv::AsyncCsvAppender;

mod helpers;
pub use helpers::{generate_random_id, write_file};
mod api;
pub use api::{GraphQLServerDetails, GraphQLServerSettings};

mod logger;
pub mod provider;
mod start;
pub use logger::setup_logger;

pub use start::{
    start_rindexer, start_rindexer_no_code, IndexingDetails, StartDetails, StartNoCodeDetails,
};

// export 3rd party dependencies
pub use async_trait::async_trait;
pub use futures::FutureExt;
pub use lazy_static::lazy_static;
pub use tokio::main as rindexer_main;
