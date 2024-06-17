#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
pub use helpers::{generate_random_id, write_file, WriteFileError};
mod api;
pub use api::{GraphQLServerDetails, GraphQLServerSettings};

mod logger;
pub mod provider;
mod start;
mod types;

pub use logger::setup_logger;

pub use start::{
    start_rindexer, start_rindexer_no_code, GraphqlNoCodeDetails, IndexerNoCodeDetails,
    IndexingDetails, StartDetails, StartNoCodeDetails,
};

// export 3rd party dependencies
pub use async_trait::async_trait;
pub use futures::FutureExt;
pub use lazy_static::lazy_static;
pub use tokio::main as rindexer_main;
