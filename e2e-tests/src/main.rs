use clap::Parser;
use std::path::Path;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

mod anvil_setup;
mod docker;
mod health_client;
mod live_feeder;
mod rindexer_client;
mod test_suite;
mod tests;

use tests::run_tests;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the Rindexer binary
    #[arg(short, long, default_value = "../target/release/rindexer_cli")]
    rindexer_binary: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Specific tests to run (comma-separated). If not provided, runs all tests.
    #[arg(long)]
    tests: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load env from common locations if present
    // Try project root .env, then src/.env
    let _ = dotenvy::from_filename(".env");
    if !Path::new(".env").exists() {
        let _ = dotenvy::from_filename("src/.env");
    }

    let args = Args::parse();

    // Initialize tracing with configurable log level
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();

    info!("Starting Rindexer E2E Test Suite");
    info!("Binary: {}", args.rindexer_binary);

    // Run the test suite using the new registry-based runner
    let test_names = args.tests.map(|t| t.split(',').map(|s| s.trim().to_string()).collect());

    match run_tests(args.rindexer_binary, test_names).await {
        Ok(_) => {
            info!("Test suite completed successfully");
        }
        Err(e) => {
            error!("Test suite failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
