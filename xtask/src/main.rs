use anyhow::Context;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use rindexer::blockclock::DeltaEncoder;
use std::fmt::Debug;
use std::path::PathBuf;
use tracing_subscriber::filter::{FilterExt, LevelFilter};
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(name = "xtask", version, about = "Backend scripts")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// RunLength Encode the timestamps for block on a specific network.
    ///
    /// # Example
    ///
    /// ```sh
    /// cargo xtask encode-block-clock \
    ///   --network 137 \
    ///   --rpc-url "https://polygon-mainnet.g.alchemy.com/v2/API_KEY" \
    ///   --batch-size 1000
    /// ```
    EncodeBlockClock {
        /// Name of the continuous aggregate (without _base suffix)
        #[arg(long)]
        network: u32,
        /// RPC Url for the Network.
        #[arg(long)]
        rpc_url: String,
        /// Batch size for block requests.
        #[arg(long)]
        batch_size: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter =
        EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy();
    let fmt = fmt::layer().with_timer(fmt::time::UtcTime::rfc_3339()).with_target(false);

    tracing_subscriber::registry().with(fmt).with(filter).init();

    let res: anyhow::Result<()> = match cli.command {
        Commands::EncodeBlockClock { network, rpc_url, batch_size } => {
            let base = std::env::var("CARGO_MANIFEST_DIR").context("missing CARGO_MANIFEST_DIR")?;
            let path = PathBuf::from(base);
            let path = path
                .parent()
                .context("missing parent directory")?
                .join("core")
                .join("resources")
                .join("blockclock");
            let path = path.join(format!("{}.blockclock", network));

            tracing::info!("Writing to directory: {}", path.to_str().context("path to string")?);

            let mut encoder = DeltaEncoder::from_file(network, Some(&rpc_url), &path)?;
            encoder.poll_encode_loop(batch_size.unwrap_or(100)).await?;

            Ok(())
        }
    };

    if let Err(e) = res {
        tracing::error!("Error running xtask: {}", e);
    };

    Ok(())
}
