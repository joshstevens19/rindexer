use anyhow::Context;
use clap::{Parser, Subcommand};
use rindexer::blockclock::DeltaEncoder;
use std::path::PathBuf;
use tracing_subscriber::filter::LevelFilter;
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

    /// Update the timestamps encodings for blocks on all already supported networks.
    ///
    /// # Example
    ///
    /// ```sh
    /// cargo xtask update-block-clocks \
    ///   --alchemy-api-key "API_KEY" \
    ///   --batch-size 1000
    /// ```
    UpdateBlockClocks {
        /// RPC Url for the Network.
        #[arg(long)]
        alchemy_api_key: String,
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

            let mut encoder = DeltaEncoder::from_file_inner(network, Some(&rpc_url), &path)?;
            encoder.poll_encode_loop(batch_size.unwrap_or(100)).await?;

            Ok(())
        }
        Commands::UpdateBlockClocks { alchemy_api_key, batch_size } => {
            let base = std::env::var("CARGO_MANIFEST_DIR").context("missing CARGO_MANIFEST_DIR")?;
            let path = PathBuf::from(base);
            let blockclock_dir = path
                .parent()
                .context("missing parent directory")?
                .join("core")
                .join("resources")
                .join("blockclock");

            for entry in
                std::fs::read_dir(&blockclock_dir).context("reading blockclock directory")?
            {
                let entry = entry?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("blockclock") {
                    continue;
                }

                let stem =
                    path.file_stem().and_then(|s| s.to_str()).context("invalid file name")?;
                let network: u32 = stem
                    .parse()
                    .with_context(|| format!("invalid network id in file: {}", stem))?;

                tracing::info!("--------------------------------------------------");
                tracing::info!(
                    "Updating blockclock for network {} from file {}",
                    network,
                    path.display()
                );
                tracing::info!("--------------------------------------------------");

                let subdomain = match network {
                    1 => "eth-mainnet",
                    137 => "polygon-mainnet",
                    10 => "opt-mainnet",
                    42161 => "arb-mainnet",
                    56 => "bnb-mainnet",
                    324 => "zksync-mainnet",
                    8453 => "base-mainnet",
                    43114 => "avax-mainnet",
                    534352 => "scroll-mainnet",
                    _ => anyhow::bail!("unsupported network {network}"),
                };

                let rpc_url = format!("https://{}.g.alchemy.com/v2/{}", subdomain, alchemy_api_key);

                let mut encoder = DeltaEncoder::from_file_inner(network, Some(&rpc_url), &path)
                    .with_context(|| format!("failed to create encoder for network {}", network))?;

                encoder.poll_encode_loop(batch_size.unwrap_or(100)).await?;
            }

            tracing::info!(
                "Finished updating all supported block clocks: {}",
                path.to_str().context("path to string")?
            );

            Ok(())
        }
    };

    if let Err(e) = res {
        tracing::error!("Error running xtask: {}", e);
    };

    Ok(())
}
