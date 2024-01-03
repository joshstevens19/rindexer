use clap::{arg, Parser, Subcommand};

/// Command line arguments for the momoka-rs program.
// #[derive(Parser)]
// #[command(author, version, about, long_about = None)]
// struct Cli {
//     /// The URL of the node.
//     #[arg(short = 'n', value_name = "NODE")]
//     node: Option<String>,

//     /// The environment (e.g., "MUMBAI" or "POLYGON").
//     #[arg(short = 'e', value_name = "ENVIRONMENT")]
//     environment: Option<String>,

//     /// The deployment (e.g., "PRODUCTION").
//     #[arg(short = 'd', value_name = "DEPLOYMENT")]
//     deployment: Option<String>,

//     /// The transaction ID to check proof for.
//     #[arg(short = 't', value_name = "TX_ID")]
//     tx_id: Option<MomokaTxId>,

//     /// Flag indicating whether to perform a resync.
//     #[arg(short = 'r', value_name = "RESYNC")]
//     resync: bool,
// }

#[derive(Parser, Debug)]
#[clap(author = "Author Name", version, about)]
/// A Very simple Package Hunter
struct Arguments {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// Start the indexer
    Start {
        #[arg(short = 'm', value_name = "path")]
        /// Name of the package to search
        manifest_path: String,

        #[arg(short = 'n', value_name = "[network]")]
        /// Name of the package to search
        network: String,
    },
    /// list all the projects
    Projects {
        #[arg(short = 't', value_name = "TX_ID")]
        /// directory to start exploring from
        start_path: String,
    },
}

fn main() {
    let args = Arguments::parse();

    match args.cmd {
        SubCommand::Start {
            manifest_path,
            network,
        } => {
            println!("{} uses found", manifest_path)
        }
        SubCommand::Projects { start_path } => {
            println!("{} uses found", start_path)
        }
    }
}
