use clap::{Args, Parser, Subcommand};

#[allow(clippy::upper_case_acronyms)]
#[derive(Parser, Debug)]
#[clap(name = "rindexer", about, version, author = "Your Name")]
pub struct CLI {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Parser, Debug)]
pub struct NewDetails {
    #[clap(short, long)]
    name: Option<String>,

    #[clap(short, long)]
    project_description: Option<String>,

    #[clap(short, long)]
    repository: Option<String>,

    #[clap(short, long)]
    database: Option<bool>,
}

#[derive(Parser, Debug)]
#[clap(author = "Josh Stevens", version = "1.0", about = "Blazing fast EVM indexing tool built in rust", long_about = None)]
pub enum Commands {
    /// Creates a new rindexer no-code project or rust project.
    ///
    /// no-code = Best choice when starting, no extra code required.
    /// rust = Customise advanced indexer by writing rust code.
    ///
    /// This command initialises a new workspace project with rindexer
    /// with everything populated to start using rindexer.
    ///
    /// Example:
    /// `rindexer new no-code` or `rindexer new rust`
    #[clap(name = "new")]
    New {
        #[clap(subcommand)]
        subcommand: NewSubcommands,

        /// optional - The path to create the project in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
    /// Start various services like indexers, GraphQL APIs or both together
    ///
    /// `rindexer start indexer` or `rindexer start graphql` or `rindexer start all`
    #[clap(name = "start")]
    Start {
        #[clap(subcommand)]
        subcommand: StartSubcommands,

        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },

    /// Add elements such as contracts to the rindexer.yaml file.
    ///
    /// This command helps you build up your yaml file.
    ///
    /// Example:
    /// `rindexer add`
    #[clap(name = "add")]
    Add {
        #[clap(subcommand)]
        subcommand: AddSubcommands,

        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },

    /// Generates rust code based on rindexer.yaml or graphql queries
    ///
    /// Example:
    /// `rindexer codegen typings` or `rindexer codegen handlers` or `rindexer codegen graphql
    /// --endpoint=graphql_api` or `rindexer codegen rust-all`
    #[clap(name = "codegen")]
    Codegen {
        #[clap(subcommand)]
        subcommand: CodegenSubcommands,

        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
    /// Delete data from the postgres database or csv files.
    ///
    /// This command deletes rindexer project data from the postgres database or csv files.
    ///
    /// Example:
    /// `rindexer delete`
    Delete {
        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
    /// Use phantom events to add your own events to contracts
    ///
    /// This command helps you use phantom events within rindexer.
    ///
    /// Example:
    /// `rindexer phantom`
    #[clap(name = "phantom")]
    Phantom {
        #[clap(subcommand)]
        subcommand: PhantomSubcommands,

        /// optional - The path to create the project in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum NewSubcommands {
    /// Creates a new no-code project
    ///
    /// Best choice when starting, no extra code required.
    /// Example:
    /// `rindexer new no-code`
    #[clap(name = "no-code")]
    NoCode,

    /// Creates a new rust project
    ///
    /// Customise advanced indexer by writing rust code
    /// Example:
    /// `rindexer new rust`
    #[clap(name = "rust")]
    Rust,
}

#[derive(Subcommand, Debug)]
pub enum StartSubcommands {
    /// Starts the indexing service based on the rindexer.yaml file.
    ///
    /// Starts an indexer based on the rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer start indexer`
    Indexer,

    /// Starts the GraphQL server based on the rindexer.yaml file.
    ///
    /// Optionally specify a port to override the default.
    ///
    /// Example:
    /// `rindexer start graphql --port 4000`
    Graphql {
        #[clap(short, long, help = "Specify the port number for the GraphQL server")]
        port: Option<String>,
    },

    /// Starts the indexers and the GraphQL together based on the rindexer.yaml file.
    ///
    /// You can specify a port which will be used by all services that require one.
    ///
    /// Example:
    /// `rindexer start all --port 3000`
    All {
        #[clap(short, long, help = "Specify the port number for all services")]
        port: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum AddSubcommands {
    /// Add a contract from a network to the rindexer.yaml file. It will download the ABI and add
    /// it to the abis folder and map it in the yaml file.
    ///
    /// Example:
    /// `rindexer add contract`
    Contract,
}

#[derive(Subcommand, Debug)]
pub enum CodegenSubcommands {
    /// Generates the rindexer rust typings based on the rindexer.yaml file.
    ///
    /// This should not be edited manually and always generated.
    ///
    /// This is not relevant for no-code projects.
    ///
    /// Example:
    /// `rindexer codegen typings`
    Typings,

    /// Generates the rindexer rust indexers handlers based on the rindexer.yaml file.
    ///
    /// You can use these as the foundations to build your advanced indexers.
    ///
    /// This is not relevant for no-code projects.
    ///
    /// Example:
    /// `rindexer codegen indexer`
    Indexer,

    /// Generates the GraphQL queries from a GraphQL schema
    ///
    /// You can then use this in your dApp instantly to interact with the GraphQL API
    ///
    /// Example:
    /// `rindexer codegen graphql`
    #[clap(name = "graphql")]
    GraphQL {
        #[clap(long, help = "The graphql endpoint - defaults to localhost:3001")]
        endpoint: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct PhantomBaseArgs {
    /// The name of the contract to clone
    #[clap(value_parser)]
    pub contract_name: String,

    /// The network to clone the contract on
    #[clap(value_parser)]
    pub network: String,
}

#[derive(Args, Debug)]
pub struct PhantomCloneArgs {
    /// The name of the contract to clone
    #[clap(value_parser)]
    pub contract_name: String,

    /// The network to clone the contract on
    #[clap(value_parser)]
    pub network: String,
}

impl From<PhantomCloneArgs> for PhantomBaseArgs {
    fn from(value: PhantomCloneArgs) -> Self {
        Self { contract_name: value.contract_name, network: value.network }
    }
}

impl<'a> From<&'a PhantomCloneArgs> for PhantomBaseArgs {
    fn from(value: &'a PhantomCloneArgs) -> Self {
        Self { contract_name: value.contract_name.clone(), network: value.network.clone() }
    }
}

#[derive(Args, Debug)]
pub struct PhantomCompileArgs {
    /// The name of the contract to clone
    #[clap(value_parser)]
    pub contract_name: String,

    /// The network to clone the contract on
    #[clap(value_parser)]
    pub network: String,
}

impl From<PhantomCompileArgs> for PhantomBaseArgs {
    fn from(value: PhantomCompileArgs) -> Self {
        Self { contract_name: value.contract_name, network: value.network }
    }
}

impl<'a> From<&'a PhantomCompileArgs> for PhantomBaseArgs {
    fn from(value: &'a PhantomCompileArgs) -> Self {
        Self { contract_name: value.contract_name.clone(), network: value.network.clone() }
    }
}

#[derive(Args, Debug)]
pub struct PhantomDeployArgs {
    /// The name of the contract to clone
    #[clap(value_parser)]
    pub contract_name: String,

    /// The network to clone the contract on
    #[clap(value_parser)]
    pub network: String,
}

impl From<PhantomDeployArgs> for PhantomBaseArgs {
    fn from(value: PhantomDeployArgs) -> Self {
        Self { contract_name: value.contract_name, network: value.network }
    }
}

impl<'a> From<&'a PhantomDeployArgs> for PhantomBaseArgs {
    fn from(value: &'a PhantomDeployArgs) -> Self {
        Self { contract_name: value.contract_name.clone(), network: value.network.clone() }
    }
}

#[derive(Subcommand, Debug)]
pub enum PhantomSubcommands {
    /// Sets up phantom events on rindexer
    ///
    /// Want to add your own custom events to contracts? This command will help you do that.
    ///
    /// Example:
    /// `rindexer phantom init`
    #[clap(name = "init")]
    Init,

    /// Clone the contract with the network you wish to add phantom events to.
    ///
    /// Note contract name and network are your values in your rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer phantom clone <contract::name> <network>`
    #[clap(name = "clone")]
    Clone(PhantomCloneArgs),

    /// Compiles the phantom contract
    ///
    /// Note contract name and network are your values in your rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer phantom clone <contract::name> <network>`
    #[clap(name = "compile")]
    Compile(PhantomCompileArgs),

    /// Deploy the modified phantom contract
    ///
    /// This will compile and update your rindexer project with the phantom events.
    ///
    /// Example:
    /// `rindexer phantom deploy <contract::name> <network>`
    #[clap(name = "deploy")]
    Deploy(PhantomDeployArgs),
}
