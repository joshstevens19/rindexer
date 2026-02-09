# 🦀 rindexer 🦀 

Note rindexer is brand new and actively under development, things will change and bugs will exist - if you find any bugs or have any
feature requests please open an issue on [github](https://github.com/joshstevens19/rindexer/issues).

rindexer is an opensource powerful, high-speed indexing toolset developed in Rust, designed for compatibility with any EVM chain.
This tool allows you to index chain events using a simple YAML file, requiring no additional coding.
For more advanced needs, the rindexer provides foundations and advanced capabilities to build whatever you want.
It's highly extendable, enabling you to construct indexing pipelines with ease and focus exclusively on the logic.
rindexer out the box also gives you a GraphQL API to query the data you have indexed instantly.

You can get to the full rindexer [documentation](https://rindexer.xyz/).

## Install 

```bash
curl -L https://rindexer.xyz/install.sh | bash
```

If you’re on Windows, you will need to install and use Git BASH or WSL, as your terminal,
since rindexer installation does not support Powershell or Cmd.

## Use rindexer

Once installed you can run `rindexer --help` in your terminal to see all the commands available to you.

```bash
rindexer --help
```

```bash
Blazing fast EVM indexing tool built in rust

Usage: rindexer [COMMAND]

Commands:
  new           Creates a new rindexer no-code project or rust project
  start         Start various services like indexers, GraphQL APIs or both together
  add           Add elements such as contracts to the rindexer.yaml file
  codegen       Generates rust code based on rindexer.yaml or graphql queries
  delete        Delete data from the postgres database or csv files
  phantom       Use phantom events to add your own events to contracts
  help          Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

We have full documentation https://rindexer.xyz/docs/introduction/installation which goes into more detail on how to use 
rindexer and all the commands available to you.

## Docker

There's a pre-built docker image which can be used to run `rindexer` inside your dockerized infra:

- Docker image: [`ghcr.io/joshstevens19/rindexer`](https://github.com/users/joshstevens19/packages/container/package/rindexer)

### Create new project
To create a new `no-code` project in your current directory, you can run the following:

`docker run -it -v $PWD:/app/project_path ghcr.io/joshstevens19/rindexer new -p /app/project_path no-code`

### Use with existing project
To use it with an existing project and a running postgres instance you can simply invoke:

```
export PROJECT_PATH=/path/to/your/project
export DATABASE_URL="postgresql://user:pass@postgres/db"

docker-compose up -d
```

This will start all local indexing and if you have enabled the graphql endpoint, it will become exposed under:

http://localhost:3001

## Helm Chart

We also provide a Helm chart for deploying `rindexer` in Kubernetes environments. The Helm chart simplifies the deployment process and allows for easy customization of the deployment parameters.

You can find the Helm chart in the following directory:

- **[rindexer Helm Chart](https://github.com/joshstevens19/rindexer/tree/master/helm/rindexer)**

To use the Helm chart, follow the instructions in the [Helm Chart README](https://github.com/joshstevens19/rindexer/tree/master/helm/rindexer/README.md) to deploy `rindexer` to your Kubernetes cluster.

## What can I use rindexer for?

- Hackathons: spin up a quick indexer to index events for your dApp with an API without any code needed
- Data reporting
- Building advanced indexers
- Building a custom indexer for your project
- Fast prototyping and MVP developments
- Quick proof-of-concept projects
- Enterprise standard indexing solutions for projects
- Much more... 

## Crate.io

rindexer rust project building is available on crate.io but we strongly recommend using the git repository to install it
and use it in your project. To use the CLI please install it using the above instructions.

https://crates.io/crates/rindexer

## What networks do you support?

rindexer supports any EVM chain out of the box. If you have a custom chain, you can easily add support for it by
adding the chain's RPC URL to the YAML configuration file and defining the chain ID. No code changes are required.

## Code structure

### core

This is the core of rindexer, it contains all the logic for indexing and where most the code lives.

### cli

This
is the cli for rindexer, it contains all the logic for the cli and is how users interact with rindexer.

### graphql

This is the express project which leverages postgraphile rindexer GraphQL, it is automatically built into a binary during the Rust build process using `pkg`.

**Build Process:**
- Automatically builds during `cargo build`
- Detects target architecture (macOS, Linux, Windows) 
- Smart rebuilding - only rebuilds when source files change
- Requires Node.js and npm for development/building

**Development:**
```bash
cd graphql
npm install
npm start
```

The binary is embedded into the Rust application and started automatically when GraphQL functionality is enabled.

### documentation

This is the documentation for rindexer, it is built using [voc](https://vocs.dev/) which is an incredible
tool to build documentation. Big shout out to `wevm` team for all the work they have done on `vocs`, `viem` and `wagmi`.

### examples

Example projects showing different ways to use rindexer. There are three types: **no-code** (YAML-only, no Rust project), **table** (data transformations before storage), and **rust** (full Rust projects with custom handlers).

#### No-code examples

- **[rindexer_demo_cli](examples/rindexer_demo_cli/)** — Basic PostgreSQL indexing example for RocketPool ETH transfers.
- **[nocode_clickhouse](examples/nocode_clickhouse/)** — Basic ClickHouse indexing example for RocketPool ETH transfers.
- **[rindexer_demo_custom_indexing](examples/rindexer_demo_custom_indexing/)** — Custom indexing patterns demonstration.
- **[rindexer_native_transfers](examples/rindexer_native_transfers/)** — Index native ETH transfers using trace_block (requires specific RPC support).
- **[streams_playground](examples/streams_playground/)** — Stream integrations: Kafka, RabbitMQ, Redis, and Webhook examples.

#### Rust examples

- **[rindexer_rust_playground](examples/rindexer_rust_playground/)** — Multi-contract, multi-network, dual storage (PostgreSQL + CSV), event filtering, and global contracts.
- **[rindexer_factory_indexing](examples/rindexer_factory_indexing/)** — Factory pattern indexing: dynamically discovers and indexes contracts created by Uniswap V3 factory events.
- **[clickhouse_factory_indexing](examples/clickhouse_factory_indexing/)** — Same factory pattern as above, but using ClickHouse as the storage backend.
- **[rust_clickhouse](examples/rust_clickhouse/)** — Minimal "hello world" for rindexer with ClickHouse: single contract, single network.

#### Table examples

- **[tables_erc20_balances](examples/tables_erc20_balances/)** — Track ERC20 token balances per holder with add/subtract operations.
- **[tables_erc20_allowances](examples/tables_erc20_allowances/)** — Track ERC20 approval allowances (owner → spender).
- **[tables_erc721_ownership](examples/tables_erc721_ownership/)** — Track NFT ownership per token ID.
- **[tables_erc1155_balances](examples/tables_erc1155_balances/)** — Track ERC1155 balances with TransferBatch iteration support.
- **[tables_dex_pool](examples/tables_dex_pool/)** — Track Uniswap V2 pool state: reserves, volume, and LP positions.
- **[tables_factory_uniswap](examples/tables_factory_uniswap/)** — Factory indexing with tables: track Uniswap V3 pool swap metrics.
- **[tables_governance](examples/tables_governance/)** — Track governance votes with compound primary keys.
- **[tables_token_supply](examples/tables_token_supply/)** — Track token supply with mints/burns using global tables.
- **[tables_registry_delete](examples/tables_registry_delete/)** — Demonstrates delete operations for maintaining active sender registry.
- **[tables_view_calls](examples/tables_view_calls/)** — Enrich events with on-chain view call data.
- **[tables_cron_chainlink_price](examples/tables_cron_chainlink_price/)** — Cron-triggered tables to fetch Chainlink prices on schedule.
- **[tables_cron_historical_sync](examples/tables_cron_historical_sync/)** — Cron historical sync: replay cron operations at past blocks.
- **[tables_factory_cron](examples/tables_factory_cron/)** — Combine factory indexing with cron operations.

#### Summary table

| Example | Type | Key differentiator | Storage | Complexity |
|---|---|---|---|---|
| `rindexer_demo_cli` | No-code | Basic PostgreSQL indexing | PostgreSQL | Low |
| `nocode_clickhouse` | No-code | Basic ClickHouse indexing | ClickHouse | Low |
| `rindexer_demo_custom_indexing` | No-code | Custom indexing patterns | PostgreSQL | Low |
| `rindexer_native_transfers` | No-code | Native ETH transfer indexing | Streams | Medium |
| `streams_playground` | No-code | Stream integrations (Kafka, RabbitMQ, etc.) | Streams | Medium |
| `rust_clickhouse` | Rust | Minimal starter example | ClickHouse | Low |
| `rindexer_factory_indexing` | Rust | Factory pattern (dynamic contract discovery) | PG + CSV | Medium |
| `clickhouse_factory_indexing` | Rust | Factory pattern on ClickHouse | ClickHouse | Medium |
| `rindexer_rust_playground` | Rust | Multi-contract, multi-network, dual storage | PG + CSV | High |
| `tables_erc20_balances` | Table | Balance tracking with add/subtract | PostgreSQL | Medium |
| `tables_erc20_allowances` | Table | Approval allowances tracking | PostgreSQL | Medium |
| `tables_erc721_ownership` | Table | NFT ownership tracking | PostgreSQL | Medium |
| `tables_erc1155_balances` | Table | Multi-token balance tracking | PostgreSQL | Medium |
| `tables_dex_pool` | Table | DEX pool state tracking | PostgreSQL | Medium |
| `tables_factory_uniswap` | Table | Factory + tables pattern | PostgreSQL | High |
| `tables_governance` | Table | Compound primary keys demo | PostgreSQL | Medium |
| `tables_token_supply` | Table | Global tables for supply tracking | PostgreSQL | Medium |
| `tables_registry_delete` | Table | Delete operations demo | PostgreSQL | Medium |
| `tables_view_calls` | Table | On-chain view call enrichment | PostgreSQL | Medium |
| `tables_cron_chainlink_price` | Table | Cron-triggered data fetching | PostgreSQL | Medium |
| `tables_cron_historical_sync` | Table | Historical cron replay | PostgreSQL | Medium |
| `tables_factory_cron` | Table | Factory + cron pattern | PostgreSQL | High |

## Building

### Requirements

- Rust (latest stable)
- Node.js and npm (for GraphQL server build)

### Locally 

To build locally you can just run `cargo build` in the root of the project. This will build everything for you as this is a workspace, including the GraphQL server binary.

**Note:** The first build may take longer as it needs to:
1. Install npm dependencies for the GraphQL server
2. Build the GraphQL binary for your target platform
3. Compile all Rust code

Subsequent builds use smart caching and will only rebuild components that have changed.

### Prod

To build for prod you can run `make prod_build` this will build everything for you and optimise it for production.

## Formatting

you can run `cargo fmt` to format the code, rules have been mapped in the `rustfmt.toml` file.

## Contributing

Anyone is welcome to contribute to rindexer, feel free to look over the issues or open a new one if you have
any new ideas or bugs you have found.

### Playing around with the CLI locally

You can use the `make` commands to run the CLI commands locally, this is useful for testing and developing.
These are located in the `cli` folder > `Makefile`. It uses `CURDIR` to resolve the paths for you, so they should work
out of the box. The examples repo has a `rindexer_demo_cli` folder which you can modify (please do not commit any changes though) 
or spin up a new no-code project using the make commands.

## Release

To release a new rindexer:

1. Checkout `release/x.x.x` branch depending on the next version number
2. Push the branch to GitHub which will queue a build on the CI
3. Once build is successful, a PR will be automatically created with updated changelog and version
4. Review and merge the auto-generated PR - this will auto-deploy the release with binaries built from the release branch
