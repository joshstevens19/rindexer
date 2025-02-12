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

- Running GraphQL and Indexer: [`ghcr.io/joshstevens19/rindexer-bundled`](https://github.com/users/joshstevens19/packages/container/package/rindexer-bundled)
- Running Indexer only: [`ghcr.io/joshstevens19/rindexer`](https://github.com/users/joshstevens19/packages/container/package/rindexer)

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

This is the express project which leverages postgraphile rindexer GraphQL, we package it into a binary and run it within the rindexer
to avoid having to have node/postgraphile installed on the machine running it.

### documentation

This is the documentation for rindexer, it is built using [voc](https://vocs.dev/) which is an incredible
tool to build documentation. Big shout out to `wevm` team for all the work they have done on `vocs`, `viem` and `wagmi`.

### examples

This just holds some no-code examples for rindexer which is referenced in the docs or used for new users to see
how a project is setup.

## Building

### Locally 

To build locally you can just run `cargo build` in the root of the project. This will build everything for you
as this is a workspace.

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

To release a new rindexer you have to do a few things:

1) Checkout release/x.x.x branch depending on the next version number
2) Update the changelog in documentation/docs/pages/docs/introduction/changelog.mdx
3) Open up the cli folder and go to the Cargo.toml and update the version number
4) Push the branch up to GitHub which will queue a build on the CI
5) Once the build is successful you can open a PR merging the release branch into master
6) Merge will auto deploy the release

