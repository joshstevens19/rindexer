# rindexer

rindexer is a opensource powerful, high-speed indexing toolset developed in Rust, designed for compatibility with any EVM chain.
This tool allows you to index chain events using a simple YAML file, requiring no additional coding.
For more advanced needs, the rindexer provides foundations and advanced capabilities to build whatever you want.
It's highly extendable, enabling you to construct indexing pipelines with ease and focus exclusively on the logic.
rindexer out the box also gives you a GraphQL API to query the data you have indexed instantly.

You can get to the full rindexer [documentation](https://rindexer.xyz/docs/introduction/installation).

## Install 

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
  download-abi  Downloads ABIs from etherscan to build up your rindexer.yaml mappings
  codegen       Generates rust code based on rindexer.yaml or graphql queries
  delete        Delete data from the postgres database or csv files
  help          Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

We have full documentation https://rindexer.xyz/docs/introduction/installation which goes into more detail on how to use 
rindexer and all the commands available to you.

## What can I use rindexer for?

- Hackathons: spin up a quick indexer to index events for your dApp with an API without any code needed
- Data reporting
- Building advanced indexers
- Building a custom indexer for your project
- Fast prototyping and MVP developments
- Quick proof-of-concept projects
- Enterprise standard indexing solutions for projects
- Much more...

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

## Contributing

Anyone is welcome to contribute to rindexer, feel free to look over the issues or open a new one if you have
any new ideas or bugs you have found.

## Release

To release a new rindexer you have to do a few things:

1) Make sure you have update the changelog in documentation/docs/pages/docs/introduction/changelog.mdx
2) Open up the cli folder and go to the Cargo.toml and update the version number
3) Then run shell script ./release.sh this will prepare everything for you
4) Then you can just push all the file changes and it will deploy it. The rindexer binary is served through the
   documentation site.
5) We should then tag the release on github so its all clear

