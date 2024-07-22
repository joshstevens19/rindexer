# rindexer_cli

This is the cli for rindexer, it contains all the logic for the cli and is how users interact with rindexer.

You can get to the full rindexer [documentation](https://rindexer.xyz/docs/introduction/installation).

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

## Working with CLI locally

The best way to work with the CLI is to use the `Makefile` predefined commands.

You can also run your own commands using cargo run, example below would create a new no-code project in the path you specified.

```bash
cargo run -- new --path PATH_TO_CREATE_PROJECT no-code
```