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
  help          Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

## Working with CLI locally

The best way to work with the CLI is to use the `cargo run` command with args after it inside the CLI project, 
for example if I wanted to create a new project I would run:

```bash
cargo run -- new --path PATH_TO_CREATE_PROJECT no-code
```

This would create a new no-code project in the path you specified.

If you wanted to look at the help you can run:

```bash
cargo run -- help
```

This will show you all the commands available to you.