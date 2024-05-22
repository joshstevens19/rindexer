# rindexer

inspiration - https://ponder.sh/docs/guides/add-contracts

networks - https://github.com/ponder-sh/ponder/blob/83e2b4a7a05d847832ba60adde361736deeb3b2c/packages/core/src/config/networks.ts#L22

eth_getLogs - https://github.com/ponder-sh/ponder/blob/83e2b4a7a05d847832ba60adde361736deeb3b2c/packages/core/src/sync-historical/service.ts#L946

checklist v1.0:
- complete command line
- investigate graphql/REST API exposing
  - npx postgraphile -c postgres://rindexer_user:U3uaAFmEbv9dnxjKOo9SbUFwc9wMU5ADBHW%2BHUT%2F7%2BDpQaDeUYV%2F@localhost:5440/postgres --host 0.0.0.0 --port 5005 --watch --schema public,lens_registry_example --default-role rindexer_user --enhance-graphiql --cors --disable-default-mutations
- create documentation
- look into deployments to make it easy to do
- look into setting your own schema for the database using diesel

nice to have:

- investigate indexing contracts that are deployed within an event onchain
  - register a manifest defining factory including address, event, parameter name, and ABI
  - when it emits the event of the factory start a new log polling for the new contract
  - emit the log through the same event register as the event defined in the manifest

bugs:
- start + end dates are reading wrong from manifest.yaml as U64 takes numbers as hex

future features:
- investigate handle advanced top of head reorgs process
- cron registering for networks to fire
- multiple ABIs merged into one
- look into https://diesel.rs/ for mapping schemas etc
- look into load balancing of RPCs
- other db support
- look into internal caching to make things faster
- look into dependency mappings to allow you to index based on trees structure
- POC with shadow events using foundry as you index
- merge subgraph to rindexer yaml

// cargo new node --lib

// cargo run -- start help

Flows:

- User creates a new yaml file and maps config
- User runs rindexer generate which generates the mappings files types
- User then uses that generated file to register mappings with the indexer
