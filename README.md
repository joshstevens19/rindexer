# rindexer

inspiration - https://ponder.sh/docs/guides/add-contracts

networks - https://github.com/ponder-sh/ponder/blob/83e2b4a7a05d847832ba60adde361736deeb3b2c/packages/core/src/config/networks.ts#L22

eth_getLogs - https://github.com/ponder-sh/ponder/blob/83e2b4a7a05d847832ba60adde361736deeb3b2c/packages/core/src/sync-historical/service.ts#L946

checklist:

- rindexer contract mappings
- rindexer mappings functions (ability to call smart contract functions)
- rindexer schema designer
- postgres support, mysql support, sql server support
- mongodb support
- graphql API
- rest API
- from command line shortcuts https://ponder.sh/docs/api-reference/create-ponder
- use with foundry - https://ponder.sh/docs/advanced/foundry
- POC with shadow events using foundry as you index
- handle reorgs

// cargo new node --lib

// cargo run -- start help

Flows:

- User creates a new yaml file and maps config
- User runs rindexer generate which generates the mappings files types
- User then uses that generated file to register mappings with the indexer
