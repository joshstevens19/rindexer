# rindexer

inspiration - https://ponder.sh/docs/guides/add-contracts

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
