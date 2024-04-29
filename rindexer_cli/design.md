rindexer init --from-contract <CONTRACT_ADDRESS> --network <ETHEREUM_NETWORK>

Options:

      --abi <path>              Path to the contract ABI (default: download from Etherscan)
      --contract-name           Name of the contract (default: Contract)
      --merge-entities          Whether to merge entities with the same name (default: false)
      --network-file <path>     Networks config file path (default: "./networks.json")

rindexer init --from-subgraph-id <id>
rindexer init --from-contract <CONTRACT_ADDRESS> --network <ETHEREUM_NETWORK> --abi <FILE>

rindexer add <address>

Options:

      --abi <path>              Path to the contract ABI (default: download from Etherscan)
      --contract-name           Name of the contract (default: Contract)
      --chain-id <chainId>      The chain id

rindexer ls

rindexer start indexer --mainifest <path> --port <port>

rindexer start graphql --mainifest <path> --port <port>

rindexer start rest-api --mainifest <path> --port <port>

rindexer stop <name>

rindexer status <name>
