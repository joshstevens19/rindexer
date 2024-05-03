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

// rethink about CLI 

rindexer ls indexers

rindexer ls networks

rindexer ls global

rindexer init 
1. Project Name
1. Project description: 
2. Repository (can be blank):

rindexer add network 
1. Network name:
2. RPC url:
3. Max block range (skip): 
4. Max concurrency (skip):

rindexer remove network <network>
1. This will remove any mappings for this network including contracts: yes / no

rindexer add indexer
1. Indexer name:
2. For Network: <select from list | or add new>
   3. <select from list | or add new>
   4. Add another? < repeat
3. Add Contract:
   4. Network: <select from list | or add new>
   5. Address: 
   6. Name: (preselect contract name)
   7. ABI can not be found from etherscan please attach ABI location: 
   8. Start block: (add deploy block)
   9. End block (can leave as null if you want to keep resyncing): 
   10. Polling every (ms): <block time>
   11. Add another? < repeat
4. Add global contracts (these can be used to do view lookups while indexing):
   5. Network: <select from list>
   6. Address:
   7. Name: (preselect contract name)
   8. ABI can not be found from etherscan please attach ABI location:
   9. Add another? repeat
5. Enable database? <postgres | none>
   6. Setting up env:
      7. If you just want to run this indexer locally skip this step and it will set you up a docker file with postgres - skip / continue
      8. database name:
      9. database user:
      10. database password:
      11. database host:
      12. database port: 

reindexer remove indexer <indexer_name>
1. Are you sure you wish to remove this indexer? yes / no

rindexer add <indexer_name> contract
1. Network: <select from list | or add new>
2. Address:
3. Name: (preselect contract name)
4. ABI can not be found from etherscan please attach ABI location:
5. Start block: (add deploy block)
6. End block (can leave as null if you want to keep resyncing):Polling every (ms): <block time>
7. Add another? < repeat

rindexer remove <indexer_name> contract <contract_name>
1. Are you sure you wish to remove this indexer? yes / no

rindexer add <indexer_name> global contract
1. Network: <select from list | or add new>
2. Address:
3. Name: (preselect contract name)
4. ABI can not be found from etherscan please attach ABI location:
5. Add another? < repeat

rindexer remove <indexer_name> global contract <contract_name>
1. Are you sure you wish to remove this indexer? yes / no
