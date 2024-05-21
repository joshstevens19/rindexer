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

# NEW VISION

rindexer new 
1. Project name 
2. Project description:
3. Repository (can be blank):
4. Do you want an example indexing manifest? yes / no
5. what data-layers to enable? postgres/csv/none? <list> (none = console)
6. if postgres > docker support out the box? yes / no
7. postgres only (generated env file..)
- this creates a manifest file with ethereum 
- git init happens automatically and git commit -m 'setup rindexer'
- give information about what to do next which is run indexer, regenerate typings and start graphql

rindexer dev (this one uses fast build but slower to index)
- dev would run docker-compose up for you if you have postgres enabled
rindexer start (this one uses slower build but faster to index) 
--type=graphql/indexer/both --hostname=0.0.0.0 --port=5000 --indexers=<list>
rindexer start indexer
rindexer start graphql --hostname=localhost --port=5001
rindexer start full --hostname=localhost --port=5001
rindexer start full

rindexer start
1. do you want to start: indexer or graphql or both ?
2. graphql hostname: 0.0.0.0 (default)
3. graphql port: 5000 (default)
4. SKIP IF ONLY 1 INDEXER: which indexers to start? <list> (default: all)
5. **warning note if your indexer is not running your graphql will be empty or have the old data**

rindexer codegen typings / indexers / both
1. what do you want to regenerate? typings or handlers or both? (skip if above)
2. git diff on indexers folder to see what has changed
3. if changed ask them "we see you got changes in the indexers folder, do you want to override them?" yes / no

rindexer download-abi --path <path>
console.log('rindexer only supports downloading ABIs from <list>. If you want to add ABI from a network not supported please add it to the ABIs folder manually')
1. Network: <list> (polygon,base,bsc)
2. Contract address: <insert>
- download to ABIs folder

https://api.etherscan.io/api?module=contract&action=getabi&address=0x1111111254EEB25477B68fb85Ed929f73A960582 
(1 request per 5 seconds)

rindexer prune (based on storage config in manifest.yaml)
1. Are you sure you wish to prune the database (it can not be reverted)? yes / no
2. Are you sure you wish to prune the csv files (it can not be reverted)? yes / no

# END OF NEW VISION

rindexer ls

rindexer generate-typings

rindexer generate-full-project

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
