# Using Reth Execution Extensions (ExEx)
Reth Execution Extensions (ExEx) is a powerful framework introduced by Reth for building high-performance off-chain infrastructure as post-execution hooks.

The key advantage of ExEx is that it provides a reorg-aware stream called ExExNotification which includes blocks, transactions, receipts, state changes, and trie updates. This allows your applications to safely operate at native block time while handling chain reorganizations properly.

## Configuring Reth ExEx in rindexer
rindexer supports using Reth ExEx through the reth configuration in your [network settings](/docs/start-building/yaml-config/networks#reth).

```yaml [rindexer.yaml]
name: rETHIndexer
description: My first rindexer project
repository: https://github.com/joshstevens19/rindexer
project_type: no-code
networks:
- name: ethereum
  chain_id: 1
  rpc: https://mainnet.gateway.tenderly.co
  reth: // [!code focus]
    enabled: true // [!code focus]
    cli_config: // [!code focus]
      data_dir: /path/to/reth/data // [!code focus]
      authrpc_jwtsecret: /path/to/jwt.hex // [!code focus]
      authrpc_addr: 127.0.0.1 // [!code focus]
      authrpc_port: 8551 // [!code focus]
      full: false // [!code focus]
      metrics: null // [!code focus]
      chain: mainnet // [!code focus]
      http: true // [!code focus]
```

## Getting Started
To get started with Reth ExEx in your rindexer project:
1. Ensure you have an archive Reth node running
2. Configure your rindexer.yaml file with the reth section
3. Run your rindexer project normally