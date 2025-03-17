# Extension for Block Traces

Chains / Providers supporting `debug_traceBlockByNumber` (looks like quicknode supports all).

- Zksync: https://www.quicknode.com/docs/zksync/debug_traceBlockByNumber
- Polygon: https://www.quicknode.com/docs/polygon/debug_traceBlockByNumber
- Worldchain: https://www.quicknode.com/docs/worldchain/debug_traceBlockByNumber
- Ethereum: https://www.quicknode.com/docs/ethereum/debug_traceBlockByNumber 
- Optimism: https://www.quicknode.com/docs/optimism/debug_traceBlockByNumber
- Arbitrum: https://www.quicknode.com/docs/arbitrum/debug_traceBlockByNumber
- Scroll: https://www.quicknode.com/docs/scroll/debug_traceBlockByNumber
- Blast: https://www.quicknode.com/docs/blast/debug_traceBlockByNumber
- Base: (https://www.quicknode.com/docs/base/debug_traceBlockByNumber)

**Config Proposal**

Below is an example Native ETH trasnfer from `0x471af209448eef106c9bd6decac524bafa95bab7` to `0xe7678d3aad5108906e55ffd9785bced293e8ae7c`.

```json
{
  "action": {
    "from": "0x471af209448eef106c9bd6decac524bafa95bab7",
    "callType": "call",
    "gas": "0x0",
    "input": "0x",
    "to": "0xe7678d3aad5108906e55ffd9785bced293e8ae7c",
    "value": "0x110d9316ec000"
  },
  "blockHash": "0x6da844a2dd81085c0b3703cadb0a4f364783d75b8da98d6b915083b2b6b99bc9",
  "blockNumber": 22043336,
  "result": {
    "gasUsed": "0x0",
    "output": "0x"
  },
  "subtraces": 0,
  "traceAddress": [],
  "transactionHash": "0x8d50c03d36d323d3fa886ffc55d52f469d8d7d48f41ac201b9a033f60c29aa67",
  "transactionPosition": 152,
  "type": "call"
}
```

Based on the above we really only need to filter on the `action` object.

```yaml
name: rNativeTokenIndexer
description: My first native token rindexer project
repository: https://github.com/joshstevens19/rindexer
project_type: no-code
networks:
  - name: ethereum
    chain_id: 1
    rpc: ${RPC_URL}
  - name: optimism
    chain_id: 10
    rpc: ${RPC_URL}
storage:
  postgres:
    enabled: true
native_transfers:
  enabled: true
  details:
    - network: ethereum
      start_block: 18600000  # Optional
      end_block: 18718056  # Optional
    - network: optimism
      start_block: 100  # Optional
      end_block: 1000  # Optional
contracts:
  - name: ERC20
    details:
      - network: ethereum
        filter:
          - event_name: Transfer
          - event_name: Approval


########
#
# The conditions can be defined on the Stream publish mechanism rather than the ingest config.
# 
# conditions:
#     - "from": "0x0338ce5020c447f7e668dc2ef778025ce3982662 || 0x0338ce5020c447f7e668dc2ef778025ce398266d"
#     - "value": ">0"
#     - "gas": "0"
#     - "input": "0x",
#     - "call_type": "call || delegateCall"
```