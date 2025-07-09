# Using Reth Execution Extensions (ExEx)

Reth Execution Extensions (ExEx) is a powerful framework introduced by Reth for building high-performance off-chain infrastructure as post-execution hooks. rindexer leverages ExEx to provide superior indexing performance and native reorg handling.

## What is ExEx?

ExEx provides a reorg-aware stream called `ExExNotification` which includes:
- Blocks with full transaction data
- Receipts with logs and state changes
- Native reorg notifications
- Trie updates for state verification

This allows rindexer to:
- Process blocks at native speed without RPC overhead
- Handle reorganizations automatically
- Maintain consistency during chain splits
- Access pending transactions and state

## Architecture

When running in Reth mode, rindexer operates as an execution extension within the Reth node:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Reth Node  │────▶│ rindexer ExEx│────▶│ PostgreSQL  │
│             │     │              │     │             │
│             │◀────│   Indexing   │     │   Storage   │
└─────────────┘     └──────────────┘     └─────────────┘
     ExEx              Process              Write
  Notifications         Events              Data
```

## Chain State Notifications

rindexer processes three types of chain state notifications:

### 1. Committed
Emitted when new blocks are added to the canonical chain:
```rust
Committed {
    from_block: 19000000,
    to_block: 19000100,
    tip_hash: 0x123...
}
```

### 2. Reorged
Emitted during reorganizations:
```rust
Reorged {
    // Blocks to revert
    revert_from_block: 19000098,
    revert_to_block: 19000100,
    // New canonical blocks
    new_from_block: 19000098,
    new_to_block: 19000101,
    new_tip_hash: 0x456...
}
```

### 3. Reverted
Emitted when blocks are reverted (chain rollback):
```rust
Reverted {
    from_block: 19000099,
    to_block: 19000100
}
```

## Configuration

### Basic Configuration

```yaml [rindexer.yaml]
name: HighPerformanceIndexer
networks:
- name: ethereum
  chain_id: 1
  rpc: https://eth.llamarpc.com  # Fallback RPC
  reth:
    enabled: true
    logging: true  # Enable Reth logs
    cli_args:
      - "--datadir /data/reth"
      - "--authrpc.jwtsecret /secrets/jwt.hex"
      - "--authrpc.port 8551"
      - "--chain mainnet"
```

### Advanced Configuration

```yaml [rindexer.yaml]
networks:
- name: ethereum
  chain_id: 1
  reth:
    enabled: true
    logging: false  # Disable for production
    cli_args:
      # Core settings
      - "--datadir /nvme/reth"  # Fast NVMe storage
      - "--authrpc.jwtsecret /secrets/jwt.hex"
      - "--authrpc.addr 127.0.0.1"
      - "--authrpc.port 8551"
      
      # Archive node (required)
      - "--full false"
      
      # Performance tuning
      - "--db.log-level error"
      - "--max-outbound-peers 100"
      - "--max-inbound-peers 50"
      
      # Metrics
      - "--metrics 127.0.0.1:9001"
      
      # HTTP RPC (optional)
      - "--http"
      - "--http.addr 0.0.0.0"
      - "--http.port 8545"
      - "--http.api eth,net,web3,debug,trace"
```

## Performance Considerations

### Hardware Requirements

For optimal ExEx performance:
- **CPU**: 8+ cores recommended
- **RAM**: 32GB minimum, 64GB recommended
- **Storage**: NVMe SSD with 2TB+ for mainnet archive
- **Network**: Stable connection for peer synchronization


## Best Practices

1. **Use Archive Node**: Run Reth in archive mode for ExEx.
2. **Monitor Resources**: Set up alerts for disk, CPU, and memory

## Migration from Standard Mode

To migrate an existing project to ExEx:

1. **Sync Reth Node**: Ensure fully synced archive node
2. **Update Config**: Add `reth` section to networks
4. **Reindex**: Consider full reindex for consistency

## Further Resources

- [Reth ExEx Documentation](https://reth.rs/developers/exex.html)
- [Running Reth on Ethereum](https://reth.rs/run/ethereum)
- [rindexer Reth Mode Guide](/docs/start-building/create-new-project/reth-mode)