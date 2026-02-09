# rindexer_rust_playground

Shows the broadest range of rindexer features in a single Rust project.

## What it demonstrates

- **Multi-contract**: indexes 4 contracts (RocketPoolETH, a generic ERC20, UniswapV3Pool, USDT)
- **Multi-network**: Ethereum + Base
- **Dual storage**: PostgreSQL **and** CSV enabled simultaneously
- **Event filtering**: block-range-level event filtering
- **Global contracts**: defining a single contract (USDT) deployed across multiple chains

## Contracts & Events

| Contract | Network | Events |
|---|---|---|
| RocketPoolETH | Ethereum | Transfer, Approval |
| ERC20 (generic) | Ethereum | Transfer, Approval |
| UniswapV3Pool | Base | Swap |
| USDT (global) | Ethereum + Base | Transfer, Approval |

## Storage

PostgreSQL + CSV (both active at the same time)

## Getting started

1. Copy `.env.example` to `.env` and configure your `DATABASE_URL`
2. Run `docker-compose up -d` to start the Postgress instance in the `docker-compose.yml` file
3. Run `cargo run` (defaults to starting both the indexer and GraphQL API), optionally you can add the flags: `--indexer`, to only index the data or `--graphql`, `--port=<PORT>` to start the GraphQL API.
4. You can access the GraphQL playground with your created data at http://localhost:3001/playground. Run the query in the UI:
    ```GRAPHQL
    query Erc20TransfersPlusUniV3Swaps {
      allErc20Transfers(first: 3) {
        nodes {
          nodeId
          rindexerId
          contractAddress
          from
          to
          valueds
          txHash
          blockNumber
          blockTimestamp
          blockHash
          network
          txIndex
          logIndex
        }
      }
      allUniswapV3PoolSwaps(first: 3) {
        nodes {
          nodeId
          rindexerId
          contractAddress
          sender
          recipient
          amount0
          amount1
          sqrtPriceX96
          liquidity
          tick
          txHash
          blockNumber
          blockTimestamp
          blockHash
          network
          txIndex
          logIndex
        }
      }
    }
    ```