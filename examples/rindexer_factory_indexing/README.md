# rindexer_factory_indexing

Demonstrates **factory pattern indexing** — dynamically discovering and indexing contracts created by a factory contract (Uniswap V3).

## What it demonstrates

- Watches **UniswapV3Factory** for `PoolCreated` events
- Automatically derives **pool addresses** and starts indexing their `Swap` events
- Extracts **token0/token1** addresses from the same factory event and tracks their `Transfer` events
- Multi-network support (Ethereum + Base)

## Contracts & Events

| Contract | Discovery | Events |
|---|---|---|
| UniswapV3Factory | Static address | OwnerChanged |
| UniswapV3Pool | Derived from `PoolCreated.pool` | Swap |
| UniswapV3PoolToken | Derived from `PoolCreated.token0` and `PoolCreated.token1` | Transfer |

## Storage

PostgreSQL + CSV

## Getting started

1. Copy `.env.example` to `.env` and configure your `DATABASE_URL`
2. Run `docker-compose up -d` to start the Postgress instance in the `docker-compose.yml` file
3. Run `cargo run` (defaults to starting both the indexer and GraphQL API), optionally you can add the flags: `--indexer`, to only index the data or `--graphql`, `--port=<PORT>` to start the GraphQL API.
4. You can access the GraphQL playground with your created data at http://localhost:3001/playground. Run the query in the UI:
    ```GRAPHQL
    query SwapsQuery {
      allSwaps(first: 10) {
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
