# clickhouse_factory_indexing

Same **factory pattern** as `rindexer_factory_indexing`, but using **ClickHouse** as the storage backend instead of PostgreSQL.

## What it demonstrates

- Identical Uniswap V3 factory-to-pool-to-token indexing pipeline
- **ClickHouse** as the storage backend (columnar, analytics-optimized)
- `drop_each_run: true` for easy test/demo cycles
- Smaller block ranges (500 blocks) for quick test runs

## Contracts & Events

| Contract | Discovery | Events |
|---|---|---|
| UniswapV3Factory | Static address | OwnerChanged |
| UniswapV3Pool | Derived from `PoolCreated.pool` | Swap |
| UniswapV3PoolToken | Derived from `PoolCreated.token0` and `PoolCreated.token1` | Transfer |

## Storage

ClickHouse (no CSV)

## Getting started

1. Copy `.env.example` to `.env` and configure your connection
2. Run `docker-compose up -d` to start the ClickHouse instance in the `docker-compose.yml` file
3. Run `cargo run`. You can access the ClickHouse playground with your created data at http://localhost:8123. Run the lines individually in the Web SQL UI.
    ```SQL
    SHOW DATABASES;
    SHOW TABLES FROM rindexer_factory_contract_uniswap_v3_factory_pool_created_pool;
    SELECT * FROM rindexer_factory_contract_uniswap_v3_factory_pool_created_pool.pool_created;
    ```
