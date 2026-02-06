# rust_clickhouse

The **simplest Rust example** — a minimal "hello world" for rindexer, but using ClickHouse as storage.

## What it demonstrates

- **1 network** (Ethereum only)
- **1 contract** (RocketPoolETH, `Transfer` event only)
- **ClickHouse-only** storage
- `block_poll_frequency: "rapid"` for near-real-time indexing
- Includes a `docker-compose.yml` for standing up ClickHouse locally

## Contracts & Events

| Contract | Network | Events |
|---|---|---|
| RocketPoolETH | Ethereum | Transfer |

## Storage

ClickHouse only

## Getting started

1. Copy `.env.example` to `.env` and configure your connection
2. Run `docker-compose up -d` to start the ClickHouse instance in the `docker-compose.yml` file
3. Run `cargo run`. You can access the ClickHouse playground with your created data at http://localhost:8123. Run the lines individually in the Web SQL UI.
```SQL
SHOW DATABASES;
SHOW TABLES FROM clickhouse_indexer_rocket_pool;
SELECT * FROM clickhouse_indexer_rocket_pool.transfer;
```
