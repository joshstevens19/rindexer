//! End-to-end integration test for rindexer's reorg handling system.
//!
//! Requires Docker. Run with:
//!   cargo test -q -p rindexer --test e2e_reorg -- --ignored

use std::sync::Arc;

use alloy::primitives::{Address, B256};
use rindexer::indexer::reorg::{
    persistence::LatestBlocksPersistence,
    task::{EventTableInfo, ReorgTask},
    window::{BlockChainWindow, ParentValidation},
};
use rindexer::PostgresClient;
use reqwest::Client as HttpClient;
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PING_PONG_BYTECODE: &str =
    "6080604052348015600e575f5ffd5b506101748061001c5f395ff3fe608060405234801561000f575f5ffd5b5060043610610029575f3560e01c8063773acdef1461002d575b5f5ffd5b610047600480360381019061004291906100bb565b610049565b005b807fc05b373e05c47417d9c7204807552389e512c0e21cbc01a03d1554561080ac6e336040516100799190610125565b60405180910390a250565b5f5ffd5b5f819050919050565b61009a81610088565b81146100a4575f5ffd5b50565b5f813590506100b581610091565b92915050565b5f602082840312156100d0576100cf610084565b5b5f6100dd848285016100a7565b91505092915050565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f61010f826100e6565b9050919050565b61011f81610105565b82525050565b5f6020820190506101385f830184610116565b9291505056fea2646970667358221220dc07dd9f297d16a6d4ac329e4565c9ecb79b34df9738da42d568df67b039348764736f6c634300081c0033";

/// Function selector for `ping(uint256)` = 0x773acdef
const PING_SELECTOR: [u8; 4] = [0x77, 0x3a, 0xcd, 0xef];

// ---------------------------------------------------------------------------
// RPC helpers
// ---------------------------------------------------------------------------

async fn rpc_call(http: &HttpClient, rpc_url: &str, method: &str, params: Value) -> Value {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let resp = http
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .expect("RPC request failed");
    let json: Value = resp.json().await.expect("RPC response not JSON");
    if let Some(err) = json.get("error") {
        panic!("RPC error calling {}: {:?}", method, err);
    }
    json["result"].clone()
}

async fn try_get_block_number(http: &HttpClient, rpc_url: &str) -> Option<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": [],
    });
    let resp = http.post(rpc_url).json(&body).send().await.ok()?;
    let json: Value = resp.json().await.ok()?;
    let result = json.get("result")?.as_str()?;
    u64::from_str_radix(result.trim_start_matches("0x"), 16).ok()
}

async fn get_block_number(http: &HttpClient, rpc_url: &str) -> u64 {
    let result = rpc_call(http, rpc_url, "eth_blockNumber", json!([])).await;
    u64::from_str_radix(result.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

/// Returns `(block_hash, parent_hash)` for the given block number.
async fn get_block_by_number(http: &HttpClient, rpc_url: &str, block_number: u64) -> (B256, B256) {
    let hex_block = format!("0x{:x}", block_number);
    let result = rpc_call(http, rpc_url, "eth_getBlockByNumber", json!([hex_block, false])).await;
    let hash_str = result["hash"].as_str().expect("block has no hash");
    let parent_str = result["parentHash"].as_str().expect("block has no parentHash");
    let hash: B256 = hash_str.parse().expect("invalid block hash");
    let parent: B256 = parent_str.parse().expect("invalid parent hash");
    (hash, parent)
}

/// Returns `(block_number, block_hash, parent_hash)` for the given block number.
async fn get_block_full(
    http: &HttpClient,
    rpc_url: &str,
    block_number: u64,
) -> (u64, B256, B256) {
    let (hash, parent) = get_block_by_number(http, rpc_url, block_number).await;
    (block_number, hash, parent)
}

async fn get_accounts(http: &HttpClient, rpc_url: &str) -> Vec<Address> {
    let result = rpc_call(http, rpc_url, "eth_accounts", json!([])).await;
    result
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().parse::<Address>().unwrap())
        .collect()
}

async fn wait_for_receipt(http: &HttpClient, rpc_url: &str, tx_hash: &str) -> Value {
    for _ in 0..60 {
        let result =
            rpc_call(http, rpc_url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !result.is_null() {
            return result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("Timed out waiting for receipt of {}", tx_hash);
}

async fn deploy_ping_pong(
    http: &HttpClient,
    rpc_url: &str,
    from: Address,
) -> (Address, u64) {
    let tx = json!({
        "from": format!("{:#x}", from),
        "data": format!("0x{}", PING_PONG_BYTECODE),
        "gas": "0x100000",
    });
    let tx_hash = rpc_call(http, rpc_url, "eth_sendTransaction", json!([tx])).await;
    let tx_hash_str = tx_hash.as_str().unwrap();
    let receipt = wait_for_receipt(http, rpc_url, tx_hash_str).await;
    let contract_addr: Address = receipt["contractAddress"]
        .as_str()
        .expect("no contractAddress in receipt")
        .parse()
        .unwrap();
    let block_hex = receipt["blockNumber"].as_str().unwrap();
    let block_num = u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap();
    (contract_addr, block_num)
}

async fn send_ping(
    http: &HttpClient,
    rpc_url: &str,
    from: Address,
    contract: Address,
    id: u64,
) -> u64 {
    // Encode calldata: selector + uint256(id)
    let mut calldata = Vec::with_capacity(36);
    calldata.extend_from_slice(&PING_SELECTOR);
    let mut id_bytes = [0u8; 32];
    id_bytes[24..32].copy_from_slice(&id.to_be_bytes());
    calldata.extend_from_slice(&id_bytes);

    let tx = json!({
        "from": format!("{:#x}", from),
        "to": format!("{:#x}", contract),
        "data": format!("0x{}", hex::encode(&calldata)),
        "gas": "0x100000",
    });
    let tx_hash = rpc_call(http, rpc_url, "eth_sendTransaction", json!([tx])).await;
    let tx_hash_str = tx_hash.as_str().unwrap();
    let receipt = wait_for_receipt(http, rpc_url, tx_hash_str).await;
    let block_hex = receipt["blockNumber"].as_str().unwrap();
    u64::from_str_radix(block_hex.trim_start_matches("0x"), 16).unwrap()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_detection_and_rollback() {
    // -----------------------------------------------------------------------
    // 1. Start containers
    // -----------------------------------------------------------------------

    // Postgres
    let pg_container = Postgres::default()
        .start()
        .await
        .expect("failed to start postgres container");
    let pg_port = pg_container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get postgres port");
    let pg_conn_str = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        pg_port
    );

    // Reth dev node
    let reth_container = GenericImage::new("ghcr.io/paradigmxyz/reth", "v1.8.2")
        .with_exposed_port(8545_u16.into())
        .with_cmd(vec![
            "node".to_string(),
            "--dev".to_string(),
            "--dev.block-time".to_string(),
            "1s".to_string(),
            "--http".to_string(),
            "--http.addr".to_string(),
            "0.0.0.0".to_string(),
            "--http.port".to_string(),
            "8545".to_string(),
            "--http.api".to_string(),
            "eth,net,web3,debug,admin".to_string(),
        ])
        .with_startup_timeout(std::time::Duration::from_secs(60))
        .start()
        .await
        .expect("failed to start reth container");
    let reth_port = reth_container
        .get_host_port_ipv4(8545)
        .await
        .expect("failed to get reth port");
    let rpc_url = format!("http://127.0.0.1:{}", reth_port);

    let http = HttpClient::new();

    // Wait for reth to produce at least 1 block
    for _ in 0..30 {
        let ok = try_get_block_number(&http, &rpc_url).await;
        if ok.map_or(false, |n| n >= 1) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    let initial_block = get_block_number(&http, &rpc_url).await;
    assert!(initial_block >= 1, "reth should have produced at least 1 block");

    // -----------------------------------------------------------------------
    // 2. Set up Postgres tables
    // -----------------------------------------------------------------------
    let (pg_client, pg_conn) = tokio_postgres::connect(&pg_conn_str, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to postgres");
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            eprintln!("postgres connection error: {}", e);
        }
    });

    pg_client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS rindexer_internal;
             CREATE TABLE IF NOT EXISTS rindexer_internal.latest_blocks (
                 network VARCHAR(50) NOT NULL,
                 block_number BIGINT NOT NULL,
                 block_hash CHAR(66) NOT NULL,
                 parent_hash CHAR(66) NOT NULL,
                 PRIMARY KEY (network, block_number)
             );
             CREATE SCHEMA IF NOT EXISTS test_schema;
             CREATE TABLE IF NOT EXISTS test_schema.ping_pong_ping (
                 rindexer_id SERIAL PRIMARY KEY,
                 id NUMERIC NOT NULL,
                 sender CHAR(42),
                 tx_hash CHAR(66) NOT NULL,
                 block_number NUMERIC NOT NULL,
                 block_hash CHAR(66) NOT NULL,
                 network VARCHAR(50) NOT NULL,
                 tx_index NUMERIC NOT NULL,
                 log_index VARCHAR(78) NOT NULL
             );",
        )
        .await
        .expect("failed to create tables");

    // -----------------------------------------------------------------------
    // 3. Deploy PingPong and emit events
    // -----------------------------------------------------------------------
    let accounts = get_accounts(&http, &rpc_url).await;
    let deployer = accounts[0];

    let (contract, _deploy_block) = deploy_ping_pong(&http, &rpc_url, deployer).await;

    let block1 = send_ping(&http, &rpc_url, deployer, contract, 1).await;
    let block2 = send_ping(&http, &rpc_url, deployer, contract, 2).await;
    let block3 = send_ping(&http, &rpc_url, deployer, contract, 3).await;

    // -----------------------------------------------------------------------
    // 4. Build BlockChainWindow from real blocks
    // -----------------------------------------------------------------------
    let start_block = block1.saturating_sub(1); // include parent of block1
    let mut window = BlockChainWindow::new(256);
    for block_num in start_block..=block3 {
        let (hash, parent_hash) = get_block_by_number(&http, &rpc_url, block_num).await;
        window.insert(block_num, hash, parent_hash);
    }

    // -----------------------------------------------------------------------
    // 5. Insert fake events and latest_blocks into postgres
    // -----------------------------------------------------------------------
    let network = "dev";
    for (ping_id, block_num) in [(1u64, block1), (2, block2), (3, block3)] {
        let (hash, _parent) = get_block_by_number(&http, &rpc_url, block_num).await;
        let hash_str = format!("{:#x}", hash);

        pg_client
            .execute(
                "INSERT INTO test_schema.ping_pong_ping \
                 (id, sender, tx_hash, block_number, block_hash, network, tx_index, log_index) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &rust_decimal::Decimal::from(ping_id),
                    &format!("{:#x}", deployer),
                    &"0x0000000000000000000000000000000000000000000000000000000000000000",
                    &rust_decimal::Decimal::from(block_num),
                    &hash_str.as_str(),
                    &network,
                    &rust_decimal::Decimal::from(0u64),
                    &"0",
                ],
            )
            .await
            .expect("failed to insert event");
    }

    // Insert latest_blocks entries
    for block_num in start_block..=block3 {
        let (hash, parent_hash) = get_block_by_number(&http, &rpc_url, block_num).await;
        pg_client
            .execute(
                "INSERT INTO rindexer_internal.latest_blocks \
                 (network, block_number, block_hash, parent_hash) VALUES ($1, $2, $3, $4)",
                &[
                    &network,
                    &(block_num as i64),
                    &format!("{:#x}", hash).as_str(),
                    &format!("{:#x}", parent_hash).as_str(),
                ],
            )
            .await
            .expect("failed to insert latest_block");
    }

    // Verify we have 3 events before reorg
    let pre_count: i64 = pg_client
        .query_one("SELECT count(*) FROM test_schema.ping_pong_ping", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(pre_count, 3, "should have 3 events before reorg");

    // -----------------------------------------------------------------------
    // 6. Trigger reorg via debug_setHead
    // -----------------------------------------------------------------------
    // Reorg back to the block of Ping(1) — block2 and block3 should be invalidated
    let reorg_target_hex = format!("0x{:x}", block1);
    rpc_call(&http, &rpc_url, "debug_setHead", json!([reorg_target_hex])).await;

    // Wait for the chain to advance past the reorg point with new blocks
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let new_tip = get_block_number(&http, &rpc_url).await;
    assert!(
        new_tip >= block1,
        "chain should have advanced past reorg target"
    );

    // -----------------------------------------------------------------------
    // 7. Detect reorg via window validation
    // -----------------------------------------------------------------------
    // Pick a block after the reorg target — its parent hash should NOT match
    // what we stored for block1+1 (old block2).
    // We need a block that's > block1 to check the parent mismatch.
    let check_block = block1 + 1;
    // Wait until the chain has produced this block
    for _ in 0..30 {
        let tip = get_block_number(&http, &rpc_url).await;
        if tip >= check_block {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let (new_hash, new_parent) = get_block_by_number(&http, &rpc_url, check_block).await;

    // The parent of check_block should match block1's hash on the canonical chain.
    // But check_block's hash should differ from what we stored (old block2 hash).
    let _validation = window.validate_parent(check_block, new_parent);
    // After debug_setHead, the new block at check_block should have block1's hash as parent.
    // Our window has block1's hash stored, so parent validation should be Valid.
    // However the HASH of check_block itself should differ from the old block2 hash.
    let old_entry = window.get(check_block);
    if let Some((old_hash, _)) = old_entry {
        assert_ne!(
            *old_hash, new_hash,
            "block {} hash should differ after reorg",
            check_block
        );
    }

    // -----------------------------------------------------------------------
    // 8. Find fork point
    // -----------------------------------------------------------------------
    // Fetch canonical hashes for all blocks in our window
    let block_numbers = window.block_numbers();
    let mut canonical_blocks = Vec::new();
    for &bn in &block_numbers {
        let tip = get_block_number(&http, &rpc_url).await;
        if bn <= tip {
            let (canonical_hash, _) = get_block_by_number(&http, &rpc_url, bn).await;
            canonical_blocks.push((bn, canonical_hash));
        }
    }

    let fork_point = window.find_fork_point(&canonical_blocks);
    assert!(fork_point.is_some(), "should find a fork point");
    let fork_block = fork_point.unwrap();
    // Fork point should be block1 (the last block that matches on both chains)
    assert_eq!(
        fork_block, block1,
        "fork point should be block1 (reorg target)"
    );

    // -----------------------------------------------------------------------
    // 9. Execute ReorgTask against postgres
    // -----------------------------------------------------------------------
    let task = ReorgTask {
        network: network.to_string(),
        fork_point: fork_block + 1, // first invalidated block
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    // Build a PostgresClient from a pool using the same connection string.
    // Since PostgresClient::new() reads DATABASE_URL from env, set it.
    std::env::set_var("DATABASE_URL", format!(
        "postgres://postgres:postgres@127.0.0.1:{}/postgres",
        pg_port
    ));
    let rindexer_pg = PostgresClient::new().await.expect("failed to create PostgresClient");

    let mut task_window = BlockChainWindow::new(256);
    let persistence = LatestBlocksPersistence::new(Some(Arc::new(rindexer_pg)), None);

    // Re-create a fresh PostgresClient for the task execution
    let rindexer_pg2 = PostgresClient::new().await.expect("failed to create PostgresClient for task");

    // Collect the pre-reorg hashes for the reorged range so we can verify they change.
    let old_block2_hash = {
        let (h, _) = get_block_by_number(&http, &rpc_url, block2).await;
        format!("{:#x}", h)
    };

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg2), None, None)
        .await
        .expect("reorg task execution failed");

    // -----------------------------------------------------------------------
    // Assertion: events_deleted
    // -----------------------------------------------------------------------
    // Events for block2 and block3 should be deleted (Ping(2) and Ping(3))
    assert_eq!(result.events_deleted, 2, "should have deleted 2 stale events");

    // -----------------------------------------------------------------------
    // Assertion: affected_tx_hashes is non-empty
    // -----------------------------------------------------------------------
    // collect_affected_tx_hashes gathers distinct tx_hash values for the reorged
    // block range. Our fake events all carry the zero hash, so expect exactly one
    // entry (deduplicated) corresponding to those two stale rows.
    assert!(
        !result.affected_tx_hashes.is_empty(),
        "affected_tx_hashes should be non-empty after reorg recovery"
    );
    // Both stale events share the same placeholder tx_hash; after dedup only one entry.
    assert_eq!(
        result.affected_tx_hashes.len(),
        1,
        "expected exactly 1 distinct tx_hash from the two stale events"
    );
    assert_eq!(
        result.affected_tx_hashes[0],
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "affected tx_hash should be the zero hash used in test fixtures"
    );

    // -----------------------------------------------------------------------
    // Assertion: only Ping(1) remains in the event table
    // -----------------------------------------------------------------------
    let post_count: i64 = pg_client
        .query_one("SELECT count(*) FROM test_schema.ping_pong_ping", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(post_count, 1, "only Ping(1) should remain after rollback");

    // -----------------------------------------------------------------------
    // Assertion: latest_blocks for invalidated range are removed
    // -----------------------------------------------------------------------
    // When execute() is called without a provider the corrected_blocks list is
    // empty, so the transaction deletes the stale rows but inserts nothing back.
    let latest_blocks_count: i64 = pg_client
        .query_one(
            "SELECT count(*) FROM rindexer_internal.latest_blocks \
             WHERE network = $1 AND block_number >= $2 AND block_number <= $3",
            &[&network, &(task.fork_point as i64), &(task.detection_point as i64)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        latest_blocks_count, 0,
        "latest_blocks for invalidated range should be deleted when no provider is given"
    );

    // The entry for block1 (the fork point itself, which is NOT in the invalidated
    // range) should still exist unchanged.
    let fork_block_row = pg_client
        .query_one(
            "SELECT block_hash FROM rindexer_internal.latest_blocks \
             WHERE network = $1 AND block_number = $2",
            &[&network, &(block1 as i64)],
        )
        .await
        .unwrap();
    let stored_block1_hash: &str = fork_block_row.get(0);
    assert!(
        !stored_block1_hash.is_empty(),
        "block1 entry in latest_blocks should survive the reorg rollback"
    );
    // Sanity: stored hash should differ from the (post-reorg) hash we captured
    // for block2 — they are different blocks.
    assert_ne!(
        stored_block1_hash, old_block2_hash.as_str(),
        "block1 hash in latest_blocks should not equal block2 hash"
    );

    // -----------------------------------------------------------------------
    // Assertion: window is NOT updated when provider is None
    // -----------------------------------------------------------------------
    // task_window was empty before execute() and, since no provider was given,
    // update_range was never called — the window should remain empty.
    assert!(
        task_window.is_empty(),
        "task_window should remain empty when execute() is called without a provider"
    );

    // -----------------------------------------------------------------------
    // Assertion: validate_parent on the canonical chain still works
    // -----------------------------------------------------------------------
    // Wait until chain has block1+1 available (may already exist from earlier).
    let check_after = block1 + 1;
    for _ in 0..30 {
        let tip = get_block_number(&http, &rpc_url).await;
        if tip >= check_after {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    let (_check_num, _check_hash, check_parent) =
        get_block_full(&http, &rpc_url, check_after).await;
    // Our original `window` still holds block1's canonical hash; the new block
    // at check_after must have block1's hash as its parent.
    let validation = window.validate_parent(check_after, check_parent);
    assert!(
        matches!(validation, ParentValidation::Valid),
        "block at check_after should have block1's hash as its parent (canonical chain is intact)"
    );

    // -----------------------------------------------------------------------
    // 10. Verify startup validation detects stale hashes
    // -----------------------------------------------------------------------
    // Build a window with old (stale) hashes that no longer match canonical chain
    let mut stale_window = BlockChainWindow::new(256);
    // Re-insert the original (now stale) hashes from our initial window
    for &bn in &block_numbers {
        if let Some(&(hash, parent)) = window.get(bn) {
            stale_window.insert(bn, hash, parent);
        }
    }

    // Fetch canonical hashes again and verify our stale window detects the mismatch
    let mut canonical_after = Vec::new();
    for &bn in &block_numbers {
        let tip = get_block_number(&http, &rpc_url).await;
        if bn <= tip {
            let (canonical_hash, _) = get_block_by_number(&http, &rpc_url, bn).await;
            canonical_after.push((bn, canonical_hash));
        }
    }

    // The stale window should have a fork point at block1 (blocks after that diverge)
    let startup_fork = stale_window.find_fork_point(&canonical_after);
    assert!(startup_fork.is_some(), "startup validation should find fork point");
    assert_eq!(
        startup_fork.unwrap(),
        block1,
        "startup validation fork point should be block1"
    );
    // Since fork_point < latest_block, this confirms an offline reorg occurred
    let latest = stale_window.latest_block().unwrap();
    assert!(
        startup_fork.unwrap() < latest,
        "fork point should be less than latest block, indicating offline reorg"
    );
}
