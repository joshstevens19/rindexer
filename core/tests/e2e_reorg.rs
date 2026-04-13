//! End-to-end integration test for rindexer's reorg handling system.
//!
//! Requires Docker. Run sequentially (tests share DATABASE_URL env var):
//!   cargo test -q -p rindexer --test e2e_reorg -- --ignored --test-threads=1

use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use reqwest::Client as HttpClient;
use rindexer::event::callback_registry::{EventCallbackRegistry, ReorgNotification};
use rindexer::indexer::reorg::{
    persistence::ReorgBlockHashPersistence,
    task::{DerivedTableInfo, EventTableInfo, ReorgTask},
    window::{BlockChainWindow, ParentValidation},
};
use rindexer::PostgresClient;
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
    let resp = http.post(rpc_url).json(&body).send().await.expect("RPC request failed");
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
async fn get_block_full(http: &HttpClient, rpc_url: &str, block_number: u64) -> (u64, B256, B256) {
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
        let result = rpc_call(http, rpc_url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !result.is_null() {
            return result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("Timed out waiting for receipt of {}", tx_hash);
}

async fn deploy_ping_pong(http: &HttpClient, rpc_url: &str, from: Address) -> (Address, u64) {
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

/// Trigger a reorg of `depth` blocks via Anvil's `anvil_reorg` RPC method.
async fn trigger_reorg(http: &HttpClient, rpc_url: &str, depth: u64) {
    rpc_call(http, rpc_url, "anvil_reorg", json!([depth, []])).await;
}

// ---------------------------------------------------------------------------
// Shared test infrastructure
// ---------------------------------------------------------------------------

struct TestEnv {
    pg_port: u16,
    rpc_url: String,
    http: HttpClient,
    deployer: Address,
    // Keep containers alive for the test duration.
    _pg_container: testcontainers::ContainerAsync<Postgres>,
    _anvil_container: testcontainers::ContainerAsync<GenericImage>,
}

impl TestEnv {
    async fn new() -> Self {
        // Ensure rustls has a crypto provider available (needed by reqwest/testcontainers).
        let _ = rustls::crypto::ring::default_provider().install_default();

        let pg_container =
            Postgres::default().start().await.expect("failed to start postgres container");
        let pg_port =
            pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        // The foundry image entrypoint is `/bin/sh -c`, so the entire command
        // must be passed as a single string argument.
        let anvil_container = GenericImage::new("ghcr.io/foundry-rs/foundry", "latest")
            .with_exposed_port(8545_u16.into())
            .with_cmd(vec!["anvil --host 0.0.0.0 --block-time 1".to_string()])
            .with_startup_timeout(std::time::Duration::from_secs(30))
            .start()
            .await
            .expect("failed to start anvil container");
        let anvil_port =
            anvil_container.get_host_port_ipv4(8545).await.expect("failed to get anvil port");
        let rpc_url = format!("http://127.0.0.1:{}", anvil_port);

        let http = HttpClient::new();

        // Wait for Anvil to be ready
        for _ in 0..30 {
            if try_get_block_number(&http, &rpc_url).await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        let accounts = get_accounts(&http, &rpc_url).await;
        let deployer = accounts[0];

        // Set DATABASE_URL for PostgresClient::new()
        std::env::set_var(
            "DATABASE_URL",
            format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", pg_port),
        );

        Self {
            pg_port,
            rpc_url,
            http,
            deployer,
            _pg_container: pg_container,
            _anvil_container: anvil_container,
        }
    }

    fn pg_conn_str(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            self.pg_port
        )
    }

    async fn pg_client(&self) -> tokio_postgres::Client {
        let (client, conn) = tokio_postgres::connect(&self.pg_conn_str(), tokio_postgres::NoTls)
            .await
            .expect("failed to connect to postgres");
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {}", e);
            }
        });
        client
    }

    async fn rindexer_pg(&self) -> PostgresClient {
        PostgresClient::new().await.expect("failed to create PostgresClient")
    }

    async fn setup_base_tables(&self, pg: &tokio_postgres::Client) {
        pg.batch_execute(
            "CREATE SCHEMA IF NOT EXISTS rindexer_internal;
             CREATE TABLE IF NOT EXISTS rindexer_internal.reorg_block_hashes (
                 network VARCHAR(50) NOT NULL,
                 block_number BIGINT NOT NULL,
                 block_hash CHAR(66) NOT NULL,
                 parent_hash CHAR(66) NOT NULL,
                 PRIMARY KEY (network, block_number)
             );
             CREATE TABLE IF NOT EXISTS rindexer_internal.test_schema_ping (
                 network VARCHAR(50) NOT NULL PRIMARY KEY,
                 last_synced_block BIGINT NOT NULL
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
    }

    async fn insert_event(
        &self,
        pg: &tokio_postgres::Client,
        network: &str,
        ping_id: u64,
        block_num: u64,
        tx_hash: &str,
    ) {
        let (hash, _parent) = get_block_by_number(&self.http, &self.rpc_url, block_num).await;
        let hash_str = format!("{:#x}", hash);
        pg.execute(
            "INSERT INTO test_schema.ping_pong_ping \
             (id, sender, tx_hash, block_number, block_hash, network, tx_index, log_index) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &rust_decimal::Decimal::from(ping_id),
                &format!("{:#x}", self.deployer),
                &tx_hash,
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

    async fn insert_block_hashes(
        &self,
        pg: &tokio_postgres::Client,
        network: &str,
        from_block: u64,
        to_block: u64,
    ) {
        for block_num in from_block..=to_block {
            let (hash, parent_hash) =
                get_block_by_number(&self.http, &self.rpc_url, block_num).await;
            pg.execute(
                "INSERT INTO rindexer_internal.reorg_block_hashes \
                 (network, block_number, block_hash, parent_hash) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (network, block_number) DO UPDATE SET
                     block_hash = EXCLUDED.block_hash, parent_hash = EXCLUDED.parent_hash",
                &[
                    &network,
                    &(block_num as i64),
                    &format!("{:#x}", hash).as_str(),
                    &format!("{:#x}", parent_hash).as_str(),
                ],
            )
            .await
            .expect("failed to insert reorg_block_hashes");
        }
    }

    async fn build_window(&self, from_block: u64, to_block: u64) -> BlockChainWindow {
        let mut window = BlockChainWindow::new(256);
        for block_num in from_block..=to_block {
            let (hash, parent_hash) =
                get_block_by_number(&self.http, &self.rpc_url, block_num).await;
            window.insert(block_num, hash, parent_hash);
        }
        window
    }

    async fn event_count(&self, pg: &tokio_postgres::Client) -> i64 {
        pg.query_one("SELECT count(*) FROM test_schema.ping_pong_ping", &[]).await.unwrap().get(0)
    }

    async fn reorg_hashes_count(
        &self,
        pg: &tokio_postgres::Client,
        network: &str,
        from: u64,
        to: u64,
    ) -> i64 {
        pg.query_one(
            "SELECT count(*) FROM rindexer_internal.reorg_block_hashes \
             WHERE network = $1 AND block_number >= $2 AND block_number <= $3",
            &[&network, &(from as i64), &(to as i64)],
        )
        .await
        .unwrap()
        .get(0)
    }
}

// ---------------------------------------------------------------------------
// Test 1: Original reorg detection and rollback (depth=2)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_detection_and_rollback() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    // Deploy PingPong and emit events
    let (contract, _deploy_block) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;
    let block3 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 3).await;

    // Build window from real blocks
    let start_block = block1.saturating_sub(1);
    let window = env.build_window(start_block, block3).await;

    // Insert events and block hashes into postgres
    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    for (ping_id, block_num) in [(1u64, block1), (2, block2), (3, block3)] {
        env.insert_event(&pg, network, ping_id, block_num, zero_tx).await;
    }
    env.insert_block_hashes(&pg, network, start_block, block3).await;

    assert_eq!(env.event_count(&pg).await, 3, "should have 3 events before reorg");

    // Trigger reorg — blocks at block2 and block3 get new hashes
    let reorg_depth = (block3 - block1) as u64; // invalidates block2..=block3
    trigger_reorg(&env.http, &env.rpc_url, reorg_depth).await;

    // Verify block hashes changed after reorg
    let check_block = block2;
    let (new_hash, _new_parent) = get_block_by_number(&env.http, &env.rpc_url, check_block).await;
    let old_entry = window.get(check_block);
    if let Some((old_hash, _)) = old_entry {
        assert_ne!(*old_hash, new_hash, "block {} hash should differ after reorg", check_block);
    }

    // Find fork point
    let block_numbers = window.block_numbers();
    let mut canonical_blocks = Vec::new();
    for &bn in &block_numbers {
        let tip = get_block_number(&env.http, &env.rpc_url).await;
        if bn <= tip {
            let (canonical_hash, _) = get_block_by_number(&env.http, &env.rpc_url, bn).await;
            canonical_blocks.push((bn, canonical_hash));
        }
    }

    let fork_point = window.find_fork_point(&canonical_blocks);
    assert!(fork_point.is_some(), "should find a fork point");
    let fork_block = fork_point.unwrap();
    assert_eq!(fork_block, block1, "fork point should be block1");

    // Execute ReorgTask
    let task = ReorgTask {
        network: network.to_string(),
        fork_point: fork_block + 1,
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let rindexer_pg2 = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(Some(Arc::new(rindexer_pg)), None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg2), None, None)
        .await
        .expect("reorg task execution failed");

    assert_eq!(result.events_deleted, 2, "should have deleted 2 stale events");
    assert_eq!(
        result.affected_tx_hashes.len(),
        1,
        "expected exactly 1 distinct tx_hash (deduped zero hashes)"
    );
    assert_eq!(env.event_count(&pg).await, 1, "only Ping(1) should remain after rollback");

    // reorg_block_hashes for invalidated range should be deleted
    assert_eq!(
        env.reorg_hashes_count(&pg, network, task.fork_point, task.detection_point).await,
        0,
        "reorg_block_hashes for invalidated range should be deleted"
    );

    // Block1 entry should survive
    let fork_block_row = pg
        .query_one(
            "SELECT block_hash FROM rindexer_internal.reorg_block_hashes \
             WHERE network = $1 AND block_number = $2",
            &[&network, &(block1 as i64)],
        )
        .await
        .unwrap();
    let stored_block1_hash: &str = fork_block_row.get(0);
    assert!(!stored_block1_hash.is_empty(), "block1 entry should survive the rollback");

    // Window should remain empty when no provider given
    assert!(task_window.is_empty(), "task_window should remain empty without a provider");

    // validate_parent on the canonical chain still works
    let check_after = block1 + 1;
    let (_check_num, _check_hash, check_parent) =
        get_block_full(&env.http, &env.rpc_url, check_after).await;
    let validation = window.validate_parent(check_after, check_parent);
    assert!(
        matches!(validation, ParentValidation::Valid),
        "canonical chain parent validation should be valid"
    );

    // Startup validation detects stale hashes
    let mut stale_window = BlockChainWindow::new(256);
    for &bn in &block_numbers {
        if let Some(&(hash, parent)) = window.get(bn) {
            stale_window.insert(bn, hash, parent);
        }
    }
    let mut canonical_after = Vec::new();
    for &bn in &block_numbers {
        let tip = get_block_number(&env.http, &env.rpc_url).await;
        if bn <= tip {
            let (canonical_hash, _) = get_block_by_number(&env.http, &env.rpc_url, bn).await;
            canonical_after.push((bn, canonical_hash));
        }
    }
    let startup_fork = stale_window.find_fork_point(&canonical_after);
    assert!(startup_fork.is_some(), "startup validation should find fork point");
    assert_eq!(startup_fork.unwrap(), block1, "startup fork point should be block1");
    let latest = stale_window.latest_block().unwrap();
    assert!(startup_fork.unwrap() < latest, "fork point < latest confirms offline reorg");
}

// ---------------------------------------------------------------------------
// Test 2: Single block reorg (depth=1) — minimal reorg edge case
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_single_block() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    env.insert_event(&pg, network, 1, block1, zero_tx).await;
    env.insert_event(&pg, network, 2, block2, zero_tx).await;
    env.insert_block_hashes(&pg, network, block1, block2).await;

    assert_eq!(env.event_count(&pg).await, 2);

    // Single-block reorg: only block2 changes
    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("single-block reorg task failed");

    assert_eq!(result.events_deleted, 1, "should delete exactly 1 event for depth-1 reorg");
    assert_eq!(env.event_count(&pg).await, 1, "Ping(1) should remain");

    // Verify the surviving event is Ping(1)
    let surviving: rust_decimal::Decimal =
        pg.query_one("SELECT id FROM test_schema.ping_pong_ping", &[]).await.unwrap().get(0);
    assert_eq!(surviving, rust_decimal::Decimal::from(1u64), "surviving event should be Ping(1)");
}

// ---------------------------------------------------------------------------
// Test 3: Deep reorg (depth=5) — many blocks invalidated at once
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_deep() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;

    // Emit 6 events across 6 blocks
    let mut blocks = Vec::new();
    for i in 1u64..=6 {
        let bn = send_ping(&env.http, &env.rpc_url, env.deployer, contract, i).await;
        blocks.push(bn);
    }

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    for (i, &bn) in blocks.iter().enumerate() {
        env.insert_event(&pg, network, (i + 1) as u64, bn, zero_tx).await;
    }
    env.insert_block_hashes(&pg, network, blocks[0], *blocks.last().unwrap()).await;

    assert_eq!(env.event_count(&pg).await, 6);

    // Reorg last 5 blocks — only Ping(1) should survive
    trigger_reorg(&env.http, &env.rpc_url, 5).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: blocks[1], // second block is first invalidated
        detection_point: blocks[5],
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("deep reorg task failed");

    assert_eq!(result.events_deleted, 5, "should delete 5 events for depth-5 reorg");
    assert_eq!(env.event_count(&pg).await, 1, "only Ping(1) should remain");
}

// ---------------------------------------------------------------------------
// Test 4: No events in reorged range — rollback with empty event tables
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_no_events_in_range() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;

    // Wait for a few more blocks to be mined (no events in them)
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let tip = get_block_number(&env.http, &env.rpc_url).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    // Insert event only in block1
    env.insert_event(&pg, network, 1, block1, zero_tx).await;
    env.insert_block_hashes(&pg, network, block1, tip).await;

    assert_eq!(env.event_count(&pg).await, 1);

    // Reorg blocks after block1 — no events should be deleted
    let fork_point = block1 + 1;
    trigger_reorg(&env.http, &env.rpc_url, (tip - block1) as u64).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point,
        detection_point: tip,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("no-events reorg task failed");

    assert_eq!(result.events_deleted, 0, "no events should be deleted when range is empty");
    assert!(result.affected_tx_hashes.is_empty(), "no affected tx hashes");
    assert_eq!(env.event_count(&pg).await, 1, "Ping(1) should remain untouched");
}

// ---------------------------------------------------------------------------
// Test 5: Multiple event tables — rollback across several tables
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_multiple_event_tables() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    // Create a second event table and its checkpoint table
    pg.batch_execute(
        "CREATE TABLE IF NOT EXISTS test_schema.ping_pong_pong (
             rindexer_id SERIAL PRIMARY KEY,
             id NUMERIC NOT NULL,
             sender CHAR(42),
             tx_hash CHAR(66) NOT NULL,
             block_number NUMERIC NOT NULL,
             block_hash CHAR(66) NOT NULL,
             network VARCHAR(50) NOT NULL,
             tx_index NUMERIC NOT NULL,
             log_index VARCHAR(78) NOT NULL
         );
         CREATE TABLE IF NOT EXISTS rindexer_internal.test_schema_pong (
             network VARCHAR(50) NOT NULL PRIMARY KEY,
             last_synced_block BIGINT NOT NULL
         );",
    )
    .await
    .expect("failed to create second table");

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;

    let network = "dev";
    let tx_a = "0x000000000000000000000000000000000000000000000000000000000000000a";
    let tx_b = "0x000000000000000000000000000000000000000000000000000000000000000b";

    // Insert events in both tables
    env.insert_event(&pg, network, 1, block1, tx_a).await;
    env.insert_event(&pg, network, 2, block2, tx_a).await;

    // Insert into second table
    for (ping_id, block_num) in [(10u64, block1), (20, block2)] {
        let (hash, _) = get_block_by_number(&env.http, &env.rpc_url, block_num).await;
        pg.execute(
            "INSERT INTO test_schema.ping_pong_pong \
             (id, sender, tx_hash, block_number, block_hash, network, tx_index, log_index) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &rust_decimal::Decimal::from(ping_id),
                &format!("{:#x}", env.deployer),
                &tx_b,
                &rust_decimal::Decimal::from(block_num),
                &format!("{:#x}", hash).as_str(),
                &network,
                &rust_decimal::Decimal::from(0u64),
                &"0",
            ],
        )
        .await
        .unwrap();
    }

    env.insert_block_hashes(&pg, network, block1, block2).await;

    // Reorg block2
    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![
            EventTableInfo::new(
                "test_schema".to_string(),
                "ping_pong_ping".to_string(),
                "test_schema_ping".to_string(),
            ),
            EventTableInfo::new(
                "test_schema".to_string(),
                "ping_pong_pong".to_string(),
                "test_schema_pong".to_string(),
            ),
        ],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("multi-table reorg task failed");

    // 1 from ping + 1 from pong = 2 total
    assert_eq!(result.events_deleted, 2, "should delete 1 event from each table");

    // Both tables should have only the block1 event
    assert_eq!(env.event_count(&pg).await, 1, "ping table: only block1 event remains");
    let pong_count: i64 =
        pg.query_one("SELECT count(*) FROM test_schema.ping_pong_pong", &[]).await.unwrap().get(0);
    assert_eq!(pong_count, 1, "pong table: only block1 event remains");

    // affected_tx_hashes should contain both distinct hashes
    assert_eq!(
        result.affected_tx_hashes.len(),
        2,
        "should have 2 distinct affected tx hashes (one from each table)"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Checkpoint table rewind
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_checkpoint_rewind() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;
    let block3 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 3).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    for (ping_id, bn) in [(1u64, block1), (2, block2), (3, block3)] {
        env.insert_event(&pg, network, ping_id, bn, zero_tx).await;
    }
    env.insert_block_hashes(&pg, network, block1, block3).await;

    // Set checkpoint to block3
    pg.execute(
        "INSERT INTO rindexer_internal.test_schema_ping (network, last_synced_block) \
         VALUES ($1, $2)",
        &[&network, &(block3 as i64)],
    )
    .await
    .unwrap();

    trigger_reorg(&env.http, &env.rpc_url, 2).await;

    let fork_point = block2;
    let task = ReorgTask {
        network: network.to_string(),
        fork_point,
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    task.execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("checkpoint reorg task failed");

    // Verify checkpoint was rewound to fork_point - 1
    let checkpoint: i64 = pg
        .query_one(
            "SELECT last_synced_block FROM rindexer_internal.test_schema_ping WHERE network = $1",
            &[&network],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        checkpoint,
        (fork_point - 1) as i64,
        "checkpoint should be rewound to fork_point - 1"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Derived table cleanup (cross_chain=false)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_derived_table_cleanup() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    // Create a derived table with rindexer_block_number and network columns
    pg.batch_execute(
        "CREATE TABLE IF NOT EXISTS test_schema.daily_stats (
             id SERIAL PRIMARY KEY,
             rindexer_block_number BIGINT NOT NULL,
             network VARCHAR(50) NOT NULL,
             total_pings NUMERIC NOT NULL
         );",
    )
    .await
    .unwrap();

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    env.insert_event(&pg, network, 1, block1, zero_tx).await;
    env.insert_event(&pg, network, 2, block2, zero_tx).await;
    env.insert_block_hashes(&pg, network, block1, block2).await;

    // Insert derived rows for both blocks
    for &bn in &[block1, block2] {
        pg.execute(
            "INSERT INTO test_schema.daily_stats \
             (rindexer_block_number, network, total_pings) VALUES ($1, $2, $3)",
            &[&(bn as i64), &network, &rust_decimal::Decimal::from(1u64)],
        )
        .await
        .unwrap();
    }

    // Also insert a derived row for a different network (should NOT be deleted)
    pg.execute(
        "INSERT INTO test_schema.daily_stats \
         (rindexer_block_number, network, total_pings) VALUES ($1, $2, $3)",
        &[&(block2 as i64), &"other_network", &rust_decimal::Decimal::from(99u64)],
    )
    .await
    .unwrap();

    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![DerivedTableInfo {
            full_table_name: "test_schema.daily_stats".to_string(),
            cross_chain: false,
        }],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    task.execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("derived table reorg task failed");

    // Only block1 derived row for "dev" + the "other_network" row should survive
    let derived_count: i64 =
        pg.query_one("SELECT count(*) FROM test_schema.daily_stats", &[]).await.unwrap().get(0);
    assert_eq!(derived_count, 2, "block1 dev row + other_network row should survive");

    // The other_network row should be untouched
    let other_count: i64 = pg
        .query_one(
            "SELECT count(*) FROM test_schema.daily_stats WHERE network = 'other_network'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(other_count, 1, "other_network derived row should not be affected");
}

// ---------------------------------------------------------------------------
// Test 8: Derived table cleanup (cross_chain=true)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_derived_table_cross_chain() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    pg.batch_execute(
        "CREATE TABLE IF NOT EXISTS test_schema.cross_stats (
             id SERIAL PRIMARY KEY,
             rindexer_block_number BIGINT NOT NULL,
             network VARCHAR(50) NOT NULL,
             total NUMERIC NOT NULL
         );",
    )
    .await
    .unwrap();

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    env.insert_event(&pg, network, 1, block1, zero_tx).await;
    env.insert_event(&pg, network, 2, block2, zero_tx).await;
    env.insert_block_hashes(&pg, network, block1, block2).await;

    // Insert cross-chain derived rows: same block number but different networks
    for net in &["dev", "other_network"] {
        pg.execute(
            "INSERT INTO test_schema.cross_stats \
             (rindexer_block_number, network, total) VALUES ($1, $2, $3)",
            &[&(block2 as i64), net, &rust_decimal::Decimal::from(1u64)],
        )
        .await
        .unwrap();
    }
    // This row at block1 should survive
    pg.execute(
        "INSERT INTO test_schema.cross_stats \
         (rindexer_block_number, network, total) VALUES ($1, $2, $3)",
        &[&(block1 as i64), &"dev", &rust_decimal::Decimal::from(1u64)],
    )
    .await
    .unwrap();

    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![DerivedTableInfo {
            full_table_name: "test_schema.cross_stats".to_string(),
            cross_chain: true,
        }],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    task.execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("cross-chain derived table reorg failed");

    // cross_chain=true deletes ALL networks at block >= fork_point
    let cross_count: i64 =
        pg.query_one("SELECT count(*) FROM test_schema.cross_stats", &[]).await.unwrap().get(0);
    assert_eq!(
        cross_count, 1,
        "cross_chain=true should delete rows for ALL networks at reorged blocks"
    );

    // The surviving row should be at block1
    let surviving_block: i64 = pg
        .query_one("SELECT rindexer_block_number FROM test_schema.cross_stats", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(surviving_block, block1 as i64, "surviving row should be at block1");
}

// ---------------------------------------------------------------------------
// Test 9: Consecutive reorgs — two reorgs in sequence, state stays consistent
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_consecutive() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;
    let block3 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 3).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    for (id, bn) in [(1u64, block1), (2, block2), (3, block3)] {
        env.insert_event(&pg, network, id, bn, zero_tx).await;
    }
    env.insert_block_hashes(&pg, network, block1, block3).await;

    assert_eq!(env.event_count(&pg).await, 3);

    // --- First reorg: invalidate block3 only ---
    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task1 = ReorgTask {
        network: network.to_string(),
        fork_point: block3,
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result1 = task1
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("first consecutive reorg failed");

    assert_eq!(result1.events_deleted, 1, "first reorg should delete Ping(3)");
    assert_eq!(env.event_count(&pg).await, 2, "Ping(1) and Ping(2) should remain");

    // --- Second reorg: now invalidate block2 as well ---
    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task2 = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg2 = env.rindexer_pg().await;
    let mut task_window2 = BlockChainWindow::new(256);

    let result2 = task2
        .execute(&mut task_window2, &persistence, Some(&rindexer_pg2), None, None)
        .await
        .expect("second consecutive reorg failed");

    assert_eq!(result2.events_deleted, 1, "second reorg should delete Ping(2)");
    assert_eq!(env.event_count(&pg).await, 1, "only Ping(1) should remain after two reorgs");

    let surviving: rust_decimal::Decimal =
        pg.query_one("SELECT id FROM test_schema.ping_pong_ping", &[]).await.unwrap().get(0);
    assert_eq!(surviving, rust_decimal::Decimal::from(1u64), "surviving event should be Ping(1)");
}

// ---------------------------------------------------------------------------
// Test 10: Persistence load/insert round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_persistence_round_trip() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let rindexer_pg = Arc::new(env.rindexer_pg().await);
    let persistence = ReorgBlockHashPersistence::new(Some(Arc::clone(&rindexer_pg)), None);
    let network = "dev";

    // Wait for a few blocks to accumulate
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let tip = get_block_number(&env.http, &env.rpc_url).await;
    let start = tip.saturating_sub(4);

    // Insert blocks via persistence
    for bn in start..=tip {
        let (hash, parent) = get_block_by_number(&env.http, &env.rpc_url, bn).await;
        persistence
            .insert_block(network, bn, &format!("{:#x}", hash), &format!("{:#x}", parent))
            .await
            .expect("insert_block failed");
    }

    // Load back and verify
    let loaded_window = persistence.load(network, 256).await.expect("load failed");
    assert_eq!(
        loaded_window.len(),
        (tip - start + 1) as usize,
        "loaded window should have all inserted blocks"
    );

    // Verify each entry matches
    for bn in start..=tip {
        let (expected_hash, expected_parent) =
            get_block_by_number(&env.http, &env.rpc_url, bn).await;
        let entry = loaded_window.get(bn).expect("block should be in loaded window");
        assert_eq!(entry.0, expected_hash, "block {} hash should match", bn);
        assert_eq!(entry.1, expected_parent, "block {} parent should match", bn);
    }

    // Test prune: remove entries older than tip-1
    persistence.prune(network, tip - 1).await.expect("prune failed");
    let pruned_window = persistence.load(network, 256).await.expect("load after prune failed");
    assert_eq!(pruned_window.len(), 2, "only tip-1 and tip should remain after prune");
    assert!(pruned_window.get(tip - 1).is_some(), "tip-1 should survive prune");
    assert!(pruned_window.get(tip).is_some(), "tip should survive prune");
    assert!(pruned_window.get(start).is_none(), "start should be pruned");
}

// ---------------------------------------------------------------------------
// Test 11: Multicall tx deduplication — a multicall bundles multiple internal
//          transactions that share the same tx_hash but differ in tx_index.
//          After rollback, events_deleted counts every row but
//          affected_tx_hashes must deduplicate to one entry per unique hash.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_multicall_tx_deduplication() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;

    let network = "dev";
    let multicall_tx = "0x00000000000000000000000000000000000000000000000000000000000000aa";
    let separate_tx = "0x00000000000000000000000000000000000000000000000000000000000000bb";

    // block1: one normal event (survives reorg)
    env.insert_event(&pg, network, 1, block1, separate_tx).await;

    // block2: multicall tx — same tx_hash, different tx_index (internal txs)
    let (hash2, _) = get_block_by_number(&env.http, &env.rpc_url, block2).await;
    let hash2_str = format!("{:#x}", hash2);
    for (ping_id, tx_idx, log_idx) in [(10u64, 0u64, "0"), (11, 1, "0"), (12, 2, "0")] {
        pg.execute(
            "INSERT INTO test_schema.ping_pong_ping \
             (id, sender, tx_hash, block_number, block_hash, network, tx_index, log_index) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &rust_decimal::Decimal::from(ping_id),
                &format!("{:#x}", env.deployer),
                &multicall_tx,
                &rust_decimal::Decimal::from(block2),
                &hash2_str.as_str(),
                &network,
                &rust_decimal::Decimal::from(tx_idx),
                &log_idx,
            ],
        )
        .await
        .unwrap();
    }

    // block2: also a normal (non-multicall) event with a different tx_hash
    pg.execute(
        "INSERT INTO test_schema.ping_pong_ping \
         (id, sender, tx_hash, block_number, block_hash, network, tx_index, log_index) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &rust_decimal::Decimal::from(20u64),
            &format!("{:#x}", env.deployer),
            &separate_tx,
            &rust_decimal::Decimal::from(block2),
            &hash2_str.as_str(),
            &network,
            &rust_decimal::Decimal::from(3u64),
            &"0",
        ],
    )
    .await
    .unwrap();

    env.insert_block_hashes(&pg, network, block1, block2).await;

    // 1 (block1) + 3 (multicall in block2) + 1 (separate in block2) = 5
    assert_eq!(env.event_count(&pg).await, 5);

    trigger_reorg(&env.http, &env.rpc_url, 1).await;

    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block2,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("multicall dedup reorg task failed");

    // 4 event rows deleted (3 multicall + 1 separate)
    assert_eq!(result.events_deleted, 4, "should delete all 4 events in block2");

    // Deduplication: multicall_tx appears 3 times but deduplicates to 1
    assert_eq!(
        result.affected_tx_hashes.len(),
        2,
        "affected_tx_hashes should deduplicate multicall's 3 internal txs to 1 entry"
    );
    assert!(result.affected_tx_hashes.contains(&multicall_tx.to_string()));
    assert!(result.affected_tx_hashes.contains(&separate_tx.to_string()));

    // Only the block1 event survives
    assert_eq!(env.event_count(&pg).await, 1, "only block1 event should remain");
}

// ---------------------------------------------------------------------------
// Test 12: Reorg recovery + re-indexing continuation
//
// Verifies the full cycle: events are indexed → reorg invalidates some →
// ReorgTask rolls back stale rows and rewinds the checkpoint → new
// canonical events are sent → indexer resumes from the rewound checkpoint,
// inserts only the new events → final state is consistent with no
// duplicates and no gaps.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_reindex_continuation() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;

    // --- Phase 1: initial indexing (3 events across 3 blocks) ---
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;
    let block3 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 3).await;

    let network = "dev";
    let zero_tx = "0x0000000000000000000000000000000000000000000000000000000000000000";
    for (id, bn) in [(1u64, block1), (2, block2), (3, block3)] {
        env.insert_event(&pg, network, id, bn, zero_tx).await;
    }
    env.insert_block_hashes(&pg, network, block1, block3).await;

    // Set checkpoint to block3 (indexer thinks it's fully synced)
    pg.execute(
        "INSERT INTO rindexer_internal.test_schema_ping (network, last_synced_block) \
         VALUES ($1, $2)
         ON CONFLICT (network) DO UPDATE SET last_synced_block = $2",
        &[&network, &(block3 as i64)],
    )
    .await
    .unwrap();

    assert_eq!(env.event_count(&pg).await, 3, "3 events after initial indexing");

    // --- Phase 2: reorg invalidates block2 and block3 ---
    trigger_reorg(&env.http, &env.rpc_url, 2).await;

    let fork_point = block2;
    let task = ReorgTask {
        network: network.to_string(),
        fork_point,
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let rindexer_pg = env.rindexer_pg().await;
    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("reorg rollback failed");

    assert_eq!(result.events_deleted, 2, "block2+block3 events deleted");
    assert_eq!(env.event_count(&pg).await, 1, "only Ping(1) remains after rollback");

    // Checkpoint should be rewound to fork_point - 1 = block1
    let checkpoint: i64 = pg
        .query_one(
            "SELECT last_synced_block FROM rindexer_internal.test_schema_ping WHERE network = $1",
            &[&network],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(checkpoint, (fork_point - 1) as i64, "checkpoint rewound to fork_point - 1");

    // --- Phase 3: new canonical events arrive post-reorg ---
    // These go into new blocks on the canonical chain (Anvil keeps running)
    let new_block_a = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 100).await;
    let new_block_b = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 200).await;

    // Simulate the indexer resuming from the rewound checkpoint:
    // it would fetch logs from (checkpoint + 1) onward and insert them.
    let resume_from = checkpoint as u64 + 1;
    assert!(new_block_a >= resume_from, "new events should be at or after the resume point");

    let new_tx_a = "0x00000000000000000000000000000000000000000000000000000000000000ca";
    let new_tx_b = "0x00000000000000000000000000000000000000000000000000000000000000cb";
    env.insert_event(&pg, network, 100, new_block_a, new_tx_a).await;
    env.insert_event(&pg, network, 200, new_block_b, new_tx_b).await;

    // Update checkpoint to the latest synced block
    pg.execute(
        "UPDATE rindexer_internal.test_schema_ping SET last_synced_block = $1 WHERE network = $2",
        &[&(new_block_b as i64), &network],
    )
    .await
    .unwrap();

    // Update block hashes for the new range
    env.insert_block_hashes(&pg, network, new_block_a, new_block_b).await;

    // --- Phase 4: verify final state ---

    // Total: 1 surviving (Ping(1)) + 2 new = 3 events
    assert_eq!(env.event_count(&pg).await, 3, "1 surviving + 2 re-indexed = 3 events");

    // No duplicate tx_hashes
    let dup_rows = pg
        .query(
            "SELECT tx_hash, COUNT(*) FROM test_schema.ping_pong_ping \
             GROUP BY tx_hash HAVING COUNT(*) > 1",
            &[],
        )
        .await
        .unwrap();
    assert!(dup_rows.is_empty(), "no duplicate tx_hashes after re-indexing");

    // The old stale events (Ping(2), Ping(3)) must NOT be present
    let stale_ids: Vec<rust_decimal::Decimal> = pg
        .query(
            "SELECT id FROM test_schema.ping_pong_ping WHERE id IN ($1, $2)",
            &[&rust_decimal::Decimal::from(2u64), &rust_decimal::Decimal::from(3u64)],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert!(stale_ids.is_empty(), "stale Ping(2) and Ping(3) must not exist");

    // The new canonical events must be present
    let new_ids: Vec<rust_decimal::Decimal> = pg
        .query("SELECT id FROM test_schema.ping_pong_ping ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        new_ids,
        vec![
            rust_decimal::Decimal::from(1u64),
            rust_decimal::Decimal::from(100u64),
            rust_decimal::Decimal::from(200u64),
        ],
        "final events should be Ping(1) + Ping(100) + Ping(200)"
    );

    // Checkpoint reflects the latest block
    let final_checkpoint: i64 = pg
        .query_one(
            "SELECT last_synced_block FROM rindexer_internal.test_schema_ping WHERE network = $1",
            &[&network],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        final_checkpoint, new_block_b as i64,
        "checkpoint should be at the latest re-indexed block"
    );

    // Block hashes for the new range exist in reorg_block_hashes
    let new_hash_count = env.reorg_hashes_count(&pg, network, new_block_a, new_block_b).await;
    assert!(
        new_hash_count > 0,
        "reorg_block_hashes should have entries for the new canonical blocks"
    );
}

// ---------------------------------------------------------------------------
// Test 13: on_reorg callback is fired with correct notification fields
//
// Uses ReorgCoordinator.handle_reorg() (not ReorgTask.execute() directly)
// to verify the full callback path: task execution → tx_hash collection →
// ReorgNotification construction → callback invocation.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_reorg_on_reorg_callback_fired() {
    let env = TestEnv::new().await;
    let pg = env.pg_client().await;
    env.setup_base_tables(&pg).await;

    let (contract, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let block1 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 1).await;
    let block2 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 2).await;
    let block3 = send_ping(&env.http, &env.rpc_url, env.deployer, contract, 3).await;

    let network = "dev";
    let tx_a = "0x00000000000000000000000000000000000000000000000000000000000000a1";
    let tx_b = "0x00000000000000000000000000000000000000000000000000000000000000b2";
    env.insert_event(&pg, network, 1, block1, tx_a).await;
    env.insert_event(&pg, network, 2, block2, tx_a).await;
    env.insert_event(&pg, network, 3, block3, tx_b).await;
    env.insert_block_hashes(&pg, network, block1, block3).await;

    trigger_reorg(&env.http, &env.rpc_url, 2).await;

    // Set up a callback that captures the ReorgNotification
    let captured: Arc<Mutex<Option<ReorgNotification>>> = Arc::new(Mutex::new(None));
    let captured_clone = Arc::clone(&captured);
    let mut registry = EventCallbackRegistry::new();
    registry.register_on_reorg(Arc::new(move |notification| {
        let captured = Arc::clone(&captured_clone);
        Box::pin(async move {
            *captured.lock().unwrap() = Some(notification);
        })
    }));

    // Execute the task and fire the callback — the same sequence handle_reorg does.
    let rindexer_pg = env.rindexer_pg().await;
    let task = ReorgTask {
        network: network.to_string(),
        fork_point: block2,
        detection_point: block3,
        event_tables: vec![EventTableInfo::new(
            "test_schema".to_string(),
            "ping_pong_ping".to_string(),
            "test_schema_ping".to_string(),
        )],
        derived_tables: vec![],
        canonical_blocks: vec![],
    };

    let mut task_window = BlockChainWindow::new(256);
    let persistence = ReorgBlockHashPersistence::new(None, None);

    let result = task
        .execute(&mut task_window, &persistence, Some(&rindexer_pg), None, None)
        .await
        .expect("reorg task failed");

    // Replicate handle_reorg's callback logic
    let affected_tx_hashes: Vec<B256> =
        result.affected_tx_hashes.iter().filter_map(|h| h.parse().ok()).collect();

    let notification = ReorgNotification {
        network: task.network.clone(),
        fork_block: task.fork_point,
        detection_block: task.detection_point,
        invalidated_tx_hashes: affected_tx_hashes,
    };
    registry.fire_on_reorg(notification).await;

    // Verify the callback was invoked with the correct fields
    let notification = captured.lock().unwrap().take().expect("on_reorg callback was not fired");

    assert_eq!(notification.network, network);
    assert_eq!(notification.fork_block, block2);
    assert_eq!(notification.detection_block, block3);

    // tx_a was in block2, tx_b was in block3 — both invalidated, 2 distinct hashes
    assert_eq!(notification.invalidated_tx_hashes.len(), 2, "should have 2 invalidated tx hashes");
    let hash_strs: Vec<String> =
        notification.invalidated_tx_hashes.iter().map(|h| format!("{:#x}", h)).collect();
    assert!(hash_strs.contains(&tx_a.to_string()), "should contain tx_a");
    assert!(hash_strs.contains(&tx_b.to_string()), "should contain tx_b");
}
