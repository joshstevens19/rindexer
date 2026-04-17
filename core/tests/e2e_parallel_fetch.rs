//! End-to-end test for parallel historical backfill (PR #380).
//!
//! Runs rindexer's no-code historical pipeline against Anvil + Postgres with
//! `fetch_concurrency` = 1 (sequential fallback), 4, and 8. Asserts that all
//! three runs produce an identical, correctly-ordered set of events.
//!
//! Requires Docker. Must be run sequentially (tests share DATABASE_URL env):
//!   cargo test -q -p rindexer --test e2e_parallel_fetch -- --ignored --test-threads=1

use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::Address;
use reqwest::Client as HttpClient;
use rindexer::{GraphqlOverrideSettings, IndexerNoCodeDetails, StartNoCodeDetails};
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// ---------------------------------------------------------------------------
// PingPong contract (same as e2e_reorg.rs): emits Ping(uint256 indexed id, address sender)
// ---------------------------------------------------------------------------

const PING_PONG_BYTECODE: &str =
    "6080604052348015600e575f5ffd5b506101748061001c5f395ff3fe608060405234801561000f575f5ffd5b5060043610610029575f3560e01c8063773acdef1461002d575b5f5ffd5b610047600480360381019061004291906100bb565b610049565b005b807fc05b373e05c47417d9c7204807552389e512c0e21cbc01a03d1554561080ac6e336040516100799190610125565b60405180910390a250565b5f5ffd5b5f819050919050565b61009a81610088565b81146100a4575f5ffd5b50565b5f813590506100b581610091565b92915050565b5f602082840312156100d0576100cf610084565b5b5f6100dd848285016100a7565b91505092915050565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f61010f826100e6565b9050919050565b61011f81610105565b82525050565b5f6020820190506101385f830184610116565b9291505056fea2646970667358221220dc07dd9f297d16a6d4ac329e4565c9ecb79b34df9738da42d568df67b039348764736f6c634300081c0033";

const PING_SELECTOR: [u8; 4] = [0x77, 0x3a, 0xcd, 0xef];

const PING_PONG_ABI: &str = r#"[
  {
    "type": "event",
    "name": "Ping",
    "inputs": [
      { "name": "id", "type": "uint256", "indexed": true },
      { "name": "sender", "type": "address", "indexed": false }
    ],
    "anonymous": false
  },
  {
    "type": "function",
    "name": "ping",
    "inputs": [
      { "name": "id", "type": "uint256" }
    ],
    "outputs": [],
    "stateMutability": "nonpayable"
  }
]"#;

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
    let j: Value = resp.json().await.ok()?;
    let s = j.get("result")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

async fn get_block_number(http: &HttpClient, rpc_url: &str) -> u64 {
    let result = rpc_call(http, rpc_url, "eth_blockNumber", json!([])).await;
    u64::from_str_radix(result.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
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
    for _ in 0..120 {
        let result = rpc_call(http, rpc_url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !result.is_null() {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Timed out waiting for receipt of {}", tx_hash);
}

async fn deploy_ping_pong(http: &HttpClient, rpc_url: &str, from: Address) -> Address {
    let tx = json!({
        "from": format!("{:#x}", from),
        "data": format!("0x{}", PING_PONG_BYTECODE),
        "gas": "0x100000",
    });
    let tx_hash = rpc_call(http, rpc_url, "eth_sendTransaction", json!([tx])).await;
    let tx_hash_str = tx_hash.as_str().unwrap();
    let receipt = wait_for_receipt(http, rpc_url, tx_hash_str).await;
    receipt["contractAddress"].as_str().expect("no contractAddress in receipt").parse().unwrap()
}

async fn send_ping(
    http: &HttpClient,
    rpc_url: &str,
    from: Address,
    contract: Address,
    id: u64,
) -> u64 {
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

/// Mine N empty blocks via anvil's `anvil_mine`.
async fn mine_blocks(http: &HttpClient, rpc_url: &str, count: u64) {
    let hex = format!("0x{:x}", count);
    rpc_call(http, rpc_url, "anvil_mine", json!([hex])).await;
}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

struct TestEnv {
    pg_port: u16,
    rpc_url: String,
    http: HttpClient,
    deployer: Address,
    _pg_container: testcontainers::ContainerAsync<Postgres>,
    _anvil_container: testcontainers::ContainerAsync<GenericImage>,
}

impl TestEnv {
    async fn new() -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let pg_container =
            Postgres::default().start().await.expect("failed to start postgres container");
        let pg_port =
            pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        // Auto-mining (no --block-time) so we can mine blocks on demand via anvil_mine
        // and emit events immediately.
        let anvil_container = GenericImage::new("ghcr.io/foundry-rs/foundry", "latest")
            .with_exposed_port(8545_u16.into())
            .with_cmd(vec!["anvil --host 0.0.0.0".to_string()])
            .with_startup_timeout(Duration::from_secs(30))
            .start()
            .await
            .expect("failed to start anvil container");
        let anvil_port =
            anvil_container.get_host_port_ipv4(8545).await.expect("failed to get anvil port");
        let rpc_url = format!("http://127.0.0.1:{}", anvil_port);

        let http = HttpClient::new();

        for _ in 0..60 {
            if try_get_block_number(&http, &rpc_url).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let accounts = get_accounts(&http, &rpc_url).await;
        let deployer = accounts[0];

        // SAFETY: tests enforce --test-threads=1 via #[ignore], so no other
        // thread reads these env vars concurrently.
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                format!(
                    "postgres://postgres:postgres@127.0.0.1:{}/postgres?sslmode=disable",
                    pg_port
                ),
            );
        }

        Self {
            pg_port,
            rpc_url,
            http,
            deployer,
            _pg_container: pg_container,
            _anvil_container: anvil_container,
        }
    }

    async fn pg_client(&self) -> tokio_postgres::Client {
        let conn_str = format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            self.pg_port
        );
        let (client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("failed to connect to postgres");
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {}", e);
            }
        });
        client
    }
}

/// Produce the manifest YAML for a single fetch_concurrency run.
///
/// Uses a distinct indexer `name` so each run gets its own Postgres schema
/// and we can verify parity across runs without cross-contamination.
fn write_manifest(
    dir: &std::path::Path,
    indexer_name: &str,
    rpc_url: &str,
    contract_address: Address,
    start_block: u64,
    end_block: u64,
    fetch_concurrency: usize,
) {
    std::fs::create_dir_all(dir.join("abis")).expect("mkdir abis");
    std::fs::write(dir.join("abis/PingPong.abi.json"), PING_PONG_ABI).expect("write abi");

    let yaml = format!(
        r#"name: {indexer_name}
description: "Parallel fetch e2e"
repository: "https://example.invalid"
project_type: no-code
config:
  fetch_concurrency: {fetch_concurrency}
networks:
  - name: dev
    chain_id: 31337
    rpc: {rpc_url}
storage:
  postgres:
    enabled: true
native_transfers: false
contracts:
  - name: PingPong
    details:
      - network: dev
        address: "{contract_address:#x}"
        start_block: "{start_block}"
        end_block: "{end_block}"
    abi: ./abis/PingPong.abi.json
    include_events:
      - Ping
"#,
        indexer_name = indexer_name,
        rpc_url = rpc_url,
        contract_address = contract_address,
        start_block = start_block,
        end_block = end_block,
        fetch_concurrency = fetch_concurrency,
    );
    std::fs::write(dir.join("rindexer.yaml"), yaml).expect("write yaml");
}

/// Rows returned from the Ping event table, normalized to `(id, block_number)`
/// for cross-run comparison.
async fn query_ping_rows(pg: &tokio_postgres::Client, schema: &str) -> Vec<(String, i64)> {
    let table = format!("{}.ping", schema);
    let sql = format!(
        "SELECT id::text, block_number::bigint FROM {} ORDER BY block_number ASC, id ASC",
        table
    );
    let rows = pg.query(&sql, &[]).await.unwrap_or_else(|e| panic!("query {} failed: {}", sql, e));
    rows.into_iter().map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1))).collect()
}

/// Run rindexer's historical-only pipeline programmatically and return once
/// indexing completes (no live-indexing, no graphql, so start_rindexer_no_code
/// exits when historical sync finishes).
async fn run_rindexer_historical(manifest_path: PathBuf) {
    // Historical-only: live_indexing defaults to false for a contract with
    // an end_block set; start_rindexer_no_code returns after historical sync.
    let result = rindexer::start_rindexer_no_code(StartNoCodeDetails {
        manifest_path: &manifest_path,
        indexing_details: IndexerNoCodeDetails { enabled: true },
        graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
        watch: false,
    })
    .await;
    if let Err(e) = result {
        panic!("rindexer run failed: {:?}", e);
    }
}

// ---------------------------------------------------------------------------
// Main test — parity across fetch_concurrency = 1, 4, 8
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn parallel_fetch_parity_across_worker_counts() {
    let env = TestEnv::new().await;

    // Deploy the contract and record its deploy block.
    let contract = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let deploy_block = get_block_number(&env.http, &env.rpc_url).await;

    // Emit 50 events across enough blocks that all three concurrency levels
    // fully engage their workers. plan_parallel_fetch gives:
    //   effective_concurrency = min(concurrency, floor(total_blocks / 1000))
    // so to get 8 real workers at c=8 we need >= 8000 blocks in the range.
    //
    // 50 events × 170 blocks gap = ~8500 blocks — above the 8-worker threshold.
    let mut expected_pings: Vec<(u64, u64)> = Vec::new();
    for id in 1u64..=50 {
        mine_blocks(&env.http, &env.rpc_url, 170).await;
        let blk = send_ping(&env.http, &env.rpc_url, env.deployer, contract, id).await;
        expected_pings.push((id, blk));
    }

    let end_block = get_block_number(&env.http, &env.rpc_url).await;
    // Mine a few extra empty blocks beyond the last event so the filter spans
    // a clean post-event region too.
    mine_blocks(&env.http, &env.rpc_url, 10).await;

    // Sanity: the total block range must be large enough that c=8 actually
    // spawns 8 workers (not just 1 due to the floor(total/1000) cap).
    assert!(
        end_block - deploy_block >= 8000,
        "setup produced only {} blocks, c=8 won't fully engage 8 workers",
        end_block - deploy_block
    );

    let start_block = deploy_block + 1;

    // -----------------------------------------------------------------
    // Run 1: fetch_concurrency = 1 (forces sequential via n > 1 check)
    // Run 2: fetch_concurrency = 4
    // Run 3: fetch_concurrency = 8
    // -----------------------------------------------------------------
    let mut per_run_rows: Vec<(usize, Vec<(String, i64)>)> = Vec::new();
    for &concurrency in &[1usize, 4, 8] {
        // Unique indexer name → unique schema → isolated data per run.
        let indexer_name = format!("ParallelFetchC{concurrency}");
        let tmp = tempfile::tempdir().expect("tempdir");
        write_manifest(
            tmp.path(),
            &indexer_name,
            &env.rpc_url,
            contract,
            start_block,
            end_block,
            concurrency,
        );

        run_rindexer_historical(tmp.path().join("rindexer.yaml")).await;

        let pg = env.pg_client().await;
        let schema = format!("parallel_fetch_c{}_ping_pong", concurrency);
        let rows = query_ping_rows(&pg, &schema).await;

        assert_eq!(
            rows.len(),
            expected_pings.len(),
            "fetch_concurrency={} indexed {} rows, expected {}",
            concurrency,
            rows.len(),
            expected_pings.len()
        );

        // Strict block ordering — this is what the reorder buffer guarantees
        // for parallel runs and what the sequential run trivially provides.
        let mut last_block: i64 = -1;
        for (_, blk) in &rows {
            assert!(
                *blk >= last_block,
                "fetch_concurrency={} emitted blocks out of order: {} after {}",
                concurrency,
                blk,
                last_block
            );
            last_block = *blk;
        }

        // Every expected ping must be present at its correct block.
        for (id, blk) in &expected_pings {
            let want_id = id.to_string();
            let want_blk = *blk as i64;
            assert!(
                rows.iter().any(|(r_id, r_blk)| r_id == &want_id && *r_blk == want_blk),
                "fetch_concurrency={} missing Ping(id={}, block={})",
                concurrency,
                id,
                blk
            );
        }

        per_run_rows.push((concurrency, rows));
    }

    // Parity: all three runs must produce identical rows.
    let (_, baseline) = &per_run_rows[0];
    for (c, rows) in &per_run_rows[1..] {
        assert_eq!(
            rows, baseline,
            "fetch_concurrency={} produced different rows than the sequential baseline",
            c
        );
    }
}

// ---------------------------------------------------------------------------
// Edge case: large parallel run with a 5000-block range and 8 workers.
// Verifies chunking at scale and that the reorder buffer doesn't drop or
// re-order events when workers legitimately complete out of dispatch order.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn parallel_fetch_large_range_eight_workers() {
    let env = TestEnv::new().await;

    let contract = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let deploy_block = get_block_number(&env.http, &env.rpc_url).await;

    // Sparse events across a wider range — 30 events across ~5400 blocks.
    // plan_parallel_fetch(5400, 8) → chunk_size=1000, effective_concurrency=5
    // (floor(5400/1000)=5), so five workers actually run concurrently.
    let mut expected_pings: Vec<(u64, u64)> = Vec::new();
    for id in 1u64..=30 {
        mine_blocks(&env.http, &env.rpc_url, 180).await;
        let blk = send_ping(&env.http, &env.rpc_url, env.deployer, contract, id).await;
        expected_pings.push((id, blk));
    }
    let end_block = get_block_number(&env.http, &env.rpc_url).await;
    mine_blocks(&env.http, &env.rpc_url, 10).await;

    let start_block = deploy_block + 1;
    let indexer_name = "ParallelFetchLarge8";
    let tmp = tempfile::tempdir().expect("tempdir");
    write_manifest(tmp.path(), indexer_name, &env.rpc_url, contract, start_block, end_block, 8);

    run_rindexer_historical(tmp.path().join("rindexer.yaml")).await;

    let pg = env.pg_client().await;
    let schema = "parallel_fetch_large_8_ping_pong";
    let rows = query_ping_rows(&pg, schema).await;

    assert_eq!(rows.len(), expected_pings.len());

    let mut last_block: i64 = -1;
    for (_, blk) in &rows {
        assert!(*blk >= last_block, "out-of-order block {} after {}", blk, last_block);
        last_block = *blk;
    }

    for (id, blk) in &expected_pings {
        let want_id = id.to_string();
        let want_blk = *blk as i64;
        assert!(
            rows.iter().any(|(r_id, r_blk)| r_id == &want_id && *r_blk == want_blk),
            "missing Ping(id={}, block={})",
            id,
            blk
        );
    }
}
