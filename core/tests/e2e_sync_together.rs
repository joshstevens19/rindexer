//! End-to-end test for `sync_together` lockstep live indexing.
//!
//! Two PingPong contracts (PingA / PingB) form one sync_together group, with a
//! custom aggregation table on PingA. After the indexer reaches the live
//! lockstep phase, every iteration fires one Ping at EACH contract inside the
//! SAME block. Because grouped writes commit in one transaction per block, a
//! concurrent Postgres poller must never observe:
//!
//! - a different number of PingA vs PingB rows, or
//! - the custom table's aggregate diverging from the raw PingA rows.
//!
//! Any divergence in a sample is a torn block — exactly what sync_together
//! exists to prevent. We also assert both members' internal checkpoints are
//! identical at the end and that no duplicate (tx_hash, log_index) rows exist
//! (the hist→live seam guarantee).
//!
//! Requires Docker. Run via nextest so each test gets its own process:
//!   cargo nextest run -q -p rindexer --test e2e_sync_together
//!
//! # Isolation invariant
//! Mutates process-global state (`DATABASE_URL`, shutdown flag) — one
//! `#[tokio::test]` per process, same as the other e2e files.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use reqwest::Client as HttpClient;
use rindexer::{GraphqlOverrideSettings, IndexerNoCodeDetails, StartNoCodeDetails};
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// PingPong emits Ping(uint256 indexed id, address sender) — same bytecode as
// the other e2e files (they cannot share code across test crates).
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
  }
]"#;

async fn rpc_call(http: &HttpClient, rpc_url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let resp = http.post(rpc_url).json(&body).send().await.expect("RPC request failed");
    let json: Value = resp.json().await.expect("RPC response not JSON");
    if let Some(err) = json.get("error") {
        panic!("RPC error calling {}: {:?}", method, err);
    }
    json["result"].clone()
}

async fn try_get_block_number(http: &HttpClient, rpc_url: &str) -> Option<u64> {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []});
    let resp = http.post(rpc_url).json(&body).send().await.ok()?;
    let json: Value = resp.json().await.ok()?;
    let result = json.get("result")?.as_str()?;
    u64::from_str_radix(result.trim_start_matches("0x"), 16).ok()
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
    for _ in 0..60 {
        let result = rpc_call(http, rpc_url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !result.is_null() {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("Timed out waiting for receipt of {}", tx_hash);
}

async fn mine_one(http: &HttpClient, rpc_url: &str) {
    let _ = rpc_call(http, rpc_url, "anvil_mine", json!([1])).await;
}

async fn deploy_ping_pong(http: &HttpClient, rpc_url: &str, from: Address) -> Address {
    let tx = json!({
        "from": format!("{:#x}", from),
        "data": format!("0x{}", PING_PONG_BYTECODE),
        "gas": "0x100000",
    });
    let tx_hash = rpc_call(http, rpc_url, "eth_sendTransaction", json!([tx])).await;
    mine_one(http, rpc_url).await;
    let receipt = wait_for_receipt(http, rpc_url, tx_hash.as_str().unwrap()).await;
    receipt["contractAddress"].as_str().unwrap().parse().unwrap()
}

/// Sends a ping WITHOUT mining, so multiple pings can share one block.
async fn send_ping_no_mine(
    http: &HttpClient,
    rpc_url: &str,
    from: Address,
    contract: Address,
    id: u64,
) -> String {
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
    tx_hash.as_str().unwrap().to_string()
}

fn write_manifest(
    dir: &std::path::Path,
    indexer_name: &str,
    rpc_url: &str,
    contract_a: Address,
    contract_b: Address,
    start_block: u64,
) {
    std::fs::create_dir_all(dir.join("abis")).expect("mkdir abis");
    std::fs::write(dir.join("abis/PingPong.abi.json"), PING_PONG_ABI).expect("write abi");

    let yaml = format!(
        r#"name: {indexer_name}
description: "sync_together lockstep e2e"
repository: "https://example.invalid"
project_type: no-code
networks:
  - name: dev
    chain_id: 31337
    rpc: {rpc_url}
storage:
  postgres:
    enabled: true
native_transfers: false
contracts:
  - name: PingA
    details:
      - network: dev
        address: "{contract_a:#x}"
        start_block: "{start_block}"
    abi: ./abis/PingPong.abi.json
    include_events:
      - Ping
    tables:
      - name: totals
        columns:
          - name: sender
          - name: pings
            type: uint256
            default: "0"
        events:
          - event: Ping
            operations:
              - type: upsert
                where:
                  sender: $sender
                set:
                  - column: pings
                    action: add
                    value: "1"
  - name: PingB
    details:
      - network: dev
        address: "{contract_b:#x}"
        start_block: "{start_block}"
    abi: ./abis/PingPong.abi.json
    include_events:
      - Ping
sync_together:
  - group: pair
    contracts:
      - name: PingA
        events:
          - Ping
      - name: PingB
        events:
          - Ping
"#
    );
    std::fs::write(dir.join("rindexer.yaml"), yaml).expect("write yaml");
}

static TEST_ENV_ALREADY_INITIALIZED: AtomicBool = AtomicBool::new(false);

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
        assert!(
            !TEST_ENV_ALREADY_INITIALIZED.swap(true, Ordering::SeqCst),
            "TestEnv::new called twice in one process — run under cargo nextest"
        );

        let _ = rustls::crypto::ring::default_provider().install_default();

        let pg_container =
            Postgres::default().start().await.expect("failed to start postgres container");
        let pg_port =
            pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        // --no-mining: blocks advance only on explicit anvil_mine, so paired
        // pings deterministically share a block.
        let anvil_container = GenericImage::new("ghcr.io/foundry-rs/foundry", "latest")
            .with_exposed_port(8545_u16.into())
            .with_cmd(vec!["anvil --host 0.0.0.0 --no-mining".to_string()])
            .with_startup_timeout(Duration::from_secs(30))
            .start()
            .await
            .expect("failed to start anvil container");
        let anvil_port =
            anvil_container.get_host_port_ipv4(8545).await.expect("failed to get anvil port");
        let rpc_url = format!("http://127.0.0.1:{}", anvil_port);

        let http = HttpClient::new();
        for _ in 0..30 {
            if try_get_block_number(&http, &rpc_url).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let accounts = get_accounts(&http, &rpc_url).await;
        let deployer = accounts[0];

        // SAFETY: nextest gives this test its own process.
        unsafe {
            std::env::set_var(
                "DATABASE_URL",
                format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", pg_port),
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
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("failed to connect to postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

async fn count_rows(pg: &tokio_postgres::Client, table: &str) -> i64 {
    match pg.query_one(&format!("SELECT COUNT(*)::bigint FROM {table}"), &[]).await {
        Ok(row) => row.get(0),
        Err(_) => -1, // table not created yet
    }
}

async fn sum_pings(pg: &tokio_postgres::Client, table: &str) -> i64 {
    match pg.query_one(&format!("SELECT COALESCE(SUM(pings), 0)::bigint FROM {table}"), &[]).await {
        Ok(row) => row.get(0),
        Err(_) => -1,
    }
}

async fn duplicate_count(pg: &tokio_postgres::Client, table: &str) -> i64 {
    match pg
        .query_one(
            &format!(
                "SELECT COALESCE(SUM(c - 1), 0)::bigint FROM (SELECT COUNT(*) AS c FROM {table} GROUP BY tx_hash, log_index) d WHERE c > 1"
            ),
            &[],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => -1,
    }
}

async fn checkpoint(pg: &tokio_postgres::Client, internal_table: &str) -> i64 {
    match pg
        .query_one(
            &format!(
                "SELECT last_synced_block::bigint FROM rindexer_internal.{internal_table} WHERE network = 'dev'"
            ),
            &[],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(_) => -1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_together_lockstep_is_atomic_per_block() {
    let env = TestEnv::new().await;

    let contract_a = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let contract_b = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let deploy_block = get_block_number(&env.http, &env.rpc_url).await;

    // NOTE: no digits — camel_to_snake splits digit boundaries, which would
    // make the schema name hard to predict (see e2e_hist_to_live_dup.rs).
    let indexer_name = "sync_lockstep_test";
    let table_a = format!("{indexer_name}_ping_a.ping");
    let table_b = format!("{indexer_name}_ping_b.ping");
    let totals_table = format!("{indexer_name}_ping_a.totals");
    let checkpoint_a = format!("{indexer_name}_ping_a_ping");
    let checkpoint_b = format!("{indexer_name}_ping_b_ping");

    let tmp = tempfile::tempdir().expect("tempdir");
    write_manifest(
        tmp.path(),
        indexer_name,
        &env.rpc_url,
        contract_a,
        contract_b,
        deploy_block + 1,
    );
    let manifest_path: PathBuf = tmp.path().join("rindexer.yaml");

    let rindexer_fut = rindexer::start_rindexer_no_code(StartNoCodeDetails {
        manifest_path: &manifest_path,
        indexing_details: IndexerNoCodeDetails { enabled: true },
        graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
        watch: false,
    });

    // Concurrent atomicity poller: from the moment tables exist, every sample
    // must satisfy count_a == count_b and sum(totals.pings) == count_a. A
    // torn sample means a block was committed partially across the group.
    let violations: Arc<tokio::sync::Mutex<Vec<String>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let poller_stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let violations = Arc::clone(&violations);
        let stop = Arc::clone(&poller_stop);
        let pg = env.pg_client().await;
        let (table_a, table_b, totals_table) =
            (table_a.clone(), table_b.clone(), totals_table.clone());
        tokio::spawn(async move {
            // All three counts in ONE statement: a single query runs under one
            // snapshot, so the values are mutually consistent. Sampling them
            // with separate queries would let a commit land between queries
            // and report a false "torn" state.
            let sample_sql = format!(
                "SELECT (SELECT COUNT(*)::bigint FROM {table_a}) AS a, \
                        (SELECT COUNT(*)::bigint FROM {table_b}) AS b, \
                        (SELECT COALESCE(SUM(pings), 0)::bigint FROM {totals_table}) AS t"
            );
            while !stop.load(Ordering::SeqCst) {
                if let Ok(row) = pg.query_one(&sample_sql, &[]).await {
                    let a: i64 = row.get("a");
                    let b: i64 = row.get("b");
                    let total: i64 = row.get("t");
                    if a != b {
                        violations
                            .lock()
                            .await
                            .push(format!("torn block: ping_a rows={a} ping_b rows={b}"));
                    }
                    if a != total {
                        violations
                            .lock()
                            .await
                            .push(format!("torn custom table: ping_a rows={a} totals sum={total}"));
                    }
                }
                // Query errors are expected until the indexer creates the
                // tables; ignore them.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };

    const PAIRS: u64 = 12;

    let driver = {
        let pg = env.pg_client().await;
        let (table_a, table_b) = (table_a.clone(), table_b.clone());
        let (checkpoint_a, checkpoint_b) = (checkpoint_a.clone(), checkpoint_b.clone());
        let stop = Arc::clone(&poller_stop);
        let env = &env;
        async move {
            // Wait until the LOCKSTEP loop is provably live before firing
            // pings: blocks at or below the startup head snapshot go through
            // the ordinary independent historical pipelines, which commit
            // PingA and PingB at different instants — pings landing there
            // would trip the atomicity poller as false torn blocks on a slow
            // start. Only the lockstep loop can checkpoint past the snapshot,
            // so mine a block and require BOTH members' checkpoints to reach
            // a tip mined inside this loop (the manifest sets no
            // reorg_safe_distance, so lockstep checkpoints reach the tip).
            let gate_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                mine_one(&env.http, &env.rpc_url).await;
                let tip = get_block_number(&env.http, &env.rpc_url).await as i64;
                tokio::time::sleep(Duration::from_millis(200)).await;
                let a = checkpoint(&pg, &checkpoint_a).await;
                let b = checkpoint(&pg, &checkpoint_b).await;
                if a >= tip && b >= tip {
                    break;
                }
                if tokio::time::Instant::now() > gate_deadline {
                    panic!("timed out waiting for the lockstep loop to go live: checkpoints a={a} b={b}, tip {tip}");
                }
            }

            // Fire PAIRS iterations: one ping at each contract in the SAME
            // block.
            for id in 1..=PAIRS {
                let tx_a =
                    send_ping_no_mine(&env.http, &env.rpc_url, env.deployer, contract_a, id).await;
                let tx_b =
                    send_ping_no_mine(&env.http, &env.rpc_url, env.deployer, contract_b, id).await;
                mine_one(&env.http, &env.rpc_url).await;
                wait_for_receipt(&env.http, &env.rpc_url, &tx_a).await;
                wait_for_receipt(&env.http, &env.rpc_url, &tx_b).await;
                tokio::time::sleep(Duration::from_millis(150)).await;
            }

            // Keep mining so the lockstep loop drains everything.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                mine_one(&env.http, &env.rpc_url).await;
                let a = count_rows(&pg, &table_a).await;
                let b = count_rows(&pg, &table_b).await;
                if a == PAIRS as i64 && b == PAIRS as i64 {
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    panic!(
                        "timed out waiting for all pings: ping_a={a} ping_b={b} (expected {PAIRS})"
                    );
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            // Settle, then stop sampling before shutdown (shutdown itself is
            // not required to be atomic across the WHOLE window, only per
            // block — but rows never appear partially, so keep polling until
            // just before shutdown for maximum coverage).
            tokio::time::sleep(Duration::from_millis(500)).await;
            stop.store(true, Ordering::SeqCst);
            rindexer::initiate_shutdown().await;
        }
    };

    tokio::select! {
        res = rindexer_fut => {
            panic!("rindexer exited before driver finished: {:?}", res);
        }
        () = driver => {}
    }

    poller.await.expect("poller task");

    let pg = env.pg_client().await;

    // No torn samples.
    let violations = violations.lock().await;
    assert!(violations.is_empty(), "atomicity violations observed: {violations:?}");

    // Exactly-once indexing (hist→live seam guarantee).
    assert_eq!(duplicate_count(&pg, &table_a).await, 0, "duplicate rows in ping_a");
    assert_eq!(duplicate_count(&pg, &table_b).await, 0, "duplicate rows in ping_b");

    // Custom-table aggregate matches raw rows exactly (no double-counted
    // add operations).
    assert_eq!(sum_pings(&pg, &totals_table).await, PAIRS as i64, "totals aggregate wrong");

    // Member checkpoints move in lockstep: equal once live.
    let cp_a = checkpoint(&pg, &checkpoint_a).await;
    let cp_b = checkpoint(&pg, &checkpoint_b).await;
    assert!(cp_a > 0, "checkpoint_a missing");
    assert_eq!(cp_a, cp_b, "member checkpoints diverged: a={cp_a} b={cp_b}");
}
