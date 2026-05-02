//! Reproducer for a historical→live transition duplicate-row bug.
//!
//! The e2e test `test_reorg_with_finalized_delivery` intermittently fails
//! with "Found N duplicate tx_hash entries". Logs show the same block is
//! INDEXED twice during phase 2 (live start) before any reorg fires:
//!
//! ```
//! SYNCING - Fetched 1 logs between: 7 - 7
//! INDEXED - 1 events - blocks: 7 - 7          (first dispatch)
//! INDEXED - 1 events - blocks: 7 - 7          (second — duplicate)
//! ```
//!
//! This test exercises only the historical→live transition with a tight
//! `reorg_safe_distance` (which surfaces the race) and asserts PG sees each
//! event exactly once. It runs the same scenario in a loop so flakiness
//! (~20-30% in the original e2e harness) is visible as a failure probability
//! rather than a once-in-a-blue-moon CI flake.
//!
//! Requires Docker. Run via nextest so each test gets its own process:
//!   cargo nextest run -q -p rindexer --test e2e_hist_to_live_dup
//!
//! # Isolation invariant
//!
//! This test mutates process-global state (`DATABASE_URL` env var,
//! `_test_reset_shutdown_flag()` AtomicBool in `rindexer::system_state`).
//! It MUST run in its own process. `.github/workflows/ci.yml` pins
//! `cargo nextest run` everywhere including coverage, and nextest's default
//! `run-as-child = true` fork-per-test guarantees isolation.
//!
//! `TestEnv::new` enforces the invariant at runtime via a per-process
//! `AtomicBool`: constructing a second `TestEnv` in the same process panics.
//! This surfaces a broken test harness immediately if someone either (a)
//! adds a second `#[tokio::test]` to this file and runs under plain
//! `cargo test`, or (b) removes the nextest requirement from CI.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use alloy::primitives::Address;
use reqwest::Client as HttpClient;
use rindexer::{GraphqlOverrideSettings, IndexerNoCodeDetails, StartNoCodeDetails};
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// PingPong emits Ping(uint256 indexed id, address sender). Same contract as
// e2e_reorg.rs / e2e_parallel_fetch.rs — keeps ABI handling identical.
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

// ---------------------------------------------------------------------------
// RPC helpers (minimal — copied from e2e_reorg.rs, not extracted because the
// helpers there live in the test crate and `core/tests` files don't share)
// ---------------------------------------------------------------------------

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
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("Timed out waiting for receipt of {}", tx_hash);
}

/// Under `--no-mining` Anvil never auto-mines, so every `eth_sendTransaction`
/// call must be paired with an explicit `anvil_mine` to surface the receipt.
/// `deploy_ping_pong` and `send_ping` both rely on this — they send, mine one
/// block, then wait for the receipt.
async fn mine_one(http: &HttpClient, rpc_url: &str) {
    let _ = rpc_call(http, rpc_url, "anvil_mine", json!([1])).await;
}

async fn deploy_ping_pong(http: &HttpClient, rpc_url: &str, from: Address) -> (Address, u64) {
    let tx = json!({
        "from": format!("{:#x}", from),
        "data": format!("0x{}", PING_PONG_BYTECODE),
        "gas": "0x100000",
    });
    let tx_hash = rpc_call(http, rpc_url, "eth_sendTransaction", json!([tx])).await;
    mine_one(http, rpc_url).await;
    let receipt = wait_for_receipt(http, rpc_url, tx_hash.as_str().unwrap()).await;
    let contract: Address = receipt["contractAddress"].as_str().unwrap().parse().unwrap();
    let block =
        u64::from_str_radix(receipt["blockNumber"].as_str().unwrap().trim_start_matches("0x"), 16)
            .unwrap();
    (contract, block)
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
    mine_one(http, rpc_url).await;
    let receipt = wait_for_receipt(http, rpc_url, tx_hash.as_str().unwrap()).await;
    u64::from_str_radix(receipt["blockNumber"].as_str().unwrap().trim_start_matches("0x"), 16)
        .unwrap()
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

/// Write a no-code manifest with a tight `reorg_safe_distance` and NO
/// `end_block` — so the indexer transitions from historical to live. The
/// small safe distance puts the head very close to the last-synced block at
/// the moment phase 2 takes over, which is when the bug reproduces.
fn write_manifest(
    dir: &std::path::Path,
    indexer_name: &str,
    rpc_url: &str,
    contract_address: Address,
    start_block: u64,
    reorg_safe_distance: u64,
) {
    std::fs::create_dir_all(dir.join("abis")).expect("mkdir abis");
    std::fs::write(dir.join("abis/PingPong.abi.json"), PING_PONG_ABI).expect("write abi");

    let yaml = format!(
        r#"name: {indexer_name}
description: "Historical→live transition duplicate repro"
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
  - name: PingPong
    details:
      - network: dev
        address: "{contract_address:#x}"
        start_block: "{start_block}"
    reorg_safe_distance: {reorg_safe_distance}
    abi: ./abis/PingPong.abi.json
    include_events:
      - Ping
"#,
        indexer_name = indexer_name,
        rpc_url = rpc_url,
        contract_address = contract_address,
        start_block = start_block,
        reorg_safe_distance = reorg_safe_distance,
    );
    std::fs::write(dir.join("rindexer.yaml"), yaml).expect("write yaml");
}

struct TestEnv {
    pg_port: u16,
    rpc_url: String,
    http: HttpClient,
    deployer: Address,
    _pg_container: testcontainers::ContainerAsync<Postgres>,
    _anvil_container: testcontainers::ContainerAsync<GenericImage>,
}

/// Tripped when `TestEnv::new` sets `DATABASE_URL`. Constructing a second
/// `TestEnv` in the same process panics — see the module-level "Isolation
/// invariant" docs for rationale.
static TEST_ENV_ALREADY_INITIALIZED: AtomicBool = AtomicBool::new(false);

impl TestEnv {
    async fn new() -> Self {
        assert!(
            !TEST_ENV_ALREADY_INITIALIZED.swap(true, Ordering::SeqCst),
            "TestEnv::new called more than once in the same process — this test \
             mutates process-global state (DATABASE_URL, shutdown flag) and MUST \
             run under `cargo nextest run` so each test gets its own process. \
             See the `// Isolation invariant` module docs."
        );

        let _ = rustls::crypto::ring::default_provider().install_default();

        let pg_container =
            Postgres::default().start().await.expect("failed to start postgres container");
        let pg_port =
            pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");

        // `--no-mining`: Anvil will not auto-mine. Every block advance
        // comes from an explicit `anvil_mine` call. We need this because
        // the duplicate we're chasing depends on precisely controlled block
        // numbers — phase 1 historical must end with `last_synced` pointing
        // at an event-bearing block, the chain tip must then advance by a
        // few more event-bearing blocks before phase 2 begins, and phase 2
        // historical must run a short mini-pass. Auto-mining makes those
        // boundaries non-deterministic.
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

/// Count rows and inspect for duplicate (tx_hash, log_index) pairs. The
/// indexer schema has no natural UNIQUE constraint on these columns (the PK
/// is `rindexer_id SERIAL`), so duplicates surface as extra rows.
async fn query_ping_duplicates(
    pg: &tokio_postgres::Client,
    schema: &str,
) -> (usize, Vec<(i64, String, String)>) {
    let table = format!("{}.ping", schema);
    let total_sql = format!("SELECT COUNT(*)::bigint FROM {}", table);
    let total: i64 = pg
        .query_one(&total_sql, &[])
        .await
        .unwrap_or_else(|e| panic!("count {} failed: {}", total_sql, e))
        .get(0);

    let dup_sql = format!(
        "SELECT block_number::bigint, tx_hash, log_index::text, COUNT(*)::bigint \
         FROM {} \
         GROUP BY block_number, tx_hash, log_index \
         HAVING COUNT(*) > 1 \
         ORDER BY block_number",
        table
    );
    let rows = pg.query(&dup_sql, &[]).await.unwrap_or_else(|e| panic!("dup query failed: {}", e));
    let dups: Vec<(i64, String, String)> = rows
        .into_iter()
        .map(|r| {
            let bn: i64 = r.get(0);
            let tx: String = r.get(1);
            let li: String = r.get(2);
            (bn, tx.trim().to_string(), li)
        })
        .collect();
    (total as usize, dups)
}

/// Wait until at least `expected` rows are visible in the Ping table, or
/// `timeout` expires. Poll interval is 200ms. Returns the row count when
/// reached.
async fn wait_for_ping_rows(
    env: &TestEnv,
    schema: &str,
    expected: usize,
    timeout: Duration,
) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    let pg = env.pg_client().await;
    let sql = format!("SELECT rindexer_id FROM {}.ping", schema);
    let mut last = 0usize;
    loop {
        match pg.query(&sql, &[]).await {
            Ok(rows) => {
                last = rows.len();
                if last >= expected {
                    return last;
                }
            }
            Err(_) => {
                // Table not created yet — rindexer still bootstrapping.
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timeout waiting for {} rows in {}.ping — got {}", expected, schema, last);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Run one repro iteration and return `Err((count, duplicates))` on bug-hit.
///
/// Scenario (mirrors `test_reorg_with_finalized_delivery` phase 1, the part
/// that was double-indexing pre-reorg):
/// 1. Fire `n_events` Ping events, each mined in its own block, BEFORE
///    starting the indexer.
/// 2. Start rindexer in live mode with `reorg_safe_distance = safe_distance`.
///    Historical processes `0..tip-safe`, live picks up the rest.
/// 3. Mine extra blocks so live has something to bury and advance through.
/// 4. Wait for all `n_events` rows in PG, then assert no duplicate
///    (tx_hash, log_index) pairs.
///
/// The `indexer_name` MUST be unique per call — it maps to a PG schema, and
/// reusing names across iterations leaks state.
async fn run_one_iteration(
    env: &TestEnv,
    iteration: usize,
    n_events: u64,
    safe_distance: u64,
) -> Result<(), (usize, Vec<(i64, String, String)>)> {
    let (contract_addr, _) = deploy_ping_pong(&env.http, &env.rpc_url, env.deployer).await;
    let deploy_block = get_block_number(&env.http, &env.rpc_url).await;

    // Fire n_events pings, one per block, all before the indexer starts.
    // With `--no-mining`, `send_ping` mines exactly one block per call, so
    // each event lands in a distinct, contiguous block from
    // `deploy_block+1..=deploy_block+n_events`. Tip == deploy_block+n_events
    // at this point.
    //
    // The driver below mines additional blocks AFTER rindexer starts so the
    // chain tip advances between phase 1 and phase 2. That's the window
    // where the historical→live handoff hands a stale `current_filter` to
    // the live loop.
    let mut expected_pings: Vec<(u64, u64)> = Vec::new();
    for id in 1..=n_events {
        let blk = send_ping(&env.http, &env.rpc_url, env.deployer, contract_addr, id).await;
        expected_pings.push((id, blk));
    }

    // Keep the indexer name simple so rindexer's schema-name generator
    // produces a predictable `<indexer>_ping_pong` snake_case result.
    let indexer_name = format!("hist_to_live_dup_{:03}", iteration);
    let schema = format!("{}_ping_pong", indexer_name);
    let tmp = tempfile::tempdir().expect("tempdir");
    write_manifest(
        tmp.path(),
        &indexer_name,
        &env.rpc_url,
        contract_addr,
        deploy_block + 1,
        safe_distance,
    );
    let manifest_path: PathBuf = tmp.path().join("rindexer.yaml");

    // Run rindexer concurrently with a driver that mines a few extra blocks
    // to keep live's safe-distance threshold advancing, then waits for all
    // rows and shuts down.
    let rindexer_fut = rindexer::start_rindexer_no_code(StartNoCodeDetails {
        manifest_path: &manifest_path,
        indexing_details: IndexerNoCodeDetails { enabled: true },
        graphql_details: GraphqlOverrideSettings { enabled: false, override_port: None },
        watch: false,
    });

    let driver = {
        let schema = schema.clone();
        async move {
            // The phase 1 → phase 2 handoff is the window of interest. Phase
            // 1 captures `current_tip - safe_distance`. If the tip stays
            // pinned at `deploy_block + n_events` through phase 1, phase 1
            // captures all events (blocks are below safe boundary) and the
            // bug's code path is never exercised — phase 2 starts with no
            // historical work.
            //
            // To force phase 2 into its historical sub-phase with
            // event-bearing blocks, we need the tip to advance BEYOND the
            // last event block BETWEEN phase 1 and phase 2. The handoff gap
            // was ~165ms in the observed failing e2e run. Mine aggressively
            // with no initial delay and no inter-tick sleep so the tip
            // races ahead of rindexer's phase 1 completion.
            //
            // We mine `n_events + safe_distance + 5` blocks:
            //   - `n_events` — advance past all event blocks so phase 2's
            //     snapshot includes them all in its historical range.
            //   - `safe_distance` — push the safe boundary above the last
            //     event so phase 2 historical actually fetches them.
            //   - `+5` extra — keep live's safe boundary advancing so any
            //     stragglers drain.
            for _ in 0..(n_events + safe_distance + 5) {
                mine_one(&env.http, &env.rpc_url).await;
            }

            // After the aggressive mining burst, continue mining slowly so
            // the live loop has fresh blocks to observe (and any
            // re-dispatch of an event block has time to land in PG).
            for _ in 0..20 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                mine_one(&env.http, &env.rpc_url).await;
            }

            wait_for_ping_rows(env, &schema, n_events as usize, Duration::from_secs(30)).await;
            // Settle: give any lingering re-dispatch a chance to surface
            // before we sample row counts.
            tokio::time::sleep(Duration::from_millis(500)).await;
            rindexer::initiate_shutdown().await;
        }
    };

    tokio::select! {
        res = rindexer_fut => {
            // rindexer should only exit after shutdown signal. An early return
            // is not itself a duplicate-row bug but is still a failure mode
            // worth surfacing — report it as zero rows to force test fail.
            panic!("iteration {}: rindexer exited before driver finished: {:?}", iteration, res);
        }
        () = driver => {}
    }

    let pg = env.pg_client().await;
    let (total, dups) = query_ping_duplicates(&pg, &schema).await;
    if !dups.is_empty() || total != n_events as usize {
        return Err((total, dups));
    }
    Ok(())
}

/// Loop the historical→live repro. The original e2e harness hit duplicate
/// rows ~20-30% of the time; running N iterations here makes the bug visible
/// without relying on a single coin-flip. A single duplicate failure in any
/// iteration fails the whole test, and we report the iteration count + offending
/// rows so regressions are easy to triage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn historical_to_live_transition_produces_no_duplicates() {
    let env = TestEnv::new().await;

    // 12 iterations, 5 events each, safe_distance=2. Pre-fix hit rate was
    // ~25-33% per iteration in the full e2e harness, so 12 here gives ~99%
    // odds of seeing at least one duplicate if the bug recurs. Kept below 20
    // to stay within the 30-minute CI test budget.
    const ITERATIONS: usize = 12;
    const N_EVENTS: u64 = 5;
    const SAFE_DISTANCE: u64 = 2;

    #[allow(clippy::type_complexity)]
    let mut failures: Vec<(usize, usize, Vec<(i64, String, String)>)> = Vec::new();

    for i in 0..ITERATIONS {
        match run_one_iteration(&env, i, N_EVENTS, SAFE_DISTANCE).await {
            Ok(()) => {
                eprintln!("iter {}: OK", i);
            }
            Err((total, dups)) => {
                eprintln!(
                    "iter {}: FAIL total={} duplicates={:?}",
                    i,
                    total,
                    dups.iter().take(3).collect::<Vec<_>>()
                );
                failures.push((i, total, dups));
            }
        }
        // `initiate_shutdown` flips the global IS_RUNNING atomic which gates
        // every indexer loop's `is_running()` check. The flag is one-way in
        // production, but without resetting it the next iteration's rindexer
        // would see `is_running() == false` at startup and exit immediately.
        rindexer::_test_reset_shutdown_flag();
    }

    if !failures.is_empty() {
        panic!(
            "historical→live transition produced duplicates in {}/{} iterations: {:?}",
            failures.len(),
            ITERATIONS,
            failures
        );
    }
}
