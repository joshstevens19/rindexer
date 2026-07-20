//! Live e2e for `sync_together` with FACTORY-DEPLOYED members (children).
//!
//! Uniswap V3 pools are factory-deployed children; `Swap` and `Mint` on the
//! pool contract feed a `sync_together: true` table. Discovery (PoolCreated)
//! runs on the ordinary eager factory pipeline; the group loop clamps its
//! window to the factory-discovery checkpoint so the pool address set is
//! complete for every block the group commits.
//!
//! Asserted:
//! - the clamp invariant at every poll sample: member checkpoints never pass
//!   the factory-discovery checkpoint,
//! - lockstep at the end: Swap and Mint checkpoints identical,
//! - count consistency: the aggregate table exactly matches raw rows,
//! - liveness: checkpoints advance even while few/no child events exist.
//!
//! `#[ignore]`d: needs docker + the public ethereum gateway. Run with:
//!   cargo test -p rindexer --test e2e_sync_together_factory -- --ignored --nocapture

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

fn uniswap_abis_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples/tables_factory_uniswap/abis")
}

async fn rpc_block_number(http: &reqwest::Client, rpc_url: &str) -> Option<u64> {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]});
    let resp = http.post(rpc_url).json(&body).send().await.ok()?;
    let json: Value = resp.json().await.ok()?;
    u64::from_str_radix(json.get("result")?.as_str()?.trim_start_matches("0x"), 16).ok()
}

const RPC: &str = "https://mainnet.gateway.tenderly.co";

fn write_manifest(dir: &std::path::Path, start_block: u64) {
    std::fs::create_dir_all(dir.join("abis")).unwrap();
    for abi in ["uniswap-v3-factory-abi.json", "uniswap-v3-pool-abi.json"] {
        std::fs::copy(uniswap_abis_dir().join(abi), dir.join("abis").join(abi)).unwrap();
    }

    let yaml = format!(
        r#"name: FactorySyncExample
description: "sync_together with factory-deployed children"
repository: "https://example.invalid"
project_type: no-code
networks:
  - name: ethereum
    chain_id: 1
    rpc: {RPC}
storage:
  postgres:
    enabled: true
    drop_each_run: true
native_transfers: false
contracts:
  - name: Pool
    details:
      - network: ethereum
        start_block: "{start_block}"
        factory:
          name: PoolFactory
          address: "0x1F98431c8aD98523631AE4a59f267346ea31F984"
          abi: ./abis/uniswap-v3-factory-abi.json
          event_name: PoolCreated
          input_name: "pool"
    abi: ./abis/uniswap-v3-pool-abi.json
    include_events:
      - Swap
      - Mint
    tables:
      - name: pool_activity
        sync_together: true
        columns:
          - name: pool_address
          - name: swaps
            type: uint64
            default: "0"
          - name: mints
            type: uint64
            default: "0"
        events:
          - event: Swap
            operations:
              - type: upsert
                where:
                  pool_address: $rindexer_contract_address
                set:
                  - column: swaps
                    action: increment
          - event: Mint
            operations:
              - type: upsert
                where:
                  pool_address: $rindexer_contract_address
                set:
                  - column: mints
                    action: increment
"#
    );
    std::fs::write(dir.join("rindexer.yaml"), yaml).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs docker + public RPC gateway; run manually"]
async fn factory_children_lockstep_is_clamped_by_discovery() {
    use testcontainers::runners::AsyncRunner;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();

    // Look back far enough that some pools were CREATED after start_block and
    // traded (only children discovered from start_block onward are indexed).
    let tip = rpc_block_number(&http, RPC).await.expect("reach ethereum gateway");
    let start_block = tip.saturating_sub(5000); // ~17h of mainnet

    let tmp = tempfile::tempdir().expect("tempdir");
    write_manifest(tmp.path(), start_block);
    let manifest_path = tmp.path().join("rindexer.yaml");

    let pg_container = testcontainers_modules::postgres::Postgres::default()
        .start()
        .await
        .expect("start postgres");
    let pg_port = pg_container.get_host_port_ipv4(5432).await.expect("pg port");
    // SAFETY: nextest / manual single-test run — one test per process.
    unsafe {
        std::env::set_var(
            "DATABASE_URL",
            format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"),
        );
    }

    let rindexer_fut = rindexer::start_rindexer_no_code(rindexer::StartNoCodeDetails {
        manifest_path: &manifest_path,
        indexing_details: rindexer::IndexerNoCodeDetails { enabled: true },
        graphql_details: rindexer::GraphqlOverrideSettings { enabled: false, override_port: None },
        watch: false,
    });

    let conn_str =
        format!("host=127.0.0.1 port={pg_port} user=postgres password=postgres dbname=postgres");
    let (pg, connection) =
        tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await.expect("pg connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // The factory-discovery event lives on a SYNTHESIZED contract whose name
    // is derived from the factory config — locate its checkpoint table rather
    // than hardcoding the derivation.
    let factory_cp_table_sql = "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'rindexer_internal' \
           AND table_name LIKE 'factory_sync_example%pool_created%' LIMIT 1";

    // One snapshot per sample: members' checkpoints, the factory checkpoint,
    // raw counts and aggregates.
    let sample_sql = |factory_cp_table: &str| {
        format!(
            "SELECT \
                (SELECT last_synced_block::bigint FROM rindexer_internal.factory_sync_example_pool_swap WHERE network = 'ethereum') AS swap_cp, \
                (SELECT last_synced_block::bigint FROM rindexer_internal.factory_sync_example_pool_mint WHERE network = 'ethereum') AS mint_cp, \
                (SELECT last_synced_block::bigint FROM rindexer_internal.{factory_cp_table} WHERE network = 'ethereum') AS factory_cp, \
                (SELECT COUNT(*)::bigint FROM factory_sync_example_pool.swap) AS raw_swaps, \
                (SELECT COUNT(*)::bigint FROM factory_sync_example_pool.mint) AS raw_mints, \
                (SELECT COALESCE(SUM(swaps), 0)::bigint FROM factory_sync_example_pool.pool_activity) AS agg_swaps, \
                (SELECT COALESCE(SUM(mints), 0)::bigint FROM factory_sync_example_pool.pool_activity) AS agg_mints"
        )
    };

    let violations: Arc<tokio::sync::Mutex<Vec<String>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let sample_sql_shared: Arc<tokio::sync::Mutex<Option<String>>> =
        Arc::new(tokio::sync::Mutex::new(None));

    // Everything below runs INSIDE the driver: `rindexer_fut` is lazy and only
    // makes progress once the select! polls it, so any waiting must happen
    // concurrently with it.
    let driver = {
        let violations = Arc::clone(&violations);
        let stop = Arc::clone(&stop);
        let sample_sql_shared = Arc::clone(&sample_sql_shared);
        let conn_str = conn_str.clone();
        async move {
            // Wait for the synthesized factory-discovery checkpoint table.
            let factory_cp_table = {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
                loop {
                    if let Ok(Some(row)) = pg.query_opt(factory_cp_table_sql, &[]).await {
                        break row.get::<_, String>("table_name");
                    }
                    if tokio::time::Instant::now() > deadline {
                        panic!("factory checkpoint table never appeared");
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            };
            eprintln!("factory checkpoint table: {factory_cp_table}");
            let sample_sql = sample_sql(&factory_cp_table);
            *sample_sql_shared.lock().await = Some(sample_sql.clone());

            // Clamp-invariant poller: member checkpoints must never pass the
            // factory-discovery checkpoint (holds through backfill,
            // fast-forward, and lockstep).
            {
                let violations = Arc::clone(&violations);
                let stop = Arc::clone(&stop);
                let sample_sql = sample_sql.clone();
                let (pg2, connection2) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                    .await
                    .expect("pg connect 2");
                tokio::spawn(async move {
                    let _conn = tokio::spawn(async move {
                        let _ = connection2.await;
                    });
                    while !stop.load(Ordering::SeqCst) {
                        if let Ok(row) = pg2.query_one(&sample_sql, &[]).await {
                            let swap_cp: i64 = row.get("swap_cp");
                            let mint_cp: i64 = row.get("mint_cp");
                            let factory_cp: i64 = row.get("factory_cp");
                            if swap_cp > factory_cp || mint_cp > factory_cp {
                                violations.lock().await.push(format!(
                                    "clamp violated: swap_cp={swap_cp} mint_cp={mint_cp} factory_cp={factory_cp}"
                                ));
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }

            // Wait for the factory to advance and the members to enter
            // lockstep (checkpoints equal, above start, and advancing), then
            // settle and stop.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(420);
            let mut last: Option<(i64, i64)> = None;
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if let Ok(row) = pg.query_one(&sample_sql, &[]).await {
                    let swap_cp: i64 = row.get("swap_cp");
                    let mint_cp: i64 = row.get("mint_cp");
                    let factory_cp: i64 = row.get("factory_cp");
                    let raw_swaps: i64 = row.get("raw_swaps");
                    eprintln!(
                        "swap_cp={swap_cp} mint_cp={mint_cp} factory_cp={factory_cp} raw_swaps={raw_swaps}"
                    );
                    if swap_cp == mint_cp && swap_cp > start_block as i64 {
                        if let Some((prev_swap, _)) = last {
                            if swap_cp > prev_swap {
                                break;
                            }
                        }
                        last = Some((swap_cp, mint_cp));
                    }
                }
                if tokio::time::Instant::now() > deadline {
                    panic!("timed out waiting for lockstep to advance past start block");
                }
            }

            tokio::time::sleep(Duration::from_secs(20)).await;
            stop.store(true, Ordering::SeqCst);
            rindexer::initiate_shutdown().await;
        }
    };

    tokio::select! {
        res = rindexer_fut => panic!("rindexer exited early: {res:?}"),
        () = driver => {}
    }

    let violations = violations.lock().await;
    assert!(violations.is_empty(), "clamp violations: {violations:?}");

    let sample_sql =
        sample_sql_shared.lock().await.clone().expect("driver populated the sample sql");
    let (pg_final, connection_final) =
        tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await.expect("pg connect final");
    tokio::spawn(async move {
        let _ = connection_final.await;
    });
    let row = pg_final.query_one(&sample_sql, &[]).await.expect("final sample");
    let swap_cp: i64 = row.get("swap_cp");
    let mint_cp: i64 = row.get("mint_cp");
    let factory_cp: i64 = row.get("factory_cp");
    let raw_swaps: i64 = row.get("raw_swaps");
    let raw_mints: i64 = row.get("raw_mints");
    let agg_swaps: i64 = row.get("agg_swaps");
    let agg_mints: i64 = row.get("agg_mints");
    eprintln!(
        "final: swap_cp={swap_cp} mint_cp={mint_cp} factory_cp={factory_cp} \
         raw_swaps={raw_swaps} agg_swaps={agg_swaps} raw_mints={raw_mints} agg_mints={agg_mints}"
    );

    assert_eq!(swap_cp, mint_cp, "member checkpoints diverged");
    assert!(swap_cp <= factory_cp, "member checkpoint passed the factory checkpoint");
    assert!(factory_cp > start_block as i64, "factory discovery never advanced");
    assert!(swap_cp > start_block as i64, "group never advanced");
    assert_eq!(raw_swaps, agg_swaps, "swap aggregate diverged from raw rows");
    assert_eq!(raw_mints, agg_mints, "mint aggregate diverged from raw rows");
}
