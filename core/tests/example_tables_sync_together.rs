//! Guards for `examples/tables_sync_together`.
//!
//! `example_manifest_validates` runs everywhere (no docker/network): it keeps
//! the shipped example in sync with `sync_together` validation rules.
//!
//! `example_runs_live` is `#[ignore]`d: it needs docker + the public RPC
//! gateways. It copies the example to a temp dir, moves the start blocks to
//! just behind the live tips, runs the real indexer for a bit, and asserts
//! the invariant the example exists to demonstrate: the cross-chain
//! `total_balances` aggregate exactly matches the raw transfer rows.
//! Run manually with:
//!   cargo test -p rindexer --test example_tables_sync_together -- --ignored --nocapture

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};

fn example_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples/tables_sync_together")
}

#[test]
fn example_manifest_validates() {
    let manifest_path = example_dir().join("rindexer.yaml");
    let manifest = rindexer::manifest::yaml::read_manifest(&manifest_path)
        .expect("examples/tables_sync_together/rindexer.yaml must pass validation");

    // The demo's point: the table opts into lockstep.
    let table = &manifest.contracts[0].tables.as_ref().expect("tables")[0];
    assert!(table.sync_together, "example table must set sync_together: true");
    assert!(table.cross_chain, "example table must set cross_chain: true");
}

async fn rpc_block_number(http: &reqwest::Client, rpc_url: &str) -> Option<u64> {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]});
    let resp = http.post(rpc_url).json(&body).send().await.ok()?;
    let json: Value = resp.json().await.ok()?;
    u64::from_str_radix(json.get("result")?.as_str()?.trim_start_matches("0x"), 16).ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs docker + public RPC gateways; run manually"]
async fn example_runs_live() {
    use testcontainers::runners::AsyncRunner;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::new();

    // Copy the example into a temp dir so generated state never pollutes the
    // repo, and rewrite each start_block to just behind the live tip so the
    // backfill is seconds, not hours.
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    std::fs::create_dir_all(dir.join("abis")).unwrap();
    std::fs::copy(example_dir().join("abis/ERC20.abi.json"), dir.join("abis/ERC20.abi.json"))
        .unwrap();

    let mut yaml = std::fs::read_to_string(example_dir().join("rindexer.yaml")).unwrap();
    // (network, rpc, blocks_behind_tip)
    let networks = [
        ("ethereum", "https://mainnet.gateway.tenderly.co", 10u64),
        ("arbitrum", "https://arbitrum.gateway.tenderly.co", 200u64),
        ("optimism", "https://optimism.gateway.tenderly.co", 60u64),
    ];
    let manifest: Value =
        serde_yaml::from_str::<serde_yaml::Value>(&yaml).map(|v| json!(v)).unwrap();
    let details = manifest["contracts"][0]["details"].as_array().unwrap().clone();
    for (network, rpc, behind) in networks {
        let tip =
            rpc_block_number(&http, rpc).await.unwrap_or_else(|| panic!("could not reach {rpc}"));
        let old = details
            .iter()
            .find(|d| d["network"] == network)
            .and_then(|d| d["start_block"].as_str())
            .unwrap()
            .to_string();
        yaml = yaml.replace(&old, &(tip.saturating_sub(behind)).to_string());
    }
    std::fs::write(dir.join("rindexer.yaml"), &yaml).unwrap();

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

    let manifest_path = dir.join("rindexer.yaml");
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

    // Accounting invariant: SUM(all balances) must equal credits - debits
    // derived from the raw transfer rows (mints/burns via the zero address
    // excluded per the operation `if` conditions). This holds even when
    // starting mid-stream because a holder first seen as a SENDER starts at
    // -value (the upsert-subtract fresh-insert fix).
    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    let consistency_sql = format!(
        "SELECT \
            (SELECT COUNT(*)::bigint FROM sync_together_example_usdc.total_balances) AS holders, \
            (SELECT COUNT(DISTINCT network)::bigint FROM sync_together_example_usdc.transfer) AS networks, \
            (SELECT COUNT(*)::bigint FROM sync_together_example_usdc.transfer) AS transfers, \
            (SELECT COALESCE(SUM(balance), 0)::numeric FROM sync_together_example_usdc.total_balances) AS total, \
            (SELECT COALESCE(SUM(CASE WHEN \"to\" <> '{ZERO}' THEN value::numeric ELSE 0 END), 0) \
                  - COALESCE(SUM(CASE WHEN \"from\" <> '{ZERO}' THEN value::numeric ELSE 0 END), 0) \
             FROM sync_together_example_usdc.transfer) AS expected"
    );

    let driver = async {
        // Wait for live USDC transfers to land (backfill is tiny; USDC has
        // near-constant volume on all three networks).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if let Ok(row) = pg.query_one(&consistency_sql, &[]).await {
                let transfers: i64 = row.get("transfers");
                eprintln!("transfers so far: {transfers}");
                if transfers >= 50 {
                    break;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for USDC transfers to be indexed");
            }
        }

        // Let the lockstep loop run a while longer, then settle.
        tokio::time::sleep(Duration::from_secs(30)).await;
        rindexer::initiate_shutdown().await;
    };

    tokio::select! {
        res = rindexer_fut => panic!("rindexer exited early: {res:?}"),
        () = driver => {}
    }

    let row = pg.query_one(&consistency_sql, &[]).await.expect("consistency query");
    let holders: i64 = row.get("holders");
    let networks: i64 = row.get("networks");
    let transfers: i64 = row.get("transfers");
    let total: rust_decimal::Decimal = row.get("total");
    let expected: rust_decimal::Decimal = row.get("expected");
    eprintln!(
        "indexed {transfers} transfers from {networks} networks; {holders} holders; \
         SUM(balances) = {total}, credits - debits = {expected}"
    );
    assert!(transfers >= 50, "expected at least 50 transfers, got {transfers}");
    assert_eq!(networks, 3, "expected transfers from all three networks");
    assert!(holders > 0, "cross-chain totals table is empty");
    assert_eq!(total, expected, "cross-chain aggregate diverged from raw transfers");

    // Lockstep checkpoints advanced on every network.
    let checkpoints: i64 = pg
        .query_one(
            "SELECT COUNT(*)::bigint FROM rindexer_internal.sync_together_example_usdc_transfer WHERE last_synced_block > 0",
            &[],
        )
        .await
        .expect("checkpoint query")
        .get(0);
    assert_eq!(checkpoints, 3, "expected advanced checkpoints on all three networks");

    // No duplicate rows (per network) — the seam guarantee.
    let dups: i64 = pg
        .query_one(
            "SELECT COALESCE(SUM(c - 1), 0)::bigint FROM (SELECT COUNT(*) AS c FROM sync_together_example_usdc.transfer GROUP BY network, tx_hash, log_index) d WHERE c > 1",
            &[],
        )
        .await
        .expect("dup query")
        .get(0);
    assert_eq!(dups, 0, "duplicate transfer rows");
}
