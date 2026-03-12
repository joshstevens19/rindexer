//! Shared helpers for E2E test modules.

use anyhow::{Context, Result};

use crate::test_suite::TestContext;

/// Build the expected CSV output path for a given contract and event.
pub fn produced_csv_path_for(
    context: &TestContext,
    contract_name: &str,
    event_slug_lowercase: &str,
) -> String {
    let file_name = format!("{}-{}.csv", contract_name.to_lowercase(), event_slug_lowercase);
    let path = context.get_csv_output_path().join(contract_name).join(file_name);
    path.to_string_lossy().to_string()
}

/// Load tx_hash values from a CSV file into a sorted set.
pub fn load_tx_hashes_from_csv(path: &str) -> Result<std::collections::BTreeSet<String>> {
    use std::io::Read;
    let mut file =
        std::fs::File::open(path).with_context(|| format!("Cannot open CSV at {}", path))?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| anyhow::anyhow!("CSV missing header"))?;
    let headers: Vec<&str> = header.split(',').collect();
    let tx_idx = headers
        .iter()
        .position(|h| *h == "tx_hash")
        .ok_or_else(|| anyhow::anyhow!("tx_hash column not found"))?;

    let mut set = std::collections::BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= tx_idx {
            continue;
        }
        let tx = cols[tx_idx].trim().to_lowercase();
        if !tx.is_empty() {
            set.insert(tx);
        }
    }
    Ok(set)
}

/// Derive (start_block, end_block) from a CSV file's block_number column.
pub fn derive_block_range_from_csv(path: &str) -> Result<(u64, u64)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open expected CSV at {}", path))?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| anyhow::anyhow!("CSV missing header"))?;
    let headers: Vec<&str> = header.split(',').collect();
    let block_idx = headers
        .iter()
        .position(|h| *h == "block_number")
        .ok_or_else(|| anyhow::anyhow!("block_number column not found"))?;

    let mut min_b: Option<u64> = None;
    let mut max_b: Option<u64> = None;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= block_idx {
            continue;
        }
        if let Ok(b) = cols[block_idx].parse::<u64>() {
            min_b = Some(min_b.map_or(b, |m| m.min(b)));
            max_b = Some(max_b.map_or(b, |m| m.max(b)));
        }
    }
    match (min_b, max_b) {
        (Some(s), Some(e)) => Ok((s, e)),
        _ => Err(anyhow::anyhow!("Could not derive block range from CSV")),
    }
}

/// Copy ABI files from the e2e-tests crate's `abis/` directory to a project path.
pub fn copy_abis_to_project(project_path: &std::path::Path) -> Result<()> {
    let abis_src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("abis");
    let abis_dst = project_path.join("abis");
    std::fs::create_dir_all(&abis_dst).context("Failed to create abis directory")?;

    if let Ok(entries) = std::fs::read_dir(&abis_src) {
        for entry in entries.flatten() {
            if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                if let Some(name) = entry.path().file_name() {
                    let _ = std::fs::copy(entry.path(), abis_dst.join(name));
                }
            }
        }
    }
    Ok(())
}

/// Generate a deterministic test address from a counter.
/// Uses the same algorithm as `LiveFeeder::generate_test_address`.
pub fn generate_test_address(counter: u64) -> ethers::types::Address {
    let mut bytes = [0u8; 20];
    bytes[0] = 0x42;
    bytes[1..8].copy_from_slice(&counter.to_be_bytes()[..7]);
    ethers::types::Address::from(bytes)
}
