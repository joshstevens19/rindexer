//! Shared helpers for E2E test modules.

use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::test_suite::TestContext;

// ---------------------------------------------------------------------------
// Typed CSV row
// ---------------------------------------------------------------------------

/// Parsed row from a rindexer-produced Transfer CSV.
///
/// NOTE: rindexer CSV currently outputs 8 columns (no tx_index/log_index).
/// The headers in abi.rs include tx_index/log_index but the data rows in
/// no_code.rs do not — this is a known discrepancy. We parse based on
/// actual data output (8 fields).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRow {
    pub contract_address: String,
    pub from: String,
    pub to: String,
    pub value: String,
    pub tx_hash: String,
    pub block_number: u64,
    pub block_hash: String,
    pub network: String,
}

/// Unique identity for a log event — tx_hash is unique per-tx but NOT per-log.
/// Without log_index in CSV, tx_hash is the best dedup key we have.
/// Multiple events per tx will share the same identity (limitation of current CSV output).
pub type EventIdentity = String; // tx_hash

impl TransferRow {
    pub fn identity(&self) -> EventIdentity {
        self.tx_hash.clone()
    }
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

/// Expected CSV column headers in order.
/// NOTE: rindexer abi.rs writes 10 headers (including tx_index, log_index)
/// but no_code.rs only writes 8 data fields. We validate against the actual
/// header row written to the file (10 columns) but parse data flexibly.
pub const EXPECTED_HEADERS: &[&str] = &[
    "contract_address",
    "from",
    "to",
    "value",
    "tx_hash",
    "block_number",
    "block_hash",
    "network",
    "tx_index",
    "log_index",
];

/// Parse a rindexer Transfer CSV file into typed rows.
/// Uses the `csv` crate for correct field handling (not `split(',')`).
///
/// NOTE: rindexer writes 10 CSV headers but only 8 data fields per row
/// (tx_index and log_index are missing from data). We use `flexible(true)`
/// to tolerate this mismatch.
pub fn parse_transfer_csv(path: &str) -> Result<(Vec<String>, Vec<TransferRow>)> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("Cannot open CSV at {}", path))?;

    let headers: Vec<String> = rdr.headers()?.iter().map(|h| h.to_string()).collect();

    let mut rows = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if record.len() < 8 {
            continue;
        }
        rows.push(TransferRow {
            contract_address: record[0].to_lowercase(),
            from: record[1].to_lowercase(),
            to: record[2].to_lowercase(),
            value: record[3].to_string(),
            tx_hash: record[4].to_lowercase(),
            block_number: record[5].parse::<u64>()?,
            block_hash: record[6].to_lowercase(),
            network: record[7].to_string(),
        });
    }

    Ok((headers, rows))
}

/// Collect all EventIdentity values from parsed rows.
pub fn event_identities(rows: &[TransferRow]) -> BTreeSet<EventIdentity> {
    rows.iter().map(|r| r.identity()).collect()
}

/// Group rows by block_number.
pub fn rows_by_block(rows: &[TransferRow]) -> BTreeMap<u64, Vec<&TransferRow>> {
    let mut map: BTreeMap<u64, Vec<&TransferRow>> = BTreeMap::new();
    for row in rows {
        map.entry(row.block_number).or_default().push(row);
    }
    map
}

// ---------------------------------------------------------------------------
// CSV structure validation
// ---------------------------------------------------------------------------

/// Validate CSV column headers match expected schema in order.
/// Accepts both 8-column (legacy) and 10-column (with tx_index/log_index) headers.
pub fn validate_csv_headers(headers: &[String]) -> Result<()> {
    // Must have at least the 8 core columns
    let core_headers = &EXPECTED_HEADERS[..8];
    if headers.len() < 8 {
        return Err(anyhow::anyhow!(
            "Expected at least 8 columns, got {}: {:?}",
            headers.len(),
            headers
        ));
    }
    for (i, (got, expected)) in headers.iter().zip(core_headers.iter()).enumerate() {
        if got != expected {
            return Err(anyhow::anyhow!(
                "Column {} mismatch: got '{}', expected '{}'",
                i,
                got,
                expected
            ));
        }
    }
    // If 10 columns, validate the extra two
    if headers.len() >= 10 {
        if headers[8] != "tx_index" {
            return Err(anyhow::anyhow!("Column 8 mismatch: got '{}', expected 'tx_index'", headers[8]));
        }
        if headers[9] != "log_index" {
            return Err(anyhow::anyhow!("Column 9 mismatch: got '{}', expected 'log_index'", headers[9]));
        }
    }
    Ok(())
}

/// Validate format of all fields in parsed rows.
pub fn validate_row_formats(rows: &[TransferRow]) -> Result<()> {
    let addr_re = regex::Regex::new(r"^0x[0-9a-f]{40}$").unwrap();
    let hash_re = regex::Regex::new(r"^0x[0-9a-f]{64}$").unwrap();

    for (i, row) in rows.iter().enumerate() {
        // Address fields
        if !addr_re.is_match(&row.contract_address) {
            return Err(anyhow::anyhow!(
                "Row {}: invalid contract_address format: '{}'",
                i,
                row.contract_address
            ));
        }
        if !addr_re.is_match(&row.from) {
            return Err(anyhow::anyhow!("Row {}: invalid from format: '{}'", i, row.from));
        }
        if !addr_re.is_match(&row.to) {
            return Err(anyhow::anyhow!("Row {}: invalid to format: '{}'", i, row.to));
        }

        // Hash fields
        if !hash_re.is_match(&row.tx_hash) {
            return Err(anyhow::anyhow!("Row {}: invalid tx_hash format: '{}'", i, row.tx_hash));
        }
        if !hash_re.is_match(&row.block_hash) {
            return Err(anyhow::anyhow!(
                "Row {}: invalid block_hash format: '{}'",
                i,
                row.block_hash
            ));
        }

        // Value should be numeric (decimal string)
        if row.value.is_empty() || !row.value.chars().all(|c| c.is_ascii_digit()) {
            return Err(anyhow::anyhow!(
                "Row {}: value should be numeric decimal string, got: '{}'",
                i,
                row.value
            ));
        }
    }
    Ok(())
}

/// Validate block_numbers are monotonically non-decreasing.
pub fn validate_ordering(rows: &[TransferRow]) -> Result<()> {
    for window in rows.windows(2) {
        let (a, b) = (&window[0], &window[1]);
        if b.block_number < a.block_number {
            return Err(anyhow::anyhow!(
                "Block number not monotonic: block {} followed by {}",
                a.block_number,
                b.block_number
            ));
        }
    }
    Ok(())
}

/// Validate that rows sharing the same block_number have the same block_hash.
pub fn validate_block_hash_consistency(rows: &[TransferRow]) -> Result<()> {
    let by_block = rows_by_block(rows);
    for (block_num, block_rows) in &by_block {
        let first_hash = &block_rows[0].block_hash;
        for row in block_rows.iter().skip(1) {
            if &row.block_hash != first_hash {
                return Err(anyhow::anyhow!(
                    "Block {} has inconsistent block_hash: '{}' vs '{}'",
                    block_num,
                    first_hash,
                    row.block_hash
                ));
            }
        }
    }
    Ok(())
}

/// Validate no duplicate EventIdentity values.
pub fn validate_no_duplicates(rows: &[TransferRow]) -> Result<()> {
    let identities = event_identities(rows);
    if identities.len() != rows.len() {
        return Err(anyhow::anyhow!(
            "Duplicate events detected: {} unique identities for {} rows",
            identities.len(),
            rows.len()
        ));
    }
    Ok(())
}

/// Run all structural validations on a parsed CSV.
pub fn validate_csv_structure(headers: &[String], rows: &[TransferRow]) -> Result<()> {
    validate_csv_headers(headers)?;
    validate_row_formats(rows)?;
    validate_ordering(rows)?;
    validate_block_hash_consistency(rows)?;
    validate_no_duplicates(rows)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy helpers (kept for backward compatibility)
// ---------------------------------------------------------------------------

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
pub fn load_tx_hashes_from_csv(path: &str) -> Result<BTreeSet<String>> {
    let (_headers, rows) = parse_transfer_csv(path)?;
    Ok(rows.iter().map(|r| r.tx_hash.clone()).collect())
}

/// Derive (start_block, end_block) from a CSV file's block_number column.
pub fn derive_block_range_from_csv(path: &str) -> Result<(u64, u64)> {
    let (_headers, rows) = parse_transfer_csv(path)?;
    if rows.is_empty() {
        return Err(anyhow::anyhow!("Could not derive block range from CSV"));
    }
    let min = rows.iter().map(|r| r.block_number).min().unwrap();
    let max = rows.iter().map(|r| r.block_number).max().unwrap();
    Ok((min, max))
}

/// Copy ABI files from the e2e-tests crate's `abis/` directory to a project path.
pub fn copy_abis_to_project(project_path: &Path) -> Result<()> {
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
pub fn generate_test_address(counter: u64) -> ethers::types::Address {
    let mut bytes = [0u8; 20];
    bytes[0] = 0x42;
    bytes[1..8].copy_from_slice(&counter.to_be_bytes()[..7]);
    ethers::types::Address::from(bytes)
}

/// Format an ethers Address as a lowercase 0x-prefixed hex string.
pub fn format_address(addr: &ethers::types::Address) -> String {
    format!("0x{}", hex::encode(addr.as_bytes()))
}
