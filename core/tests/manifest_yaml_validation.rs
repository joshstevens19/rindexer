use std::{env, fs, path::Path};

use rindexer::manifest::core::{Manifest, ProjectType};
use rindexer::manifest::yaml::{read_manifest, read_manifest_raw, write_manifest};

const ERC20_ABI: &str = r#"[
  {
    "type": "event",
    "name": "Transfer",
    "inputs": [
      { "indexed": true, "name": "from", "type": "address" },
      { "indexed": true, "name": "to", "type": "address" },
      { "name": "value", "type": "uint256" }
    ]
  },
  {
    "type": "event",
    "name": "Approval",
    "inputs": [
      { "indexed": true, "name": "owner", "type": "address" },
      { "indexed": true, "name": "spender", "type": "address" },
      { "name": "value", "type": "uint256" }
    ]
  }
]"#;

fn write_project_file(project: &Path, relative_path: &str, contents: &str) {
    let full_path = project.join(relative_path);
    fs::create_dir_all(full_path.parent().expect("file has a parent")).expect("parent dir");
    fs::write(full_path, contents).expect("file writes");
}

fn write_abi(project: &Path) {
    write_project_file(project, "abis/token.json", ERC20_ABI);
}

fn manifest_path(project: &Path) -> std::path::PathBuf {
    project.join("rindexer.yaml")
}

fn base_manifest(contract_details: &str, extra_contract_fields: &str) -> String {
    format!(
        r#"
name: coverage
project_type: no-code
networks:
  - name: mainnet
    chain_id: 1
    rpc: http://localhost:8545
contracts:
  - name: Token
    details:
{contract_details}
    abi: ./abis/token.json
{extra_contract_fields}
"#
    )
}

#[test]
fn read_manifest_raw_rejects_duplicate_contract_names_before_abi_validation() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let yaml = r#"
name: coverage
project_type: no-code
networks: []
contracts:
  - name: Token
    details: []
    abi: ./missing.json
  - name: Token
    details: []
    abi: ./missing.json
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest_raw(&path).expect_err("duplicate names should fail");

    assert!(err.to_string().contains("Contract names Token must be unique"));
}

#[test]
fn read_manifest_rejects_contract_detail_for_unknown_network() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: optimism
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1""#,
        "    include_events:\n      - Transfer\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("unknown network should fail");

    let err = err.to_string();
    assert!(
        err.contains("Invalid network mapped to contract: network - optimism contract - Token"),
        "{err}"
    );
}

#[test]
fn read_manifest_rejects_missing_include_event() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: mainnet
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1""#,
        "    include_events:\n      - MissingEvent\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("missing include event should fail");

    let err = err.to_string();
    assert!(
        err.contains(
            "Event MissingEvent included in include_events for contract Token but not found in ABI"
        ),
        "{err}"
    );
}

#[test]
fn read_manifest_rejects_indexed_filter_that_defines_too_many_topics() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    write_abi(temp_dir.path());
    let yaml = base_manifest(
        r#"      - network: mainnet
        address: "0x0000000000000000000000000000000000000001"
        start_block: "1"
        indexed_filters:
          - event_name: Transfer
            indexed_1:
              - 0x0000000000000000000000000000000000000001
            indexed_2:
              - 0x0000000000000000000000000000000000000002
            indexed_3:
              - 0x0000000000000000000000000000000000000003"#,
        "    include_events:\n      - Transfer\n",
    );
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let err = read_manifest(&path).expect_err("too many indexed filters should fail");

    let err = err.to_string();
    assert!(
        err.contains(
            "Indexed filter defined more than allowed for event Transfer for contract Token"
        ),
        "{err}"
    );
}

#[test]
fn read_manifest_expands_simple_native_transfers_to_all_root_networks() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let yaml = r#"
name: coverage
project_type: no-code
native_transfers: true
networks:
  - name: mainnet
    chain_id: 1
    rpc: http://localhost:8545
  - name: base
    chain_id: 8453
    rpc: http://localhost:8546
contracts: []
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let manifest = read_manifest(&path).expect("manifest reads");
    let networks = manifest.native_transfers.networks.as_ref().expect("native transfer networks");

    assert_eq!(networks.len(), 2);
    assert_eq!(networks[0].network, "mainnet");
    assert_eq!(networks[1].network, "base");
    assert!(manifest.has_any_live_indexing());
}

#[test]
fn read_manifest_keeps_rust_project_network_rpc_as_env_placeholder() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    env::set_var("COVERAGE_RPC_URL", "http://localhost:8545");
    let yaml = r#"
name: coverage
project_type: rust
networks:
  - name: mainnet
    chain_id: 1
    rpc: ${COVERAGE_RPC_URL}
contracts: []
"#;
    let path = manifest_path(temp_dir.path());
    fs::write(&path, yaml).expect("manifest writes");

    let manifest = read_manifest(&path).expect("manifest reads");

    assert_eq!(manifest.project_type, ProjectType::Rust);
    assert_eq!(manifest.networks[0].rpc, "COVERAGE_RPC_URL");
}

#[test]
fn write_manifest_round_trips_manifest_to_yaml_file() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let manifest: Manifest = serde_yaml::from_str(
        r#"
name: coverage
project_type: no-code
networks: []
contracts: []
graphql:
  port: 4001
"#,
    )
    .expect("manifest parses");
    let path = manifest_path(temp_dir.path());

    write_manifest(&manifest, &path).expect("manifest writes");

    let written = fs::read_to_string(path).expect("manifest reads");
    assert!(written.contains("name: coverage"));
    assert!(written.contains("project_type: no-code"));
    assert!(written.contains("port: 4001"));
}
