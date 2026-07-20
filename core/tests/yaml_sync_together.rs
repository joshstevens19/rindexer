//! Validation tests for `sync_together` manifest rules.
//!
//! Each test writes a manifest + ABI into a temp dir and runs the full
//! `read_manifest` pipeline (parse + validate), asserting the specific
//! `ValidateManifestError` variant.

use std::fs;
use std::path::PathBuf;

use rindexer::manifest::yaml::{read_manifest, ReadManifestError, ValidateManifestError};

const ERC20_ABI: &str = r#"[
  {"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},
  {"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}
]"#;

struct Project {
    _dir: tempfile::TempDir,
    manifest_path: PathBuf,
}

fn write_project(manifest_yaml: &str) -> Project {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join("erc20.json"), ERC20_ABI).expect("write abi");
    let manifest_path = dir.path().join("rindexer.yaml");
    fs::write(&manifest_path, manifest_yaml).expect("write manifest");
    Project { _dir: dir, manifest_path }
}

#[allow(clippy::result_large_err)]
fn validate(manifest_yaml: &str) -> Result<(), ValidateManifestError> {
    let project = write_project(manifest_yaml);
    match read_manifest(&project.manifest_path) {
        Ok(_) => Ok(()),
        Err(ReadManifestError::CouldNotValidateManifest(e)) => Err(e),
        Err(other) => panic!("unexpected non-validation error: {other}"),
    }
}

fn base_manifest(project_type: &str, storage: &str, contracts: &str, extra: &str) -> String {
    format!(
        r#"
name: sync_together_test
project_type: {project_type}

storage:
{storage}

networks:
  - name: ethereum
    chain_id: 1
    rpc: https://example.com
  - name: arbitrum
    chain_id: 42161
    rpc: https://example.com

contracts:
{contracts}

{extra}
"#
    )
}

const POSTGRES_STORAGE: &str = "  postgres:\n    enabled: true";

fn two_contracts(details_a: &str, details_b: &str, extra_a: &str, extra_b: &str) -> String {
    format!(
        r#"  - name: TokenA
    details:
{details_a}
    abi: ./erc20.json
    include_events:
      - Transfer
      - Approval
{extra_a}
  - name: TokenB
    details:
{details_b}
    abi: ./erc20.json
    include_events:
      - Transfer
{extra_b}"#
    )
}

const ETH_DETAIL: &str = "      - network: ethereum\n        address: \"0x1111111111111111111111111111111111111111\"\n        start_block: 100";
const ETH_DETAIL_B: &str = "      - network: ethereum\n        address: \"0x2222222222222222222222222222222222222222\"\n        start_block: 100";

const GROUP_A_TRANSFER_B_TRANSFER: &str = r#"sync_together:
  - group: pair
    contracts:
      - name: TokenA
        events:
          - Transfer
      - name: TokenB
        events:
          - Transfer"#;

#[test]
fn valid_explicit_group_passes() {
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(validate(&manifest).is_ok());
}

#[test]
fn valid_table_flag_passes_with_single_event() {
    let tables = r#"    tables:
      - name: balances
        sync_together: true
        columns:
          - name: holder
          - name: balance
            type: uint256
            default: "0"
        events:
          - event: Transfer
            operations:
              - type: upsert
                where:
                  holder: $to
                set:
                  - column: balance
                    action: add
                    value: $value"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, tables, ""),
        "",
    );
    assert!(validate(&manifest).is_ok());
}

#[test]
fn rust_project_rejected() {
    let manifest = base_manifest(
        "rust",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherRequiresNoCodeProjectType)
    ));
}

#[test]
fn missing_postgres_rejected() {
    let manifest = base_manifest(
        "no-code",
        "  csv:\n    enabled: true\n    path: ./csv",
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherRequiresPostgres)
    ));
}

#[test]
fn clickhouse_storage_rejected() {
    // Note: the Storage deserializer already rejects postgres+clickhouse dual
    // storage at parse time, so ClickHouse-only is the reachable case here.
    let manifest = base_manifest(
        "no-code",
        "  clickhouse:\n    enabled: true",
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherClickhouseNotSupported)
    ));
}

#[test]
fn duplicate_group_names_rejected() {
    let groups = r#"sync_together:
  - group: pair
    contracts:
      - name: TokenA
        events: [Transfer]
      - name: TokenB
        events: [Transfer]
  - group: pair
    contracts:
      - name: TokenA
        events: [Approval]
      - name: TokenB
        events: [Transfer]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherGroupNameMustBeUnique(name)) if name == "pair"
    ));
}

#[test]
fn single_event_explicit_group_rejected() {
    let groups = r#"sync_together:
  - group: solo
    contracts:
      - name: TokenA
        events: [Transfer]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherGroupTooSmall(name)) if name == "solo"
    ));
}

#[test]
fn event_in_two_groups_rejected() {
    let groups = r#"sync_together:
  - group: one
    contracts:
      - name: TokenA
        events: [Transfer]
      - name: TokenB
        events: [Transfer]
  - group: two
    contracts:
      - name: TokenA
        events: [Transfer, Approval]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherEventInMultipleGroups(..))
    ));
}

#[test]
fn table_flag_conflicting_with_explicit_group_rejected() {
    let tables = r#"    tables:
      - name: balances
        sync_together: true
        columns:
          - name: holder
          - name: balance
            type: uint256
            default: "0"
        events:
          - event: Transfer
            operations:
              - type: upsert
                where:
                  holder: $to
                set:
                  - column: balance
                    action: add
                    value: $value"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, tables, ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherTableFlagConflictsWithExplicitGroup(..))
    ));
}

#[test]
fn unknown_contract_rejected() {
    let groups = r#"sync_together:
  - group: ghost
    contracts:
      - name: TokenA
        events: [Transfer]
      - name: Nonexistent
        events: [Transfer]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherContractNotFound(name, _)) if name == "Nonexistent"
    ));
}

#[test]
fn missing_start_block_allowed() {
    // start_block is optional for grouped members: first boot starts at the
    // tip, restarts resume from the checkpoint (unlike ungrouped events,
    // which jump to the new tip and skip the downtime).
    let detail_no_start =
        "      - network: ethereum\n        address: \"0x1111111111111111111111111111111111111111\"";
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(detail_no_start, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(validate(&manifest).is_ok());
}

#[test]
fn end_block_rejected() {
    let detail_with_end = "      - network: ethereum\n        address: \"0x1111111111111111111111111111111111111111\"\n        start_block: 100\n        end_block: 200";
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(detail_with_end, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherEndBlockNotSupported(contract, _, _))
            if contract == "TokenA"
    ));
}

#[test]
fn network_set_mismatch_rejected() {
    let detail_b_two_networks = "      - network: ethereum\n        address: \"0x2222222222222222222222222222222222222222\"\n        start_block: 100\n      - network: arbitrum\n        address: \"0x2222222222222222222222222222222222222222\"\n        start_block: 100";
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, detail_b_two_networks, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherNetworkMismatch(..))
    ));
}

#[test]
fn reorg_distance_mismatch_rejected() {
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "    reorg_safe_distance: true", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherReorgDistanceMismatch(..))
    ));
}

#[test]
fn factory_child_allowed() {
    // Factory-DEPLOYED members (children, e.g. factory-created vaults) are
    // supported: discovery stays on the eager factory pipeline and the group
    // loop clamps its window to the factory's checkpoint.
    let factory_detail = r#"      - network: ethereum
        factory:
          name: PoolFactory
          address: "0x3333333333333333333333333333333333333333"
          event_name: Transfer
          input_name: to
          abi: ./erc20.json
        start_block: 100"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(factory_detail, ETH_DETAIL_B, "", ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(validate(&manifest).is_ok());
}

#[test]
fn factory_parent_rejected() {
    // TokenC is factory-deployed with TokenA (a group member) as its factory
    // source by address on a shared network.
    let contracts = format!(
        r#"{}
  - name: TokenC
    details:
      - network: ethereum
        factory:
          name: TokenAFactory
          address: "0x1111111111111111111111111111111111111111"
          event_name: Transfer
          input_name: to
          abi: ./erc20.json
        start_block: 100
    abi: ./erc20.json
    include_events:
      - Approval"#,
        two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", "")
    );
    let manifest =
        base_manifest("no-code", POSTGRES_STORAGE, &contracts, GROUP_A_TRANSFER_B_TRANSFER);
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherFactoryParentNotSupported(member, _, other))
            if member == "TokenA" && other == "TokenC"
    ));
}

#[test]
fn event_not_indexed_rejected() {
    // Approval is in TokenB's ABI but not in its include_events (and no tables).
    let groups = r#"sync_together:
  - group: pair
    contracts:
      - name: TokenA
        events: [Transfer]
      - name: TokenB
        events: [Approval]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherEventNotIndexed(contract, event, _))
            if contract == "TokenB" && event == "Approval"
    ));
}

#[test]
fn dependency_events_overlap_rejected() {
    let dep = r#"    dependency_events:
      events:
        - Transfer
      then:
        events:
          - Approval"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, dep, ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherEventInDependencyEvents(contract, event, _))
            if contract == "TokenA" && event == "Transfer"
    ));
}

#[test]
fn cron_table_on_grouped_contract_rejected() {
    let tables = r#"    tables:
      - name: snapshots
        columns:
          - name: total
            type: uint256
            default: "0"
        events:
          - event: Transfer
            operations:
              - type: upsert
                where:
                  total: "0"
                set:
                  - column: total
                    action: add
                    value: $value
        cron:
          - interval: 1h
            operations:
              - type: update
                where:
                  total: "0"
                set:
                  - column: total
                    action: set
                    value: "0""#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, tables, ""),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherCronTablesNotSupported(contract, table, _))
            if contract == "TokenA" && table == "snapshots"
    ));
}

#[test]
fn no_groups_means_no_sync_together_validation() {
    // ClickHouse-only project without sync_together must remain valid.
    let manifest = base_manifest(
        "no-code",
        "  clickhouse:\n    enabled: true",
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        "",
    );
    assert!(validate(&manifest).is_ok());
}

#[test]
fn native_transfers_table_flag_rejected() {
    // The table-level flag only desugars for contract tables; on a
    // native_transfers table it must be a hard error, not a silent no-op.
    let native_transfers = r#"native_transfers:
  enabled: true
  tables:
    - name: nt_totals
      sync_together: true
      columns:
        - name: holder
        - name: balance
          type: uint256
          default: "0"
      events:
        - event: NativeTransfer
          operations:
            - type: upsert
              where:
                holder: $to
              set:
                - column: balance
                  action: add
                  value: $value"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        native_transfers,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherNativeTransfersNotSupported(table))
            if table == "nt_totals"
    ));
}

#[test]
fn reserved_group_name_prefix_rejected() {
    // "table:" prefixed names are reserved for generated implicit groups; a
    // user group with that name would be misclassified by is_implicit().
    let groups = r#"sync_together:
  - group: "table:sneaky"
    contracts:
      - name: TokenA
        events: [Transfer]
      - name: TokenB
        events: [Transfer]"#;
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", ""),
        groups,
    );
    assert!(matches!(
        validate(&manifest),
        Err(ValidateManifestError::SyncTogetherGroupNameReserved(name, _))
            if name == "table:sneaky"
    ));
}

#[test]
fn omitted_and_false_reorg_distance_are_congruent() {
    // Omitted and `reorg_safe_distance: false` are documented as identical
    // (index at head, distance 0) — the congruence rule must not split them.
    let manifest = base_manifest(
        "no-code",
        POSTGRES_STORAGE,
        &two_contracts(ETH_DETAIL, ETH_DETAIL_B, "", "    reorg_safe_distance: false"),
        GROUP_A_TRANSFER_B_TRANSFER,
    );
    assert!(validate(&manifest).is_ok());
}
