use crate::manifest::core::Manifest;

/// Represents the action to take after diffing two manifests.
#[derive(Debug)]
pub enum ReloadAction {
    /// No meaningful changes detected.
    NoChange,

    /// Only config tuning changed (buffer, concurrency, etc). Can apply without restarting
    /// indexers.
    HotApply,

    /// Need to stop and restart affected indexing components.
    SelectiveRestart(RestartPlan),

    /// Change is too fundamental to hot-reload. Requires a full process restart.
    RequiresFullRestart(String),
}

/// Details about which components need to be restarted.
#[derive(Debug, Default)]
pub struct RestartPlan {
    /// New contracts that need to be started from scratch.
    pub contracts_to_add: Vec<String>,

    /// Contracts that have been removed and should be stopped.
    pub contracts_to_remove: Vec<String>,

    /// Existing contracts whose config changed (events, addresses, etc).
    pub contracts_to_restart: Vec<String>,

    /// Networks whose RPC URL changed (need provider reconnection).
    pub networks_to_reconnect: Vec<String>,

    /// Whether the storage config changed.
    pub storage_changed: bool,
}

impl RestartPlan {
    fn is_empty(&self) -> bool {
        self.contracts_to_add.is_empty()
            && self.contracts_to_remove.is_empty()
            && self.contracts_to_restart.is_empty()
            && self.networks_to_reconnect.is_empty()
            && !self.storage_changed
    }
}

/// Individual change detected between two manifests.
#[derive(Debug, Clone)]
pub enum ManifestChange {
    ProjectNameChanged,
    ProjectTypeChanged,
    ContractAdded(String),
    ContractRemoved(String),
    ContractModified(String),
    NetworkAdded(String),
    NetworkRemoved(String),
    NetworkRpcChanged(String),
    NetworkConfigChanged(String),
    ConfigChanged,
    StorageChanged,
    StreamsChanged(String),
    GraphqlChanged,
    NativeTransfersChanged,
    GlobalChanged,
}

/// Result of diffing two manifests.
#[derive(Debug)]
pub struct ManifestDiff {
    pub action: ReloadAction,
    pub changes: Vec<ManifestChange>,
}

/// Compare two manifests and produce a diff describing what changed and what action to take.
pub fn compute_diff(old: &Manifest, new: &Manifest) -> ManifestDiff {
    let mut changes = Vec::new();
    let mut plan = RestartPlan::default();
    let mut config_only = true;

    // Check for changes that require full restart
    if old.name != new.name {
        changes.push(ManifestChange::ProjectNameChanged);
        return ManifestDiff {
            action: ReloadAction::RequiresFullRestart(
                "Indexer name changed -- this affects DB schema naming. Restart required."
                    .to_string(),
            ),
            changes,
        };
    }

    if old.project_type != new.project_type {
        changes.push(ManifestChange::ProjectTypeChanged);
        return ManifestDiff {
            action: ReloadAction::RequiresFullRestart(
                "Project type changed (rust <-> no-code). Restart required.".to_string(),
            ),
            changes,
        };
    }

    // Compare networks
    diff_networks(old, new, &mut changes, &mut plan, &mut config_only);

    // Compare contracts
    diff_contracts(old, new, &mut changes, &mut plan, &mut config_only);

    // Compare config tuning
    diff_config(old, new, &mut changes);

    // Compare storage
    diff_storage(old, new, &mut changes, &mut plan, &mut config_only);

    // Compare GraphQL settings
    diff_graphql(old, new, &mut changes);

    // Compare native transfers
    diff_native_transfers(old, new, &mut changes, &mut plan, &mut config_only);

    // Compare global settings
    diff_global(old, new, &mut changes);

    // Determine the action
    if changes.is_empty() {
        return ManifestDiff { action: ReloadAction::NoChange, changes };
    }

    // If only config/graphql/global changes (no contract/network/storage changes)
    if config_only && plan.is_empty() {
        return ManifestDiff { action: ReloadAction::HotApply, changes };
    }

    ManifestDiff { action: ReloadAction::SelectiveRestart(plan), changes }
}

fn diff_networks(
    old: &Manifest,
    new: &Manifest,
    changes: &mut Vec<ManifestChange>,
    plan: &mut RestartPlan,
    config_only: &mut bool,
) {
    let old_networks: std::collections::HashMap<&str, &crate::manifest::network::Network> =
        old.networks.iter().map(|n| (n.name.as_str(), n)).collect();
    let new_networks: std::collections::HashMap<&str, &crate::manifest::network::Network> =
        new.networks.iter().map(|n| (n.name.as_str(), n)).collect();

    // Detect added networks
    for name in new_networks.keys() {
        if !old_networks.contains_key(name) {
            changes.push(ManifestChange::NetworkAdded(name.to_string()));
            *config_only = false;
        }
    }

    // Detect removed networks
    for name in old_networks.keys() {
        if !new_networks.contains_key(name) {
            changes.push(ManifestChange::NetworkRemoved(name.to_string()));
            *config_only = false;
        }
    }

    // Detect modified networks
    for (name, old_net) in &old_networks {
        if let Some(new_net) = new_networks.get(name) {
            if old_net.rpc != new_net.rpc {
                changes.push(ManifestChange::NetworkRpcChanged(name.to_string()));
                plan.networks_to_reconnect.push(name.to_string());
                *config_only = false;
            }

            // Check for other config changes (block_poll_frequency, compute_units, etc.)
            let other_changed = old_net.chain_id != new_net.chain_id
                || format!("{:?}", old_net.block_poll_frequency)
                    != format!("{:?}", new_net.block_poll_frequency)
                || old_net.compute_units_per_second != new_net.compute_units_per_second
                || format!("{:?}", old_net.max_block_range)
                    != format!("{:?}", new_net.max_block_range);

            if other_changed {
                changes.push(ManifestChange::NetworkConfigChanged(name.to_string()));
                *config_only = false;
            }
        }
    }
}

fn diff_contracts(
    old: &Manifest,
    new: &Manifest,
    changes: &mut Vec<ManifestChange>,
    plan: &mut RestartPlan,
    config_only: &mut bool,
) {
    let old_contracts: std::collections::HashMap<&str, &crate::manifest::contract::Contract> =
        old.contracts.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_contracts: std::collections::HashMap<&str, &crate::manifest::contract::Contract> =
        new.contracts.iter().map(|c| (c.name.as_str(), c)).collect();

    // Detect added contracts
    for name in new_contracts.keys() {
        if !old_contracts.contains_key(name) {
            changes.push(ManifestChange::ContractAdded(name.to_string()));
            plan.contracts_to_add.push(name.to_string());
            *config_only = false;
        }
    }

    // Detect removed contracts
    for name in old_contracts.keys() {
        if !new_contracts.contains_key(name) {
            changes.push(ManifestChange::ContractRemoved(name.to_string()));
            plan.contracts_to_remove.push(name.to_string());
            *config_only = false;
        }
    }

    // Detect modified contracts using serialized YAML comparison.
    // This avoids requiring PartialEq on deeply nested types (Contract, ContractDetails,
    // FactoryDetailsYaml, StreamsConfig, etc.) while still catching all changes.
    for (name, old_contract) in &old_contracts {
        if let Some(new_contract) = new_contracts.get(name) {
            let old_yaml = serde_yaml::to_string(old_contract).unwrap_or_default();
            let new_yaml = serde_yaml::to_string(new_contract).unwrap_or_default();

            if old_yaml != new_yaml {
                changes.push(ManifestChange::ContractModified(name.to_string()));
                plan.contracts_to_restart.push(name.to_string());
                *config_only = false;
            }
        }
    }
}

fn diff_config(old: &Manifest, new: &Manifest, changes: &mut Vec<ManifestChange>) {
    let old_yaml = serde_yaml::to_string(&old.config).unwrap_or_default();
    let new_yaml = serde_yaml::to_string(&new.config).unwrap_or_default();

    if old_yaml != new_yaml {
        changes.push(ManifestChange::ConfigChanged);
    }
}

fn diff_storage(
    old: &Manifest,
    new: &Manifest,
    changes: &mut Vec<ManifestChange>,
    plan: &mut RestartPlan,
    config_only: &mut bool,
) {
    let old_yaml = serde_yaml::to_string(&old.storage).unwrap_or_default();
    let new_yaml = serde_yaml::to_string(&new.storage).unwrap_or_default();

    if old_yaml != new_yaml {
        changes.push(ManifestChange::StorageChanged);
        plan.storage_changed = true;
        *config_only = false;
    }
}

fn diff_graphql(old: &Manifest, new: &Manifest, changes: &mut Vec<ManifestChange>) {
    let old_yaml = serde_yaml::to_string(&old.graphql).unwrap_or_default();
    let new_yaml = serde_yaml::to_string(&new.graphql).unwrap_or_default();

    if old_yaml != new_yaml {
        changes.push(ManifestChange::GraphqlChanged);
    }
}

fn diff_native_transfers(
    old: &Manifest,
    new: &Manifest,
    changes: &mut Vec<ManifestChange>,
    plan: &mut RestartPlan,
    config_only: &mut bool,
) {
    let old_yaml = serde_yaml::to_string(&old.native_transfers).unwrap_or_default();
    let new_yaml = serde_yaml::to_string(&new.native_transfers).unwrap_or_default();

    if old_yaml != new_yaml {
        changes.push(ManifestChange::NativeTransfersChanged);
        *config_only = false;
    }
}

fn diff_global(old: &Manifest, new: &Manifest, changes: &mut Vec<ManifestChange>) {
    let old_yaml = serde_yaml::to_string(&old.global).unwrap_or_default();
    let new_yaml = serde_yaml::to_string(&new.global).unwrap_or_default();

    if old_yaml != new_yaml {
        changes.push(ManifestChange::GlobalChanged);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_from_yaml(yaml: &str) -> Manifest {
        serde_yaml::from_str(yaml).expect("Failed to parse test YAML")
    }

    const BASE_MANIFEST: &str = r#"
name: test-indexer
project_type: no-code
networks:
  - name: ethereum
    chain_id: 1
    rpc: https://eth.rpc.example.com
contracts:
  - name: USDC
    details:
      - network: ethereum
        address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        start_block: "1000000"
    abi: ./abis/erc20.json
storage:
  postgres:
    enabled: true
"#;

    #[test]
    fn test_no_change() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new = manifest_from_yaml(BASE_MANIFEST);
        let diff = compute_diff(&old, &new);

        assert!(diff.changes.is_empty());
        assert!(matches!(diff.action, ReloadAction::NoChange));
    }

    #[test]
    fn test_name_changed_requires_full_restart() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new_yaml = BASE_MANIFEST.replace("name: test-indexer", "name: renamed-indexer");
        let new = manifest_from_yaml(&new_yaml);
        let diff = compute_diff(&old, &new);

        assert!(matches!(diff.action, ReloadAction::RequiresFullRestart(_)));
        assert!(diff.changes.iter().any(|c| matches!(c, ManifestChange::ProjectNameChanged)));
    }

    #[test]
    fn test_contract_added() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new_yaml = format!(
            r#"{}
  - name: WETH
    details:
      - network: ethereum
        address: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        start_block: "2000000"
    abi: ./abis/erc20.json
"#,
            BASE_MANIFEST
        );
        let new = manifest_from_yaml(&new_yaml);
        let diff = compute_diff(&old, &new);

        assert!(matches!(diff.action, ReloadAction::SelectiveRestart(_)));
        assert!(diff.changes.iter().any(|c| matches!(c, ManifestChange::ContractAdded(name) if name == "WETH")));

        if let ReloadAction::SelectiveRestart(plan) = &diff.action {
            assert!(plan.contracts_to_add.contains(&"WETH".to_string()));
        }
    }

    #[test]
    fn test_contract_removed() {
        let old_yaml = format!(
            r#"{}
  - name: WETH
    details:
      - network: ethereum
        address: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        start_block: "2000000"
    abi: ./abis/erc20.json
"#,
            BASE_MANIFEST
        );
        let old = manifest_from_yaml(&old_yaml);
        let new = manifest_from_yaml(BASE_MANIFEST);
        let diff = compute_diff(&old, &new);

        assert!(matches!(diff.action, ReloadAction::SelectiveRestart(_)));
        assert!(diff.changes.iter().any(|c| matches!(c, ManifestChange::ContractRemoved(name) if name == "WETH")));

        if let ReloadAction::SelectiveRestart(plan) = &diff.action {
            assert!(plan.contracts_to_remove.contains(&"WETH".to_string()));
        }
    }

    #[test]
    fn test_network_rpc_changed() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new_yaml =
            BASE_MANIFEST.replace("https://eth.rpc.example.com", "https://new.rpc.example.com");
        let new = manifest_from_yaml(&new_yaml);
        let diff = compute_diff(&old, &new);

        assert!(matches!(diff.action, ReloadAction::SelectiveRestart(_)));
        assert!(diff.changes.iter().any(
            |c| matches!(c, ManifestChange::NetworkRpcChanged(name) if name == "ethereum")
        ));

        if let ReloadAction::SelectiveRestart(plan) = &diff.action {
            assert!(plan.networks_to_reconnect.contains(&"ethereum".to_string()));
        }
    }

    #[test]
    fn test_config_only_change_is_hot_apply() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new_yaml = format!(
            r#"{}
config:
  buffer: 100
  callback_concurrency: 4
"#,
            // Rebuild without the default empty config
            r#"
name: test-indexer
project_type: no-code
networks:
  - name: ethereum
    chain_id: 1
    rpc: https://eth.rpc.example.com
contracts:
  - name: USDC
    details:
      - network: ethereum
        address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        start_block: "1000000"
    abi: ./abis/erc20.json
storage:
  postgres:
    enabled: true
"#
        );
        let new = manifest_from_yaml(&new_yaml);
        let diff = compute_diff(&old, &new);

        assert!(diff.changes.iter().any(|c| matches!(c, ManifestChange::ConfigChanged)));
        assert!(matches!(diff.action, ReloadAction::HotApply));
    }

    #[test]
    fn test_storage_changed() {
        let old = manifest_from_yaml(BASE_MANIFEST);
        let new_yaml = BASE_MANIFEST.replace(
            "storage:\n  postgres:\n    enabled: true",
            "storage:\n  csv:\n    enabled: true",
        );
        let new = manifest_from_yaml(&new_yaml);
        let diff = compute_diff(&old, &new);

        assert!(matches!(diff.action, ReloadAction::SelectiveRestart(_)));
        assert!(diff.changes.iter().any(|c| matches!(c, ManifestChange::StorageChanged)));

        if let ReloadAction::SelectiveRestart(plan) = &diff.action {
            assert!(plan.storage_changed);
        }
    }
}
