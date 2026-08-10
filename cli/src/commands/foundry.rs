use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env, fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};
use rindexer::{
    generator::generate_docker_file,
    manifest::{
        config::Config,
        contract::{Contract, ContractDetails, ContractEvent},
        core::{Manifest, ProjectType},
        global::Global,
        native_transfer::NativeTransfers,
        network::Network,
        storage::{PostgresDetails, Storage},
        yaml::{read_manifest_raw, write_manifest, YAML_CONFIG_NAME},
    },
    write_file, StringOrArray,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;

use crate::console::{print_error_message, print_success_message, print_warn_message};

const ORIGIN_CONFIG_NAME: &str = "rindexer-foundry.json";
const ORIGIN_VERSION: u32 = 1;
const COMMAND_OUTPUT_LIMIT: usize = 4_000;
const FORGE_BUILD_OUTPUT_LIMIT: usize = 1_200;

type CliResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone)]
struct CliError(String);

impl fmt::Debug for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CliError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FoundrySourceKind {
    Local,
    Git,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FoundrySourceSpec {
    kind: FoundrySourceKind,
    location: String,
    git_ref: Option<String>,
    subdir: Option<String>,
}

struct PreparedFoundrySource {
    spec: FoundrySourceSpec,
    root: PathBuf,
    last_commit: Option<String>,
    _temp_dir: Option<TempDir>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundryOrigin {
    version: u32,
    source: FoundryOriginSource,
    foundry: FoundryResolvedConfig,
    managed_contracts: Vec<FoundryManagedContract>,
    synced_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundryOriginSource {
    kind: String,
    location: String,
    #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
    git_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    subdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_commit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundryResolvedConfig {
    out: String,
    broadcast: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundryManagedContract {
    name: String,
    foundry_contract_name: String,
    artifact_path: String,
    abi_path: String,
    deployments: Vec<FoundryManagedDeployment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundryManagedDeployment {
    chain_id: u64,
    network: String,
    address: String,
    start_block: u64,
}

#[derive(Debug, Clone)]
struct FoundryConfig {
    out: PathBuf,
    broadcast: PathBuf,
    out_setting: String,
    broadcast_setting: String,
}

#[derive(Debug, Clone)]
struct RawDeployment {
    chain_id: u64,
    contract_name: String,
    contract_address: Address,
    start_block: u64,
    transaction_input: Option<String>,
}

#[derive(Debug, Clone)]
struct FoundryArtifact {
    relative_path: String,
    contract_name: Option<String>,
    source_path: Option<String>,
    abi_json: String,
    event_names: Vec<String>,
    bytecode: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum ArtifactSelection<'a> {
    Found(&'a FoundryArtifact),
    TestOnly,
    Missing,
}

#[derive(Debug, Clone)]
struct DesiredContract {
    key: String,
    name: String,
    foundry_contract_name: String,
    artifact_path: String,
    abi_path: String,
    abi_json: String,
    event_names: Vec<String>,
    deployments: Vec<DesiredDeployment>,
}

#[derive(Debug, Clone)]
struct DesiredDeployment {
    chain_id: u64,
    address: Address,
    start_block: u64,
}

#[derive(Debug)]
struct Discovery {
    config: FoundryConfig,
    contracts: Vec<DesiredContract>,
    skipped: Vec<SkippedDeployment>,
}

#[derive(Debug)]
struct BroadcastDiscovery {
    deployments: Vec<RawDeployment>,
    skipped: Vec<SkippedDeployment>,
    run_file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkippedDeployment {
    label: String,
    reason: SkipReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum SkipReason {
    MissingChainId,
    MissingContractName,
    MissingContractAddress,
    InvalidContractAddress,
    MissingReceiptBlock,
    MissingArtifact,
    TestArtifact,
    NoEvents,
}

#[derive(Debug, Default)]
struct SyncReport {
    added: Vec<String>,
    updated: Vec<String>,
    stale: Vec<String>,
    abi_updated: Vec<String>,
    assigned: HashMap<String, AssignedContractIdentity>,
}

#[derive(Debug, Clone)]
struct AssignedContractIdentity {
    name: String,
    abi_path: String,
}

#[derive(Debug, Clone, Copy)]
struct KnownChainDefault {
    name: &'static str,
}

impl SkippedDeployment {
    fn new(label: impl Into<String>, reason: SkipReason) -> Self {
        Self { label: label.into(), reason }
    }

    fn for_deployment(deployment: &RawDeployment, reason: SkipReason) -> Self {
        Self::new(
            format!(
                "{} at {} on chain {}",
                deployment.contract_name, deployment.contract_address, deployment.chain_id
            ),
            reason,
        )
    }

    fn message(&self) -> String {
        format!("{}: {}", self.label, self.reason.description())
    }
}

impl SkipReason {
    fn description(self) -> &'static str {
        match self {
            Self::MissingChainId => "could not resolve chain id",
            Self::MissingContractName => "missing contractName",
            Self::MissingContractAddress => "missing contractAddress",
            Self::InvalidContractAddress => "invalid contractAddress",
            Self::MissingReceiptBlock => "missing receipt blockNumber",
            Self::MissingArtifact => "no matching artifact found",
            Self::TestArtifact => "test deployment skipped",
            Self::NoEvents => "ABI has no events",
        }
    }

    fn summary_label(self) -> &'static str {
        match self {
            Self::MissingChainId => "missing chain id",
            Self::MissingContractName => "missing contract name",
            Self::MissingContractAddress => "missing deployment address",
            Self::InvalidContractAddress => "invalid deployment address",
            Self::MissingReceiptBlock => "missing receipt block number",
            Self::MissingArtifact => "missing artifact",
            Self::TestArtifact => "test deployment",
            Self::NoEvents => "ABI without events",
        }
    }

    fn hint(self) -> &'static str {
        match self {
            Self::MissingChainId => {
                "Keep broadcast files under `broadcast/<script>/<chain_id>/` or include the `chain` field in each run file."
            }
            Self::MissingContractName => {
                "Regenerate the Foundry broadcast with a recent Foundry release so CREATE/CREATE2 transactions include `contractName`."
            }
            Self::MissingContractAddress | Self::InvalidContractAddress => {
                "Run the deployment with `forge script --broadcast` so CREATE/CREATE2 transactions include usable contract addresses."
            }
            Self::MissingReceiptBlock => {
                "Commit real broadcast receipts from `forge script --broadcast`; dry-run or simulated broadcasts usually do not include usable receipt block numbers."
            }
            Self::MissingArtifact => {
                "Make sure the broadcast was produced from the same source tree, then run `forge build` so `out/**/<contractName>.json` exists."
            }
            Self::TestArtifact => {
                "rindexer skips deployments whose only matching artifacts come from `test/` source paths or contracts ending in `Test` to avoid importing fixtures."
            }
            Self::NoEvents => {
                "rindexer indexes contract events, so contracts whose ABIs contain no events are intentionally skipped."
            }
        }
    }
}

pub fn handle_new_foundry_command(
    current_dir: PathBuf,
    source: Option<String>,
    output: Option<String>,
    name: Option<String>,
) -> CliResult<()> {
    print_success_message("Importing Foundry project into a new rindexer project...");

    let spec = parse_foundry_source(source.as_deref(), &current_dir)?;
    let prepared = prepare_foundry_source(spec)?;
    let project_name = name.unwrap_or_else(|| derive_project_name(&prepared.spec, &prepared.root));
    let project_name = sanitize_project_name(&project_name);
    let output_path =
        resolve_new_output_path(output.as_deref(), &current_dir, &prepared, &project_name);

    if output_path.exists() {
        print_error_message(
            "Output directory already exists. Please choose a different output path.",
        );
        return Err(boxed_error("Output directory already exists."));
    }

    let discovery = discover_foundry_project(&prepared.root)?;
    if discovery.contracts.is_empty() {
        print_skipped_contracts(&discovery.skipped);
        return Err(boxed_error(empty_discovery_error(&discovery.skipped)));
    }

    fs::create_dir_all(&output_path)?;
    fs::create_dir_all(output_path.join("abis"))?;

    for contract in &discovery.contracts {
        write_file(&output_path.join(&contract.abi_path), &contract.abi_json)?;
    }

    let networks = build_default_networks(&discovery.contracts, None);
    let network_names_by_chain = chain_network_names(&networks);
    let contracts = discovery
        .contracts
        .iter()
        .map(|contract| contract_to_manifest(contract, &network_names_by_chain, None, None))
        .collect::<Vec<_>>();

    let manifest = Manifest {
        name: project_name.clone(),
        description: Some("Generated from a Foundry project.".to_string()),
        repository: repository_value(&prepared.spec),
        project_type: ProjectType::NoCode,
        config: Config::default(),
        constants: HashMap::new(),
        timestamps: None,
        networks,
        storage: default_postgres_storage(),
        native_transfers: NativeTransfers::default(),
        contracts,
        phantom: None,
        global: Global::default(),
        graphql: None,
    };

    write_manifest(&manifest, &output_path.join(YAML_CONFIG_NAME))?;
    write_project_support_files(&output_path, &project_name, &manifest.networks)?;

    let origin = build_origin(&prepared, &discovery, &output_path, &network_names_by_chain, None);
    write_origin(&output_path, &origin)?;

    print_skipped_contracts(&discovery.skipped);
    print_success_message(&format!(
        "Generated rindexer Foundry project at {} with {} managed contract(s).\n cd {}\n- run `rindexer start all`\n- run `rindexer foundry sync` after new Foundry broadcasts",
        output_path.display(),
        origin.managed_contracts.len(),
        output_path.display()
    ));

    Ok(())
}

pub async fn handle_foundry_sync_command(
    project_path: PathBuf,
    source: Option<String>,
    dry_run: bool,
) -> CliResult<()> {
    let origin_path = project_path.join(ORIGIN_CONFIG_NAME);
    if !origin_path.exists() {
        print_error_message(&format!(
            "{} does not exist. Run `rindexer foundry new` to create a Foundry-managed project first.",
            origin_path.display()
        ));
        return Err(boxed_error("Foundry origin file not found."));
    }

    let previous_origin = read_origin(&project_path)?;
    let spec = if let Some(source) = source {
        parse_foundry_source(Some(&source), &env::current_dir()?)?
    } else {
        source_spec_from_origin(&previous_origin.source, &project_path)?
    };

    let prepared = prepare_foundry_source(spec)?;
    let discovery = discover_foundry_project(&prepared.root)?;
    if discovery.contracts.is_empty() {
        print_skipped_contracts(&discovery.skipped);
        return Err(boxed_error(empty_discovery_error(&discovery.skipped)));
    }

    let mut manifest = read_manifest_raw(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could not read rindexer.yaml: {e}"));
        e
    })?;

    let report = sync_manifest_and_abis(
        &project_path,
        &mut manifest,
        &previous_origin,
        &prepared,
        &discovery,
        dry_run,
    )?;

    print_sync_report(&report, dry_run);
    print_skipped_contracts(&discovery.skipped);

    if dry_run {
        return Ok(());
    }

    write_manifest(&manifest, &project_path.join(YAML_CONFIG_NAME))?;
    ensure_env_network_values(&project_path, &manifest.networks)?;

    let network_names_by_chain = chain_network_names(&manifest.networks);
    let next_origin = build_origin(
        &prepared,
        &discovery,
        &project_path,
        &network_names_by_chain,
        Some(&report.assigned),
    );
    write_origin(&project_path, &next_origin)?;

    print_success_message("Foundry sync completed.");
    Ok(())
}

fn sync_manifest_and_abis(
    project_path: &Path,
    manifest: &mut Manifest,
    previous_origin: &FoundryOrigin,
    prepared: &PreparedFoundrySource,
    discovery: &Discovery,
    dry_run: bool,
) -> CliResult<SyncReport> {
    let mut report = SyncReport::default();
    let previous_by_key = previous_origin
        .managed_contracts
        .iter()
        .map(|contract| (managed_contract_key(contract), contract))
        .collect::<HashMap<_, _>>();

    let desired_keys =
        discovery.contracts.iter().map(|contract| contract.key.clone()).collect::<HashSet<_>>();

    for previous in &previous_origin.managed_contracts {
        let key = managed_contract_key(previous);
        if !desired_keys.contains(&key) {
            report.stale.push(previous.name.clone());
        }
    }

    ensure_networks_for_discovery(manifest, discovery);
    let network_names_by_chain = chain_network_names(&manifest.networks);
    let manifest_names = manifest.contracts.iter().map(|c| c.name.clone()).collect::<HashSet<_>>();
    let mut names_in_use = manifest_names.clone();

    for desired in &discovery.contracts {
        let previous = previous_by_key.get(&desired.key);
        let identity = previous.map_or_else(
            || {
                let name = unique_name(&desired.name, &names_in_use);
                AssignedContractIdentity { abi_path: format!("abis/{name}.abi.json"), name }
            },
            |contract| AssignedContractIdentity {
                name: contract.name.clone(),
                abi_path: contract.abi_path.clone(),
            },
        );
        names_in_use.insert(identity.name.clone());
        let previous_contract =
            manifest.contracts.iter().find(|contract| contract.name == identity.name);
        let existing_extensions = previous_contract.map(preserved_contract_extensions);
        let next_contract = contract_to_manifest(
            desired,
            &network_names_by_chain,
            existing_extensions,
            Some(&identity),
        );

        if let Some(index) =
            manifest.contracts.iter().position(|contract| contract.name == identity.name)
        {
            report.updated.push(identity.name.clone());
            if abi_changed(project_path, &identity.abi_path, &desired.abi_json) {
                report.abi_updated.push(identity.name.clone());
            }
            if !dry_run {
                manifest.contracts[index] = next_contract;
            }
        } else {
            report.added.push(identity.name.clone());
            if !dry_run {
                manifest.contracts.push(next_contract);
            }
        }

        if !dry_run {
            write_file(&project_path.join(&identity.abi_path), &desired.abi_json)?;
        }

        report.assigned.insert(desired.key.clone(), identity);
    }

    if source_was_overridden(prepared, &previous_origin.source, project_path) && !dry_run {
        print_success_message("Updated Foundry source origin for future syncs.");
    }

    // Only report stale contracts still present in rindexer.yaml; the user may have
    // already removed or renamed them by hand.
    report.stale.retain(|name| manifest_names.contains(name));

    Ok(report)
}

#[derive(Clone)]
struct ContractExtensions {
    index_event_in_order: Option<Vec<String>>,
    dependency_events: Option<rindexer::manifest::contract::DependencyEventTreeYaml>,
    reorg_safe_distance: Option<rindexer::manifest::contract::ReorgSafeDistance>,
    generate_csv: Option<bool>,
    streams: Option<rindexer::manifest::stream::StreamsConfig>,
    chat: Option<rindexer::manifest::chat::ChatConfig>,
    tables: Option<Vec<rindexer::manifest::contract::Table>>,
}

fn preserved_contract_extensions(contract: &Contract) -> ContractExtensions {
    ContractExtensions {
        index_event_in_order: contract.index_event_in_order.clone(),
        dependency_events: contract.dependency_events.clone(),
        reorg_safe_distance: contract.reorg_safe_distance,
        generate_csv: contract.generate_csv,
        streams: contract.streams.clone(),
        chat: contract.chat.clone(),
        tables: contract.tables.clone(),
    }
}

fn contract_to_manifest(
    desired: &DesiredContract,
    network_names_by_chain: &BTreeMap<u64, String>,
    extensions: Option<ContractExtensions>,
    identity: Option<&AssignedContractIdentity>,
) -> Contract {
    let mut grouped = BTreeMap::<u64, Vec<&DesiredDeployment>>::new();
    for deployment in &desired.deployments {
        grouped.entry(deployment.chain_id).or_default().push(deployment);
    }

    let details = grouped
        .into_iter()
        .map(|(chain_id, deployments)| {
            let addresses = deployments.iter().map(|d| d.address).collect::<Vec<_>>();
            let address = if addresses.len() == 1 {
                ValueOrArray::Value(addresses[0])
            } else {
                ValueOrArray::Array(addresses)
            };
            let start_block = deployments.iter().map(|d| d.start_block).min().unwrap_or_default();
            ContractDetails::new_with_address(
                network_names_by_chain
                    .get(&chain_id)
                    .cloned()
                    .unwrap_or_else(|| default_network_name(chain_id)),
                address,
                None,
                Some(U64::from(start_block)),
                None,
            )
        })
        .collect::<Vec<_>>();

    let extensions = extensions.unwrap_or_else(empty_contract_extensions);
    Contract {
        name: identity.map_or_else(|| desired.name.clone(), |identity| identity.name.clone()),
        details,
        abi: StringOrArray::Single(format!(
            "./{}",
            identity
                .map_or_else(|| desired.abi_path.as_str(), |identity| identity.abi_path.as_str())
        )),
        include_events: Some(
            desired
                .event_names
                .iter()
                .map(|name| ContractEvent { name: name.clone(), timestamps: None })
                .collect(),
        ),
        index_event_in_order: extensions.index_event_in_order,
        dependency_events: extensions.dependency_events,
        reorg_safe_distance: extensions.reorg_safe_distance,
        generate_csv: extensions.generate_csv,
        streams: extensions.streams,
        chat: extensions.chat,
        tables: extensions.tables,
    }
}

fn empty_contract_extensions() -> ContractExtensions {
    ContractExtensions {
        index_event_in_order: None,
        dependency_events: None,
        reorg_safe_distance: None,
        generate_csv: None,
        streams: None,
        chat: None,
        tables: None,
    }
}

fn discover_foundry_project(foundry_root: &Path) -> CliResult<Discovery> {
    let source_root = foundry_root;
    let resolved_root = resolve_foundry_project_root(source_root);
    if resolved_root.as_path() != source_root {
        print_success_message(&format!(
            "Resolved Foundry project root {} from source {}.",
            resolved_root.display(),
            source_root.display()
        ));
    }
    let foundry_root = resolved_root.as_path();

    print_success_message(&format!("Reading Foundry config in {}...", foundry_root.display()));
    let config = read_foundry_config(foundry_root)?;
    print_success_message("Building Foundry project with `forge build`...");
    run_forge_build(foundry_root)?;

    print_success_message(&format!(
        "Reading Foundry broadcasts from {}...",
        config.broadcast.display()
    ));
    let broadcast_discovery = discover_broadcast_deployments(&config.broadcast)?;
    if broadcast_discovery.run_file_count == 0 {
        return Err(boxed_error(format!(
            "No Foundry broadcast run files found under {}. Run `forge script --broadcast` and retry.",
            config.broadcast.display()
        )));
    }

    if broadcast_discovery.deployments.is_empty() && broadcast_discovery.skipped.is_empty() {
        return Err(boxed_error(format!(
            "Foundry broadcast files were found under {}, but none contained CREATE or CREATE2 deployment transactions. Run `forge script --broadcast` for the deployment script you want to index and retry.",
            config.broadcast.display()
        )));
    }

    if broadcast_discovery.deployments.is_empty() {
        return Ok(Discovery {
            config,
            contracts: Vec::new(),
            skipped: broadcast_discovery.skipped,
        });
    }

    print_success_message(&format!("Reading Foundry artifacts from {}...", config.out.display()));
    let artifacts = read_artifacts(&config.out)?;
    let mut skipped = broadcast_discovery.skipped;
    let (contracts, artifact_skipped) =
        build_desired_contracts(broadcast_discovery.deployments, &artifacts);
    skipped.extend(artifact_skipped);

    Ok(Discovery { config, contracts, skipped })
}

fn resolve_foundry_project_root(source_root: &Path) -> PathBuf {
    let mut current = source_root;
    loop {
        if current.join("foundry.toml").exists() {
            return current.to_path_buf();
        }
        let Some(parent) = current.parent() else {
            return source_root.to_path_buf();
        };
        current = parent;
    }
}

fn build_desired_contracts(
    raw_deployments: Vec<RawDeployment>,
    artifacts: &[FoundryArtifact],
) -> (Vec<DesiredContract>, Vec<SkippedDeployment>) {
    let mut skipped = Vec::new();
    let mut grouped = BTreeMap::<String, DesiredContract>::new();

    for deployment in raw_deployments {
        let artifact = match find_artifact(&deployment, artifacts) {
            ArtifactSelection::Found(artifact) => artifact,
            ArtifactSelection::TestOnly => {
                skipped
                    .push(SkippedDeployment::for_deployment(&deployment, SkipReason::TestArtifact));
                continue;
            }
            ArtifactSelection::Missing => {
                skipped.push(SkippedDeployment::for_deployment(
                    &deployment,
                    SkipReason::MissingArtifact,
                ));
                continue;
            }
        };

        if artifact.event_names.is_empty() {
            skipped.push(SkippedDeployment::for_deployment(&deployment, SkipReason::NoEvents));
            continue;
        }

        let key = desired_contract_key(&deployment.contract_name, &artifact.relative_path);
        let entry = grouped.entry(key.clone()).or_insert_with(|| {
            let name = sanitize_contract_name(&deployment.contract_name);
            DesiredContract {
                key,
                name: name.clone(),
                foundry_contract_name: deployment.contract_name.clone(),
                artifact_path: artifact.relative_path.clone(),
                abi_path: format!("abis/{name}.abi.json"),
                abi_json: artifact.abi_json.clone(),
                event_names: artifact.event_names.clone(),
                deployments: Vec::new(),
            }
        });

        entry.deployments.push(DesiredDeployment {
            chain_id: deployment.chain_id,
            address: deployment.contract_address,
            start_block: deployment.start_block,
        });
    }

    let mut contracts = grouped.into_values().collect::<Vec<_>>();
    ensure_unique_desired_names(&mut contracts);

    for contract in &mut contracts {
        contract.abi_path = format!("abis/{}.abi.json", contract.name);
    }

    (contracts, skipped)
}

fn read_foundry_config(foundry_root: &Path) -> CliResult<FoundryConfig> {
    let stdout = run_command("forge", &["config", "--json"], foundry_root)?;
    let value: Value = serde_json::from_str(&stdout)?;
    let out = value.get("out").and_then(Value::as_str).unwrap_or("out");
    let broadcast = value.get("broadcast").and_then(Value::as_str).unwrap_or("broadcast");

    Ok(FoundryConfig {
        out: resolve_path_from(foundry_root, out),
        broadcast: resolve_path_from(foundry_root, broadcast),
        out_setting: out.to_string(),
        broadcast_setting: broadcast.to_string(),
    })
}

fn discover_broadcast_deployments(broadcast_root: &Path) -> CliResult<BroadcastDiscovery> {
    if !broadcast_root.exists() {
        return Ok(BroadcastDiscovery {
            deployments: Vec::new(),
            skipped: Vec::new(),
            run_file_count: 0,
        });
    }

    let run_files = find_broadcast_run_files(broadcast_root)?;
    let run_file_count = run_files.len();
    let mut deployments = Vec::new();
    let mut skipped = Vec::new();
    let mut seen = HashSet::new();

    for run_file in run_files {
        let contents = fs::read_to_string(&run_file)?;
        let value: Value = serde_json::from_str(&contents)?;
        let chain_id = value
            .get("chain")
            .and_then(Value::as_u64)
            .or_else(|| chain_id_from_run_path(broadcast_root, &run_file));

        let transactions =
            value.get("transactions").and_then(Value::as_array).cloned().unwrap_or_default();
        let receipts = value.get("receipts").and_then(Value::as_array).cloned().unwrap_or_default();
        let Some(chain_id) = chain_id else {
            for tx in transactions.iter().filter(|tx| is_create_transaction(tx)) {
                skipped.push(SkippedDeployment::new(
                    broadcast_deployment_label(&run_file, tx, None),
                    SkipReason::MissingChainId,
                ));
            }
            continue;
        };

        for (index, tx) in transactions.iter().enumerate() {
            if !is_create_transaction(tx) {
                continue;
            }

            let Some(contract_name) =
                tx.get("contractName").and_then(Value::as_str).filter(|name| !name.is_empty())
            else {
                skipped.push(SkippedDeployment::new(
                    broadcast_deployment_label(&run_file, tx, Some(chain_id)),
                    SkipReason::MissingContractName,
                ));
                continue;
            };
            let Some(contract_address) = tx
                .get("contractAddress")
                .and_then(Value::as_str)
                .filter(|address| !address.is_empty())
            else {
                skipped.push(SkippedDeployment::new(
                    format!("{contract_name} on chain {chain_id}"),
                    SkipReason::MissingContractAddress,
                ));
                continue;
            };

            let Ok(address) = contract_address.parse::<Address>() else {
                skipped.push(SkippedDeployment::new(
                    format!("{contract_name} at {contract_address} on chain {chain_id}"),
                    SkipReason::InvalidContractAddress,
                ));
                continue;
            };

            // Interrupted or resumed broadcasts can hold fewer receipts than transactions,
            // so only trust positional receipts when the two arrays are aligned.
            let aligned_receipt =
                if receipts.len() == transactions.len() { receipts.get(index) } else { None };
            let receipt = receipt_for_contract(&receipts, contract_address).or(aligned_receipt);
            let Some(block_number) =
                receipt.and_then(|receipt| receipt.get("blockNumber")).and_then(parse_u64_json)
            else {
                skipped.push(SkippedDeployment::new(
                    format!("{contract_name} at {address} on chain {chain_id}"),
                    SkipReason::MissingReceiptBlock,
                ));
                continue;
            };

            let key = format!("{chain_id}:{address}");
            if !seen.insert(key) {
                continue;
            }

            deployments.push(RawDeployment {
                chain_id,
                contract_name: contract_name.to_string(),
                contract_address: address,
                start_block: block_number,
                transaction_input: tx
                    .get("transaction")
                    .and_then(|transaction| transaction.get("input"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
    }

    Ok(BroadcastDiscovery { deployments, skipped, run_file_count })
}

fn is_create_transaction(tx: &Value) -> bool {
    matches!(tx.get("transactionType").and_then(Value::as_str), Some("CREATE" | "CREATE2"))
}

fn broadcast_deployment_label(run_file: &Path, tx: &Value, chain_id: Option<u64>) -> String {
    let contract_name =
        tx.get("contractName").and_then(Value::as_str).filter(|name| !name.is_empty());
    let contract_address =
        tx.get("contractAddress").and_then(Value::as_str).filter(|address| !address.is_empty());
    match (contract_name, contract_address, chain_id) {
        (Some(name), Some(address), Some(chain_id)) => {
            format!("{name} at {address} on chain {chain_id}")
        }
        (Some(name), None, Some(chain_id)) => format!("{name} on chain {chain_id}"),
        (None, Some(address), Some(chain_id)) => {
            format!("deployment at {address} on chain {chain_id}")
        }
        (None, None, Some(chain_id)) => format!("deployment on chain {chain_id}"),
        (Some(name), Some(address), None) => {
            format!("{name} at {address} in {}", run_file.display())
        }
        (Some(name), None, None) => format!("{name} in {}", run_file.display()),
        (None, Some(address), None) => format!("deployment at {address} in {}", run_file.display()),
        (None, None, None) => format!("deployment in {}", run_file.display()),
    }
}

fn find_broadcast_run_files(broadcast_root: &Path) -> CliResult<Vec<PathBuf>> {
    let mut chain_dirs = Vec::new();
    collect_chain_dirs(broadcast_root, &mut chain_dirs)?;

    let mut files = Vec::new();
    for chain_dir in chain_dirs {
        let latest = chain_dir.join("run-latest.json");
        if latest.exists() {
            files.push(latest);
            continue;
        }

        if let Some(newest) = newest_run_file(&chain_dir)? {
            files.push(newest);
        }
    }

    Ok(files)
}

fn collect_chain_dirs(dir: &Path, chain_dirs: &mut Vec<PathBuf>) -> CliResult<()> {
    if !dir.exists() {
        return Ok(());
    }

    let latest = dir.join("run-latest.json");
    if latest.exists() || newest_run_file(dir)?.is_some() {
        chain_dirs.push(dir.to_path_buf());
        return Ok(());
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_chain_dirs(&entry.path(), chain_dirs)?;
        }
    }

    Ok(())
}

fn newest_run_file(dir: &Path) -> CliResult<Option<PathBuf>> {
    let mut newest = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.starts_with("run-")
            || !file_name.ends_with(".json")
            || file_name == "run-latest.json"
        {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        match &newest {
            Some((current_modified, _)) if *current_modified >= modified => {}
            _ => newest = Some((modified, entry.path())),
        }
    }

    Ok(newest.map(|(_, path)| path))
}

fn chain_id_from_run_path(broadcast_root: &Path, run_file: &Path) -> Option<u64> {
    let parent = run_file.parent()?;
    let relative = parent.strip_prefix(broadcast_root).ok()?;
    relative.components().next_back()?.as_os_str().to_str()?.parse().ok()
}

fn receipt_for_contract<'a>(receipts: &'a [Value], contract_address: &str) -> Option<&'a Value> {
    receipts.iter().find(|receipt| {
        receipt
            .get("contractAddress")
            .and_then(Value::as_str)
            .is_some_and(|address| address.eq_ignore_ascii_case(contract_address))
    })
}

fn parse_u64_json(value: &Value) -> Option<u64> {
    if let Some(number) = value.as_u64() {
        return Some(number);
    }

    let raw = value.as_str()?;
    if let Some(hex) = raw.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).ok()
    } else {
        raw.parse().ok()
    }
}

fn read_artifacts(out_root: &Path) -> CliResult<Vec<FoundryArtifact>> {
    if !out_root.exists() {
        return Err(boxed_error(format!(
            "Foundry artifact directory does not exist: {}",
            out_root.display()
        )));
    }

    let mut artifact_paths = Vec::new();
    collect_artifact_paths(out_root, &mut artifact_paths)?;
    artifact_paths.sort();

    let mut artifacts = Vec::new();
    for path in artifact_paths {
        let contents = fs::read_to_string(&path)?;
        let value: Value = match serde_json::from_str(&contents) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(abi) = value.get("abi").cloned() else {
            continue;
        };
        if !abi.is_array() {
            continue;
        }

        let event_names = extract_event_names(&abi);
        let abi_json = serde_json::to_string_pretty(&abi)?;
        let bytecode = value
            .get("bytecode")
            .and_then(|bytecode| bytecode.get("object"))
            .and_then(Value::as_str)
            .filter(|bytecode| !bytecode.is_empty() && *bytecode != "0x")
            .map(str::to_string);
        let relative_path =
            path.strip_prefix(out_root).unwrap_or(&path).to_string_lossy().replace('\\', "/");
        let (source_path, contract_name) =
            artifact_compilation_target(&value).unwrap_or((None, None));

        artifacts.push(FoundryArtifact {
            relative_path,
            contract_name,
            source_path,
            abi_json,
            event_names,
            bytecode,
        });
    }

    Ok(artifacts)
}

fn collect_artifact_paths(dir: &Path, artifact_paths: &mut Vec<PathBuf>) -> CliResult<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            if entry.file_name() == "build-info" {
                continue;
            }
            collect_artifact_paths(&path, artifact_paths)?;
        } else if path.extension().is_some_and(|ext| ext == "json") {
            artifact_paths.push(path);
        }
    }

    Ok(())
}

fn find_artifact<'a>(
    deployment: &RawDeployment,
    artifacts: &'a [FoundryArtifact],
) -> ArtifactSelection<'a> {
    let candidates = artifacts
        .iter()
        .filter(|artifact| artifact_matches_contract(artifact, &deployment.contract_name))
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return ArtifactSelection::Missing;
    }

    let candidates =
        candidates.into_iter().filter(|artifact| !is_test_artifact(artifact)).collect::<Vec<_>>();
    if candidates.is_empty() {
        return ArtifactSelection::TestOnly;
    }

    ArtifactSelection::Found(select_best_artifact_candidate(deployment, &candidates))
}

fn select_best_artifact_candidate<'a>(
    deployment: &RawDeployment,
    candidates: &[&'a FoundryArtifact],
) -> &'a FoundryArtifact {
    if let Some(input) = &deployment.transaction_input {
        let input = input.to_lowercase();
        let bytecode_matches = candidates
            .iter()
            .copied()
            .filter(|artifact| {
                artifact.bytecode.as_ref().is_some_and(|bytecode| {
                    let bytecode = bytecode.to_lowercase();
                    bytecode != "0x" && input.starts_with(&bytecode)
                })
            })
            .collect::<Vec<_>>();
        if let Some(best) = bytecode_matches
            .into_iter()
            .min_by_key(|artifact| artifact_candidate_rank(artifact, &deployment.contract_name))
        {
            return best;
        }
    }

    candidates
        .iter()
        .copied()
        .min_by_key(|artifact| artifact_candidate_rank(artifact, &deployment.contract_name))
        .expect("artifact candidates are non-empty")
}

fn artifact_matches_contract(artifact: &FoundryArtifact, contract_name: &str) -> bool {
    artifact.contract_name.as_deref() == Some(contract_name)
        || artifact_file_contract_name(&artifact.relative_path) == Some(contract_name)
}

fn artifact_file_contract_name(relative_path: &str) -> Option<&str> {
    let file_name = Path::new(relative_path).file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".json")?;
    Some(stem.split_once('.').map_or(stem, |(contract_name, _)| contract_name))
}

fn artifact_candidate_rank(
    artifact: &FoundryArtifact,
    contract_name: &str,
) -> (u8, u8, usize, String) {
    let profile_rank = artifact_profile_rank(&artifact.relative_path, contract_name);
    let source_rank = artifact_source_rank(artifact);
    (profile_rank, source_rank, artifact.relative_path.len(), artifact.relative_path.clone())
}

fn artifact_profile_rank(relative_path: &str, contract_name: &str) -> u8 {
    let Some(file_name) = Path::new(relative_path).file_name().and_then(|name| name.to_str())
    else {
        return 3;
    };
    let Some(stem) = file_name.strip_suffix(".json") else {
        return 3;
    };
    if stem == contract_name {
        return 0;
    }
    let Some(profile) = stem.strip_prefix(&format!("{contract_name}.")) else {
        return 3;
    };
    match profile {
        "default" => 1,
        _ => 2,
    }
}

fn artifact_source_rank(artifact: &FoundryArtifact) -> u8 {
    let Some(source_path) = artifact.source_path.as_deref() else {
        return 2;
    };
    let normalized = source_path.replace('\\', "/");
    let mut components = normalized.split('/').filter(|component| !component.is_empty());
    match components.next() {
        Some("src" | "contracts") => 0,
        _ if normalized.contains("/src/") || normalized.contains("/contracts/") => 1,
        _ => 2,
    }
}

fn is_test_artifact(artifact: &FoundryArtifact) -> bool {
    artifact.source_path.as_deref().is_some_and(source_path_is_test)
        || artifact.contract_name.as_deref().is_some_and(contract_name_is_test)
        || artifact_file_contract_name(&artifact.relative_path).is_some_and(contract_name_is_test)
        || source_file_name_is_test(&artifact.relative_path)
}

fn source_path_is_test(source_path: &str) -> bool {
    let normalized = source_path.replace('\\', "/");
    normalized.split('/').any(|component| {
        let component = component.to_ascii_lowercase();
        component == "test" || component == "tests"
    }) || source_file_name_is_test(&normalized)
}

fn source_file_name_is_test(path: &str) -> bool {
    Path::new(path).file_stem().and_then(|stem| stem.to_str()).is_some_and(contract_name_is_test)
}

fn contract_name_is_test(contract_name: &str) -> bool {
    contract_name.ends_with("Test") || contract_name.ends_with("Tests")
}

fn artifact_compilation_target(value: &Value) -> Option<(Option<String>, Option<String>)> {
    let target = value
        .get("metadata")
        .and_then(|metadata| metadata.get("settings"))
        .and_then(|settings| settings.get("compilationTarget"))
        .and_then(Value::as_object)?;
    let (source_path, contract_name) = target.iter().next()?;
    Some((Some(source_path.clone()), contract_name.as_str().map(str::to_string)))
}

fn extract_event_names(abi: &Value) -> Vec<String> {
    let Some(items) = abi.as_array() else {
        return Vec::new();
    };

    let mut seen = BTreeSet::new();
    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("event") {
            continue;
        }
        if let Some(name) = item.get("name").and_then(Value::as_str) {
            seen.insert(name.to_string());
        }
    }

    seen.into_iter().collect()
}

fn parse_foundry_source(source: Option<&str>, base_dir: &Path) -> CliResult<FoundrySourceSpec> {
    let source = source.unwrap_or(".").trim();
    if source.is_empty() {
        return Err(boxed_error("Foundry source can not be empty."));
    }

    let (source_without_suffix, explicit_ref, explicit_subdir) = split_git_suffix(source);
    if let Some((clone_url, github_ref, github_subdir)) = parse_github_url(source_without_suffix) {
        return Ok(FoundrySourceSpec {
            kind: FoundrySourceKind::Git,
            location: clone_url,
            git_ref: explicit_ref.or(github_ref),
            subdir: explicit_subdir.or(github_subdir),
        });
    }

    let candidate_path = resolve_path_from(base_dir, source_without_suffix);
    if candidate_path.exists() {
        return Ok(FoundrySourceSpec {
            kind: FoundrySourceKind::Local,
            location: candidate_path.to_string_lossy().to_string(),
            git_ref: None,
            subdir: explicit_subdir,
        });
    }

    if looks_like_git_url(source_without_suffix) {
        return Ok(FoundrySourceSpec {
            kind: FoundrySourceKind::Git,
            location: source_without_suffix.to_string(),
            git_ref: explicit_ref,
            subdir: explicit_subdir,
        });
    }

    Err(boxed_error(format!(
        "Foundry source does not exist and does not look like a Git URL: {source_without_suffix}"
    )))
}

fn split_git_suffix(source: &str) -> (&str, Option<String>, Option<String>) {
    let Some((base, suffix)) = source.rsplit_once('#') else {
        return (source, None, None);
    };

    if suffix.is_empty() {
        return (base, None, None);
    }

    match suffix.split_once(':') {
        Some((git_ref, subdir)) => (
            base,
            empty_to_none(git_ref).map(str::to_string),
            empty_to_none(subdir).map(str::to_string),
        ),
        None => (base, Some(suffix.to_string()), None),
    }
}

fn empty_to_none(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn parse_github_url(source: &str) -> Option<(String, Option<String>, Option<String>)> {
    let source = source.trim_end_matches('/');
    let rest = source.strip_prefix("https://github.com/")?;
    let mut segments = rest.split('/').collect::<Vec<_>>();
    if segments.len() < 2 {
        return None;
    }

    let org = segments.remove(0);
    let repo = segments.remove(0).trim_end_matches(".git");
    if org.is_empty() || repo.is_empty() {
        return None;
    }

    let clone_url = format!("https://github.com/{org}/{repo}.git");
    if segments.len() >= 2 && matches!(segments[0], "tree" | "blob") {
        let git_ref = segments[1].to_string();
        let subdir = if segments.len() > 2 { Some(segments[2..].join("/")) } else { None };
        Some((clone_url, Some(git_ref), subdir))
    } else {
        Some((clone_url, None, None))
    }
}

fn looks_like_git_url(source: &str) -> bool {
    source.starts_with("http://")
        || source.starts_with("https://")
        || source.starts_with("ssh://")
        || source.starts_with("git://")
        || source.starts_with("file://")
        || source.starts_with("git@")
        || source.ends_with(".git")
        || (source.contains('@') && source.contains(':'))
}

fn prepare_foundry_source(spec: FoundrySourceSpec) -> CliResult<PreparedFoundrySource> {
    match spec.kind {
        FoundrySourceKind::Local => {
            let root =
                resolve_source_subdir(PathBuf::from(&spec.location), spec.subdir.as_deref())?;
            print_success_message(&format!("Using local Foundry source at {}.", root.display()));
            Ok(PreparedFoundrySource { spec, root, last_commit: None, _temp_dir: None })
        }
        FoundrySourceKind::Git => {
            let temp_dir = TempDir::new()?;
            let clone_root = temp_dir.path().join("repo");
            print_success_message(&format!("Cloning Foundry source {}...", spec.location));
            clone_git_source(&spec, &clone_root)?;

            if let Some(git_ref) = &spec.git_ref {
                print_success_message(&format!("Checking out Foundry source ref {git_ref}..."));
                run_command("git", &["checkout", "--quiet", git_ref], &clone_root)?;
            }
            initialize_git_submodules(&clone_root)?;

            let last_commit = run_command("git", &["rev-parse", "HEAD"], &clone_root)
                .ok()
                .map(|commit| commit.trim().to_string());
            let root = resolve_source_subdir(clone_root, spec.subdir.as_deref())?;
            print_success_message(&format!("Using Foundry root {}.", root.display()));
            Ok(PreparedFoundrySource { spec, root, last_commit, _temp_dir: Some(temp_dir) })
        }
    }
}

fn clone_git_source(spec: &FoundrySourceSpec, clone_root: &Path) -> CliResult<()> {
    let clone_root = clone_root.to_string_lossy().to_string();
    let mut args = vec!["clone", "--progress", "--filter=blob:none"];
    if spec.git_ref.is_none() {
        args.extend(["--depth", "1"]);
    }
    args.push(&spec.location);
    args.push(&clone_root);

    let output = command_output_streaming("git", &args, Path::new("."))?;
    if output.status.success() {
        return Ok(());
    }

    print_warn_message(
        "Optimized Git clone failed; retrying with a full clone for compatibility...",
    );
    let _ = fs::remove_dir_all(&clone_root);
    run_streaming_command(
        "git",
        &["clone", "--progress", &spec.location, &clone_root],
        Path::new("."),
    )
}

fn resolve_source_subdir(root: PathBuf, subdir: Option<&str>) -> CliResult<PathBuf> {
    let foundry_root = if let Some(subdir) = subdir { root.join(subdir) } else { root };

    if !foundry_root.exists() {
        return Err(boxed_error(format!(
            "Foundry source directory does not exist: {}",
            foundry_root.display()
        )));
    }

    Ok(foundry_root)
}

fn source_spec_from_origin(
    source: &FoundryOriginSource,
    project_path: &Path,
) -> CliResult<FoundrySourceSpec> {
    let kind = match source.kind.as_str() {
        "local" => FoundrySourceKind::Local,
        "git" => FoundrySourceKind::Git,
        other => {
            return Err(boxed_error(format!("Unknown Foundry source kind in origin: {other}")));
        }
    };

    let location = if matches!(kind, FoundrySourceKind::Local) {
        resolve_stored_local_path(&source.location, project_path).to_string_lossy().to_string()
    } else {
        source.location.clone()
    };

    Ok(FoundrySourceSpec {
        kind,
        location,
        git_ref: source.git_ref.clone(),
        subdir: source.subdir.clone(),
    })
}

fn build_origin(
    prepared: &PreparedFoundrySource,
    discovery: &Discovery,
    project_path: &Path,
    network_names_by_chain: &BTreeMap<u64, String>,
    assigned: Option<&HashMap<String, AssignedContractIdentity>>,
) -> FoundryOrigin {
    let source = origin_source(&prepared.spec, prepared.last_commit.clone(), project_path);
    let managed_contracts = discovery
        .contracts
        .iter()
        .map(|contract| FoundryManagedContract {
            name: assigned
                .and_then(|assigned| assigned.get(&contract.key))
                .map_or_else(|| contract.name.clone(), |identity| identity.name.clone()),
            foundry_contract_name: contract.foundry_contract_name.clone(),
            artifact_path: contract.artifact_path.clone(),
            abi_path: assigned
                .and_then(|assigned| assigned.get(&contract.key))
                .map_or_else(|| contract.abi_path.clone(), |identity| identity.abi_path.clone()),
            deployments: contract
                .deployments
                .iter()
                .map(|deployment| FoundryManagedDeployment {
                    chain_id: deployment.chain_id,
                    network: network_names_by_chain
                        .get(&deployment.chain_id)
                        .cloned()
                        .unwrap_or_else(|| default_network_name(deployment.chain_id)),
                    address: deployment.address.to_string(),
                    start_block: deployment.start_block,
                })
                .collect(),
        })
        .collect();

    FoundryOrigin {
        version: ORIGIN_VERSION,
        source,
        foundry: FoundryResolvedConfig {
            out: discovery.config.out_setting.clone(),
            broadcast: discovery.config.broadcast_setting.clone(),
        },
        managed_contracts,
        synced_at_unix: now_unix(),
    }
}

fn origin_source(
    spec: &FoundrySourceSpec,
    last_commit: Option<String>,
    project_path: &Path,
) -> FoundryOriginSource {
    match spec.kind {
        FoundrySourceKind::Local => FoundryOriginSource {
            kind: "local".to_string(),
            location: stored_local_path(Path::new(&spec.location), project_path),
            git_ref: None,
            subdir: spec.subdir.clone(),
            last_commit: None,
        },
        FoundrySourceKind::Git => FoundryOriginSource {
            kind: "git".to_string(),
            location: spec.location.clone(),
            git_ref: spec.git_ref.clone(),
            subdir: spec.subdir.clone(),
            last_commit,
        },
    }
}

fn read_origin(project_path: &Path) -> CliResult<FoundryOrigin> {
    let contents = fs::read_to_string(project_path.join(ORIGIN_CONFIG_NAME))?;
    Ok(serde_json::from_str(&contents)?)
}

fn write_origin(project_path: &Path, origin: &FoundryOrigin) -> CliResult<()> {
    let contents = serde_json::to_string_pretty(origin)?;
    write_file(&project_path.join(ORIGIN_CONFIG_NAME), &contents)?;
    Ok(())
}

fn build_default_networks(
    contracts: &[DesiredContract],
    existing: Option<&[Network]>,
) -> Vec<Network> {
    let chains = contracts
        .iter()
        .flat_map(|contract| contract.deployments.iter().map(|deployment| deployment.chain_id))
        .collect::<BTreeSet<_>>();
    let mut networks = existing.map_or_else(Vec::new, |networks| networks.to_vec());
    let mut names = networks.iter().map(|network| network.name.clone()).collect::<HashSet<_>>();

    for chain_id in chains {
        if networks.iter().any(|network| network.chain_id == chain_id) {
            continue;
        }

        let mut name = default_network_name(chain_id);
        if names.contains(&name) {
            name = unique_name(&name, &names);
        }
        names.insert(name.clone());
        networks.push(default_network(chain_id, name));
    }

    networks
}

fn ensure_networks_for_discovery(manifest: &mut Manifest, discovery: &Discovery) {
    manifest.networks = build_default_networks(&discovery.contracts, Some(&manifest.networks));
}

fn default_network(chain_id: u64, name: String) -> Network {
    Network {
        name,
        chain_id,
        rpc: format!("${{{}}}", rpc_env_key(chain_id)).into(),
        block_poll_frequency: None,
        compute_units_per_second: None,
        max_block_range: None,
        disable_logs_bloom_checks: None,
        get_logs_settings: None,
        reth: None,
        multicall3_address: None,
        reorg_handling: None,
    }
}

fn default_network_name(chain_id: u64) -> String {
    known_chain_default(chain_id)
        .map(|chain| chain.name.to_string())
        .unwrap_or_else(|| format!("chain_{chain_id}"))
}

fn known_chain_default(chain_id: u64) -> Option<KnownChainDefault> {
    match chain_id {
        1 => Some(KnownChainDefault { name: "ethereum" }),
        10 => Some(KnownChainDefault { name: "optimism" }),
        25 => Some(KnownChainDefault { name: "cronos" }),
        56 => Some(KnownChainDefault { name: "bsc" }),
        100 => Some(KnownChainDefault { name: "gnosis" }),
        130 => Some(KnownChainDefault { name: "unichain" }),
        1301 => Some(KnownChainDefault { name: "unichain_sepolia" }),
        137 => Some(KnownChainDefault { name: "polygon" }),
        146 => Some(KnownChainDefault { name: "sonic" }),
        8453 => Some(KnownChainDefault { name: "base" }),
        31337 => Some(KnownChainDefault { name: "anvil" }),
        42161 => Some(KnownChainDefault { name: "arbitrum" }),
        42220 => Some(KnownChainDefault { name: "celo" }),
        43114 => Some(KnownChainDefault { name: "avalanche" }),
        59144 => Some(KnownChainDefault { name: "linea" }),
        81457 => Some(KnownChainDefault { name: "blast" }),
        534352 => Some(KnownChainDefault { name: "scroll" }),
        80002 => Some(KnownChainDefault { name: "polygon_amoy" }),
        84532 => Some(KnownChainDefault { name: "base_sepolia" }),
        421614 => Some(KnownChainDefault { name: "arbitrum_sepolia" }),
        11155111 => Some(KnownChainDefault { name: "sepolia" }),
        11155420 => Some(KnownChainDefault { name: "optimism_sepolia" }),
        _ => None,
    }
}

fn rpc_env_key(chain_id: u64) -> String {
    match chain_id {
        31337 => "ANVIL_RPC_URL".to_string(),
        _ => format!("CHAIN_{chain_id}_RPC_URL"),
    }
}

fn default_rpc_env_value(chain_id: u64) -> &'static str {
    match chain_id {
        31337 => "http://127.0.0.1:8545",
        _ => "",
    }
}

fn chain_network_names(networks: &[Network]) -> BTreeMap<u64, String> {
    networks.iter().map(|network| (network.chain_id, network.name.clone())).collect()
}

fn default_postgres_storage() -> Storage {
    Storage {
        postgres: Some(PostgresDetails {
            enabled: true,
            relationships: None,
            indexes: None,
            drop_each_run: None,
            disable_create_tables: None,
        }),
        clickhouse: None,
        csv: None,
    }
}

fn write_project_support_files(
    project_path: &Path,
    project_name: &str,
    networks: &[Network],
) -> CliResult<()> {
    write_file(&project_path.join("docker-compose.yml"), generate_docker_file())?;
    write_file(&project_path.join(".gitignore"), ".rindexer\ngenerated_csv/**/*.txt\n")?;
    write_file(&project_path.join(".env"), &new_env_contents(networks))?;
    write_file(&project_path.join("README.md"), &readme_contents(project_name))?;
    Ok(())
}

fn new_env_contents(networks: &[Network]) -> String {
    let mut lines = vec![
        "DATABASE_URL=postgresql://postgres:rindexer@localhost:5440/postgres".to_string(),
        "POSTGRES_PASSWORD=rindexer".to_string(),
    ];

    for network in networks {
        let rpc_url = default_rpc_env_value(network.chain_id);
        lines.push(format!("{}={rpc_url}", rpc_env_key(network.chain_id)));
    }

    lines.sort();
    lines.dedup();
    lines.join("\n")
}

fn ensure_env_network_values(project_path: &Path, networks: &[Network]) -> CliResult<()> {
    let env_path = project_path.join(".env");
    let mut contents =
        if env_path.exists() { fs::read_to_string(&env_path)? } else { String::new() };

    let mut lines = contents.lines().map(str::to_string).collect::<Vec<_>>();
    let mut changed = false;
    for line in new_env_contents(networks).lines() {
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        let prefix = format!("{key}=");
        if !lines.iter().any(|existing| existing.starts_with(&prefix)) {
            lines.push(line.to_string());
            changed = true;
        }
    }

    if changed {
        contents = lines.join("\n");
        contents.push('\n');
        write_file(&env_path, &contents)?;
    }

    Ok(())
}

fn readme_contents(project_name: &str) -> String {
    format!(
        r#"# {project_name}

This rindexer project was generated from a Foundry project.

## Run locally

1. Start the chain or RPC endpoint used by the Foundry broadcast.
2. Set the generated `CHAIN_<id>_RPC_URL` values in `.env` to RPC endpoints that can read the deployed chains. Chain `31337` uses `ANVIL_RPC_URL=http://127.0.0.1:8545`.
3. Make sure Docker is running, then start indexing and GraphQL:

```bash
rindexer start all
```

rindexer starts the generated Postgres `docker-compose.yml` automatically when the `DATABASE_URL` cannot connect. You can also start it manually with `docker compose up -d`.

## Refresh from Foundry

After new Foundry broadcasts or ABI changes, run:

```bash
rindexer foundry sync
```
"#
    )
}

fn repository_value(spec: &FoundrySourceSpec) -> Option<String> {
    matches!(spec.kind, FoundrySourceKind::Git).then(|| spec.location.clone())
}

fn source_was_overridden(
    prepared: &PreparedFoundrySource,
    previous: &FoundryOriginSource,
    project_path: &Path,
) -> bool {
    source_spec_from_origin(previous, project_path)
        .map(|previous_spec| previous_spec != prepared.spec)
        .unwrap_or(true)
}

fn desired_contract_key(contract_name: &str, artifact_path: &str) -> String {
    format!("{contract_name}:{artifact_path}")
}

fn managed_contract_key(contract: &FoundryManagedContract) -> String {
    desired_contract_key(&contract.foundry_contract_name, &contract.artifact_path)
}

fn abi_changed(project_path: &Path, abi_path: &str, desired_abi: &str) -> bool {
    fs::read_to_string(project_path.join(abi_path)).map_or(true, |current| current != desired_abi)
}

fn ensure_unique_desired_names(contracts: &mut [DesiredContract]) {
    let mut seen = HashSet::new();
    for contract in contracts {
        let unique = unique_name(&contract.name, &seen);
        seen.insert(unique.clone());
        contract.name = unique;
    }
}

fn unique_name(base: &str, existing: &HashSet<String>) -> String {
    if !existing.contains(base) {
        return base.to_string();
    }

    let mut index = 2;
    loop {
        let candidate = format!("{base}{index}");
        if !existing.contains(&candidate) {
            return candidate;
        }
        index += 1;
    }
}

fn sanitize_project_name(name: &str) -> String {
    let mut sanitized = name.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>();
    if sanitized.is_empty() {
        sanitized = "FoundryIndexer".to_string();
    }
    if !sanitized.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        sanitized.insert_str(0, "Foundry");
    }
    sanitized
}

fn sanitize_contract_name(name: &str) -> String {
    let mut sanitized =
        name.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').collect::<String>();
    // `filter` is reserved in rindexer contract names in any casing, so replace it.
    loop {
        let lowered = sanitized.to_lowercase();
        let Some(index) = lowered.find("filter") else {
            break;
        };
        sanitized.replace_range(index..index + "filter".len(), "Contract");
    }
    if sanitized.is_empty() {
        sanitized = "FoundryContract".to_string();
    }
    sanitized
}

fn derive_project_name(spec: &FoundrySourceSpec, root: &Path) -> String {
    let base = match spec.kind {
        FoundrySourceKind::Local => {
            root.file_name().and_then(|name| name.to_str()).unwrap_or("Foundry")
        }
        FoundrySourceKind::Git => repo_name_from_git_url(&spec.location).unwrap_or("Foundry"),
    };
    format!("{base}Indexer")
}

fn repo_name_from_git_url(location: &str) -> Option<&str> {
    let trimmed = location.trim_end_matches('/').trim_end_matches(".git");
    trimmed.rsplit(['/', ':']).next().filter(|value| !value.is_empty())
}

fn resolve_new_output_path(
    output: Option<&str>,
    current_dir: &Path,
    prepared: &PreparedFoundrySource,
    project_name: &str,
) -> PathBuf {
    if let Some(output) = output {
        return resolve_path_from(current_dir, output);
    }

    match prepared.spec.kind {
        FoundrySourceKind::Local => prepared.root.join(project_name),
        FoundrySourceKind::Git => current_dir.join(project_name),
    }
}

fn resolve_path_from(base_dir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn stored_local_path(source_path: &Path, project_path: &Path) -> String {
    let source_path = fs::canonicalize(source_path).unwrap_or_else(|_| source_path.to_path_buf());
    let project_parent = project_path.parent().unwrap_or(project_path);
    let project_parent =
        fs::canonicalize(project_parent).unwrap_or_else(|_| project_parent.to_path_buf());
    if let Ok(relative) = source_path.strip_prefix(project_parent) {
        if relative.as_os_str().is_empty() {
            ".".to_string()
        } else {
            format!("./{}", relative.to_string_lossy())
        }
    } else {
        source_path.to_string_lossy().to_string()
    }
}

fn resolve_stored_local_path(location: &str, project_path: &Path) -> PathBuf {
    let path = PathBuf::from(location);
    if path.is_absolute() {
        return path;
    }

    project_path.parent().unwrap_or(project_path).join(path)
}

fn initialize_git_submodules(repo_root: &Path) -> CliResult<()> {
    if !repo_root.join(".gitmodules").exists() {
        return Ok(());
    }

    print_success_message("Initializing Git submodules...");
    let optimized_args = [
        "submodule",
        "update",
        "--init",
        "--recursive",
        "--recommend-shallow",
        "--filter=blob:none",
        "--progress",
    ];
    let output = command_output_streaming("git", &optimized_args, repo_root)?;
    if output.status.success() {
        return Ok(());
    }

    print_warn_message(
        "Optimized Git submodule initialization failed; retrying with a full submodule update...",
    );
    let fallback_args = ["submodule", "update", "--init", "--recursive", "--progress"];
    let output = command_output_streaming("git", &fallback_args, repo_root)?;
    if output.status.success() {
        return Ok(());
    }

    let mut message = format_failed_command("git", &fallback_args, repo_root, &output);
    message.push_str(
        "\n\nCould not initialize Git submodules. rindexer uses your existing Git credentials; if a submodule is private or unavailable, clone the Foundry project locally, initialize its dependencies, and import from that local path.",
    );
    Err(boxed_error(message))
}

fn run_forge_build(current_dir: &Path) -> CliResult<()> {
    let output = command_output("forge", &["build", "--quiet"], current_dir)?;
    if output.status.success() {
        print_success_message("Foundry build completed.");
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if looks_like_unresolved_imports(&stdout) || looks_like_unresolved_imports(&stderr) {
        return Err(boxed_error(format_unresolved_forge_import_error(
            current_dir,
            &output,
            &stdout,
            &stderr,
        )));
    }

    Err(boxed_error(format_failed_command("forge", &["build", "--quiet"], current_dir, &output)))
}

fn run_command(program: &str, args: &[&str], current_dir: &Path) -> CliResult<String> {
    let output = command_output(program, args, current_dir)?;

    if !output.status.success() {
        return Err(boxed_error(format_failed_command(program, args, current_dir, &output)));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_streaming_command(program: &str, args: &[&str], current_dir: &Path) -> CliResult<()> {
    let output = command_output_streaming(program, args, current_dir)?;

    if !output.status.success() {
        return Err(boxed_error(format_failed_command(program, args, current_dir, &output)));
    }

    Ok(())
}

fn command_output(program: &str, args: &[&str], current_dir: &Path) -> CliResult<Output> {
    Command::new(program).args(args).current_dir(current_dir).output().map_err(|e| {
        boxed_error(format!("Failed to run `{program}` in {}: {e}", current_dir.display()))
    })
}

fn command_output_streaming(program: &str, args: &[&str], current_dir: &Path) -> CliResult<Output> {
    let mut child = Command::new(program)
        .args(args)
        .current_dir(current_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            boxed_error(format!("Failed to run `{program}` in {}: {e}", current_dir.display()))
        })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        boxed_error(format!(
            "Failed to capture stdout for `{program}` in {}",
            current_dir.display()
        ))
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        boxed_error(format!(
            "Failed to capture stderr for `{program}` in {}",
            current_dir.display()
        ))
    })?;

    let stdout_handle = thread::spawn(move || forward_command_stream(stdout, false));
    let stderr_handle = thread::spawn(move || forward_command_stream(stderr, true));
    let status = child.wait()?;
    let stdout =
        stdout_handle.join().map_err(|_| boxed_error("stdout reader thread panicked"))??;
    let stderr =
        stderr_handle.join().map_err(|_| boxed_error("stderr reader thread panicked"))??;

    Ok(Output { status, stdout, stderr })
}

fn forward_command_stream<R: Read>(mut reader: R, stderr: bool) -> io::Result<Vec<u8>> {
    let mut captured = Vec::new();
    let mut buffer = [0; 8 * 1024];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let bytes = &buffer[..bytes_read];
        captured.extend_from_slice(bytes);
        if stderr {
            let mut stream = io::stderr().lock();
            stream.write_all(bytes)?;
            stream.flush()?;
        } else {
            let mut stream = io::stdout().lock();
            stream.write_all(bytes)?;
            stream.flush()?;
        }
    }

    Ok(captured)
}

fn format_failed_command(
    program: &str,
    args: &[&str],
    current_dir: &Path,
    output: &Output,
) -> String {
    format!(
        "`{program} {}` failed in {}\nstdout:\n{}\nstderr:\n{}",
        args.join(" "),
        current_dir.display(),
        trimmed_command_output(&output.stdout),
        trimmed_command_output(&output.stderr)
    )
}

fn trimmed_command_output(output: &[u8]) -> String {
    trimmed_command_output_with_limit(output, COMMAND_OUTPUT_LIMIT)
}

fn trimmed_command_output_with_limit(output: &[u8], limit: usize) -> String {
    let output = String::from_utf8_lossy(output);
    let output = output.trim();
    if output.is_empty() {
        return "<empty>".to_string();
    }

    if output.len() <= limit {
        output.to_string()
    } else {
        let truncated = output.chars().take(limit).collect::<String>();
        format!("{truncated}... <truncated>")
    }
}

fn looks_like_unresolved_imports(output: &str) -> bool {
    let output = output.to_ascii_lowercase();
    output.contains("unable to resolve imports")
        || output.contains("could not resolve import")
        || output.contains("failed to resolve file")
        || output.contains("no such file or directory")
}

fn format_unresolved_forge_import_error(
    current_dir: &Path,
    output: &Output,
    stdout: &str,
    stderr: &str,
) -> String {
    let mut message = format!(
        "`forge build` failed in {}\n\nFoundry could not resolve one or more imports.",
        current_dir.display()
    );
    let imports = unresolved_imports_from_outputs(&[stdout, stderr]);
    if !imports.is_empty() {
        message.push_str("\nmissing imports:");
        for import in imports.iter().take(5) {
            message.push_str(&format!("\n- {import}"));
        }
        if imports.len() > 5 {
            message.push_str(&format!("\n- ... {} more", imports.len() - 5));
        }
    }
    message.push_str(
        "\n\nrindexer runs `forge build` against a fresh checkout for Git sources and does not guess missing dependencies. Run the import from a local checkout after installing dependencies (`forge install`, `forge soldeer install`, or the repo's setup command), or commit/submodule the dependency directories so a fresh clone builds.",
    );
    message.push_str(&format!(
        "\n\nstdout excerpt:\n{}\nstderr excerpt:\n{}",
        trimmed_command_output_with_limit(&output.stdout, FORGE_BUILD_OUTPUT_LIMIT),
        trimmed_command_output_with_limit(&output.stderr, FORGE_BUILD_OUTPUT_LIMIT)
    ));
    message
}

fn unresolved_imports_from_outputs(outputs: &[&str]) -> Vec<String> {
    let mut imports = BTreeSet::new();
    for output in outputs {
        for line in output.lines() {
            let lower = line.to_ascii_lowercase();
            let line = line.trim();
            let likely_missing_import = line.starts_with('"')
                || lower.contains("not found")
                || lower.contains("no such file");
            if likely_missing_import {
                if let Some(import) = first_quoted_value(line) {
                    if !Path::new(&import).is_absolute() {
                        imports.insert(import);
                    }
                }
            }
        }
    }
    imports.into_iter().collect()
}

fn first_quoted_value(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = &line[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn empty_discovery_error(skipped: &[SkippedDeployment]) -> String {
    if skipped.is_empty() {
        return "No event-bearing Foundry deployments were found. Run `forge script --broadcast` and ensure deployed contracts emit events.".to_string();
    }

    let mut counts = BTreeMap::<SkipReason, usize>::new();
    for skipped in skipped {
        *counts.entry(skipped.reason).or_default() += 1;
    }

    let summary = counts
        .iter()
        .map(|(reason, count)| format!("{count} {}", reason.summary_label()))
        .collect::<Vec<_>>()
        .join(", ");
    let hints = skip_reason_hints(skipped).join(" ");
    format!("Foundry deployments were found, but none could be imported into rindexer ({summary}). {hints}")
}

fn skip_reason_hints(skipped: &[SkippedDeployment]) -> Vec<&'static str> {
    let mut reasons = BTreeSet::new();
    for skipped in skipped {
        reasons.insert(skipped.reason);
    }
    reasons.into_iter().map(SkipReason::hint).collect()
}

fn print_skipped_contracts(skipped: &[SkippedDeployment]) {
    if skipped.is_empty() {
        return;
    }

    print_warn_message("Skipped Foundry deployments:");
    for skipped in skipped.iter().take(20) {
        print_warn_message(&format!("- {}", skipped.message()));
    }
    if skipped.len() > 20 {
        print_warn_message(&format!("- ... {} more skipped deployment(s)", skipped.len() - 20));
    }
    for hint in skip_reason_hints(skipped) {
        print_warn_message(&format!("hint: {hint}"));
    }
}

fn print_sync_report(report: &SyncReport, dry_run: bool) {
    let prefix = if dry_run { "Foundry sync dry run" } else { "Foundry sync" };
    print_success_message(&format!(
        "{prefix}: {} added, {} updated, {} ABI changes, {} stale kept.",
        report.added.len(),
        report.updated.len(),
        report.abi_updated.len(),
        report.stale.len()
    ));

    for added in &report.added {
        print_success_message(&format!("- added {added}"));
    }
    for updated in &report.updated {
        print_success_message(&format!("- updated {updated}"));
    }
    for stale in &report.stale {
        print_warn_message(&format!("- stale managed contract kept: {stale}"));
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn boxed_error(message: impl Into<String>) -> Box<dyn std::error::Error> {
    Box::new(CliError(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_github_repo_url() {
        let spec =
            parse_foundry_source(Some("https://github.com/example/protocol"), Path::new("/tmp"))
                .unwrap();

        assert_eq!(spec.kind, FoundrySourceKind::Git);
        assert_eq!(spec.location, "https://github.com/example/protocol.git");
        assert_eq!(spec.git_ref, None);
        assert_eq!(spec.subdir, None);
    }

    #[test]
    fn parses_github_tree_url() {
        let spec = parse_foundry_source(
            Some("https://github.com/example/protocol/tree/main/packages/contracts"),
            Path::new("/tmp"),
        )
        .unwrap();

        assert_eq!(spec.kind, FoundrySourceKind::Git);
        assert_eq!(spec.location, "https://github.com/example/protocol.git");
        assert_eq!(spec.git_ref.as_deref(), Some("main"));
        assert_eq!(spec.subdir.as_deref(), Some("packages/contracts"));
    }

    #[test]
    fn parses_generic_git_suffix() {
        let spec = parse_foundry_source(
            Some("git@github.com:example/protocol.git#develop:packages/contracts"),
            Path::new("/tmp"),
        )
        .unwrap();

        assert_eq!(spec.kind, FoundrySourceKind::Git);
        assert_eq!(spec.location, "git@github.com:example/protocol.git");
        assert_eq!(spec.git_ref.as_deref(), Some("develop"));
        assert_eq!(spec.subdir.as_deref(), Some("packages/contracts"));
    }

    #[test]
    fn parses_hex_block_number() {
        assert_eq!(parse_u64_json(&Value::String("0x1f".to_string())), Some(31));
        assert_eq!(parse_u64_json(&Value::String("42".to_string())), Some(42));
        assert_eq!(parse_u64_json(&Value::from(7)), Some(7));
    }

    #[test]
    fn resolves_foundry_root_from_broadcast_subdir() {
        let temp = tempfile::tempdir().unwrap();
        let foundry_root = temp.path().join("protocol");
        let broadcast_root = foundry_root.join("broadcast/Deploy.s.sol/31337");
        fs::create_dir_all(&broadcast_root).unwrap();
        fs::write(foundry_root.join("foundry.toml"), "[profile.default]\n").unwrap();

        assert_eq!(resolve_foundry_project_root(&broadcast_root), foundry_root);
    }

    #[test]
    fn extracts_unique_event_names() {
        let abi = serde_json::json!([
            {"type": "event", "name": "Transfer"},
            {"type": "function", "name": "transfer"},
            {"type": "event", "name": "Approval"},
            {"type": "event", "name": "Transfer"}
        ]);

        assert_eq!(extract_event_names(&abi), vec!["Approval", "Transfer"]);
    }

    #[test]
    fn parses_broadcast_deployments_from_latest_and_run_files() {
        let temp = tempfile::tempdir().unwrap();
        let latest_dir = temp.path().join("broadcast/Deploy.s.sol/31337");
        fs::create_dir_all(&latest_dir).unwrap();
        fs::write(
            latest_dir.join("run-latest.json"),
            r#"{
              "chain": 31337,
              "transactions": [
                {
                  "transactionType": "CREATE",
                  "contractName": "Emitter",
                  "contractAddress": "0x0000000000000000000000000000000000000001",
                  "transaction": { "input": "0x6000" }
                },
                {
                  "transactionType": "CALL",
                  "contractName": "Ignored",
                  "contractAddress": "0x0000000000000000000000000000000000000003"
                },
                {
                  "transactionType": "CREATE2",
                  "contractName": "SecondEmitter",
                  "contractAddress": "0x0000000000000000000000000000000000000002"
                }
              ],
              "receipts": [
                { "contractAddress": "0x0000000000000000000000000000000000000001", "blockNumber": "0x2" },
                { "blockNumber": "0x3" },
                { "contractAddress": "0x0000000000000000000000000000000000000002", "blockNumber": 4 }
              ]
            }"#,
        )
        .unwrap();

        let fallback_dir = temp.path().join("broadcast/Other.s.sol/1");
        fs::create_dir_all(&fallback_dir).unwrap();
        fs::write(
            fallback_dir.join("run-000.json"),
            r#"{
              "transactions": [
                {
                  "transactionType": "CREATE",
                  "contractName": "MainnetEmitter",
                  "contractAddress": "0x0000000000000000000000000000000000000004"
                }
              ],
              "receipts": [
                { "contractAddress": "0x0000000000000000000000000000000000000004", "blockNumber": "42" }
              ]
            }"#,
        )
        .unwrap();

        let discovery = discover_broadcast_deployments(&temp.path().join("broadcast")).unwrap();
        let mut deployments = discovery
            .deployments
            .into_iter()
            .map(|deployment| {
                (
                    deployment.contract_name,
                    deployment.chain_id,
                    deployment.start_block,
                    deployment.contract_address.to_string(),
                )
            })
            .collect::<Vec<_>>();
        deployments.sort();

        assert_eq!(
            deployments,
            vec![
                (
                    "Emitter".to_string(),
                    31337,
                    2,
                    "0x0000000000000000000000000000000000000001".to_string()
                ),
                (
                    "MainnetEmitter".to_string(),
                    1,
                    42,
                    "0x0000000000000000000000000000000000000004".to_string()
                ),
                (
                    "SecondEmitter".to_string(),
                    31337,
                    4,
                    "0x0000000000000000000000000000000000000002".to_string()
                ),
            ]
        );
        assert!(discovery.skipped.is_empty());
        assert_eq!(discovery.run_file_count, 2);
    }

    #[test]
    fn reports_broadcast_deployments_missing_receipt_blocks() {
        let temp = tempfile::tempdir().unwrap();
        let latest_dir = temp.path().join("broadcast/Deploy.s.sol/31337");
        fs::create_dir_all(&latest_dir).unwrap();
        fs::write(
            latest_dir.join("run-latest.json"),
            r#"{
              "chain": 31337,
              "transactions": [
                {
                  "transactionType": "CREATE",
                  "contractName": "Emitter",
                  "contractAddress": "0x0000000000000000000000000000000000000001"
                }
              ],
              "receipts": []
            }"#,
        )
        .unwrap();

        let discovery = discover_broadcast_deployments(&temp.path().join("broadcast")).unwrap();

        assert!(discovery.deployments.is_empty());
        assert_eq!(discovery.skipped.len(), 1);
        assert_eq!(discovery.skipped[0].reason, SkipReason::MissingReceiptBlock);
        assert!(discovery.skipped[0].message().contains("missing receipt blockNumber"));

        let error = empty_discovery_error(&discovery.skipped);
        assert!(error.contains("missing receipt block number"));
        assert!(error.contains("Commit real broadcast receipts"));
    }

    #[test]
    fn reads_artifact_abi_arrays_and_skips_build_info() {
        let temp = tempfile::tempdir().unwrap();
        let artifact_dir = temp.path().join("out/Emitter.sol");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("Emitter.json"),
            r#"{
              "abi": [
                {"type": "event", "name": "Ping"},
                {"type": "function", "name": "ping"}
              ],
              "metadata": {
                "settings": {
                  "compilationTarget": {
                    "src/Emitter.sol": "Emitter"
                  }
                }
              },
              "bytecode": { "object": "0x6000" }
            }"#,
        )
        .unwrap();
        let build_info_dir = temp.path().join("out/build-info");
        fs::create_dir_all(&build_info_dir).unwrap();
        fs::write(build_info_dir.join("ignored.json"), r#"{"abi": []}"#).unwrap();

        let artifacts = read_artifacts(&temp.path().join("out")).unwrap();

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].relative_path, "Emitter.sol/Emitter.json");
        assert_eq!(artifacts[0].contract_name.as_deref(), Some("Emitter"));
        assert_eq!(artifacts[0].source_path.as_deref(), Some("src/Emitter.sol"));
        assert_eq!(artifacts[0].event_names, vec!["Ping"]);
        assert_eq!(artifacts[0].bytecode.as_deref(), Some("0x6000"));
        assert!(artifacts[0].abi_json.trim_start().starts_with('['));
        assert!(!artifacts[0].abi_json.contains("bytecode"));
    }

    #[test]
    fn filters_no_event_contracts_and_disambiguates_by_bytecode() {
        let event_abi = serde_json::to_string_pretty(&serde_json::json!([
            {"type": "event", "name": "Pong"}
        ]))
        .unwrap();
        let artifacts = vec![
            FoundryArtifact {
                relative_path: "A.sol/Emitter.json".to_string(),
                contract_name: Some("Emitter".to_string()),
                source_path: Some("src/A.sol".to_string()),
                abi_json: event_abi.clone(),
                event_names: vec!["Ping".to_string()],
                bytecode: Some("0x6000".to_string()),
            },
            FoundryArtifact {
                relative_path: "B.sol/Emitter.json".to_string(),
                contract_name: Some("Emitter".to_string()),
                source_path: Some("src/B.sol".to_string()),
                abi_json: event_abi,
                event_names: vec!["Pong".to_string()],
                bytecode: Some("0x7000".to_string()),
            },
            FoundryArtifact {
                relative_path: "NoEvents.sol/NoEvents.json".to_string(),
                contract_name: Some("NoEvents".to_string()),
                source_path: Some("src/NoEvents.sol".to_string()),
                abi_json: "[]".to_string(),
                event_names: Vec::new(),
                bytecode: Some("0x8000".to_string()),
            },
        ];
        let deployments = vec![
            RawDeployment {
                chain_id: 31337,
                contract_name: "Emitter".to_string(),
                contract_address: "0x0000000000000000000000000000000000000001".parse().unwrap(),
                start_block: 2,
                transaction_input: Some("0x7000abcdef".to_string()),
            },
            RawDeployment {
                chain_id: 31337,
                contract_name: "NoEvents".to_string(),
                contract_address: "0x0000000000000000000000000000000000000002".parse().unwrap(),
                start_block: 3,
                transaction_input: Some("0x8000abcdef".to_string()),
            },
        ];

        let (contracts, skipped) = build_desired_contracts(deployments, &artifacts);

        assert_eq!(contracts.len(), 1);
        assert_eq!(contracts[0].artifact_path, "B.sol/Emitter.json");
        assert_eq!(contracts[0].event_names, vec!["Pong"]);
        assert_eq!(contracts[0].deployments.len(), 1);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, SkipReason::NoEvents);
        assert!(skipped[0].message().contains("ABI has no events"));
        assert!(empty_discovery_error(&skipped).contains("contracts whose ABIs contain no events"));
    }

    #[test]
    fn matches_profile_artifacts_and_skips_test_sources() {
        let abi = serde_json::to_string_pretty(&serde_json::json!([
            {"type": "event", "name": "Swap"}
        ]))
        .unwrap();
        let artifacts = vec![
            FoundryArtifact {
                relative_path: "PoolManager.sol/PoolManager.test.json".to_string(),
                contract_name: Some("PoolManager".to_string()),
                source_path: Some("lib/v4-core/src/PoolManager.sol".to_string()),
                abi_json: abi.clone(),
                event_names: vec!["Swap".to_string()],
                bytecode: Some("0x6000".to_string()),
            },
            FoundryArtifact {
                relative_path: "PoolManager.sol/PoolManager.default.json".to_string(),
                contract_name: Some("PoolManager".to_string()),
                source_path: Some("lib/v4-core/src/PoolManager.sol".to_string()),
                abi_json: abi.clone(),
                event_names: vec!["Swap".to_string()],
                bytecode: Some("0x6000".to_string()),
            },
            FoundryArtifact {
                relative_path: "PoolSwapTest.sol/PoolSwapTest.default.json".to_string(),
                contract_name: Some("PoolSwapTest".to_string()),
                source_path: Some("lib/v4-core/src/test/PoolSwapTest.sol".to_string()),
                abi_json: abi,
                event_names: vec!["Swap".to_string()],
                bytecode: Some("0x7000".to_string()),
            },
        ];
        let deployments = vec![
            RawDeployment {
                chain_id: 1301,
                contract_name: "PoolManager".to_string(),
                contract_address: "0x0000000000000000000000000000000000000001".parse().unwrap(),
                start_block: 1,
                transaction_input: Some("0x6000abcdef".to_string()),
            },
            RawDeployment {
                chain_id: 1301,
                contract_name: "PoolSwapTest".to_string(),
                contract_address: "0x0000000000000000000000000000000000000002".parse().unwrap(),
                start_block: 2,
                transaction_input: Some("0x7000abcdef".to_string()),
            },
        ];

        let (contracts, skipped) = build_desired_contracts(deployments, &artifacts);

        assert_eq!(contracts.len(), 1);
        assert_eq!(contracts[0].name, "PoolManager");
        assert_eq!(contracts[0].artifact_path, "PoolManager.sol/PoolManager.default.json");
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].reason, SkipReason::TestArtifact);
        assert!(skipped[0].message().contains("test deployment skipped"));
    }

    #[test]
    fn detects_unresolved_foundry_import_failures() {
        assert!(looks_like_unresolved_imports(
            "Unable to resolve imports: prb-math/contracts/PRBMathUD60x18.sol"
        ));
        assert!(looks_like_unresolved_imports("failed to resolve file: ./lib/missing.sol"));
        assert!(!looks_like_unresolved_imports("Compiler run successful"));
        assert_eq!(
            unresolved_imports_from_outputs(&[
                r#""prb-math/contracts/PRBMathUD60x18.sol" in "src/Library.sol""#,
                r#"Error: Source "node_modules/prb-math/contracts/PRBMathUD60x18.sol" not found"#
            ]),
            vec![
                "node_modules/prb-math/contracts/PRBMathUD60x18.sol".to_string(),
                "prb-math/contracts/PRBMathUD60x18.sol".to_string()
            ]
        );
    }

    #[test]
    fn writes_origin_and_copied_abis() {
        let temp = tempfile::tempdir().unwrap();
        let foundry_root = temp.path().join("foundry");
        let project_path = temp.path().join("EmitterIndexer");
        fs::create_dir_all(&foundry_root).unwrap();
        fs::create_dir_all(project_path.join("abis")).unwrap();
        let abi_json =
            serde_json::to_string_pretty(&serde_json::json!([{"type": "event", "name": "Ping"}]))
                .unwrap();
        let contract = desired_test_contract("Emitter", "abis/Emitter.abi.json", &abi_json);
        let discovery = Discovery {
            config: FoundryConfig {
                out: foundry_root.join("out"),
                broadcast: foundry_root.join("broadcast"),
                out_setting: "out".to_string(),
                broadcast_setting: "broadcast".to_string(),
            },
            contracts: vec![contract],
            skipped: Vec::new(),
        };
        let prepared = PreparedFoundrySource {
            spec: FoundrySourceSpec {
                kind: FoundrySourceKind::Local,
                location: foundry_root.to_string_lossy().to_string(),
                git_ref: None,
                subdir: None,
            },
            root: foundry_root,
            last_commit: None,
            _temp_dir: None,
        };
        let networks = build_default_networks(&discovery.contracts, None);
        let network_names_by_chain = chain_network_names(&networks);

        write_file(&project_path.join(&discovery.contracts[0].abi_path), &abi_json).unwrap();
        let origin =
            build_origin(&prepared, &discovery, &project_path, &network_names_by_chain, None);
        write_origin(&project_path, &origin).unwrap();

        let copied_abi = fs::read_to_string(project_path.join("abis/Emitter.abi.json")).unwrap();
        let read_origin = read_origin(&project_path).unwrap();

        assert_eq!(copied_abi, abi_json);
        assert_eq!(read_origin.version, ORIGIN_VERSION);
        assert_eq!(read_origin.source.kind, "local");
        assert_eq!(read_origin.source.location, "./foundry");
        assert_eq!(read_origin.foundry.out, "out");
        assert_eq!(read_origin.foundry.broadcast, "broadcast");
        assert_eq!(read_origin.managed_contracts[0].abi_path, "abis/Emitter.abi.json");
        assert_eq!(read_origin.managed_contracts[0].deployments[0].network, "anvil");
    }

    #[test]
    fn network_defaults_follow_broadcast_chain_ids() {
        let abi_json =
            serde_json::to_string_pretty(&serde_json::json!([{"type": "event", "name": "Ping"}]))
                .unwrap();
        let local_chain_contract =
            desired_test_contract("Emitter", "abis/Emitter.abi.json", &abi_json);
        let mut mainnet_contract =
            desired_test_contract("MainnetEmitter", "abis/MainnetEmitter.abi.json", &abi_json);
        mainnet_contract.deployments[0].chain_id = 1;
        let mut unichain_sepolia_contract = desired_test_contract(
            "UnichainSepoliaEmitter",
            "abis/UnichainSepoliaEmitter.abi.json",
            &abi_json,
        );
        unichain_sepolia_contract.deployments[0].chain_id = 1301;

        let networks = build_default_networks(
            &[local_chain_contract, mainnet_contract, unichain_sepolia_contract],
            None,
        );

        assert!(networks.iter().any(|network| {
            network.chain_id == 31337
                && network.name == "anvil"
                && network.rpc.primary() == "${ANVIL_RPC_URL}"
        }));
        assert!(networks.iter().any(|network| {
            network.chain_id == 1
                && network.name == "ethereum"
                && network.rpc.primary() == "${CHAIN_1_RPC_URL}"
        }));
        assert!(networks.iter().any(|network| {
            network.chain_id == 1301
                && network.name == "unichain_sepolia"
                && network.rpc.primary() == "${CHAIN_1301_RPC_URL}"
        }));

        let env = new_env_contents(&networks);
        assert!(env.contains("ANVIL_RPC_URL=http://127.0.0.1:8545"));
        assert!(env.contains("CHAIN_1_RPC_URL="));
        assert!(env.contains("CHAIN_1301_RPC_URL="));
        assert!(!env.contains("publicnode"));
        assert!(!env.contains("sepolia.unichain.org"));
    }

    #[test]
    fn sync_env_preserves_existing_values_and_appends_missing_keys() {
        let temp = tempfile::tempdir().unwrap();
        let project_path = temp.path();
        fs::write(
            project_path.join(".env"),
            [
                "CHAIN_1_RPC_URL=https://ethereum-rpc.publicnode.com",
                "CHAIN_1301_RPC_URL=https://custom.example/rpc",
                "EXTRA=value",
            ]
            .join("\n"),
        )
        .unwrap();
        let networks = vec![
            default_network(1, "ethereum".to_string()),
            default_network(1301, "unichain_sepolia".to_string()),
            default_network(31337, "anvil".to_string()),
        ];

        ensure_env_network_values(project_path, &networks).unwrap();

        let env = fs::read_to_string(project_path.join(".env")).unwrap();
        assert!(env.contains("CHAIN_1_RPC_URL=https://ethereum-rpc.publicnode.com"));
        assert!(env.contains("CHAIN_1301_RPC_URL=https://custom.example/rpc"));
        assert!(env.contains("ANVIL_RPC_URL=http://127.0.0.1:8545"));
        assert!(env.contains("EXTRA=value"));
    }

    #[test]
    fn sync_merge_preserves_user_owned_fields_and_non_managed_contracts() {
        let temp = tempfile::tempdir().unwrap();
        let project_path = temp.path().join("EmitterIndexer");
        let foundry_root = temp.path().join("foundry");
        fs::create_dir_all(project_path.join("abis")).unwrap();
        fs::create_dir_all(&foundry_root).unwrap();
        fs::write(project_path.join("abis/custom.abi.json"), "[]").unwrap();

        let previous_origin = FoundryOrigin {
            version: ORIGIN_VERSION,
            source: FoundryOriginSource {
                kind: "local".to_string(),
                location: "./foundry".to_string(),
                git_ref: None,
                subdir: None,
                last_commit: None,
            },
            foundry: FoundryResolvedConfig {
                out: foundry_root.join("out").to_string_lossy().to_string(),
                broadcast: foundry_root.join("broadcast").to_string_lossy().to_string(),
            },
            managed_contracts: vec![FoundryManagedContract {
                name: "ManagedEmitter".to_string(),
                foundry_contract_name: "Emitter".to_string(),
                artifact_path: "Emitter.sol/Emitter.json".to_string(),
                abi_path: "abis/custom.abi.json".to_string(),
                deployments: vec![FoundryManagedDeployment {
                    chain_id: 31337,
                    network: "chain_31337".to_string(),
                    address: "0x0000000000000000000000000000000000000001".to_string(),
                    start_block: 2,
                }],
            }],
            synced_at_unix: 1,
        };
        let prepared = PreparedFoundrySource {
            spec: source_spec_from_origin(&previous_origin.source, &project_path).unwrap(),
            root: foundry_root,
            last_commit: None,
            _temp_dir: None,
        };
        let next_abi =
            serde_json::to_string_pretty(&serde_json::json!([{"type": "event", "name": "Pong"}]))
                .unwrap();
        let mut next_contract =
            desired_test_contract("Emitter", "abis/Emitter.abi.json", &next_abi);
        next_contract.deployments[0].address =
            "0x0000000000000000000000000000000000000002".parse().unwrap();
        next_contract.deployments[0].start_block = 4;
        let discovery = Discovery {
            config: FoundryConfig {
                out: temp.path().join("out"),
                broadcast: temp.path().join("broadcast"),
                out_setting: "out".to_string(),
                broadcast_setting: "broadcast".to_string(),
            },
            contracts: vec![next_contract],
            skipped: Vec::new(),
        };
        let mut manifest = test_manifest(vec![
            Contract {
                name: "ManagedEmitter".to_string(),
                details: vec![test_contract_detail(
                    "chain_31337",
                    "0x0000000000000000000000000000000000000001",
                    2,
                )],
                abi: StringOrArray::Single("./abis/custom.abi.json".to_string()),
                include_events: Some(vec![ContractEvent {
                    name: "Ping".to_string(),
                    timestamps: None,
                }]),
                index_event_in_order: Some(vec!["Ping".to_string()]),
                dependency_events: None,
                reorg_safe_distance: None,
                generate_csv: Some(true),
                streams: None,
                chat: None,
                tables: None,
            },
            Contract {
                name: "UserOwned".to_string(),
                details: vec![test_contract_detail(
                    "chain_31337",
                    "0x0000000000000000000000000000000000000003",
                    1,
                )],
                abi: StringOrArray::Single("./abis/user.abi.json".to_string()),
                include_events: Some(vec![ContractEvent {
                    name: "UserEvent".to_string(),
                    timestamps: None,
                }]),
                index_event_in_order: None,
                dependency_events: None,
                reorg_safe_distance: None,
                generate_csv: None,
                streams: None,
                chat: None,
                tables: None,
            },
        ]);

        let report = sync_manifest_and_abis(
            &project_path,
            &mut manifest,
            &previous_origin,
            &prepared,
            &discovery,
            false,
        )
        .unwrap();

        assert_eq!(report.updated, vec!["ManagedEmitter"]);
        assert_eq!(report.abi_updated, vec!["ManagedEmitter"]);
        assert!(report.added.is_empty());
        assert!(manifest.contracts.iter().any(|contract| contract.name == "UserOwned"));

        let managed =
            manifest.contracts.iter().find(|contract| contract.name == "ManagedEmitter").unwrap();
        assert_eq!(managed.include_events.as_ref().unwrap()[0].name, "Pong");
        assert_eq!(managed.index_event_in_order.as_ref().unwrap(), &vec!["Ping".to_string()]);
        assert_eq!(managed.generate_csv, Some(true));
        match &managed.abi {
            StringOrArray::Single(path) => assert_eq!(path, "./abis/custom.abi.json"),
            StringOrArray::Multiple(_) => panic!("expected single ABI path"),
        }
        assert_eq!(
            fs::read_to_string(project_path.join("abis/custom.abi.json")).unwrap(),
            next_abi
        );
    }

    #[test]
    fn sanitizes_reserved_filter_out_of_contract_names() {
        assert_eq!(sanitize_contract_name("PriceFilter"), "PriceContract");
        assert_eq!(sanitize_contract_name("FilterRegistry"), "ContractRegistry");
        assert_eq!(sanitize_contract_name("TokenFILTERFilterV2"), "TokenContractContractV2");
        assert_eq!(sanitize_contract_name("filter"), "Contract");
        assert!(!sanitize_contract_name("MyFilter").to_lowercase().contains("filter"));
    }

    #[test]
    fn skips_positional_receipts_when_arrays_are_misaligned() {
        let temp = tempfile::tempdir().unwrap();
        let latest_dir = temp.path().join("broadcast/Deploy.s.sol/31337");
        fs::create_dir_all(&latest_dir).unwrap();
        // The first CREATE is pending (no receipt), so receipts[1] belongs to the
        // second CREATE and positional lookup would give the pending tx a wrong block.
        fs::write(
            latest_dir.join("run-latest.json"),
            r#"{
              "chain": 31337,
              "transactions": [
                {
                  "transactionType": "CREATE",
                  "contractName": "PendingEmitter",
                  "contractAddress": "0x0000000000000000000000000000000000000001"
                },
                {
                  "transactionType": "CALL",
                  "contractName": "Ignored",
                  "contractAddress": "0x0000000000000000000000000000000000000009"
                },
                {
                  "transactionType": "CREATE",
                  "contractName": "MinedEmitter",
                  "contractAddress": "0x0000000000000000000000000000000000000002"
                }
              ],
              "receipts": [
                { "blockNumber": "0x5" },
                { "contractAddress": "0x0000000000000000000000000000000000000002", "blockNumber": 7 }
              ]
            }"#,
        )
        .unwrap();

        let discovery = discover_broadcast_deployments(&temp.path().join("broadcast")).unwrap();

        assert_eq!(discovery.deployments.len(), 1);
        assert_eq!(discovery.deployments[0].contract_name, "MinedEmitter");
        assert_eq!(discovery.deployments[0].start_block, 7);
        assert_eq!(discovery.skipped.len(), 1);
        assert_eq!(discovery.skipped[0].reason, SkipReason::MissingReceiptBlock);
        assert!(discovery.skipped[0].label.contains("PendingEmitter"));
    }

    #[test]
    fn sync_reports_stale_only_for_contracts_still_in_manifest() {
        let temp = tempfile::tempdir().unwrap();
        let project_path = temp.path().join("EmitterIndexer");
        let foundry_root = temp.path().join("foundry");
        fs::create_dir_all(project_path.join("abis")).unwrap();
        fs::create_dir_all(&foundry_root).unwrap();

        let stale_managed = |name: &str| FoundryManagedContract {
            name: name.to_string(),
            foundry_contract_name: name.to_string(),
            artifact_path: format!("{name}.sol/{name}.json"),
            abi_path: format!("abis/{name}.abi.json"),
            deployments: vec![FoundryManagedDeployment {
                chain_id: 31337,
                network: "chain_31337".to_string(),
                address: "0x0000000000000000000000000000000000000005".to_string(),
                start_block: 1,
            }],
        };
        let previous_origin = FoundryOrigin {
            version: ORIGIN_VERSION,
            source: FoundryOriginSource {
                kind: "local".to_string(),
                location: "./foundry".to_string(),
                git_ref: None,
                subdir: None,
                last_commit: None,
            },
            foundry: FoundryResolvedConfig {
                out: "out".to_string(),
                broadcast: "broadcast".to_string(),
            },
            managed_contracts: vec![stale_managed("KeptStale"), stale_managed("RemovedStale")],
            synced_at_unix: 1,
        };
        let prepared = PreparedFoundrySource {
            spec: source_spec_from_origin(&previous_origin.source, &project_path).unwrap(),
            root: foundry_root,
            last_commit: None,
            _temp_dir: None,
        };
        let abi_json =
            serde_json::to_string_pretty(&serde_json::json!([{"type": "event", "name": "Ping"}]))
                .unwrap();
        let discovery = Discovery {
            config: FoundryConfig {
                out: temp.path().join("out"),
                broadcast: temp.path().join("broadcast"),
                out_setting: "out".to_string(),
                broadcast_setting: "broadcast".to_string(),
            },
            contracts: vec![desired_test_contract("Emitter", "abis/Emitter.abi.json", &abi_json)],
            skipped: Vec::new(),
        };
        // "RemovedStale" was already deleted from rindexer.yaml by the user.
        let mut manifest = test_manifest(vec![Contract {
            name: "KeptStale".to_string(),
            details: vec![test_contract_detail(
                "chain_31337",
                "0x0000000000000000000000000000000000000005",
                1,
            )],
            abi: StringOrArray::Single("./abis/KeptStale.abi.json".to_string()),
            include_events: None,
            index_event_in_order: None,
            dependency_events: None,
            reorg_safe_distance: None,
            generate_csv: None,
            streams: None,
            chat: None,
            tables: None,
        }]);

        let report = sync_manifest_and_abis(
            &project_path,
            &mut manifest,
            &previous_origin,
            &prepared,
            &discovery,
            false,
        )
        .unwrap();

        assert_eq!(report.stale, vec!["KeptStale"]);
        assert_eq!(report.added, vec!["Emitter"]);
    }

    #[test]
    fn stores_relative_origin_for_sibling_foundry_project() {
        let location = stored_local_path(Path::new("/repo"), Path::new("/repo/RindexerProject"));
        assert_eq!(location, ".");
    }

    fn desired_test_contract(name: &str, abi_path: &str, abi_json: &str) -> DesiredContract {
        DesiredContract {
            key: desired_contract_key(name, &format!("{name}.sol/{name}.json")),
            name: name.to_string(),
            foundry_contract_name: name.to_string(),
            artifact_path: format!("{name}.sol/{name}.json"),
            abi_path: abi_path.to_string(),
            abi_json: abi_json.to_string(),
            event_names: vec![serde_json::from_str::<Value>(abi_json).unwrap()[0]["name"]
                .as_str()
                .unwrap()
                .to_string()],
            deployments: vec![DesiredDeployment {
                chain_id: 31337,
                address: "0x0000000000000000000000000000000000000001".parse().unwrap(),
                start_block: 2,
            }],
        }
    }

    fn test_manifest(contracts: Vec<Contract>) -> Manifest {
        Manifest {
            name: "FoundryTest".to_string(),
            description: None,
            repository: None,
            project_type: ProjectType::NoCode,
            config: Config::default(),
            constants: HashMap::new(),
            timestamps: None,
            networks: vec![default_network(31337, "chain_31337".to_string())],
            storage: default_postgres_storage(),
            native_transfers: NativeTransfers::default(),
            contracts,
            phantom: None,
            global: Global::default(),
            graphql: None,
        }
    }

    fn test_contract_detail(network: &str, address: &str, start_block: u64) -> ContractDetails {
        ContractDetails::new_with_address(
            network.to_string(),
            ValueOrArray::Value(address.parse().unwrap()),
            None,
            Some(U64::from(start_block)),
            None,
        )
    }
}
