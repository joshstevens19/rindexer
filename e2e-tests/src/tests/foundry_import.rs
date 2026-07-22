use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use tracing::info;

use crate::anvil_setup::ANVIL_DEFAULT_PRIVATE_KEY;
use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};
use crate::tests::test_runner::SkipTest;

pub struct FoundryImportTests;

impl TestModule for FoundryImportTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_foundry_local_import_and_sync",
                "Foundry local import copies event ABIs, writes origin, and sync repulls changes",
                foundry_local_import_and_sync_test,
            )
            .with_timeout(180),
            TestDefinition::new(
                "test_foundry_git_import",
                "Foundry Git import clones a local Git fixture and tracks the source commit",
                foundry_git_import_test,
            )
            .with_timeout(180),
        ]
    }
}

fn foundry_local_import_and_sync_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        ensure_command_available("forge")?;
        ensure_command_available("git")?;

        let foundry_root = context.project_path.join("foundry-source");
        let output_path = context.project_path.join("foundry-indexer");
        write_foundry_project(&foundry_root, false)?;
        deploy_foundry_project(&foundry_root, &context.anvil.rpc_url)?;

        run_rindexer(
            &context.rindexer_binary,
            &context.project_path,
            &[
                "foundry",
                "new",
                path_str(&foundry_root)?,
                "--output",
                path_str(&output_path)?,
                "--name",
                "FoundryLocalE2E",
            ],
        )?;

        assert_generated_project(&output_path, "local")?;
        assert_yaml_event_names(&output_path, &["Ping"])?;
        assert_abi_event_names(&output_path, &["Ping"])?;

        write_foundry_project(&foundry_root, true)?;
        run_rindexer(
            &context.rindexer_binary,
            &context.project_path,
            &["foundry", "sync", "--path", path_str(&output_path)?],
        )?;

        assert_yaml_event_names(&output_path, &["Ping", "Pong"])?;
        assert_abi_event_names(&output_path, &["Ping", "Pong"])?;

        info!("Foundry local import and sync test passed");
        Ok(())
    })
}

fn foundry_git_import_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        ensure_command_available("forge")?;
        ensure_command_available("git")?;

        let foundry_root = context.project_path.join("foundry-git-source");
        let output_path = context.project_path.join("foundry-git-indexer");
        write_foundry_project(&foundry_root, false)?;
        deploy_foundry_project(&foundry_root, &context.anvil.rpc_url)?;
        initialize_git_fixture(&foundry_root)?;

        let source = format!("file://{}", foundry_root.display());
        run_rindexer(
            &context.rindexer_binary,
            &context.project_path,
            &[
                "foundry",
                "new",
                &source,
                "--output",
                path_str(&output_path)?,
                "--name",
                "FoundryGitE2E",
            ],
        )?;

        let origin = assert_generated_project(&output_path, "git")?;
        let last_commit = origin
            .get("source")
            .and_then(|source| source.get("last_commit"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if last_commit.len() != 40 || !last_commit.chars().all(|c| c.is_ascii_hexdigit()) {
            bail!("expected Git origin to record a 40-char commit, got '{last_commit}'");
        }

        run_rindexer(
            &context.rindexer_binary,
            &context.project_path,
            &["foundry", "sync", "--path", path_str(&output_path)?, "--dry-run"],
        )?;

        info!("Foundry Git import test passed");
        Ok(())
    })
}

fn ensure_command_available(command: &str) -> Result<()> {
    let output = Command::new(command).arg("--version").output();
    match output {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => {
            Err(SkipTest(format!("`{command} --version` failed with status {}", output.status))
                .into())
        }
        Err(error) => Err(SkipTest(format!("`{command}` is not available: {error}")).into()),
    }
}

fn write_foundry_project(foundry_root: &Path, include_pong: bool) -> Result<()> {
    std::fs::create_dir_all(foundry_root.join("src"))?;
    std::fs::create_dir_all(foundry_root.join("script"))?;
    std::fs::write(
        foundry_root.join("foundry.toml"),
        r#"[profile.default]
src = "src"
out = "out"
broadcast = "broadcast"
libs = []
"#,
    )?;

    let pong_event = if include_pong { "event Pong(address indexed sender);\n" } else { "" };
    let pong_function =
        if include_pong { "function pong() external { emit Pong(msg.sender); }\n" } else { "" };

    std::fs::write(
        foundry_root.join("src/Emitter.sol"),
        format!(
            r#"// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract Emitter {{
    event Ping(address indexed sender, uint256 value);
    {pong_event}

    constructor() {{
        emit Ping(msg.sender, 1);
    }}

    function ping(uint256 value) external {{
        emit Ping(msg.sender, value);
    }}

    {pong_function}
}}

contract NoEvents {{
    function noop() external pure returns (uint256) {{
        return 1;
    }}
}}
"#
        ),
    )?;

    std::fs::write(
        foundry_root.join("script/DeployEmitter.s.sol"),
        format!(
            r#"// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "../src/Emitter.sol";

interface Vm {{
    function startBroadcast(uint256 privateKey) external;
    function stopBroadcast() external;
}}

contract DeployEmitter {{
    Vm internal constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));
    uint256 internal constant PRIVATE_KEY = {ANVIL_DEFAULT_PRIVATE_KEY};

    function run() external {{
        vm.startBroadcast(PRIVATE_KEY);
        new Emitter();
        new NoEvents();
        vm.stopBroadcast();
    }}
}}
"#
        ),
    )?;

    Ok(())
}

fn deploy_foundry_project(foundry_root: &Path, rpc_url: &str) -> Result<()> {
    run_command(
        "forge",
        &[
            "script",
            "script/DeployEmitter.s.sol:DeployEmitter",
            "--broadcast",
            "--rpc-url",
            rpc_url,
            "--chain",
            "31337",
        ],
        foundry_root,
    )?;
    Ok(())
}

fn initialize_git_fixture(foundry_root: &Path) -> Result<()> {
    run_command("git", &["init", "--quiet"], foundry_root)?;
    run_command(
        "git",
        &["-c", "user.email=fixture@example.com", "-c", "user.name=Fixture", "add", "-f", "."],
        foundry_root,
    )?;
    run_command(
        "git",
        &[
            "-c",
            "user.email=fixture@example.com",
            "-c",
            "user.name=Fixture",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
        foundry_root,
    )?;
    Ok(())
}

fn run_rindexer(binary_path: &str, current_dir: &Path, args: &[&str]) -> Result<String> {
    let binary = resolve_binary_path(binary_path)?;
    run_command(path_str(&binary)?, args, current_dir)
}

fn resolve_binary_path(binary_path: &str) -> Result<PathBuf> {
    let path = PathBuf::from(binary_path);
    let path = if path.is_absolute() { path } else { std::env::current_dir()?.join(path) };
    path.canonicalize()
        .with_context(|| format!("could not resolve rindexer binary at {}", path.display()))
}

fn run_command(program: &str, args: &[&str], current_dir: &Path) -> Result<String> {
    let output =
        Command::new(program).args(args).current_dir(current_dir).output().with_context(|| {
            format!("failed to run `{program} {}` in {}", args.join(" "), current_dir.display())
        })?;

    if !output.status.success() {
        bail!(
            "`{program} {}` failed in {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            current_dir.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn assert_generated_project(output_path: &Path, expected_source_kind: &str) -> Result<Value> {
    let yaml_path = output_path.join("rindexer.yaml");
    let origin_path = output_path.join("rindexer-foundry.json");
    let abi_path = output_path.join("abis/Emitter.abi.json");

    if !yaml_path.exists() {
        bail!("expected generated rindexer.yaml at {}", yaml_path.display());
    }
    if !origin_path.exists() {
        bail!("expected generated rindexer-foundry.json at {}", origin_path.display());
    }
    if !abi_path.exists() {
        bail!("expected copied Emitter ABI at {}", abi_path.display());
    }
    if output_path.join("abis/NoEvents.abi.json").exists() {
        bail!("NoEvents ABI should not be copied because it has no events");
    }

    let yaml: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(&yaml_path)?)?;
    let networks = yaml
        .get("networks")
        .and_then(serde_yaml::Value::as_sequence)
        .context("generated YAML missing networks")?;
    let chain_31337 = networks
        .iter()
        .find(|network| network.get("chain_id").and_then(serde_yaml::Value::as_u64) == Some(31337))
        .context("generated YAML missing chain 31337 network")?;
    if chain_31337.get("name").and_then(serde_yaml::Value::as_str) != Some("anvil") {
        bail!("chain 31337 should be named anvil");
    }
    if chain_31337.get("rpc").and_then(serde_yaml::Value::as_str) != Some("${ANVIL_RPC_URL}") {
        bail!("chain 31337 should use ANVIL_RPC_URL placeholder");
    }

    let storage = yaml
        .get("storage")
        .and_then(|storage| storage.get("postgres"))
        .and_then(|postgres| postgres.get("enabled"))
        .and_then(serde_yaml::Value::as_bool);
    if storage != Some(true) {
        bail!("generated YAML should enable Postgres storage");
    }

    let origin: Value = serde_json::from_str(&std::fs::read_to_string(&origin_path)?)?;
    if origin.get("version").and_then(Value::as_u64) != Some(1) {
        bail!("origin version should be 1");
    }
    if origin.get("source").and_then(|source| source.get("kind")).and_then(Value::as_str)
        != Some(expected_source_kind)
    {
        bail!("origin source kind should be {expected_source_kind}");
    }
    if origin.get("foundry").and_then(|foundry| foundry.get("out")).and_then(Value::as_str)
        != Some("out")
    {
        bail!("origin should store Foundry out setting");
    }
    if origin.get("foundry").and_then(|foundry| foundry.get("broadcast")).and_then(Value::as_str)
        != Some("broadcast")
    {
        bail!("origin should store Foundry broadcast setting");
    }

    let managed = origin
        .get("managed_contracts")
        .and_then(Value::as_array)
        .context("origin missing managed_contracts")?;
    if managed.len() != 1 {
        bail!("expected exactly one managed event-bearing contract, got {}", managed.len());
    }
    if managed[0].get("name").and_then(Value::as_str) != Some("Emitter") {
        bail!("expected Emitter to be the managed contract");
    }

    Ok(origin)
}

fn assert_yaml_event_names(output_path: &Path, expected: &[&str]) -> Result<()> {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(output_path.join("rindexer.yaml"))?)?;
    let contracts = yaml
        .get("contracts")
        .and_then(serde_yaml::Value::as_sequence)
        .context("generated YAML missing contracts")?;
    let emitter = contracts
        .iter()
        .find(|contract| {
            contract.get("name").and_then(serde_yaml::Value::as_str) == Some("Emitter")
        })
        .context("generated YAML missing Emitter contract")?;
    let event_names = emitter
        .get("include_events")
        .and_then(serde_yaml::Value::as_sequence)
        .context("Emitter missing include_events")?
        .iter()
        .filter_map(|event| event.get("name").and_then(serde_yaml::Value::as_str))
        .collect::<Vec<_>>();

    if event_names != expected {
        bail!("expected YAML events {:?}, got {:?}", expected, event_names);
    }

    Ok(())
}

fn assert_abi_event_names(output_path: &Path, expected: &[&str]) -> Result<()> {
    let abi: Value =
        serde_json::from_str(&std::fs::read_to_string(output_path.join("abis/Emitter.abi.json"))?)?;
    let mut event_names = abi
        .as_array()
        .context("copied ABI should be an array")?
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("event"))
        .filter_map(|item| item.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();
    event_names.sort();

    let mut expected = expected.to_vec();
    expected.sort();
    if event_names != expected {
        bail!("expected ABI events {:?}, got {:?}", expected, event_names);
    }

    Ok(())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str().with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}
