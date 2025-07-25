use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    let graphql_dir = manifest_dir.join("../graphql");

    // Verify GraphQL directory exists before proceeding
    if !graphql_dir.exists() {
        println!("cargo:warning=GraphQL directory not found, skipping GraphQL binary build");
        return;
    }

    // Check for Node.js and npm before attempting to use them
    check_node_availability();

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let resources_dir = out_dir.join("resources");

    // Ensure the resources directory exists
    fs::create_dir_all(&resources_dir)
        .unwrap_or_else(|e| panic!("Failed to create resources directory: {}", e));

    let target_info = get_target_info();
    let final_exe_path = resources_dir.join(&target_info.exe_name);

    // Check if we need to build
    if should_rebuild(&final_exe_path, &graphql_dir) {
        // Remove old binary if it exists
        if final_exe_path.exists() {
            if let Err(e) = fs::remove_file(&final_exe_path) {
                println!(
                    "cargo:warning=Failed to remove existing binary: {}. Continuing with build.",
                    e
                );
            }
        }

        println!(
            "cargo:warning=Building GraphQL binary for {}-{}...",
            target_info.os, target_info.arch
        );

        build_graphql_binary(&graphql_dir, &final_exe_path, &target_info);
    } else {
        println!("cargo:warning=GraphQL binary is up to date, skipping build");
        // Still set the environment variable for the existing binary
        println!("cargo:rustc-env=RINDEXER_GRAPHQL_EXE={}", final_exe_path.display());
    }

    // Register build dependencies
    register_build_dependencies(&manifest_dir);
}

fn should_rebuild(exe_path: &Path, graphql_dir: &Path) -> bool {
    // If binary doesn't exist, rebuild
    if !exe_path.exists() {
        println!("cargo:warning=Binary doesn't exist, rebuilding");
        return true;
    }

    // Check if package-lock.json is newer than the binary (dependencies changed)
    let package_lock = graphql_dir.join("package-lock.json");
    if package_lock.exists() {
        if let (Ok(exe_time), Ok(lock_time)) = (
            exe_path.metadata().and_then(|m| m.modified()),
            package_lock.metadata().and_then(|m| m.modified()),
        ) {
            if lock_time > exe_time {
                println!("cargo:warning=package-lock.json is newer than binary, rebuilding");
                return true;
            }
        }
    }

    // Check if any main source files are newer than the binary
    let source_files = ["index.js", "package.json"];
    if let Ok(exe_time) = exe_path.metadata().and_then(|m| m.modified()) {
        for source_file in &source_files {
            let source_path = graphql_dir.join(source_file);
            if source_path.exists() {
                if let Ok(source_time) = source_path.metadata().and_then(|m| m.modified()) {
                    if source_time > exe_time {
                        println!(
                            "cargo:warning=Source file {} is newer than binary, rebuilding",
                            source_file
                        );
                        return true;
                    }
                }
            }
        }
    }

    false
}

struct TargetInfo {
    os: String,
    arch: String,
    exe_name: String,
    pkg_target: String,
}

fn get_target_info() -> TargetInfo {
    let os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not set");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH not set");

    let node_arch = match arch.as_str() {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => panic!("Unsupported architecture: {}. Supported: x86_64, aarch64", arch),
    };

    let pkg_os = if os == "windows" { "win".to_string() } else { os.clone() };
    // Using Node.js v22 as it's the current LTS version.
    let pkg_target = format!("node22-{}-{}", pkg_os, node_arch);

    let exe_suffix = if os == "windows" { ".exe" } else { "" };
    let exe_name = format!("rindexer-graphql-{}-{}{}", os, node_arch, exe_suffix);

    TargetInfo { os: os.clone(), arch: node_arch.to_string(), exe_name, pkg_target }
}

fn check_node_availability() {
    let node_check = Command::new("node").arg("--version").output();

    match node_check {
        Ok(output) if output.status.success() => {
            let version = String::from_utf8_lossy(&output.stdout);
            println!("cargo:warning=Found Node.js version: {}", version.trim());
        }
        Ok(_) => {
            panic!("Node.js is installed but not working properly. Please reinstall Node.js.");
        }
        Err(_) => {
            panic!(
                "Node.js is not installed or not in your PATH. \
                Please install Node.js (LTS version is recommended) to build the GraphQL server.\
                \nVisit: https://nodejs.org/"
            );
        }
    }

    // Check npm
    if Command::new("npm").arg("--version").output().is_err() {
        panic!("npm is not available. Please ensure npm is installed with Node.js.");
    }
}

fn build_graphql_binary(graphql_dir: &Path, final_exe_path: &Path, target_info: &TargetInfo) {
    // 1. Install npm dependencies
    run_command(
        "npm",
        &["install"],
        graphql_dir,
        "npm install failed. Please ensure package.json is valid.",
    );

    // 2. Build the binary using pkg, passing the final output path
    run_command(
        "npm",
        &[
            "run",
            "build",
            "--",
            "--targets",
            &target_info.pkg_target,
            "--output",
            final_exe_path.to_str().expect("Invalid final_exe_path"),
        ],
        graphql_dir,
        "npm run build failed. Check the build script in package.json.",
    );

    if !final_exe_path.exists() {
        panic!("Build did not produce the expected binary: {}", final_exe_path.display());
    }

    println!("cargo:warning=Successfully built GraphQL binary: {}", final_exe_path.display());
    println!("cargo:rustc-env=RINDEXER_GRAPHQL_EXE={}", final_exe_path.display());
}

fn register_build_dependencies(manifest_dir: &Path) {
    // Watch for changes in the graphql directory, which contains the node project.
    let graphql_dir = manifest_dir.join("../graphql");
    println!("cargo:rerun-if-changed={}", graphql_dir.display());

    // Also explicitly watch the package-lock.json to ensure dependency changes trigger a rebuild.
    let lock_file = graphql_dir.join("package-lock.json");
    if lock_file.exists() {
        println!("cargo:rerun-if-changed={}", lock_file.display());
    }

    // Watch for changes to this build script itself
    println!("cargo:rerun-if-changed=build.rs");
}

/// Executes a command with enhanced error reporting and validation.
fn run_command(command: &str, args: &[&str], cwd: &Path, error_msg: &str) {
    let output = Command::new(command).args(args).current_dir(cwd).output().unwrap_or_else(|e| {
        panic!(
            "Failed to execute command '{}': {}\nCWD: {}\nError: {}",
            command,
            args.join(" "),
            cwd.display(),
            e
        )
    });

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        panic!(
            "{}\
            ╭─ Command Failed ─────────────────────────────────────\n\
            │ Command: {} {}\n\
            │ Working Directory: {}\n\
            │ Exit Status: {}\n\
            ├─ Stdout ────────────────────────────────────────────\n\
            │ {}\n\
            ├─ Stderr ────────────────────────────────────────────\n\
            │ {}\n\
            ╰──────────────────────────────────────────────────────",
            error_msg,
            command,
            args.join(" "),
            cwd.display(),
            output.status,
            if stdout.is_empty() { "(empty)" } else { stdout.trim() },
            if stderr.is_empty() { "(empty)" } else { stderr.trim() }
        );
    }
}
