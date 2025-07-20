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

    // Clean up the old binary if it exists to ensure a fresh build
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

    build_graphql_binary(&graphql_dir, &final_exe_path);

    // Register build dependencies
    register_build_dependencies(&manifest_dir);
}

struct TargetInfo {
    os: String,
    arch: String,
    exe_name: String,
}

fn get_target_info() -> TargetInfo {
    let os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not set");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH not set");

    let node_arch = match arch.as_str() {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => panic!("Unsupported architecture: {}. Supported: x86_64, aarch64", arch),
    };

    let exe_suffix = if os == "windows" { ".exe" } else { "" };
    let exe_name = format!("rindexer-graphql-{}-{}{}", os, node_arch, exe_suffix);

    TargetInfo { os: os.clone(), arch: node_arch.to_string(), exe_name }
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

fn build_graphql_binary(graphql_dir: &Path, final_exe_path: &Path) {
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
            "{}\n\
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
