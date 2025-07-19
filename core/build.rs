use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    // time::SystemTime,
};

fn main() {
    // Check if Node.js is installed before doing anything else
    if Command::new("node").arg("--version").output().is_err() {
        panic!(
            "Node.js is not installed or not in your PATH. \
            Please install Node.js (LTS version is recommended) to build the GraphQL server."
        );
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    let graphql_dir = manifest_dir.join("../graphql");
    let resource_dir = manifest_dir.join("resources");

    // Create the resources directory if it doesn't exist
    fs::create_dir_all(&resource_dir).expect("Failed to create resource directory");

    // Detect OS and architecture
    let os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not set");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH not set");
    let node_arch = match arch.as_str() {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        _ => panic!("Unsupported architecture: {}", arch),
    };

    let exe_suffix = if os == "windows" { ".exe" } else { "" };
    let final_exe_name = format!("rindexer-graphql-{}-{}{}", os, node_arch, exe_suffix);
    let final_exe_path = resource_dir.join(&final_exe_name);

    let blob_path = graphql_dir.join("rindexer-graphql.blob");
    if blob_path.exists() {
        fs::remove_file(&blob_path).expect("Failed to remove leftover blob file");
    }

    // Only build if the binary doesn't already exist
    if !final_exe_path.exists() {
        println!("cargo:warning=GraphQL binary not found for host, building with SEA...");

        // 1. Ensure npm dependencies are installed
        run_command("npm", &["install"], &graphql_dir, "'npm install' failed");

        // 2. Generate the blob for SEA
        run_command(
            "node",
            &["--experimental-sea-config", "sea-config.json"],
            &graphql_dir,
            "Failed to generate SEA blob",
        );

        // 3. Determine the source Node.js executable to use
        let node_path = which::which("node").expect("Node.js not found in PATH");

        // 4. Copy the node executable to the final destination
        fs::copy(&node_path, &final_exe_path).expect("Failed to copy node executable");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&final_exe_path, fs::Permissions::from_mode(0o755))
                .expect("Failed to set executable permissions");
        }

        // 5. Inject the blob into the copied executable
        run_command(
            "npm",
            &[
                "run",
                "postject",
                "--",
                final_exe_path.to_str().unwrap(),
                "NODE_SEA_BLOB",
                "rindexer-graphql.blob",
                "--sentinel",
                "NODE_SEA_SENTINEL",
            ],
            &graphql_dir,
            "postject failed to inject blob",
        );
    }

    // Tell Cargo when to rerun the build script
    println!("cargo:rerun-if-changed=../graphql/index.js");
    println!("cargo:rerun-if-changed=../graphql/package.json");
    println!("cargo:rerun-if-changed=../graphql/package-lock.json");
    println!("cargo:rerun-if-changed=../graphql/sea-config.json");
}

/// Executes a command and panics with detailed error information if it fails.
fn run_command(command: &str, args: &[&str], cwd: &Path, error_msg: &str) {
    let output = Command::new(command)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|e| panic!("Failed to execute command '{}': {}", command, e));

    if !output.status.success() {
        panic!(
            "{}\n--- Command ---\n{} {}\n--- CWD ---\n{}\n--- Status ---\n{}\n--- Stdout ---\n{}\n--- Stderr ---\n{}",
            error_msg,
            command,
            args.join(" "),
            cwd.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
