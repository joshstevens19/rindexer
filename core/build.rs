use std::{
    env,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

// A list of source files for the GraphQL server. Centralized for easy management.
const GRAPHQL_SOURCE_FILES: &[&str] = &[
    "../graphql/index.js",
    "../graphql/package.json",
    "../graphql/package-lock.json",
    "../graphql/sea-config.json",
];

fn main() {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
    );
    
    let graphql_dir = manifest_dir.join("../graphql");
    let resource_dir = manifest_dir.join("resources");

    // Verify GraphQL directory exists
    if !graphql_dir.exists() {
        println!("cargo:warning=GraphQL directory not found, skipping GraphQL binary build");
        return;
    }

    // Check Node.js availability
    check_node_availability();

    // Create the resources directory if it doesn't exist
    fs::create_dir_all(&resource_dir)
        .unwrap_or_else(|e| panic!("Failed to create resource directory: {}", e));

    let target_info = get_target_info();
    let final_exe_path = resource_dir.join(&target_info.exe_name);

    // Clean up any leftover blob files
    cleanup_blob_files(&graphql_dir);

    // Only build if the binary doesn't already exist
    if !final_exe_path.exists() {
        println!("cargo:warning=Building GraphQL binary for {}-{}...", 
                 target_info.os, target_info.arch);
        
        build_graphql_binary(&graphql_dir, &final_exe_path);
    } else {
        println!("cargo:warning=GraphQL binary already exists, skipping build");
    }

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

    TargetInfo {
        os: os.clone(),
        arch: node_arch.to_string(),
        exe_name,
    }
}

fn check_node_availability() {
    let node_check = Command::new("node")
        .arg("--version")
        .output();

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

fn cleanup_blob_files(graphql_dir: &Path) {
    let blob_path = graphql_dir.join("rindexer-graphql.blob");
    if blob_path.exists() {
        if let Err(e) = fs::remove_file(&blob_path) {
            println!("cargo:warning=Failed to remove leftover blob file: {}", e);
        }
    }
}

fn build_graphql_binary(graphql_dir: &Path, final_exe_path: &Path) {
    // 1. Install npm dependencies
    run_command(
        "npm", 
        &["ci"],
        graphql_dir, 
        "npm ci failed. Please ensure package-lock.json is present."
    );

    // 2. Generate the blob for SEA
    run_command(
        "node",
        &["--experimental-sea-config", "sea-config.json"],
        graphql_dir,
        "Failed to generate SEA blob. Check sea-config.json and dependencies.",
    );

    // 3. Find Node.js executable
    let node_path = which::which("node")
        .expect("Node.js not found in PATH after successful version check");

    // 4. Copy the node executable to the final destination
    fs::copy(&node_path, final_exe_path)
        .unwrap_or_else(|e| panic!("Failed to copy node executable: {}", e));

    // 5. Set executable permissions on Unix-like systems
    #[cfg(unix)]
    set_executable_permissions(final_exe_path);

    // 6. Inject the blob into the copied executable
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
        graphql_dir,
        "postject failed to inject blob. Ensure postject is installed.",
    );

    println!("cargo:warning=Successfully built GraphQL binary: {}", 
             final_exe_path.display());
}

#[cfg(unix)]
fn set_executable_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
        .unwrap_or_else(|e| panic!("Failed to set executable permissions: {}", e));
}

fn register_build_dependencies(manifest_dir: &Path) {
    for path in GRAPHQL_SOURCE_FILES {
        let full_path = manifest_dir.join(path);
        if full_path.exists() {
            println!("cargo:rerun-if-changed={}", full_path.display());
        }
    }
    
    // Watch for changes to this build script itself
    println!("cargo:rerun-if-changed=build.rs");
}

/// Executes a command with enhanced error reporting and validation.
fn run_command(command: &str, args: &[&str], cwd: &Path, error_msg: &str) {
    let output = Command::new(command)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|e| {
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
