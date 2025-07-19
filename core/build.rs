use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command},
    time::SystemTime,
};

// A list of source files for the GraphQL server. If any of these change,
// we need to trigger a rebuild of the SEA binary.
const GRAPHQL_SOURCE_FILES: &[&str] = &[
    "../graphql/index.js",
    "../graphql/package.json",
    "../graphql/package-lock.json",
    "../graphql/sea-config.json",
];

fn main() {
    // 1. --- Setup Paths and Environment ---
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let graphql_dir = manifest_dir.join("../graphql");
    let resource_dir = manifest_dir.join("resources");

    // Tell Cargo to re-run this script if the source files change.
    for path in GRAPHQL_SOURCE_FILES {
        println!("cargo:rerun-if-changed={}", manifest_dir.join(path).display());
    }

    // Find Node.js and npm executables
    let node_path = which::which("node")
        .expect("Node.js is not installed or not in your PATH. Please install it to build the GraphQL server.");
    let npm_path = which::which("npm")
        .expect("npm is not installed or not in your PATH. It should be installed with Node.js.");

    // Determine target-specific executable name.
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

    // 2. --- Decide if a Rebuild is Necessary ---
    if should_rebuild(&final_exe_path, &manifest_dir) {
        println!("cargo:warning=GraphQL binary is missing or outdated. Building with SEA...");

        // Create the resources directory if it doesn't exist.
        fs::create_dir_all(&resource_dir).expect("Failed to create resource directory");

        // Clean up previous build artifact if it exists.
        let blob_path = graphql_dir.join("rindexer-graphql.blob");
        if blob_path.exists() {
            fs::remove_file(&blob_path).expect("Failed to remove leftover blob file");
        }
        
        // 3. --- Run the Build Steps ---
        
        // a. Install npm dependencies.
        run_command(
            &npm_path,
            &["install"],
            &graphql_dir,
            "'npm install' failed",
        );

        // b. Generate the SEA blob.
        run_command(
            &node_path,
            &["--experimental-sea-config", "sea-config.json"],
            &graphql_dir,
            "Failed to generate SEA blob",
        );

        // c. Copy the base node executable.
        fs::copy(&node_path, &final_exe_path).unwrap_or_else(|e| {
            panic!(
                "Failed to copy node executable from {:?} to {:?}: {}",
                node_path, final_exe_path, e
            )
        });

        // d. Inject the blob into the copied executable using postject.
        run_command(
            &npm_path,
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
        
        println!("cargo:warning=Successfully built GraphQL binary at: {}", final_exe_path.display());
    } else {
        println!("cargo:warning=GraphQL binary is up-to-date. Skipping build.");
    }
}

/// Determines if the SEA binary needs to be rebuilt.
///
/// A rebuild is necessary if:
/// 1. The final executable does not exist.
/// 2. Any of the GraphQL source files are newer than the existing executable.
fn should_rebuild(final_exe_path: &Path, manifest_dir: &Path) -> bool {
    if !final_exe_path.exists() {
        return true;
    }

    let exe_metadata = fs::metadata(final_exe_path).ok();
    let exe_mtime = exe_metadata.and_then(|m| m.modified().ok()).unwrap_or(SystemTime::UNIX_EPOCH);

    for &src_path_str in GRAPHQL_SOURCE_FILES {
        let src_path = manifest_dir.join(src_path_str);
        if let Ok(src_mtime) = fs::metadata(&src_path).and_then(|m| m.modified()) {
            if src_mtime > exe_mtime {
                println!(
                    "cargo:warning=Source file {} is newer than the binary. Rebuilding...",
                    src_path.display()
                );
                return true;
            }
        }
    }

    false
}

/// Executes a command and panics with detailed error information if it fails.
fn run_command(command: &Path, args: &[&str], cwd: &Path, error_msg: &str) {
    let output = Command::new(command)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|e| panic!("Failed to execute command '{:?}': {}", command, e));

    if !output.status.success() {
        panic!(
            "{}\n--- Command ---\n{} {}\n--- CWD ---\n{}\n--- Status ---\n{}\n--- Stdout ---\n{}\n--- Stderr ---\n{}",
            error_msg,
            command.display(),
            args.join(" "),
            cwd.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
