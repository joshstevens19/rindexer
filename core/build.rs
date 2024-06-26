use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");

    let resources_dir = PathBuf::from("resources");

    let target_dir = PathBuf::from(out_dir)
        .join("../../../..") // Navigate up to the top-level target directory
        .join("resources");

    if target_dir.exists() {
        fs::remove_dir_all(&target_dir).expect("Failed to remove old resources directory");
    }
    fs::create_dir_all(&target_dir).expect("Failed to create resources directory");
    for entry in fs::read_dir(resources_dir).expect("Failed to read resources directory") {
        let entry = entry.expect("Failed to read directory entry");
        let path = entry.path();
        if path.is_file() {
            fs::copy(
                &path,
                target_dir.join(path.file_name().expect("Failed to get file name")),
            )
            .expect("Failed to copy file");
        }
    }

    println!("cargo:rerun-if-changed=resources");
}
