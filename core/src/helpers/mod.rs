mod thread;

pub use thread::set_thread_no_logging;

mod file;
pub use file::{
    create_mod_file, format_all_files_for_project, write_file, CreateModFileError, WriteFileError,
};

use dotenv::dotenv;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::env;
use std::env::VarError;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str;

pub fn camel_to_snake(s: &str) -> String {
    let mut snake_case = String::new();
    let mut previous_was_uppercase = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_alphanumeric() || c == '_' {
            if c.is_uppercase() {
                // Insert an underscore if it's not the first character and the previous character wasn't uppercase
                if i > 0
                    && (!previous_was_uppercase
                        || (i + 1 < s.len()
                            && s.chars()
                                .nth(i + 1)
                                .expect("Failed to get char")
                                .is_lowercase()))
                {
                    snake_case.push('_');
                }
                snake_case.push(c.to_ascii_lowercase());
                previous_was_uppercase = true;
            } else {
                snake_case.push(c);
                previous_was_uppercase = false;
            }
        }
    }

    snake_case
}

pub fn generate_random_id(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn get_full_path(project_path: &Path, file_path: &str) -> PathBuf {
    let path = PathBuf::from(file_path);
    if let Ok(canonical_path) = path.canonicalize() {
        canonical_path
    } else {
        let joined_path = project_path.join(file_path);
        joined_path
            .canonicalize()
            .expect("Failed to canonicalize path")
    }
}

pub fn kill_process_on_port(port: u16) -> Result<(), String> {
    // Use lsof to find the process using the port
    let output = Command::new("lsof")
        .arg(format!("-i:{}", port))
        .arg("-t")
        .output()
        .map_err(|e| e.to_string())?;

    let pids = str::from_utf8(&output.stdout)
        .map_err(|e| e.to_string())?
        .lines()
        .collect::<Vec<&str>>();

    for pid in pids {
        // Kill each process using the port
        Command::new("kill")
            .arg("-9")
            .arg(pid)
            .output()
            .map_err(|e| e.to_string())?;
    }

    Ok(())
}

pub fn public_read_env_value(var_name: &str) -> Result<String, VarError> {
    dotenv().ok();
    env::var(var_name)
}

pub fn replace_env_variable_to_raw_name(rpc: &str) -> String {
    if rpc.starts_with("${") && rpc.ends_with('}') {
        rpc[2..rpc.len() - 1].to_string()
    } else {
        rpc.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("CamelCase"), "camel_case");
        assert_eq!(camel_to_snake("Camel-Case"), "camel_case");
        assert_eq!(camel_to_snake("camelCase"), "camel_case");
        assert_eq!(camel_to_snake("camel_case"), "camel_case");
        assert_eq!(camel_to_snake("Camel"), "camel");
        assert_eq!(camel_to_snake("camel"), "camel");
        assert_eq!(camel_to_snake("collectNFTId"), "collect_nft_id");
        assert_eq!(camel_to_snake("ERC20"), "erc20");
    }
}
