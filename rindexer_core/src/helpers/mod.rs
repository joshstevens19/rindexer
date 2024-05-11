use ethers::types::{H256, U256};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::{error::Error, fs::File};

pub fn camel_to_snake(s: &str) -> String {
    let mut snake_case = String::new();
    let mut previous_was_uppercase = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            // Insert an underscore if it's not the first character and the previous character wasn't uppercase
            if i > 0
                && (!previous_was_uppercase
                    || (i + 1 < s.len() && s.chars().nth(i + 1).unwrap().is_lowercase()))
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

    snake_case
}

fn format_file(file_path: &str) {
    Command::new("rustfmt")
        .arg(file_path)
        .status()
        .expect("Failed to execute rustfmt.");
}

pub fn write_file(path: &str, contents: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new(path);
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }

    let mut file = File::create(path)?;
    file.write_all(contents.as_bytes())?;
    format_file(path.to_str().unwrap());
    Ok(())
}

pub fn create_mod_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let entries = fs::read_dir(path)?;

    let mut mods = Vec::new();
    let mut dirs = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                dirs.push(dir_name.to_owned());
                create_mod_file(&path)?;
            }
        } else if let Some(ext) = path.extension() {
            if ext == "rs" && path.file_stem().map_or(true, |s| s != "mod") {
                if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                    mods.push(file_stem.to_owned());
                }
            }
        }
    }

    if !mods.is_empty() || !dirs.is_empty() {
        let mod_path = path.join("mod.rs");
        let mut mod_file = File::create(mod_path)?;

        writeln!(mod_file, "#![allow(dead_code)]")?;

        for item in mods.iter().chain(dirs.iter()) {
            if item.contains("_abi_gen") {
                writeln!(mod_file, "mod {};", item)?;
            } else {
                writeln!(mod_file, "pub mod {};", item)?;
            }
        }
    }

    Ok(())
}

pub fn u256_to_hex(value: U256) -> String {
    format!("0x{:x}", value)
}

pub fn parse_hex(input: &str) -> H256 {
    // Normalize the input by removing the '0x' prefix if it exists.
    let normalized_input = if input.starts_with("0x") {
        &input[2..]
    } else {
        input
    };

    // Ensure the input has the correct length of 64 hex characters.
    if normalized_input.len() != 64 {
        panic!(
            "Failed to parse H256 from input '{}': Invalid input length",
            input
        );
    }

    // Add "0x" prefix and parse the input string.
    let formatted = format!("0x{}", normalized_input.to_lowercase());

    H256::from_str(&formatted).unwrap_or_else(|err| {
        panic!("Failed to parse H256 from input '{}': {:?}", input, err);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("CamelCase"), "camel_case");
        assert_eq!(camel_to_snake("camelCase"), "camel_case");
        assert_eq!(camel_to_snake("Camel"), "camel");
        assert_eq!(camel_to_snake("camel"), "camel");
        assert_eq!(camel_to_snake("collectNFTId"), "collect_nft_id");
        assert_eq!(camel_to_snake("ERC20"), "erc20");
    }

    #[test]
    fn test_parse_hex_valid() {
        let test_cases = [
            (
                "0x4a1a2197f307222cd67a1762d9a352f64558d9be",
                "0x4a1a2197f307222cd67a1762d9a352f64558d9be000000000000000000000000",
            ),
            (
                "4a1a2197f307222cd67a1762d9a352f64558d9be",
                "0x4a1a2197f307222cd67a1762d9a352f64558d9be000000000000000000000000",
            ),
            (
                "0X4A1A2197F307222CD67A1762D9A352F64558D9BE",
                "0x4a1a2197f307222cd67a1762d9a352f64558d9be000000000000000000000000",
            ),
            (
                "4A1A2197F307222CD67A1762D9A352F64558D9BE",
                "0x4a1a2197f307222cd67a1762d9a352f64558d9be000000000000000000000000",
            ),
        ];

        for (input, expected) in test_cases {
            let parsed = parse_hex(input);
            assert_eq!(format!("{:?}", parsed), expected);
        }
    }

    #[test]
    fn test_parse_hex_invalid_length() {
        let invalid_cases = ["0x12345", "0x4A1a2197f3", "123456789"];

        for input in &invalid_cases {
            let result = std::panic::catch_unwind(|| parse_hex(input));
            assert!(result.is_err(), "Expected panic for input: {}", input);
        }
    }

    #[test]
    fn test_parse_hex_non_hex_chars() {
        let invalid_cases = [
            "0xZZZ2197f307222cd67a1762d9a352f64558d9be",
            "GHIJKL",
            "0x4a1a2197-3072",
        ];

        for input in &invalid_cases {
            let result = std::panic::catch_unwind(|| parse_hex(input));
            assert!(result.is_err(), "Expected panic for input: {}", input);
        }
    }
}
