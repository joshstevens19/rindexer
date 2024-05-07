use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::{error::Error, fs::File};

pub fn camel_to_snake(name: &str) -> String {
    let mut snake_case = String::new();
    let mut prev_char_was_upper = false;
    let mut current_char_is_upper = false;

    for (i, ch) in name.chars().enumerate() {
        current_char_is_upper = ch.is_uppercase();

        if current_char_is_upper
            && i > 0
            && (!prev_char_was_upper
                || (i < name.len() - 1 && !name.chars().nth(i + 1).unwrap().is_uppercase()))
        {
            snake_case.push('_');
        }

        snake_case.push(ch.to_lowercase().next().unwrap());
        prev_char_was_upper = current_char_is_upper;
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
