use std::fs;
use std::io::Write;
use std::path::Path;
use std::{error::Error, fs::File};

pub fn capitalize_first_letter(s: &str) -> String {
    s.chars()
        .enumerate()
        .map(|(i, c)| {
            if i == 0 {
                c.to_uppercase().to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

pub fn camel_to_snake(name: &str) -> String {
    let mut snake_case = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() && i != 0 {
            snake_case.push('_');
        }
        snake_case.push(ch.to_lowercase().next().unwrap());
    }
    snake_case
}

pub fn write_file(path: &str, contents: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new(path);
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }

    let mut file = File::create(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}
