use colored::Colorize;
use regex::Regex;
use std::io;
use std::io::Write;
use std::str::FromStr;

pub fn print_error_message(error_message: &str) {
    println!("{}", error_message.red());
}

pub fn print_warn_message(error_message: &str) {
    println!("{}", error_message.yellow());
}

pub fn print_success_message(success_message: &str) {
    println!("{}", success_message.green());
}

pub fn prompt_for_optional_input<T: FromStr>(prompt: &str, pattern: Option<&str>) -> Option<T> {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    loop {
        print!("{} (skip by pressing Enter): ", prompt.yellow());
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return None;
        }

        if let Some(ref regex) = regex {
            if regex.is_match(trimmed) {
                match trimmed.parse::<T>() {
                    Ok(value) => return Some(value),
                    Err(_) => println!(
                        "{}",
                        "Invalid format. Please try again or press Enter to skip.".red()
                    ),
                }
            } else {
                println!(
                    "{}",
                    "Invalid input according to regex. Please try again or press Enter to skip."
                        .red()
                );
            }
        } else {
            match trimmed.parse::<T>() {
                Ok(value) => return Some(value),
                Err(_) => println!("{}", "Invalid format. Please try again.".red()),
            }
        }
    }
}

pub fn prompt_for_input_list(
    field_name: &str,
    options: &[String],
    current_value: Option<&str>,
) -> String {
    let options_str = options.join(", ");

    if let Some(value) = current_value {
        return value.to_string();
    }

    loop {
        print!(
            "{} [{}]: ",
            field_name.to_string().green(),
            options_str.yellow()
        );
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let trimmed = input.trim().to_lowercase();

        if options.contains(&trimmed) {
            return trimmed;
        } else {
            println!(
                "{}",
                format!(
                    "Invalid option. Please choose one of the following: {}",
                    options_str
                )
                .red()
            );
        }
    }
}

pub fn prompt_for_input(
    field_name: &str,
    pattern: Option<&str>,
    pattern_failure_message: Option<&str>,
    current_value: Option<&str>,
) -> String {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    match current_value {
        Some(value) => value.to_string(),
        None => loop {
            print!("{}: ", field_name.yellow());
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");
            let trimmed = input.trim();

            if let Some(ref regex) = regex {
                if regex.is_match(trimmed) {
                    return trimmed.to_string();
                } else {
                    let message = pattern_failure_message
                        .unwrap_or("Invalid input according to regex. Please try again.");
                    println!("{}", message.red());
                }
            } else if !trimmed.is_empty() {
                return trimmed.to_string();
            } else {
                println!("{}", "Input cannot be empty. Please try again.".red());
            }
        },
    }
}
