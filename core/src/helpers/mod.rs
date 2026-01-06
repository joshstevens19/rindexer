mod thread;

pub use thread::set_thread_no_logging;

mod array;
mod evm_log;
mod file;
mod solidity;

pub use array::chunk_hashset;
pub use evm_log::{
    halved_block_number, is_relevant_block, map_log_params_to_raw_values, parse_log, parse_topic,
};
pub use solidity::{
    is_irregular_width_solidity_integer_type, is_solidity_static_bytes_type,
    parse_solidity_integer_type,
};
use std::{
    env,
    env::VarError,
    path::{Path, PathBuf},
    str,
};

use dotenv::dotenv;
pub use file::{
    create_mod_file, format_all_files_for_project, load_env_from_full_path,
    load_env_from_project_path, write_file, CreateModFileError, WriteFileError,
};
use rand::{distr::Alphanumeric, Rng};

mod duration;
pub use duration::format_duration;

pub fn camel_to_snake(s: &str) -> String {
    camel_to_snake_advanced(s, false)
}

pub fn camel_to_snake_advanced(s: &str, numbers_attach_to_last_word: bool) -> String {
    let mut snake_case = String::new();
    let mut previous_was_uppercase = false;
    let mut previous_was_digit = false;
    let mut uppercase_sequence_length = 0;

    for (i, c) in s.chars().enumerate() {
        if c.is_alphanumeric() || c == '_' {
            if c.is_uppercase() {
                if i > 0
                    && (!previous_was_uppercase
                        || (i + 1 < s.len()
                            && s.chars().nth(i + 1).expect("Failed to get char").is_lowercase()))
                {
                    snake_case.push('_');
                }
                snake_case.push(c.to_ascii_lowercase());
                previous_was_uppercase = true;
                previous_was_digit = false;
                uppercase_sequence_length += 1;
            } else if c.is_ascii_digit() {
                if !numbers_attach_to_last_word
                    && i > 0
                    && !previous_was_digit
                    && !snake_case.ends_with('_')
                    && uppercase_sequence_length != 1
                {
                    snake_case.push('_');
                }
                snake_case.push(c);
                previous_was_uppercase = false;
                previous_was_digit = true;
                uppercase_sequence_length = 0;
            } else {
                snake_case.push(c);
                previous_was_uppercase = false;
                previous_was_digit = false;
                uppercase_sequence_length = 0;
            }
        }
    }

    snake_case
}

pub fn to_pascal_case(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    // Events like Erc1155 "URI" Event are capitalized and need to remain that way. Return it as it
    // is.
    if !input.contains("_") && input.chars().filter(|c| c.is_ascii_lowercase()).count() == 0 {
        return input.to_string();
    }

    let words: Vec<&str> = input.split('_').filter(|s| !s.is_empty()).collect();
    let mut result = String::with_capacity(input.len());

    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            result.push('_');
        }
        result.push_str(&capitalize_word(word, i == 0 && words.len() == 1));
    }

    result.replace('_', "")
}

fn capitalize_word(word: &str, is_single_word: bool) -> String {
    if word.chars().all(|c| c.is_ascii_uppercase()) {
        if is_single_word {
            // Convert single all-uppercase word to Pascal case
            let mut chars = word.chars();
            return chars.next().unwrap().to_string() + &chars.as_str().to_lowercase();
        } else {
            // Preserve acronyms in compound words
            return word.to_string();
        }
    }

    let mut result = String::with_capacity(word.len());
    let mut chars = word.chars();

    // Capitalize the first character
    if let Some(first) = chars.next() {
        result.extend(first.to_uppercase());
    }

    let mut prev_is_upper = false;
    for c in chars {
        if c.is_ascii_uppercase() {
            if !prev_is_upper {
                result.push('_');
            }
            result.extend(c.to_uppercase());
            prev_is_upper = true;
        } else {
            result.extend(c.to_lowercase());
            prev_is_upper = false;
        }
    }

    result
}

pub fn generate_random_id(len: usize) -> String {
    rand::rng().sample_iter(&Alphanumeric).take(len).map(char::from).collect()
}

pub fn get_full_path(project_path: &Path, file_path: &str) -> Result<PathBuf, std::io::Error> {
    let path = PathBuf::from(file_path);
    if let Ok(canonical_path) = path.canonicalize() {
        Ok(canonical_path)
    } else {
        let joined_path = project_path.join(file_path);
        joined_path.canonicalize()
    }
}

pub fn kill_process_on_port(port: u16) -> Result<(), String> {
    port_killer::kill(port).map(|_| ()).map_err(|e| e.to_string())
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

// Remove this if we're sure we no longer need it after the Alloy migration
//
// pub fn u256_to_i256(value: U256) -> I256 {
//     let max_i256_as_u256 = U256::from_str_radix(
//         "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
//         16,
//     )
//     .unwrap();
//
//     if value <= max_i256_as_u256 {
//         // If the value is less than or equal to I256::MAX, it's a positive number
//         I256::from_raw(value)
//     } else {
//         // If it's larger, it represents a negative number in two's complement
//         let twos_complement = (!value).overflowing_add(U256::ONE).0;
//         I256::from_raw(twos_complement).wrapping_neg()
//     }
// }

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
        assert_eq!(camel_to_snake("ERC20"), "erc_20");
        assert_eq!(camel_to_snake("arg1"), "arg_1");
        assert_eq!(camel_to_snake("sqrtPriceX96"), "sqrt_price_x96");

        assert_eq!(camel_to_snake_advanced("ERC20", false), "erc_20");
        assert_eq!(camel_to_snake_advanced("ERC20", true), "erc20");
    }

    #[test]
    fn test_underscore_separated() {
        assert_eq!(to_pascal_case("user_profile_update"), "UserProfileUpdate");
        assert_eq!(to_pascal_case("get_user_by_id"), "GetUserById");
    }

    #[test]
    fn test_already_pascal_case() {
        assert_eq!(to_pascal_case("UserProfile"), "UserProfile");
        assert_eq!(to_pascal_case("GetUserById"), "GetUserById");
    }

    #[test]
    fn test_mixed_case() {
        assert_eq!(to_pascal_case("getUserProfile"), "GetUserProfile");
        assert_eq!(to_pascal_case("userProfileUpdate"), "UserProfileUpdate");
    }

    #[test]
    fn test_with_numbers() {
        assert_eq!(to_pascal_case("user123_profile"), "User123Profile");
        assert_eq!(to_pascal_case("get_user_2_by_id"), "GetUser2ById");
    }

    #[test]
    fn test_with_acronyms() {
        assert_eq!(to_pascal_case("ETH_USD_price"), "ETHUSDPrice");
        assert_eq!(to_pascal_case("http_request_handler"), "HttpRequestHandler");
    }

    #[test]
    fn test_single_word() {
        assert_eq!(to_pascal_case("user"), "User");
        assert_eq!(to_pascal_case("CONSTANT"), "CONSTANT");
        assert_eq!(to_pascal_case("URI"), "URI");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn test_multiple_underscores() {
        assert_eq!(to_pascal_case("user__profile___update"), "UserProfileUpdate");
    }
}
