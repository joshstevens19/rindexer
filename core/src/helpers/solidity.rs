/// Extracts the integer type and its size (bit width) from a Solidity type string.
/// Supports array variants (e.g., `uint256[]`, `int128[3]`).
/// Panics if the type is not a valid Solidity integer type.
pub fn parse_solidity_integer_type(solidity_type: &str) -> (&str, usize) {
    // Strip array suffix (e.g., "[]" or "[N]") if present
    let base_type = solidity_type.split('[').next().unwrap_or(solidity_type);
    match base_type {
        t if t.starts_with("int") => ("int", t[3..].parse().expect("Invalid intN type")),
        t if t.starts_with("uint") => ("uint", t[4..].parse().expect("Invalid uintN type")),
        _ => panic!("Invalid Solidity type: {solidity_type}"),
    }
}

const fn is_power_of_two(n: usize) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Checks if a Solidity type is an irregular integer type (with byte width that is not power of two).
/// Panics if the type is not a valid Solidity integer type.
pub fn is_irregular_width_solidity_integer_type(solidity_type: &str) -> bool {
    let (_, size) = parse_solidity_integer_type(solidity_type);

    !is_power_of_two(size)
}

/// Checks if a Solidity type is a static bytes type (e.g., `bytes32`, `bytes64`, etc.).
pub fn is_solidity_static_bytes_type(solidity_type: &str) -> bool {
    solidity_type.starts_with("bytes")
        && solidity_type.len() > 5
        && solidity_type[5..].chars().all(char::is_numeric)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Base types — sanity checks
    #[test]
    fn test_parse_uint256() {
        assert_eq!(parse_solidity_integer_type("uint256"), ("uint", 256));
    }

    #[test]
    fn test_parse_int128() {
        assert_eq!(parse_solidity_integer_type("int128"), ("int", 128));
    }

    // Array types — the fix: these would panic with ParseIntError before
    #[test]
    fn test_parse_uint256_dynamic_array() {
        assert_eq!(parse_solidity_integer_type("uint256[]"), ("uint", 256));
    }

    #[test]
    fn test_parse_int128_dynamic_array() {
        assert_eq!(parse_solidity_integer_type("int128[]"), ("int", 128));
    }

    #[test]
    fn test_parse_uint8_dynamic_array() {
        assert_eq!(parse_solidity_integer_type("uint8[]"), ("uint", 8));
    }

    #[test]
    fn test_parse_uint256_fixed_array() {
        assert_eq!(parse_solidity_integer_type("uint256[3]"), ("uint", 256));
    }

    #[test]
    fn test_parse_int64_fixed_array() {
        assert_eq!(parse_solidity_integer_type("int64[10]"), ("int", 64));
    }

    #[test]
    #[should_panic(expected = "Invalid Solidity type")]
    fn test_parse_invalid_type_panics() {
        parse_solidity_integer_type("string");
    }
}
