/// Extracts the integer type and its size (bit width) from a Solidity type string.
/// Panics if the type is not a valid Solidity integer type.
pub fn parse_solidity_integer_type(solidity_type: &str) -> (&str, usize) {
    match solidity_type {
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
