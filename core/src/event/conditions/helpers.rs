//! Utility functions for evaluating expressions.

use super::{ast::ComparisonOperator, evaluation::EvaluationError};
use alloy::primitives::{I256, U256};
use std::str::FromStr;

/// Compares two addresses for equality, ignoring case and "0x" prefixes.
pub fn are_same_address(address1: &str, address2: &str) -> bool {
    normalize_address(address1) == normalize_address(address2)
}

/// Normalizes an address string by removing "0x" prefix and converting to lowercase.
pub fn normalize_address(address: &str) -> String {
    address.strip_prefix("0x").unwrap_or(address).replace(" ", "").to_lowercase()
}

/// Converts a string to a U256 value.
pub fn string_to_u256(value_str: &str) -> Result<U256, String> {
    let trimmed = value_str.trim();

    if trimmed.is_empty() {
        return Err("Input string is empty".to_string());
    }

    if let Some(hex_val) = trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X")) {
        // Hexadecimal parsing
        if hex_val.is_empty() {
            return Err("Hex string '0x' is missing value digits".to_string());
        }
        U256::from_str_radix(hex_val, 16)
            .map_err(|e| format!("Failed to parse hex '{hex_val}': {e}"))
    } else {
        // Decimal parsing
        U256::from_str(trimmed).map_err(|e| format!("Failed to parse decimal '{trimmed}': {e}"))
    }
}

/// Converts a string to an I256 value, handling decimal and hex formats.
pub fn string_to_i256(value_str: &str) -> Result<I256, String> {
    let trimmed = value_str.trim();
    if trimmed.is_empty() {
        return Err("Input string is empty".to_string());
    }

    if let Some(hex_val_no_sign) = trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X"))
    {
        if hex_val_no_sign.is_empty() {
            return Err("Hex string '0x' is missing value digits".to_string());
        }
        // Parse hex as U256 first
        U256::from_str_radix(hex_val_no_sign, 16)
            .map_err(|e| format!("Failed to parse hex magnitude '{hex_val_no_sign}': {e}"))
            .map(I256::from_raw)
    } else {
        I256::from_str(trimmed).map_err(|e| format!("Failed to parse decimal '{trimmed}': {e}"))
    }
}

/// Compares two values implementing the Ord trait using the specified comparison operator.
pub fn compare_ordered_values<T: Ord>(
    left: &T,
    op: &ComparisonOperator,
    right: &T,
) -> Result<bool, EvaluationError> {
    match op {
        ComparisonOperator::Eq => Ok(left == right),
        ComparisonOperator::Ne => Ok(left != right),
        ComparisonOperator::Gt => Ok(left > right),
        ComparisonOperator::Gte => Ok(left >= right),
        ComparisonOperator::Lt => Ok(left < right),
        ComparisonOperator::Lte => Ok(left <= right),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_to_u256() {
        // --- Helpers ---
        fn u256_hex_val(hex_str: &str) -> U256 {
            U256::from_str_radix(hex_str.strip_prefix("0x").unwrap_or(hex_str), 16).unwrap()
        }

        // --- Constants for testing ---
        const U256_MAX_STR: &str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        const U256_MAX_HEX_STR: &str =
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        const U256_OVERFLOW_STR: &str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639936";
        const U256_HEX_OVERFLOW_STR: &str =
            "0x10000000000000000000000000000000000000000000000000000000000000000";
        const ZERO_STR: &str = "0";
        const SMALL_NUM_STR: &str = "123";
        const SMALL_NUM_HEX_STR: &str = "0x7b"; // 123 in hex

        // --- Valid numbers cases ---
        assert_eq!(string_to_u256(ZERO_STR), Ok(U256::ZERO));
        assert_eq!(string_to_u256(SMALL_NUM_STR), Ok(U256::from_str(SMALL_NUM_STR).unwrap()));
        assert_eq!(string_to_u256(U256_MAX_STR), Ok(U256::MAX));

        // --- Valid hex cases ---
        assert_eq!(string_to_u256("0x0"), Ok(U256::ZERO));
        assert_eq!(string_to_u256("0X0"), Ok(U256::ZERO)); // Case insensitive
        assert_eq!(string_to_u256(SMALL_NUM_HEX_STR), Ok(u256_hex_val(SMALL_NUM_HEX_STR)));
        assert_eq!(string_to_u256(U256_MAX_HEX_STR), Ok(U256::MAX));

        // --- Invalid cases ---
        assert!(string_to_u256("").is_err());
        assert!(string_to_u256("   ").is_err());
        assert!(string_to_u256("0x").is_err());
        assert!(string_to_u256("abc").is_err());
        assert!(string_to_u256("-123").is_err());
        assert!(string_to_u256(U256_OVERFLOW_STR).is_err());
        assert!(string_to_u256(U256_HEX_OVERFLOW_STR).is_err());
    }

    #[test]
    fn test_string_to_i256() {
        // --- Constants for testing ---
        const I256_MAX_STR: &str =
            "57896044618658097711785492504343953926634992332820282019728792003956564819967";
        const I256_MAX_HEX_STR: &str =
            "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        const I256_MIN_STR: &str =
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
        const I256_MIN_HEX_STR: &str =
            "0x8000000000000000000000000000000000000000000000000000000000000000";
        const I256_POS_OVERFLOW_STR: &str =
            "57896044618658097711785492504343953926634992332820282019728792003956564819968";
        const I256_NEG_OVERFLOW_STR: &str =
            "-57896044618658097711785492504343953926634992332820282019728792003956564819969";
        const I256_HEX_OVERFLOW_STR: &str =
            "0x10000000000000000000000000000000000000000000000000000000000000000";

        // --- Valid numbers cases ---
        assert_eq!(string_to_i256("0"), Ok(I256::ZERO));
        assert_eq!(string_to_i256("123"), Ok(I256::from_str("123").unwrap()));
        assert_eq!(string_to_i256(I256_MAX_STR), Ok(I256::MAX));
        assert_eq!(string_to_i256(I256_MIN_STR), Ok(I256::MIN));
        assert_eq!(string_to_i256("-123"), Ok(I256::from_str("-123").unwrap()));
        assert_eq!(string_to_i256("-0"), Ok(I256::ZERO));

        // --- Valid hex cases ---
        assert_eq!(string_to_i256("0x0"), Ok(I256::ZERO));
        assert_eq!(string_to_i256("0X0"), Ok(I256::ZERO)); // Case insensitive
        assert_eq!(string_to_i256(I256_MAX_HEX_STR), Ok(I256::MAX));
        assert_eq!(string_to_i256(I256_MIN_HEX_STR), Ok(I256::MIN));

        // --- Invalid cases ---
        assert!(string_to_i256("").is_err());
        assert!(string_to_i256("   ").is_err());
        assert!(string_to_i256("0x").is_err());
        assert!(string_to_i256("abc").is_err());
        assert!(string_to_i256("-abc").is_err());
        assert!(string_to_i256(I256_POS_OVERFLOW_STR).is_err());
        assert!(string_to_i256(I256_NEG_OVERFLOW_STR).is_err());
        assert!(string_to_i256(I256_HEX_OVERFLOW_STR).is_err());
    }

    #[test]
    fn test_are_same_address() {
        assert!(are_same_address(
            "0x0123456789abcdef0123456789abcdef01234567",
            "0x0123456789ABCDEF0123456789ABCDEF01234567"
        ));
        assert!(are_same_address(
            "0123456789abcdef0123456789abcdef01234567",
            "0x0123456789abcdef0123456789abcdef01234567"
        ));
        assert!(!are_same_address(
            "0x0123456789abcdef0123456789abcdef01234567",
            "0x0123456789abcdef0123456789abcdef01234568"
        ));
    }

    #[test]
    fn test_normalize_address() {
        assert_eq!(
            normalize_address("0x0123456789ABCDEF0123456789ABCDEF01234567"),
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(
            normalize_address("0123456789ABCDEF0123456789ABCDEF01234567"),
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(
            normalize_address("0x0123456789abcdef 0123456789abcdef01234567"),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    #[test]
    fn test_compare_ordered_values_integers() {
        assert!(compare_ordered_values(&5, &ComparisonOperator::Eq, &5).unwrap());
        assert!(compare_ordered_values(&10, &ComparisonOperator::Gt, &5).unwrap());
        assert!(compare_ordered_values(&5, &ComparisonOperator::Lt, &10).unwrap());
        assert!(compare_ordered_values(&5, &ComparisonOperator::Gte, &5).unwrap());
        assert!(compare_ordered_values(&5, &ComparisonOperator::Lte, &5).unwrap());
        assert!(compare_ordered_values(&5, &ComparisonOperator::Ne, &10).unwrap());
    }
}
