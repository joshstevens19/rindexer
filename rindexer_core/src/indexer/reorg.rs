use ethers::types::{U256, U64};

/// Returns the reorganization safe distance for a given blockchain.
///
/// The reorganization safe distance is the number of blocks considered safe to avoid chain reorganization issues.
/// For Ethereum Mainnet (chain ID 1), it is set to 12 blocks. For all other chains, it is set to 64 blocks.
///
/// # Arguments
///
/// * `chain_id` - A reference to the `U256` representing the chain ID.
///
/// # Returns
///
/// A `U64` representing the reorganization safe distance in blocks.
///
pub fn reorg_safe_distance_for_chain(chain_id: &U256) -> U64 {
    match chain_id.as_u64() {
        1 => U64::from(12),
        _ => U64::from(64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::U256;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = U256::from(1);
        assert_eq!(
            reorg_safe_distance_for_chain(&mainnet_chain_id),
            U64::from(12)
        );

        let testnet_chain_id = U256::from(3);
        assert_eq!(
            reorg_safe_distance_for_chain(&testnet_chain_id),
            U64::from(64)
        );

        let other_chain_id = U256::from(42);
        assert_eq!(
            reorg_safe_distance_for_chain(&other_chain_id),
            U64::from(64)
        );
    }
}
