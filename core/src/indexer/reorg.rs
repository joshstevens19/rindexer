use alloy::primitives::{U256, U64};

pub fn reorg_safe_distance_for_chain(chain_id: &U256) -> U64 {
    if chain_id == &U256::from(1) {
        U64::from(12)
    } else {
        U64::from(64)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;

    use super::*;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = U256::from(1);
        assert_eq!(reorg_safe_distance_for_chain(&mainnet_chain_id), U64::from(12));

        let testnet_chain_id = U256::from(3);
        assert_eq!(reorg_safe_distance_for_chain(&testnet_chain_id), U64::from(64));

        let other_chain_id = U256::from(42);
        assert_eq!(reorg_safe_distance_for_chain(&other_chain_id), U64::from(64));
    }
}
