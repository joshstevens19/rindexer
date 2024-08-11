use alloy::primitives::ChainId;

pub fn reorg_safe_distance_for_chain(chain_id: &ChainId) -> ChainId {
    match chain_id {
        1 => 12,
        _ => 64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorg_safe_distance_for_chain() {
        let mainnet_chain_id = 1;
        assert_eq!(reorg_safe_distance_for_chain(&mainnet_chain_id), 12);

        let testnet_chain_id = 3;
        assert_eq!(reorg_safe_distance_for_chain(&testnet_chain_id), 64);

        let other_chain_id = 42;
        assert_eq!(reorg_safe_distance_for_chain(&other_chain_id), 64);
    }
}
