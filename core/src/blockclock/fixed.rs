use alloy_chains::Chain;
use alloy_chains::NamedChain;

#[allow(unused)]
#[derive(Debug)]
pub enum SpacedNetwork {
    Base(Chain),
    Blast(Chain),
    Soneium(Chain),
    Worldchain(Chain),
}

#[allow(unused)]
impl SpacedNetwork {
    /// The genesis unix timestamp for the network (the zero block).
    pub fn genesis_time(&self) -> u64 {
        match &self {
            SpacedNetwork::Base(_) => 1686789347,
            SpacedNetwork::Blast(_) => 1708809815,
            SpacedNetwork::Soneium(_) => 1733134751,
            SpacedNetwork::Worldchain(_) => 1719335639,
        }
    }

    /// Get the blocktime for a chain if present
    pub fn inner(&self) -> &Chain {
        match &self {
            SpacedNetwork::Base(a)
            | SpacedNetwork::Blast(a)
            | SpacedNetwork::Soneium(a)
            | SpacedNetwork::Worldchain(a) => a,
        }
    }

    /// Get the blocktime for a chain if present
    pub fn block_spacing(&self) -> Option<u64> {
        self.inner().average_blocktime_hint().map(|b| b.as_secs())
    }

    /// The maximum block for which we are sure a consistent-spacing holds true.
    pub fn max_safe_block(&self) -> u64 {
        match &self {
            SpacedNetwork::Base(_) => 32853624,
            SpacedNetwork::Blast(_) => 21843498,
            SpacedNetwork::Soneium(_) => 9746802,
            SpacedNetwork::Worldchain(_) => 16647415,
        }
    }

    /// Get the timestamp for a block in the consistently spaced network.
    ///
    /// Will return [`None`] if the block time cannot be safely or accurately calculated, in which
    /// case it is up to the caller to find the timestamp with an alternate method.
    pub fn get_block_time(&self, block: u64) -> Option<u64> {
        if let Some(spacing) = &self.block_spacing() {
            let start = &self.genesis_time();
            let block_time = start + spacing * block;
            if block <= self.max_safe_block() {
                return Some(block_time);
            }
        }
        None
    }
}

impl TryFrom<&Chain> for SpacedNetwork {
    type Error = String;

    fn try_from(value: &Chain) -> Result<Self, Self::Error> {
        match value.named() {
            Some(NamedChain::Base) => Ok(SpacedNetwork::Base(value.to_owned())),
            Some(NamedChain::Blast) => Ok(SpacedNetwork::Blast(value.to_owned())),
            Some(NamedChain::Soneium) => Ok(SpacedNetwork::Soneium(value.to_owned())),
            Some(NamedChain::World) => Ok(SpacedNetwork::Worldchain(value.to_owned())),
            _ => Err(format!("{:?} is not a spaced network", value)),
        }
    }
}

impl TryFrom<NamedChain> for SpacedNetwork {
    type Error = String;

    fn try_from(value: NamedChain) -> Result<Self, Self::Error> {
        SpacedNetwork::try_from(&Chain::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::create_client;
    use alloy::primitives::U64;
    use rand::Rng;
    use std::collections::HashMap;

    /// A generic function to help with spot-checking random block numbers against timestamps.
    ///
    /// Each provider has different limits on their free-tiers for batch-sizes, so we include a
    /// "runs" property to control the number of blocks we check in a single test.
    async fn check_block_times(rpc: &str, runs: usize, network: SpacedNetwork) {
        let mut blocks = Vec::with_capacity(runs);

        for _ in 0..runs {
            blocks.push(rand::rng().random_range(1u64..=network.max_safe_block()));
        }

        let blocks_req = blocks.iter().map(|&n| U64::from(n)).collect::<Vec<_>>();
        let block_time = create_client(
            rpc,
            network.inner().id(),
            None,
            None,
            None,
            Default::default(),
            None,
            None,
        )
        .await
        .unwrap()
        .get_block_by_number_batch(&blocks_req, false)
        .await
        .unwrap()
        .into_iter()
        .map(|t| (t.header.number, t.header.timestamp))
        .collect::<HashMap<_, _>>();

        for (k, v) in block_time {
            let time = network.get_block_time(k).unwrap_or_else(|| {
                panic!("{:?}: Missing expected time for block {}", network, k);
            });
            assert_eq!(v, time, "{:?}: Mismatch for block {}", network, k);
        }
    }

    #[tokio::test]
    async fn base_block_time() {
        check_block_times(
            "https://base.llamarpc.com",
            25,
            SpacedNetwork::try_from(NamedChain::Base).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn blast_block_time() {
        check_block_times(
            "https://rpc.ankr.com/blast",
            10,
            SpacedNetwork::try_from(NamedChain::Blast).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn soneium_block_time() {
        check_block_times(
            "https://rpc.soneium.org",
            10,
            SpacedNetwork::try_from(NamedChain::Soneium).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn worldchain_block_time() {
        check_block_times(
            "https://worldchain-mainnet.gateway.tenderly.co",
            10,
            SpacedNetwork::try_from(NamedChain::World).unwrap(),
        )
        .await;
    }
}
