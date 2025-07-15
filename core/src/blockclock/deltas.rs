//! # Return validated block timestamps
//!
//! We can return block-timestamps more intelligently than calling RPCs in two ways:
//!   1. Delta run-length encoded
//!   2. Fixed spaced block-timestamps (an extreme variant on case 1)
//!
//! ## Delta run-length encoded
//!
//! ## Fixed spaced block-timestamps

pub fn get_time_for_base_block(block: u64) -> Option<i64> {
    let start = 1686789347;
    let block_time = start + 2 * block as i64;

    if block <= 32853624 {
        Some(block_time)
    } else {
        None
    }
}

pub fn get_time_for_blast_block(block: u64) -> Option<i64> {
    let start = 1708809815;
    let block_time = start + 2 * block as i64;

    if block <= 21843498 {
        Some(block_time)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{create_client, JsonRpcCachedProvider};
    use alloy::primitives::U64;
    use proptest::prelude::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub async fn client() -> Arc<JsonRpcCachedProvider> {
        create_client(
            "https://base.llamarpc.com",
            8453,
            None,
            None,
            None,
            Default::default(),
            None,
            None,
        )
        .await
        .unwrap()
    }

    async fn check_block_times<F>(network: i32, rpc: &str, get_time_fn: F)
    where
        F: Fn(u64) -> Option<i64> + Sync,
    {
        let runs = 25;
        let mut blocks = Vec::with_capacity(runs);

        for _ in 0..runs {
            blocks.push(rand::rng().random_range(1_u64..=32_853_624));
        }

        let blocks_req = blocks.iter().map(|&n| U64::from(n)).collect::<Vec<_>>();
        let block_time = client()
            .await
            .get_block_by_number_batch(&blocks_req, false)
            .await
            .unwrap()
            .into_iter()
            .map(|t| (t.header.number, t.header.timestamp as i64))
            .collect::<HashMap<_, _>>();

        for (k, v) in block_time {
            let time = get_time_fn(k).unwrap_or_else(|| {
                panic!("{}: Missing expected time for block {}", network, k);
            });
            assert_eq!(v, time, "{}: Mismatch for block {}", network, k);
        }
    }

    #[tokio::test]
    async fn base_block_time() {
        check_block_times(8453, "https://base.llamarpc.com", get_time_for_base_block).await;
    }

    #[tokio::test]
    async fn blast_block_time() {
        check_block_times(81457, "https://blast.llamarpc.com", get_time_for_blast_block).await;
    }
}
