use alloy::primitives::B256;
use std::collections::BTreeMap;

pub struct BlockChainWindow {
    entries: BTreeMap<u64, (B256, B256)>, // block_number -> (block_hash, parent_hash)
    max_window_size: usize,
}

pub enum ParentValidation {
    Valid,
    Mismatch { expected: B256, got: B256 },
    NoPreviousBlock,
}

impl BlockChainWindow {
    pub fn try_new(max_window_size: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(max_window_size > 0, "window_size must be > 0");
        Ok(Self { entries: BTreeMap::new(), max_window_size })
    }

    pub fn insert(&mut self, block_number: u64, block_hash: B256, parent_hash: B256) {
        self.entries.insert(block_number, (block_hash, parent_hash));
        self.prune();
    }

    pub fn get(&self, block_number: u64) -> Option<&(B256, B256)> {
        self.entries.get(&block_number)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Validates whether `parent_hash` matches the stored hash of `block_number - 1`.
    pub fn validate_parent(&self, block_number: u64, parent_hash: B256) -> ParentValidation {
        let prev = match block_number.checked_sub(1) {
            Some(n) => n,
            None => return ParentValidation::NoPreviousBlock,
        };

        match self.entries.get(&prev) {
            None => ParentValidation::NoPreviousBlock,
            Some((stored_hash, _)) => {
                if *stored_hash == parent_hash {
                    ParentValidation::Valid
                } else {
                    ParentValidation::Mismatch { expected: *stored_hash, got: parent_hash }
                }
            }
        }
    }

    /// Finds the highest block number in the window that matches a hash in `canonical_blocks`.
    ///
    /// `canonical_blocks` is a slice of `(block_number, block_hash)` pairs representing
    /// the canonical chain. Iterates in reverse to find the highest match first.
    pub fn find_fork_point(&self, canonical_blocks: &[(u64, B256)]) -> Option<u64> {
        // Iterate in reverse so we find the highest matching block first,
        // without allocating a sorted copy.
        for &(block_number, ref canonical_hash) in canonical_blocks.iter().rev() {
            if let Some((stored_hash, _)) = self.entries.get(&block_number) {
                if stored_hash == canonical_hash {
                    return Some(block_number);
                }
            }
        }

        None
    }

    /// Overwrites entries for the given block numbers with new hashes.
    pub fn update_range(&mut self, blocks: &[(u64, B256, B256)]) {
        for &(block_number, block_hash, parent_hash) in blocks {
            self.entries.insert(block_number, (block_hash, parent_hash));
        }
    }

    /// Removes all entries with block_number >= `from`.
    /// Used after reorg recovery via removed-logs or ExEx paths where
    /// canonical replacement blocks are not available.
    pub fn remove_from(&mut self, from: u64) {
        self.entries.retain(|&block_number, _| block_number < from);
    }

    /// Returns all block numbers >= `from`, in ascending order.
    pub fn block_numbers_from(&self, from: u64) -> Vec<u64> {
        self.entries.range(from..).map(|(&n, _)| n).collect()
    }

    /// Returns all block numbers in the window, in ascending order.
    pub fn block_numbers(&self) -> Vec<u64> {
        self.entries.keys().copied().collect()
    }

    /// Returns the oldest (lowest) block number in the window.
    pub fn oldest_block(&self) -> Option<u64> {
        self.entries.keys().next().copied()
    }

    /// Returns the latest (highest) block number in the window.
    pub fn latest_block(&self) -> Option<u64> {
        self.entries.keys().next_back().copied()
    }

    /// Removes the oldest entries when the window exceeds `max_window_size`.
    fn prune(&mut self) {
        while self.entries.len() > self.max_window_size {
            self.entries.pop_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        B256::from(bytes)
    }

    #[test]
    fn test_insert_and_get() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(1, hash(1), hash(0));
        w.insert(2, hash(2), hash(1));

        assert_eq!(w.get(1), Some(&(hash(1), hash(0))));
        assert_eq!(w.get(2), Some(&(hash(2), hash(1))));
        assert_eq!(w.get(3), None);
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn test_prune_respects_window_size() {
        let mut w = BlockChainWindow::try_new(3).unwrap();
        for i in 1u64..=5 {
            w.insert(i, hash(i as u8), hash(i as u8 - 1));
        }

        assert_eq!(w.len(), 3);
        assert_eq!(w.oldest_block(), Some(3));
        assert_eq!(w.latest_block(), Some(5));
        // Blocks 1 and 2 should have been pruned
        assert!(w.get(1).is_none());
        assert!(w.get(2).is_none());
    }

    #[test]
    fn test_validate_parent_valid() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(10, hash(10), hash(9));

        match w.validate_parent(11, hash(10)) {
            ParentValidation::Valid => {}
            _ => panic!("expected Valid"),
        }
    }

    #[test]
    fn test_validate_parent_mismatch() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(10, hash(10), hash(9));

        match w.validate_parent(11, hash(99)) {
            ParentValidation::Mismatch { expected, got } => {
                assert_eq!(expected, hash(10));
                assert_eq!(got, hash(99));
            }
            _ => panic!("expected Mismatch"),
        }
    }

    #[test]
    fn test_validate_parent_no_previous_block_empty() {
        let w = BlockChainWindow::try_new(10).unwrap();
        match w.validate_parent(5, hash(4)) {
            ParentValidation::NoPreviousBlock => {}
            _ => panic!("expected NoPreviousBlock"),
        }
    }

    #[test]
    fn test_validate_parent_no_previous_block_zero() {
        let w = BlockChainWindow::try_new(10).unwrap();
        // block_number 0: checked_sub(1) returns None
        match w.validate_parent(0, hash(0)) {
            ParentValidation::NoPreviousBlock => {}
            _ => panic!("expected NoPreviousBlock"),
        }
    }

    #[test]
    fn test_find_fork_point_shallow_reorg() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        // Insert blocks 1..=5 with "original" hashes
        for i in 1u64..=5 {
            w.insert(i, hash(i as u8), hash(i as u8 - 1));
        }

        // Canonical chain shares blocks 1..=3 but diverges at 4
        // (block 4 and 5 have different hashes on canonical)
        let canonical = vec![
            (1u64, hash(1)),
            (2u64, hash(2)),
            (3u64, hash(3)),
            (4u64, hash(44)), // different
            (5u64, hash(55)), // different
        ];

        // Should return 3 — highest matching block
        assert_eq!(w.find_fork_point(&canonical), Some(3));
    }

    #[test]
    fn test_find_fork_point_no_match() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        for i in 1u64..=5 {
            w.insert(i, hash(i as u8), hash(i as u8 - 1));
        }

        // No canonical block matches any stored hash
        let canonical = vec![(1u64, hash(100)), (2u64, hash(101)), (3u64, hash(102))];

        assert_eq!(w.find_fork_point(&canonical), None);
    }

    #[test]
    fn test_find_fork_point_empty_canonical() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(1, hash(1), hash(0));
        assert_eq!(w.find_fork_point(&[]), None);
    }

    #[test]
    fn test_update_range() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(1, hash(1), hash(0));
        w.insert(2, hash(2), hash(1));
        w.insert(3, hash(3), hash(2));

        // Simulate re-org: blocks 2 and 3 get new hashes
        w.update_range(&[(2, hash(20), hash(1)), (3, hash(30), hash(20))]);

        assert_eq!(w.get(1), Some(&(hash(1), hash(0))));
        assert_eq!(w.get(2), Some(&(hash(20), hash(1))));
        assert_eq!(w.get(3), Some(&(hash(30), hash(20))));
    }

    #[test]
    fn test_block_numbers_from() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        for i in [1u64, 3, 5, 7, 9] {
            w.insert(i, hash(i as u8), hash(i as u8 - 1));
        }

        assert_eq!(w.block_numbers_from(5), vec![5, 7, 9]);
        assert_eq!(w.block_numbers_from(1), vec![1, 3, 5, 7, 9]);
        assert_eq!(w.block_numbers_from(10), Vec::<u64>::new());
    }

    #[test]
    fn test_block_numbers() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(3, hash(3), hash(2));
        w.insert(1, hash(1), hash(0));
        w.insert(2, hash(2), hash(1));

        assert_eq!(w.block_numbers(), vec![1, 2, 3]);
    }

    #[test]
    fn test_oldest_and_latest_block() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        assert_eq!(w.oldest_block(), None);
        assert_eq!(w.latest_block(), None);

        w.insert(5, hash(5), hash(4));
        w.insert(3, hash(3), hash(2));
        w.insert(7, hash(7), hash(6));

        assert_eq!(w.oldest_block(), Some(3));
        assert_eq!(w.latest_block(), Some(7));
    }

    #[test]
    fn test_remove_from() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        for i in 1u64..=5 {
            w.insert(i, hash(i as u8), hash(i as u8 - 1));
        }

        w.remove_from(3);
        assert_eq!(w.len(), 2);
        assert_eq!(w.block_numbers(), vec![1, 2]);
        assert!(w.get(3).is_none());
        assert!(w.get(4).is_none());
        assert!(w.get(5).is_none());
    }

    #[test]
    fn test_remove_from_all() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(5, hash(5), hash(4));
        w.insert(6, hash(6), hash(5));

        w.remove_from(5);
        assert!(w.is_empty());
    }

    #[test]
    fn test_remove_from_none() {
        let mut w = BlockChainWindow::try_new(10).unwrap();
        w.insert(1, hash(1), hash(0));
        w.insert(2, hash(2), hash(1));

        w.remove_from(10); // nothing >= 10
        assert_eq!(w.len(), 2);
    }
}
