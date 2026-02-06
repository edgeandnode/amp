//! Block range types for dataset management.

use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;
use serde::{Deserialize, Serialize};

use crate::{block_num::BlockNum, network_id::NetworkId};

/// Block range for data extraction and segment management.
///
/// This type contains all the information needed for data extraction and segment management.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRange {
    /// Inclusive range of block numbers.
    pub numbers: RangeInclusive<BlockNum>,
    /// Network identifier (e.g., "mainnet", "sepolia").
    pub network: NetworkId,
    /// Hash of the end block.
    pub hash: BlockHash,
    /// Hash of the block before the start block.
    ///
    /// Used to verify chain integrity when combining segments. Two ranges are
    /// considered adjacent if `self.hash == other.prev_hash`, allowing gaps in
    /// block numbers while maintaining hash-chain continuity.
    ///
    /// For genesis blocks (block 0), use the zero hash (default).
    pub prev_hash: BlockHash,
}

impl BlockRange {
    /// Get the start block number.
    #[inline]
    pub fn start(&self) -> BlockNum {
        *self.numbers.start()
    }

    /// Get the end block number.
    #[inline]
    pub fn end(&self) -> BlockNum {
        *self.numbers.end()
    }

    /// Return true iff `self` is sequenced immediately before `other`.
    /// We allow gaps between numbers, but the hashes must line up.
    #[inline]
    pub fn adjacent(&self, other: &Self) -> bool {
        (self.network == other.network)
            && (self.end() < other.start())
            && (self.hash == other.prev_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacent_allows_consecutive_blocks_with_matching_hashes() {
        //* Given two consecutive ranges with matching hashes
        let r1 = BlockRange {
            numbers: 5..=7,
            network: "test".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 8..=10,
            network: "test".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [1u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should be adjacent
        assert!(result);
    }

    #[test]
    fn adjacent_allows_gaps_with_matching_hashes() {
        //* Given two ranges with a gap (block 5-7, block 10-12) but matching hashes
        let r1 = BlockRange {
            numbers: 5..=7,
            network: "test".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 10..=12,
            network: "test".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [1u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should be adjacent despite the gap
        assert!(result);
    }

    #[test]
    fn adjacent_rejects_mismatched_hashes() {
        //* Given two consecutive ranges with mismatched hashes
        let r1 = BlockRange {
            numbers: 5..=7,
            network: "test".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 8..=10,
            network: "test".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [99u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should NOT be adjacent
        assert!(!result);
    }

    #[test]
    fn adjacent_rejects_different_networks() {
        //* Given two ranges on different networks with matching hashes
        let r1 = BlockRange {
            numbers: 5..=7,
            network: "mainnet".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 8..=10,
            network: "sepolia".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [1u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should NOT be adjacent
        assert!(!result);
    }

    #[test]
    fn adjacent_rejects_overlapping_ranges() {
        //* Given two overlapping ranges
        let r1 = BlockRange {
            numbers: 5..=10,
            network: "test".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 8..=12,
            network: "test".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [1u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should NOT be adjacent (ranges overlap)
        assert!(!result);
    }

    #[test]
    fn adjacent_rejects_reversed_order() {
        //* Given two ranges in reversed order
        let r1 = BlockRange {
            numbers: 10..=12,
            network: "test".parse().expect("valid network id"),
            hash: [1u8; 32].into(),
            prev_hash: [0u8; 32].into(),
        };
        let r2 = BlockRange {
            numbers: 5..=7,
            network: "test".parse().expect("valid network id"),
            hash: [2u8; 32].into(),
            prev_hash: [1u8; 32].into(),
        };

        //* When checking adjacency
        let result = r1.adjacent(&r2);

        //* Then they should NOT be adjacent (wrong order)
        assert!(!result);
    }
}
