//! Block range types for dataset management.

use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;
use serde::{Deserialize, Serialize};

use crate::BlockNum;

/// Block range for data extraction and segment management.
///
/// This type contains all the information needed for data extraction and segment management.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRange {
    /// Inclusive range of block numbers.
    pub numbers: RangeInclusive<BlockNum>,
    /// Network identifier (e.g., "mainnet", "sepolia").
    pub network: String,
    /// Hash of the end block.
    pub hash: BlockHash,
    /// Hash of the block before the start block.
    pub parent_hash: BlockHash,
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
        self.network == other.network
            && self.end() < other.start()
            && self.hash == other.parent_hash
    }
}
