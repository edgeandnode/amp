//! Core types for the Amp client
//!
//! This module defines the fundamental types used throughout the client library,
//! including block numbers, block ranges, and watermarks for stream resumption.

use std::{collections::BTreeMap, ops::RangeInclusive};

use alloy::primitives::BlockHash;
use serde::{Deserialize, Serialize};

/// Block number type alias.
pub type BlockNum = u64;

/// Error type for operations that return a boxed error.
pub type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// A block range with network and hash information.
///
/// Represents a contiguous range of blocks from a specific blockchain network,
/// including cryptographic hashes for validation and chain reorganization detection.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct BlockRange {
    /// The inclusive range of block numbers
    pub numbers: RangeInclusive<BlockNum>,
    /// Network identifier (e.g., "eth", "polygon")
    pub network: String,
    /// Hash of the end block in this range
    pub hash: BlockHash,
    /// Hash of the block immediately before the start of this range (for chain continuity)
    pub prev_hash: Option<BlockHash>,
}

impl BlockRange {
    /// Get the start block number of this range.
    #[inline]
    pub fn start(&self) -> BlockNum {
        *self.numbers.start()
    }

    /// Get the end block number of this range.
    #[inline]
    pub fn end(&self) -> BlockNum {
        *self.numbers.end()
    }

    /// Convert this block range into a watermark at its end block.
    #[inline]
    pub fn watermark(&self) -> Watermark {
        Watermark {
            number: self.end(),
            hash: self.hash,
        }
    }
}

/// A watermark representing a specific point in a blockchain.
///
/// Used to track progress and enable resumption from a known good state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// The block number at this watermark
    pub number: BlockNum,
    /// The hash of the block at this watermark
    pub hash: BlockHash,
}

/// Resume watermark for multi-network streaming.
///
/// Maps network identifiers to their respective watermarks, enabling
/// stream resumption from the last known good state across multiple chains.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeWatermark(pub BTreeMap<String, Watermark>);

impl ResumeWatermark {
    /// Create a resume watermark from a slice of block ranges.
    ///
    /// Extracts the watermark (end block and hash) from each range,
    /// grouped by network identifier.
    pub fn from_ranges(ranges: &[BlockRange]) -> Self {
        let watermark = ranges
            .iter()
            .map(|r| {
                let watermark = r.watermark();
                (r.network.clone(), watermark)
            })
            .collect();
        Self(watermark)
    }

    /// Get the watermark for a specific network.
    ///
    /// Returns an error if the network is not present in this resume watermark.
    pub fn to_watermark(self, network: &str) -> Result<Watermark, BoxError> {
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, w)| w)
            .ok_or_else(|| format!("Expected resume watermark for network '{network}'").into())
    }
}
