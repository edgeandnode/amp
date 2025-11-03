//! Response builders for creating mock ResponseBatch sequences.

use std::{ops::RangeInclusive, sync::Arc};

use alloy::primitives::BlockHash;
use common::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    },
    metadata::segments::BlockRange,
};

use crate::client::{Metadata, ResponseBatch};

/// Trait for converting various types into network ranges.
pub trait IntoRanges {
    fn into_ranges(self) -> Vec<(String, RangeInclusive<u64>)>;
}

/// Watermark end: `Step::watermark(10)` -> single block 10 for "eth"
impl IntoRanges for u64 {
    fn into_ranges(self) -> Vec<(String, RangeInclusive<u64>)> {
        vec![("eth".to_string(), self..=self)]
    }
}

/// Single range: `Step::data(0..=10)` -> range 0..=10 for "eth"
impl IntoRanges for RangeInclusive<u64> {
    fn into_ranges(self) -> Vec<(String, RangeInclusive<u64>)> {
        vec![("eth".to_string(), self)]
    }
}

/// Multi-network: `Step::data(vec![("eth", 0..=10), ("polygon", 0..=20)])`
impl IntoRanges for Vec<(&str, RangeInclusive<u64>)> {
    fn into_ranges(self) -> Vec<(String, RangeInclusive<u64>)> {
        self.into_iter()
            .map(|(net, range)| (net.to_string(), range))
            .collect()
    }
}

/// A step in a test scenario.
///
/// Each step represents a response batch that will be sent to the streaming client.
#[derive(Debug, Clone)]
pub enum Step {
    /// Data batch for one or more networks.
    Data {
        ranges: Vec<(String, RangeInclusive<u64>)>,
        label: Option<String>,
        /// Networks that should have epoch incremented (triggers reorg detection)
        reorg: Vec<String>,
    },

    /// Watermark for one or more networks.
    Watermark {
        ranges: Vec<(String, RangeInclusive<u64>)>,
        /// Networks that should have epoch incremented (triggers reorg detection)
        reorg: Vec<String>,
    },
}

impl Step {
    /// Create a data batch step.
    ///
    /// # Examples
    /// ```ignore
    /// Step::data(0..=10)                                  // Single network
    /// Step::data(0..=10).label("batch1")                  // With label
    /// Step::data(0..=10).reorg()                          // Mark as reorg
    /// Step::data(vec![("eth", 0..=10), ("polygon", 0..=20)]) // Multi-network
    /// ```
    pub fn data(ranges: impl IntoRanges) -> Self {
        Self::Data {
            ranges: ranges.into_ranges(),
            label: None,
            reorg: Vec::new(),
        }
    }

    /// Create a watermark step.
    ///
    /// # Examples
    /// ```ignore
    /// Step::watermark(10)                                 // Single network, 0..=10
    /// Step::watermark(0..=10)                             // Single network, explicit range
    /// Step::watermark(0..=10).reorg()                     // Mark as reorg
    /// Step::watermark(vec![("eth", 0..=10), ("polygon", 0..=20)]) // Multi-network
    /// ```
    pub fn watermark(ranges: impl IntoRanges) -> Self {
        Self::Watermark {
            ranges: ranges.into_ranges(),
            reorg: Vec::new(),
        }
    }

    /// Add a label to a data step for identification in tests.
    ///
    /// # Examples
    /// ```ignore
    /// Step::data(0..=10).label("initial_batch")
    /// ```
    pub fn label(self, label: impl Into<String>) -> Self {
        match self {
            Step::Data { ranges, reorg, .. } => Step::Data {
                ranges,
                label: Some(label.into()),
                reorg,
            },
            other => other,
        }
    }

    /// Mark specific networks in this step as having a reorg.
    ///
    /// This will generate block hashes that differ from the standard hash generation,
    /// causing the ProtocolStream to detect a reorg.
    ///
    /// # Examples
    /// ```ignore
    /// Step::data(11..=15).reorg(vec!["eth"])                              // Reorg for eth
    /// Step::watermark(20).reorg(vec!["eth"])                              // Reorg for eth
    /// Step::watermark(vec![...]).reorg(vec!["polygon"])                   // Reorg only polygon
    /// Step::watermark(vec![...]).reorg(vec!["eth", "polygon"])            // Reorg both
    /// ```
    pub fn reorg(self, networks: Vec<&str>) -> Self {
        let reorg: Vec<String> = networks.into_iter().map(|s| s.to_string()).collect();
        match self {
            Step::Data { ranges, label, .. } => Step::Data {
                ranges,
                label,
                reorg,
            },
            Step::Watermark { ranges, .. } => Step::Watermark { ranges, reorg },
        }
    }

    /// Get the networks that should have epoch incremented for this step.
    pub fn get_reorg_networks(&self) -> &[String] {
        match self {
            Step::Data { reorg, .. } => reorg,
            Step::Watermark { reorg, .. } => reorg,
        }
    }

    /// Convert this step into a ResponseBatch with per-network epochs.
    pub fn into_batch_with_epochs(
        self,
        epochs: &std::collections::HashMap<String, u8>,
    ) -> ResponseBatch {
        match self {
            Step::Data { ranges, label, .. } => {
                let ranges_with_epochs: Vec<(&str, RangeInclusive<u64>, u8)> = ranges
                    .iter()
                    .map(|(net, range)| {
                        let epoch = *epochs.get(net).unwrap_or(&0);
                        (net.as_str(), range.clone(), epoch)
                    })
                    .collect();
                data_multi_with_epochs(ranges_with_epochs, label.as_deref())
            }
            Step::Watermark { ranges, .. } => {
                let ranges_with_epochs: Vec<(&str, RangeInclusive<u64>, u8)> = ranges
                    .iter()
                    .map(|(net, range)| {
                        let epoch = *epochs.get(net).unwrap_or(&0);
                        (net.as_str(), range.clone(), epoch)
                    })
                    .collect();
                watermark_multi_with_epochs(ranges_with_epochs)
            }
        }
    }
}

/// Create a data ResponseBatch for multiple networks with per-network epochs.
///
/// # Example
/// ```ignore
/// let batch = response::data_multi_with_epochs(vec![
///     ("eth", 0..=10, 0),
///     ("polygon", 0..=20, 1),
/// ], Some("batch1"));
/// ```
pub fn data_multi_with_epochs(
    ranges: Vec<(&str, RangeInclusive<u64>, u8)>,
    label: Option<&str>,
) -> ResponseBatch {
    let block_ranges: Vec<BlockRange> = ranges
        .iter()
        .map(|(network, range, epoch)| {
            let end = *range.end();
            // Create 32-byte hash from network name, block number, and epoch
            // Use END block for consistent hash generation with watermarks
            let mut hash_bytes = [0u8; 32];
            hash_bytes[0] = *epoch; // First byte is epoch

            // Simple hash of network name into bytes 1-8
            let network_hash = network
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
            hash_bytes[1..9].copy_from_slice(&network_hash.to_be_bytes());

            // Block number in bytes 24-31
            hash_bytes[24..32].copy_from_slice(&end.to_be_bytes());

            let hash = BlockHash::from_slice(&hash_bytes);

            let start = *range.start();
            let prev_hash = if start > 0 {
                let mut prev_bytes = [0u8; 32];
                prev_bytes[0] = *epoch; // Same epoch for prev_hash
                prev_bytes[1..9].copy_from_slice(&network_hash.to_be_bytes());
                prev_bytes[24..32].copy_from_slice(&(start - 1).to_be_bytes());

                Some(BlockHash::from_slice(&prev_bytes))
            } else {
                None
            };

            BlockRange {
                network: network.to_string(),
                numbers: range.clone(),
                hash,
                prev_hash,
            }
        })
        .collect();

    ResponseBatch {
        data: create_mock_batch(label),
        metadata: Metadata {
            ranges: block_ranges,
            ranges_complete: false,
        },
    }
}

/// Create a watermark ResponseBatch for multiple networks with per-network epochs.
///
/// # Example
/// ```ignore
/// let batch = response::watermark_multi_with_epochs(vec![
///     ("eth", 0..=10, 0),
///     ("polygon", 0..=20, 1),
/// ]);
/// ```
pub fn watermark_multi_with_epochs(ranges: Vec<(&str, RangeInclusive<u64>, u8)>) -> ResponseBatch {
    let block_ranges: Vec<BlockRange> = ranges
        .iter()
        .map(|(network, range, epoch)| {
            let end = *range.end();
            // Create 32-byte hash from network name, block number, and epoch
            let mut hash_bytes = [0u8; 32];
            hash_bytes[0] = *epoch; // First byte is epoch

            // Simple hash of network name into bytes 1-8
            let network_hash = network
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
            hash_bytes[1..9].copy_from_slice(&network_hash.to_be_bytes());

            // Block number in bytes 24-31
            hash_bytes[24..32].copy_from_slice(&end.to_be_bytes());

            let hash = BlockHash::from_slice(&hash_bytes);

            let start = *range.start();
            let prev_hash = if start > 0 {
                let mut prev_bytes = [0u8; 32];
                prev_bytes[0] = *epoch; // Same epoch for prev_hash
                prev_bytes[1..9].copy_from_slice(&network_hash.to_be_bytes());
                prev_bytes[24..32].copy_from_slice(&(start - 1).to_be_bytes());

                Some(BlockHash::from_slice(&prev_bytes))
            } else {
                None
            };

            BlockRange {
                network: network.to_string(),
                numbers: range.clone(),
                hash,
                prev_hash,
            }
        })
        .collect();

    ResponseBatch {
        data: create_mock_batch(None),
        metadata: Metadata {
            ranges: block_ranges,
            ranges_complete: true,
        },
    }
}

/// Create a mock RecordBatch with a single row and label column.
fn create_mock_batch(label: Option<&str>) -> RecordBatch {
    use common::arrow::array::StringArray;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "label",
        DataType::Utf8,
        false,
    )]));

    let label_value = label.unwrap_or("");
    let label_array = Arc::new(StringArray::from(vec![label_value]));

    RecordBatch::try_new(schema, vec![label_array as _]).expect("Failed to create mock RecordBatch")
}
