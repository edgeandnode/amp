use std::{collections::BTreeMap, fmt::Debug, ops::RangeInclusive};

use alloy::primitives::BlockHash;
use datasets_common::network_id::NetworkId;
use metadata_db::files::FileId;
use object_store::ObjectMeta;

use crate::{BlockNum, BlockRange, block_range_intersection};

/// A watermark representing a specific block in the chain.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Watermark {
    /// The segment end block.
    pub number: BlockNum,
    /// The hash associated with the segment end block.
    pub hash: BlockHash,
}

impl From<&BlockRange> for Watermark {
    fn from(range: &BlockRange) -> Self {
        Self {
            number: range.end(),
            hash: range.hash,
        }
    }
}

/// Public interface for resuming a stream from a watermark.
// TODO: unify with `Watermark` when adding support for multi-network streaming.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResumeWatermark(pub BTreeMap<NetworkId, Watermark>);

/// Error when extracting a watermark for a specific network from a ResumeWatermark
///
/// This occurs when the ResumeWatermark does not contain an entry for the requested network.
#[derive(Debug, thiserror::Error)]
#[error("expected resume watermark for network '{0}'")]
pub struct WatermarkNotFoundError(pub NetworkId);

impl ResumeWatermark {
    /// Create a ResumeWatermark from a slice of BlockRanges.
    pub fn from_ranges(ranges: &[BlockRange]) -> Self {
        let watermark = ranges
            .iter()
            .map(|r| (r.network.clone(), r.into()))
            .collect();
        Self(watermark)
    }

    /// Extract the watermark for a specific network.
    pub fn to_watermark(self, network: &NetworkId) -> Result<Watermark, WatermarkNotFoundError> {
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, w)| w)
            .ok_or_else(|| WatermarkNotFoundError(network.clone()))
    }
}

// encoding to remote plan
impl From<ResumeWatermark> for BTreeMap<NetworkId, (BlockNum, [u8; 32])> {
    fn from(value: ResumeWatermark) -> Self {
        value
            .0
            .into_iter()
            .map(|(network, Watermark { number, hash })| (network, (number, hash.0)))
            .collect()
    }
}
// decoding from remote plan
impl From<BTreeMap<NetworkId, (BlockNum, [u8; 32])>> for ResumeWatermark {
    fn from(value: BTreeMap<NetworkId, (BlockNum, [u8; 32])>) -> Self {
        let inner = value
            .into_iter()
            .map(|(network, (number, hash))| {
                let hash = hash.into();
                (network, Watermark { number, hash })
            })
            .collect();
        Self(inner)
    }
}

/// A block range associated with the metadata from a file in object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    id: FileId,
    object: ObjectMeta,
    ranges: Vec<BlockRange>,
}

impl Segment {
    /// Returns a Segment, where the ranges are ordered by network.
    pub fn new(id: FileId, object: ObjectMeta, mut ranges: Vec<BlockRange>) -> Self {
        ranges.sort_unstable_by(|a, b| a.network.cmp(&b.network));
        Self { id, object, ranges }
    }

    /// Returns the file ID of this segment.
    pub fn id(&self) -> FileId {
        self.id
    }

    /// Returns a reference to the object metadata.
    pub fn object(&self) -> &ObjectMeta {
        &self.object
    }

    /// Returns a slice of all block ranges in this segment.
    pub fn ranges(&self) -> &[BlockRange] {
        &self.ranges
    }

    /// Returns the single block range for single-network segments.
    ///
    /// # Panics
    /// Panics if the segment does not have exactly one range. This is a temporary
    /// method to support the transition to multi-network streaming. Code outside this module
    /// should use this method when it can assume single-network segments. For now, it is safe to
    /// assume this because we cannot stream out multi-network segments, and therefore they cannot
    /// be materialized.
    pub fn single_range(&self) -> &BlockRange {
        assert_eq!(self.ranges.len(), 1);
        &self.ranges[0]
    }

    pub fn adjacent(&self, other: &Self) -> bool {
        self.ranges.len() == other.ranges.len()
            && std::iter::zip(&self.ranges, &other.ranges).all(|(a, b)| a.adjacent(b))
    }

    /// Lexicographic comparison of segments by start block numbers.
    ///
    /// Compares ranges in order: if the first network's starts differ, return that ordering.
    /// Otherwise, compare the second network's starts, and so on.
    fn cmp_by_start(&self, other: &Self) -> std::cmp::Ordering {
        for (ra, rb) in std::iter::zip(&self.ranges, &other.ranges) {
            match ra.start().cmp(&rb.start()) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Lexicographic comparison of segments by end block numbers.
    ///
    /// Compares ranges in order: if the first network's ends differ, return that ordering.
    /// Otherwise, compare the second network's ends, and so on.
    fn cmp_by_end(&self, other: &Self) -> std::cmp::Ordering {
        for (ra, rb) in std::iter::zip(&self.ranges, &other.ranges) {
            match ra.end().cmp(&rb.end()) {
                std::cmp::Ordering::Equal => continue,
                ordering => return ordering,
            }
        }
        std::cmp::Ordering::Equal
    }
}

/// A sequence of adjacent segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chain(pub Vec<Segment>);

impl Chain {
    #[cfg(test)]
    fn check_invariants(&self) {
        assert!(!self.0.is_empty());
        for segments in self.0.windows(2) {
            assert!(segments[0].adjacent(&segments[1]));
        }
    }

    /// Return all ranges from the first segment.
    pub fn first_ranges(&self) -> &[BlockRange] {
        &self.0.first().unwrap().ranges
    }

    /// Return all ranges from the last segment.
    pub fn last_ranges(&self) -> &[BlockRange] {
        &self.0.last().unwrap().ranges
    }

    /// Return the first segment in the chain.
    pub fn first_segment(&self) -> &Segment {
        self.0.first().unwrap()
    }

    /// Return the last segment in the chain.
    pub fn last_segment(&self) -> &Segment {
        self.0.last().unwrap()
    }

    /// Number of networks (consistent across all segments in the chain).
    pub fn network_count(&self) -> usize {
        self.first_ranges().len()
    }

    /// Return the overall block range of this chain (single-network only).
    pub fn range(&self) -> BlockRange {
        let first = &self.first_ranges()[0];
        let last = &self.last_ranges()[0];
        BlockRange {
            numbers: first.start()..=last.end(),
            network: first.network.clone(),
            hash: last.hash,
            prev_hash: first.prev_hash,
            timestamp: last.timestamp,
        }
    }
}

impl IntoIterator for Chain {
    type Item = Segment;
    type IntoIter = std::vec::IntoIter<Segment>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Returns the canonical chain of segments.
///
/// The canonical chain starts from the earliest available block and extends to the greatest block
/// height reachable through contiguous segments. When multiple segments cover the same block range
/// (indicating a reorg or compaction), the segment with the most recent `last_modified` timestamp
/// is chosen.
///
/// The input order of `segments` does not affect the result.
pub fn canonical_chain(segments: Vec<Segment>) -> Option<Chain> {
    chains(segments).map(|Chains { canonical, .. }| canonical)
}

/// Return the block ranges missing from this table out of the given `desired` range. The
/// returned ranges are non-overlapping and sorted by their start block number.
///
/// This function is designed for single-network segments (raw datasets). All segments must
/// have exactly one network range.
///
/// To resolve reorgs, the missing ranges may include block ranges already indexed. A reorg is
/// detected when there is some fork, which is a chain of segments that has a greater block height
/// than the canonical chain. Divergence from the canonical chain is detected using the `hash` and
/// `prev_hash` fields on the block range. When a reorg is detected, the missing ranges will
/// include any canonical ranges that overlap with the fork minus 1 block.
///
///
/// ```text
///                ┌───────────────────────────────────────────────────────┐
///   desired:     │ 00-02 │ 03-05 │ 06-08 │ 09-11 │ 12-14 │ 15-17 │ 18-20 │
///                └───────────────────────────────────────────────────────┘
///                        ┌───────────────────────────────┐
///   canonical:           │ 03-05 │ 06-08 │ 09-11 │ 12-14 │
///                        └───────────────────────────────┘
///                                        ┌───────────────────────┐
///   fork:                                │ 09-11'│ 12-14'│ 15-17'│
///                                        └───────────────────────┘
///                ┌───────┐       ┌───────┐                       ┌───────┐
///   missing:     │ 00-02 │       │ 06-08 │                       │ 18-20 │
///                └───────┘       └───────┘                       └───────┘
/// ```
///
/// - Ranges 00-02 and 18-20 are missing due to block range gap.
/// - Range 06-08 is missing due to reorg. The canonical chan overlaps with the fork,
///   so we should re-extract the previous segment.
pub fn missing_ranges(
    segments: Vec<Segment>,
    desired: RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    // Invariant: this function only works for single-network segments (raw datasets)
    if let Some(first_segment) = segments.first() {
        assert_eq!(first_segment.ranges.len(), 1);
    }

    let mut missing = vec![desired.clone()];

    // remove overlapping ranges from each segment
    for segment in &segments {
        assert_eq!(segment.ranges.len(), 1);
        let segment_range = segment.ranges[0].numbers.clone();
        let mut index = 0;
        while index < missing.len() {
            if block_range_intersection(missing[index].clone(), segment_range.clone()).is_none() {
                index += 1;
                continue;
            }
            let ranges = missing_block_ranges(segment_range.clone(), missing[index].clone());
            let next_index = index + ranges.len();
            missing.splice(index..=index, ranges);
            index = next_index;
        }
    }

    // add canonical range overlapping with reorg
    if let Some(chains) = chains(segments)
        && let Some(fork) = chains.fork
    {
        let reorg_block = fork.first_ranges()[0].start().saturating_sub(1);
        let canonical_segments = &chains.canonical.0;
        let canonical_range = canonical_segments
            .iter()
            .map(|s| s.ranges[0].numbers.clone())
            .rfind(|r| r.contains(&reorg_block));
        if let Some(canonical_range) = canonical_range {
            let reorg_range = *canonical_range.start()..=reorg_block;
            if let Some(missing_range) = block_range_intersection(reorg_range, desired.clone()) {
                missing.push(missing_range);
            }
        }
    }

    merge_ranges(missing)
}

/// Expands a block range to include full segment boundaries.
///
/// Given a desired range and existing segments, returns the smallest range that:
/// 1. Contains all blocks in the desired range
/// 2. Is aligned to segment boundaries (includes full segments that overlap)
///
/// This is used for redump operations to ensure we re-extract complete segments
/// rather than partial ranges, maintaining segment integrity.
///
/// This function is designed for single-network segments (raw datasets). All segments must
/// have exactly one network range.
///
/// ```text
///                ┌───────────────────────────────────────────────┐
///   segments:    │ 00-99 │ 100-199 │ 200-299 │ 300-399 │ 400-499 │
///                └───────────────────────────────────────────────┘
///                                      ┌───────────┐
///   desired:                           │  250-350  │
///                                      └───────────┘
///                                 ┌────────────────────┐
///   result:                       │       200-399      │
///                                 └────────────────────┘
/// ```
///
/// The desired range 250-350 overlaps with segments 200-299 and 300-399,
/// so the expanded range is 200-399.
///
/// # Panics
///
/// Panics if any segment has more than one block range (i.e., not a single-network raw dataset
/// segment).
pub fn expand_to_segment_boundaries(
    segments: &[Segment],
    desired: RangeInclusive<BlockNum>,
) -> RangeInclusive<BlockNum> {
    // Invariant: this function only works for single-network segments (raw datasets)
    if let Some(first_segment) = segments.first() {
        assert_eq!(first_segment.ranges.len(), 1);
    }

    let mut start = *desired.start();
    let mut end = *desired.end();

    for segment in segments {
        assert_eq!(segment.ranges.len(), 1);
        let seg_range = &segment.ranges[0].numbers;
        // If segment overlaps with desired range, expand to include it
        if seg_range.end() >= desired.start() && seg_range.start() <= desired.end() {
            start = start.min(*seg_range.start());
            end = end.max(*seg_range.end());
        }
    }

    start..=end
}

#[derive(Debug, PartialEq, Eq)]
struct Chains {
    /// See `canonical_segments`.
    canonical: Chain,
    /// The chain with the greatest block height past the canonical chain, where there is no gap
    /// between the canonical chain and the fork.
    ///
    /// ```text
    ///              ┌───────────────┐
    ///   canonical: │ a │ b │ c │ d │
    ///              └───────────────┘
    ///                      ┌────────────┐
    ///   fork:              │ c'│ d'│ e' │
    ///                      └────────────┘
    /// ```
    fork: Option<Chain>,
}

/// Find the canonical and fork chain from a sequence of segments. The result is independent of the
/// input order.
#[inline]
fn chains(mut segments: Vec<Segment>) -> Option<Chains> {
    if segments.is_empty() {
        return None;
    }

    // Sort by start block ascending, then by last_modified descending.
    // - start ascending: enables forward single-pass chain construction
    // - last_modified descending: when multiple segments could extend the chain, the first one
    //   encountered (most recently modified) is preferred to favor compacted segments.
    segments.sort_unstable_by(|a, b| {
        use std::cmp::Ord;
        a.cmp_by_start(b)
            .then_with(|| Ord::cmp(&b.object.last_modified, &a.object.last_modified))
    });

    // Build canonical chain in a single forward pass. Because segments are sorted by start block,
    // we simply check if each segment is adjacent to the current chain end. The first adjacent
    // segment we find is optimal due to the secondary sort by last_modified.
    let mut canonical: Vec<Segment> = Default::default();
    let mut non_canonical: Vec<Segment> = Default::default();
    for segment in segments {
        if canonical.is_empty() {
            canonical.push(segment);
            continue;
        }

        if canonical.last().unwrap().adjacent(&segment) {
            canonical.push(segment);
        } else {
            non_canonical.push(segment);
        }
    }

    let canonical = Chain(canonical);

    // Sort non-canonical segments by end block ascending, then last_modified ascending.
    // - end ascending: allows pop() to yield highest-end segments first (fork candidates)
    // - last_modified ascending: most recent segments are popped first, to favor compacted
    //   segments.
    non_canonical.sort_unstable_by(|a, b| {
        use std::cmp::Ord;
        a.cmp_by_end(b)
            .then_with(|| Ord::cmp(&a.object.last_modified, &b.object.last_modified))
    });

    // Search for a valid fork chain. A fork must:
    // 1. Extend beyond the canonical chain's end block
    // 2. Connect back to at most canonical.last() + 1 (to form a valid divergence point)
    let mut fork: Option<Chain> = None;
    while let Some(fork_end) = non_canonical.pop() {
        // Early exit: remaining segments all have end <= canonical.end() (sorted ascending),
        // so none can form a fork that extends beyond canonical.
        // For multi-network, compare lexicographically.
        if fork_end.cmp_by_end(canonical.last_segment()) != std::cmp::Ordering::Greater {
            break;
        }

        // Build a candidate fork chain backwards from fork_end.
        let mut fork_segments = vec![fork_end];
        for segment in non_canonical.iter().rev() {
            if segment.adjacent(fork_segments.first().unwrap()) {
                fork_segments.insert(0, segment.clone());
            }
        }
        // Check if this fork connects back to a valid divergence point.
        if std::iter::zip(&fork_segments[0].ranges, &canonical.last_segment().ranges)
            .all(|(fork_range, canonical_range)| fork_range.start() <= canonical_range.end() + 1)
        {
            fork = Some(Chain(fork_segments));
            break;
        }
    }

    #[cfg(test)]
    {
        canonical.check_invariants();
        if let Some(fork) = &fork {
            fork.check_invariants();
        }
    }

    Some(Chains { canonical, fork })
}

fn missing_block_ranges(
    synced: RangeInclusive<BlockNum>,
    desired: RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    // no overlap
    if (synced.end() < desired.start()) || (synced.start() > desired.end()) {
        return vec![desired];
    }
    // desired is subset of synced
    if (synced.start() <= desired.start()) && (synced.end() >= desired.end()) {
        return vec![];
    }
    // partial overlap
    let mut result = Vec::new();
    if desired.start() < synced.start() {
        result.push(*desired.start()..=(*synced.start() - 1));
    }
    if desired.end() > synced.end() {
        result.push((*synced.end() + 1)..=*desired.end());
    }
    result
}

pub fn merge_ranges(mut ranges: Vec<RangeInclusive<BlockNum>>) -> Vec<RangeInclusive<BlockNum>> {
    ranges.sort_by_key(|r| *r.start());
    let mut index = 1;
    while index < ranges.len() {
        let current_range = ranges[index - 1].clone();
        let next_range = ranges[index].clone();
        if *next_range.start() <= (*current_range.end() + 1) {
            ranges[index - 1] =
                *current_range.start()..=BlockNum::max(*current_range.end(), *next_range.end());
            ranges.remove(index);
        } else {
            index += 1;
        }
    }
    ranges
}

#[cfg(test)]
mod test {
    use std::ops::RangeInclusive;

    use alloy::primitives::BlockHash;
    use chrono::DateTime;
    use datasets_common::block_num::BlockNum;
    use metadata_db::files::FileId;
    use object_store::ObjectMeta;
    use rand::{Rng as _, RngCore as _, SeedableRng as _, rngs::StdRng, seq::SliceRandom};

    use super::{BlockRange, Chain, Chains, Segment};

    fn test_hash(number: u8, fork: u8) -> BlockHash {
        let mut hash: BlockHash = Default::default();
        hash.0[0] = number;
        hash.0[31] = fork;
        hash
    }

    fn test_object(timestamp: i64) -> ObjectMeta {
        ObjectMeta {
            location: Default::default(),
            last_modified: DateTime::from_timestamp_millis(timestamp).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn test_range(network: &str, numbers: RangeInclusive<BlockNum>, fork: (u8, u8)) -> BlockRange {
        BlockRange {
            numbers: numbers.clone(),
            network: network.parse().expect("valid network id"),
            hash: test_hash(*numbers.end() as u8, fork.1),
            prev_hash: if *numbers.start() == 0 {
                Default::default()
            } else {
                test_hash(*numbers.start() as u8 - 1, fork.0)
            },
            timestamp: None,
        }
    }

    fn test_segment(numbers: RangeInclusive<BlockNum>, fork: (u8, u8), timestamp: i64) -> Segment {
        let range = test_range("test", numbers.clone(), fork);
        Segment::new(
            FileId::try_from(1i64).expect("FileId::MIN is 1"),
            test_object(timestamp),
            vec![range],
        )
    }

    fn test_segment_multi(
        network_a: (RangeInclusive<BlockNum>, (u8, u8)),
        network_b: (RangeInclusive<BlockNum>, (u8, u8)),
        timestamp: i64,
    ) -> Segment {
        Segment::new(
            FileId::try_from(1i64).expect("FileId::MIN is 1"),
            test_object(timestamp),
            vec![
                test_range("a", network_a.0, network_a.1),
                test_range("b", network_b.0, network_b.1),
            ],
        )
    }

    #[test]
    fn chains() {
        // empty input
        assert_eq!(super::chains(vec![]), None);

        // single segment
        assert_eq!(
            super::chains(vec![test_segment(0..=0, (0, 0), 0)]),
            Some(Chains {
                canonical: Chain(vec![test_segment(0..=0, (0, 0), 0)]),
                fork: None,
            })
        );

        // 2 adjacent segments forming a canonical chain
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=5, (0, 0), 0),
                ]),
                fork: None,
            })
        );

        // 3 adjacent segments forming a canonical chain
        assert_eq!(
            super::chains(vec![
                test_segment(0..=1, (0, 0), 0),
                test_segment(2..=3, (0, 0), 0),
                test_segment(4..=5, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=1, (0, 0), 0),
                    test_segment(2..=3, (0, 0), 0),
                    test_segment(4..=5, (0, 0), 0),
                ]),
                fork: None,
            })
        );

        // non-adjacent segments
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(5..=7, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![test_segment(0..=2, (0, 0), 0)]),
                fork: None,
            })
        );

        // multiple non-adjacent segments
        assert_eq!(
            super::chains(vec![
                test_segment(5..=7, (0, 0), 0),
                test_segment(0..=2, (0, 0), 0),
                test_segment(10..=12, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![test_segment(0..=2, (0, 0), 0)]),
                fork: None,
            })
        );

        // overlapping segments outside canonical
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 0), 0),
                test_segment(4..=5, (0, 0), 0),
                test_segment(4..=6, (0, 0), 0),
                test_segment(3..=6, (0, 0), 1),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=6, (0, 0), 1),
                ]),
                fork: None,
            })
        );

        // simple fork at start block
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 0), 0),
                test_segment(3..=6, (1, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=5, (0, 0), 0),
                ]),
                fork: Some(Chain(vec![test_segment(3..=6, (1, 0), 0)])),
            })
        );

        // reorg of multiple segments
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 0), 0),
                test_segment(3..=5, (1, 1), 0),
                test_segment(6..=8, (1, 0), 0),
                test_segment(6..=8, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=5, (0, 0), 0),
                    test_segment(6..=8, (0, 0), 0),
                ]),
                fork: None,
            })
        );

        // fork of multiple segments
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 0), 0),
                test_segment(3..=5, (1, 1), 0),
                test_segment(6..=9, (1, 0), 0),
                test_segment(6..=8, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=5, (0, 0), 0),
                    test_segment(6..=8, (0, 0), 0),
                ]),
                fork: Some(Chain(vec![
                    test_segment(3..=5, (1, 1), 0),
                    test_segment(6..=9, (1, 0), 0),
                ])),
            })
        );

        // simple reorg at timestamp
        assert_eq!(
            super::chains(vec![
                test_segment(0..=2, (0, 0), 0),
                test_segment(3..=5, (0, 1), 1),
                test_segment(3..=5, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=5, (0, 1), 1),
                ]),
                fork: None,
            })
        );

        // multiple reorgs past canonical
        assert_eq!(
            super::chains(vec![
                test_segment(0..=3, (0, 0), 0),
                test_segment(4..=6, (1, 1), 1),
                test_segment(4..=8, (0, 0), 2),
                test_segment(7..=9, (1, 1), 3),
                test_segment(4..=9, (2, 2), 4),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=3, (0, 0), 0),
                    test_segment(4..=8, (0, 0), 2),
                ]),
                fork: Some(Chain(vec![test_segment(4..=9, (2, 2), 4)])),
            })
        );

        // multi-segment fork extending past canonical
        assert_eq!(
            super::chains(vec![
                test_segment(0..=3, (0, 0), 0),
                test_segment(4..=5, (1, 1), 1),
                test_segment(6..=7, (1, 1), 2),
                test_segment(8..=9, (1, 1), 3),
            ]),
            Some(Chains {
                canonical: Chain(vec![test_segment(0..=3, (0, 0), 0)]),
                fork: Some(Chain(vec![
                    test_segment(4..=5, (1, 1), 1),
                    test_segment(6..=7, (1, 1), 2),
                    test_segment(8..=9, (1, 1), 3),
                ])),
            })
        );
    }

    #[test]
    fn chains_multi_network() {
        // Case 1: Canonical chain with 2 networks
        // Both networks have adjacent ranges forming a canonical chain
        assert_eq!(
            super::chains(vec![
                test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                    test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 0),
                ]),
                fork: None,
            })
        );

        // Case 2: First network diverges to create a fork
        // Canonical chain has both networks up to block 5
        // Fork extends both networks to block 6 with different hashes
        assert_eq!(
            super::chains(vec![
                test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 0),
                test_segment_multi((3..=6, (0, 1)), (6..=7, (0, 0)), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                    test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 0),
                ]),
                fork: Some(Chain(vec![test_segment_multi(
                    (3..=6, (0, 1)),
                    (6..=7, (0, 0)),
                    0
                )])),
            })
        );

        // Case 3: First network diverges by 1 segment, second network diverges by 2 segments.
        // Canonical (2 segments):
        //   Network a: [0..=2] -> [3..=5]
        //   Network b: [0..=2] -> [3..=5]
        // Fork (2 segments extending past canonical):
        //   Segment 1: Network A [3..=6] (reorg at 3, extends to 6)
        //              Network B [3..=7] (reorg at 3, extends to 7)
        //   Segment 2: Network A [7..=8] (continues fork)
        //              Network B [8..=9] (continues fork, diverges more)
        // Fork ends at (8, 10) which is greater than canonical's (5, 5).
        // Canonical has newer timestamp (3) so it's preferred over fork (timestamp 1, 2).
        assert_eq!(
            super::chains(vec![
                test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                test_segment_multi((3..=6, (0, 1)), (3..=7, (0, 1)), 1),
                test_segment_multi((7..=8, (1, 1)), (8..=9, (1, 1)), 2),
                test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 3),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment_multi((0..=2, (0, 0)), (0..=2, (0, 0)), 0),
                    test_segment_multi((3..=5, (0, 0)), (3..=5, (0, 0)), 3),
                ]),
                fork: Some(Chain(vec![
                    test_segment_multi((3..=6, (0, 1)), (3..=7, (0, 1)), 1),
                    test_segment_multi((7..=8, (1, 1)), (8..=9, (1, 1)), 2),
                ])),
            })
        );
    }

    #[test]
    fn missing_ranges() {
        fn missing_ranges(
            ranges: &[RangeInclusive<BlockNum>],
            desired: RangeInclusive<BlockNum>,
        ) -> Vec<RangeInclusive<BlockNum>> {
            let segments = ranges
                .iter()
                .enumerate()
                .map(|(fork, range)| test_segment(range.clone(), (fork as u8, fork as u8), 0))
                .collect();
            super::missing_ranges(segments, desired)
        }

        assert_eq!(missing_ranges(&[], 0..=10), vec![0..=10]);
        // just canonical ranges
        assert_eq!(missing_ranges(&[0..=10], 0..=10), vec![]);
        assert_eq!(missing_ranges(&[0..=5], 10..=15), vec![10..=15]);
        assert_eq!(missing_ranges(&[3..=7], 0..=10), vec![0..=2, 8..=10]);
        assert_eq!(missing_ranges(&[0..=15], 5..=10), vec![]);
        assert_eq!(missing_ranges(&[5..=15], 0..=10), vec![0..=4]);
        assert_eq!(missing_ranges(&[0..=5], 0..=10), vec![6..=10]);
        // non-overlapping segment groups
        assert_eq!(missing_ranges(&[0..=5, 8..=8], 0..=10), vec![6..=7, 9..=10]);
        assert_eq!(
            missing_ranges(&[1..=2, 4..=4, 8..=9], 0..=10),
            vec![0..=0, 3..=3, 5..=7, 10..=10],
        );
        // overlapping segment groups, no reorg
        assert_eq!(missing_ranges(&[0..=8, 2..=4], 0..=10), vec![9..=10]);
        assert_eq!(missing_ranges(&[0..=3, 5..=7], 0..=10), vec![4..=4, 8..=10]);
        // overlapping segment groups, reorg
        assert_eq!(
            missing_ranges(&[0..=3, 2..=5, 4..=7], 0..=10),
            vec![0..=3, 8..=10]
        );
        assert_eq!(missing_ranges(&[0..=2, 1..=3], 0..=3), vec![0..=0]);
        assert_eq!(missing_ranges(&[0..=2, 2..=3], 0..=2), vec![0..=1]);
        assert_eq!(
            missing_ranges(&[0..=3, 5..=7, 2..=12], 0..=15),
            vec![0..=1, 13..=15]
        );
        assert_eq!(missing_ranges(&[0..=5, 3..=8], 0..=7), vec![0..=2]);
        assert_eq!(missing_ranges(&[0..=20, 10..=30], 12..=18), vec![]);
        // reorg intersection with desired range
        assert_eq!(missing_ranges(&[0..=5, 3..=8], 2..=7), vec![2..=2]);
    }

    #[test]
    fn chains_prop() {
        const MAX_BLOCK: BlockNum = 15;
        const MAX_FORK: u8 = 3;
        const SAMPLES: usize = 10_000;
        const MAX_REORGS: usize = 5;

        fn gen_chain(rng: &mut StdRng, numbers: RangeInclusive<BlockNum>, fork: u8) -> Chain {
            assert!(numbers.start() <= numbers.end());
            let mut segments: Vec<Segment> = Default::default();
            loop {
                let start = segments
                    .last()
                    .map(|s| s.ranges[0].end() + 1)
                    .unwrap_or(*numbers.start());
                let end = rng.random_range(start..=*numbers.end());
                segments.push(test_segment(start..=end, (fork, fork), 0));
                if end == *numbers.end() {
                    break;
                }
            }
            let chain = Chain(segments);
            chain.check_invariants();
            chain
        }

        for _ in 0..SAMPLES {
            let seed = rand::rng().next_u64();
            println!("seed: {seed}");
            let mut rng = StdRng::seed_from_u64(seed);

            let chain_head = rng.random_range(1..=MAX_BLOCK);
            let canonical = gen_chain(&mut rng, 0..=chain_head, 0);
            let other_chains: Vec<Chain> = (0..rng.random_range(0..MAX_REORGS))
                .map(|_| {
                    let start = rng.random_range(1..=chain_head);
                    let end = rng.random_range(start..=chain_head);
                    let fork = rng.random_range(1..=MAX_FORK);
                    gen_chain(&mut rng, start..=end, fork)
                })
                .collect();

            let mut segments = canonical.0.clone();
            for chain in &other_chains {
                segments.append(&mut chain.0.clone());
            }
            segments.shuffle(&mut rng);

            let chains = super::chains(segments).unwrap();
            assert_eq!(chains.canonical, canonical);
            if let Some(fork) = chains.fork {
                assert!(fork.last_ranges()[0].end() > canonical.last_ranges()[0].end());
                assert!(fork.first_ranges()[0].start() > canonical.first_ranges()[0].start());
                assert!(fork.first_ranges()[0].start() <= canonical.last_ranges()[0].end() + 1);
            } else {
                assert!(
                    other_chains
                        .iter()
                        .all(|c| c.last_ranges()[0].end() <= canonical.last_ranges()[0].end())
                );
            }
        }
    }

    #[test]
    fn missing_block_ranges() {
        // no overlap, desired before synced
        assert_eq!(super::missing_block_ranges(10..=20, 0..=5), vec![0..=5]);
        // no overlap, desired after synced
        assert_eq!(super::missing_block_ranges(0..=5, 10..=20), vec![10..=20]);
        // desired is subset of synced
        assert_eq!(super::missing_block_ranges(0..=10, 2..=8), vec![]);
        // desired is same as synced
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts before desired, ends with desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts with desired, ends after desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // partial overlap, desired starts before synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=7), vec![0..=4]);
        // partial overlap, desired ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 3..=10), vec![6..=10]);
        // partial overlap, desired surrounds synced
        assert_eq!(
            super::missing_block_ranges(5..=10, 0..=15),
            vec![0..=4, 11..=15]
        );
        // desired starts same as synced, ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 0..=10), vec![6..=10]);
        // desired starts before synced, ends same as synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=10), vec![0..=4]);
        // adjacent ranges (desired just before synced)
        assert_eq!(super::missing_block_ranges(5..=10, 0..=4), vec![0..=4]);
        // adjacent ranges (desired just after synced)
        assert_eq!(super::missing_block_ranges(0..=5, 6..=10), vec![6..=10]);
        // single block ranges
        assert_eq!(super::missing_block_ranges(0..=0, 0..=0), vec![]);
        assert_eq!(super::missing_block_ranges(0..=0, 1..=1), vec![1..=1]);
        assert_eq!(super::missing_block_ranges(1..=1, 0..=0), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
        assert_eq!(super::missing_block_ranges(1..=3, 0..=3), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
    }

    #[test]
    fn merge_ranges() {
        assert_eq!(super::merge_ranges(vec![]), vec![]);
        assert_eq!(super::merge_ranges(vec![1..=5]), vec![1..=5]);
        assert_eq!(
            super::merge_ranges(vec![1..=3, 5..=7, 9..=10]),
            vec![1..=3, 5..=7, 9..=10]
        );
        assert_eq!(super::merge_ranges(vec![1..=5, 3..=8]), vec![1..=8]);
        assert_eq!(super::merge_ranges(vec![1..=5, 5..=10]), vec![1..=10]);
        assert_eq!(super::merge_ranges(vec![1..=10, 3..=7]), vec![1..=10]);
        assert_eq!(
            super::merge_ranges(vec![1..=3, 2..=5, 4..=8, 7..=10]),
            vec![1..=10]
        );
        assert_eq!(
            super::merge_ranges(vec![10..=15, 1..=5, 3..=8]),
            vec![1..=8, 10..=15]
        );
        assert_eq!(
            super::merge_ranges(vec![1..=3, 2..=6, 8..=10, 9..=12, 15..=18]),
            vec![1..=6, 8..=12, 15..=18]
        );
        assert_eq!(
            super::merge_ranges(vec![5..=10, 5..=10, 5..=10]),
            vec![5..=10]
        );
        assert_eq!(super::merge_ranges(vec![1..=1, 2..=2, 3..=3]), vec![1..=3]);
        assert_eq!(
            super::merge_ranges(vec![1..=5, 7..=10]),
            vec![1..=5, 7..=10]
        );
    }

    #[test]
    fn expand_to_segment_boundaries() {
        fn expand(
            ranges: &[RangeInclusive<BlockNum>],
            desired: RangeInclusive<BlockNum>,
        ) -> RangeInclusive<BlockNum> {
            let segments = ranges
                .iter()
                .enumerate()
                .map(|(i, range)| test_segment(range.clone(), (i as u8, i as u8), 0))
                .collect::<Vec<_>>();
            super::expand_to_segment_boundaries(&segments, desired)
        }

        // empty segments - return desired as-is
        assert_eq!(expand(&[], 10..=20), 10..=20);

        // single segment fully containing desired
        assert_eq!(expand(&[0..=100], 10..=20), 0..=100);

        // single segment overlapping start
        assert_eq!(expand(&[0..=15], 10..=20), 0..=20);

        // single segment overlapping end
        assert_eq!(expand(&[15..=30], 10..=20), 10..=30);

        // multiple segments spanning desired range
        assert_eq!(expand(&[0..=99, 100..=199, 200..=299], 50..=150), 0..=199);

        // non-overlapping segment should not expand
        assert_eq!(expand(&[0..=50, 200..=300], 100..=150), 100..=150);

        // partial overlap on both ends
        assert_eq!(
            expand(&[0..=99, 100..=199, 200..=299, 300..=399], 150..=350),
            100..=399
        );

        // exact match
        assert_eq!(expand(&[100..=200], 100..=200), 100..=200);

        // desired fully outside any segment
        assert_eq!(expand(&[0..=50, 100..=150], 60..=90), 60..=90);

        // adjacent segment boundary (no overlap)
        assert_eq!(expand(&[0..=99, 100..=199], 100..=150), 100..=199);
    }
}
