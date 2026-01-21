use std::{collections::BTreeMap, fmt::Debug, ops::RangeInclusive};

use alloy::primitives::BlockHash;
use metadata_db::files::FileId;
use object_store::ObjectMeta;

use crate::{BlockNum, BlockRange, BoxError, block_range_intersection};

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
pub struct ResumeWatermark(pub BTreeMap<String, Watermark>);

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
    pub fn to_watermark(self, network: &str) -> Result<Watermark, BoxError> {
        self.0
            .into_iter()
            .find(|(n, _)| n == network)
            .map(|(_, w)| w)
            .ok_or_else(|| format!("Expected resume watermark for network '{network}'").into())
    }
}

// encoding to remote plan
impl From<ResumeWatermark> for BTreeMap<String, (BlockNum, [u8; 32])> {
    fn from(value: ResumeWatermark) -> Self {
        value
            .0
            .into_iter()
            .map(|(network, Watermark { number, hash })| (network, (number, hash.0)))
            .collect()
    }
}
// decoding from remote plan
impl From<BTreeMap<String, (BlockNum, [u8; 32])>> for ResumeWatermark {
    fn from(value: BTreeMap<String, (BlockNum, [u8; 32])>) -> Self {
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

/// A block range associated with the matadata from a file in object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub id: FileId,
    pub range: BlockRange,
    pub object: ObjectMeta,
}

/// A sequence of adjacent segments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chain(pub Vec<Segment>);

impl Chain {
    #[cfg(test)]
    fn check_invariants(&self) {
        assert!(!self.0.is_empty());
        for w in self.0.windows(2) {
            assert!(BlockRange::adjacent(&w[0].range, &w[1].range));
        }
    }

    pub fn start(&self) -> BlockNum {
        self.first().start()
    }

    pub fn end(&self) -> BlockNum {
        self.last().end()
    }

    pub fn first(&self) -> &BlockRange {
        &self.0.first().unwrap().range
    }

    pub fn last(&self) -> &BlockRange {
        &self.0.last().unwrap().range
    }

    pub fn range(&self) -> BlockRange {
        BlockRange {
            numbers: self.start()..=self.end(),
            network: self.first().network.clone(),
            hash: self.last().hash,
            prev_hash: self.first().prev_hash,
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
    let mut missing = vec![desired.clone()];

    // remove overlapping ranges from each segment
    for segment in &segments {
        let segment_range = segment.range.numbers.clone();
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
        let reorg_block = fork.start() - 1;
        let canonical_segments = &chains.canonical.0;
        let canonical_range = canonical_segments
            .iter()
            .map(|s| s.range.numbers.clone())
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
// The implementation uses depth-first from the segment with the greatest end block number,
// favoring segments with the latest object `last_modified` timestamp.
#[inline]
fn chains(segments: Vec<Segment>) -> Option<Chains> {
    let min_start = segments.iter().map(|s| s.range.start()).min()?;
    let mut by_end: BTreeMap<BlockNum, Vec<Segment>> = Default::default();
    for s in segments {
        by_end.entry(s.range.end()).or_default().push(s);
    }

    fn pick_segment(
        by_end: &mut BTreeMap<BlockNum, Vec<Segment>>,
        end: BlockNum,
        next: Option<&BlockRange>,
    ) -> Option<Segment> {
        let segments = by_end.get_mut(&end)?;
        segments.sort_unstable_by_key(|s| s.object.last_modified);
        let index = segments.iter().rposition(|s| {
            next.map(|next| BlockRange::adjacent(&s.range, next))
                .unwrap_or(true)
        })?;
        let segment = segments.remove(index);
        if segments.is_empty() {
            by_end.remove(&end);
        }
        Some(segment)
    }
    fn pick_chain(by_end: &mut BTreeMap<BlockNum, Vec<Segment>>) -> Chain {
        assert!(!by_end.is_empty());
        let latest = {
            let end = *by_end.last_key_value().unwrap().0;
            pick_segment(by_end, end, None).unwrap()
        };
        let mut chain = vec![latest];
        loop {
            let next = &chain.last().unwrap().range;
            if next.start() == 0 {
                break;
            }
            match pick_segment(by_end, next.start() - 1, Some(next)) {
                Some(segment) => chain.push(segment),
                None => break,
            };
        }
        chain.reverse();
        Chain(chain)
    }

    let latest = pick_chain(&mut by_end);
    if latest.start() == min_start {
        #[cfg(test)]
        latest.check_invariants();

        return Some(Chains {
            canonical: latest,
            fork: None,
        });
    }

    let mut chains = vec![latest];
    while chains.last().unwrap().start() != min_start {
        chains.push(pick_chain(&mut by_end));
    }

    let canonical = chains.pop().unwrap();
    let fork_min = canonical.end() + 1;
    let fork = chains
        .iter()
        .filter(|c| c.end() > canonical.end())
        .position(|c| c.start() <= fork_min)
        .map(|index| chains.remove(index));

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
    use datasets_common::BlockNum;
    use metadata_db::files::FileId;
    use object_store::ObjectMeta;
    use rand::{Rng as _, RngCore as _, SeedableRng as _, rngs::StdRng, seq::SliceRandom};

    use super::{BlockRange, Chain, Chains, Segment};

    fn test_range(numbers: RangeInclusive<BlockNum>, fork: (u8, u8)) -> BlockRange {
        fn test_hash(number: u8, fork: u8) -> BlockHash {
            let mut hash: BlockHash = Default::default();
            hash.0[0] = number;
            hash.0[31] = fork;
            hash
        }
        BlockRange {
            numbers: numbers.clone(),
            network: "test".to_string(),
            hash: test_hash(*numbers.end() as u8, fork.1),
            prev_hash: if *numbers.start() == 0 {
                Some(Default::default())
            } else {
                Some(test_hash(*numbers.start() as u8 - 1, fork.0))
            },
        }
    }

    fn test_segment(numbers: RangeInclusive<BlockNum>, fork: (u8, u8), timestamp: i64) -> Segment {
        let range = test_range(numbers.clone(), fork);
        let object = ObjectMeta {
            location: Default::default(),
            last_modified: DateTime::from_timestamp_millis(timestamp).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        };
        Segment {
            range,
            object,
            id: FileId::try_from(1i64).expect("FileId::MIN is 1"),
        }
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
                test_segment(3..=6, (0, 0), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=2, (0, 0), 0),
                    test_segment(3..=6, (0, 0), 0),
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
                test_segment(4..=6, (1, 1), 0),
                test_segment(4..=8, (0, 0), 0),
                test_segment(7..=9, (1, 1), 0),
                test_segment(4..=9, (2, 2), 0),
            ]),
            Some(Chains {
                canonical: Chain(vec![
                    test_segment(0..=3, (0, 0), 0),
                    test_segment(4..=8, (0, 0), 0),
                ]),
                fork: Some(Chain(vec![test_segment(4..=9, (2, 2), 0)])),
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
                    .map(|s| s.range.end() + 1)
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
                assert!(fork.end() > canonical.end());
                assert!(fork.start() > canonical.start());
                assert!(fork.start() <= canonical.end() + 1);
            } else {
                assert!(other_chains.iter().all(|c| c.end() <= canonical.end()));
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
}
