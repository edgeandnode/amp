use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;
use object_store::ObjectMeta;

use crate::BlockNum;

/// A BlockRange associated with the matadata from a file in object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub range: BlockRange,
    pub object: ObjectMeta,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BlockRange {
    pub numbers: RangeInclusive<BlockNum>,
    pub network: String,
    pub hash: BlockHash,
    /// If `prev_hash` is not present, conflicting ranges at the same numbers will be chosen based
    /// on last-write-wins.
    pub prev_hash: Option<BlockHash>,
}

#[derive(Debug, Default)]
pub struct TableSegmentsDiff {
    /// segments added to the canonical chain
    pub add: Vec<Segment>,
    /// segments removed from the canonical chain
    pub sub: Vec<Segment>,
}

/// Segments associated with a table. This incrementally organizes segments into either the
/// canonical chain or a set of forks. The canonical chain is defined as the set of adjacent
/// segments with the greatest block height.
// We are assuming single block range per segment, for now.
#[derive(Default)]
pub struct TableSegments {
    canonical: Option<SegmentGroup>,
    forks: Vec<SegmentGroup>,
}

impl TableSegments {
    #[cfg(any(test, debug_assertions))]
    fn check_invariants(&self) {
        let canonical = match &self.canonical {
            Some(group) => group,
            None => {
                assert!(self.forks.is_empty());
                return;
            }
        };
        canonical.check_invariants();
        for r in self.forks.iter() {
            r.check_invariants();
        }

        // canonical group contains minimum block number
        let canonical_min = canonical.start();
        let forks_min = self.forks.iter().min_by_key(|g| g.start());
        if let Some(forks_min) = forks_min.map(|g| g.start()) {
            assert!(canonical_min <= forks_min);
        }
    }

    pub fn canonical_segments(&self) -> Vec<Segment> {
        self.canonical
            .as_ref()
            .map(|c| c.0.clone())
            .unwrap_or_default()
    }

    pub fn canonical_range(&self) -> Option<BlockRange> {
        let canonical_ranges = &self.canonical.as_ref()?.0;
        let start = &canonical_ranges.first()?.range;
        let end = &canonical_ranges.last()?.range;
        Some(BlockRange {
            numbers: *start.numbers.start()..=*end.numbers.end(),
            network: start.network.clone(),
            hash: end.hash,
            prev_hash: start.prev_hash,
        })
    }

    /// Return the block ranges missing from this table out of the given `desired` range. The
    /// returned ranges will be non-overlapping. For now, we avoid overlapping between canonical
    /// and non-canonical block ranges since we don't disambiguate between the files for query
    /// execution.
    pub fn missing_ranges(
        &self,
        desired: RangeInclusive<BlockNum>,
    ) -> Vec<RangeInclusive<BlockNum>> {
        if self.canonical.is_none() && self.forks.is_empty() {
            return vec![desired];
        }

        let mut missing: Vec<RangeInclusive<BlockNum>> = Default::default();
        for range_group in self.canonical.iter().chain(&self.forks) {
            let range = range_group.start()..=range_group.end();
            if missing.is_empty() {
                missing.append(&mut missing_block_ranges(range, desired.clone()));
                continue;
            }
            let mut index = 0;
            while index < missing.len() {
                let ranges = missing_block_ranges(range.clone(), missing.remove(index));
                if ranges.is_empty() {
                    continue;
                }
                for range in ranges {
                    missing.insert(index, range);
                    index += 1;
                }
            }
        }
        missing
    }

    /// Merge known block ranges. This fails if the given block numbers do not correspond to a set
    /// of adjacent and complete block ranges. This should be done after the associated files have
    /// been merged, and the merged files have been committed to the metadata DB.
    pub fn merge(
        &mut self,
        numbers: RangeInclusive<BlockNum>,
        object: ObjectMeta,
    ) -> Result<(), ()> {
        #[cfg(any(test, debug_assertions))]
        self.check_invariants();

        if let Some(canonical) = &mut self.canonical {
            if let Ok(()) = canonical.merge(numbers.clone(), object.clone()) {
                return Ok(());
            }
            for fork in &mut self.forks {
                if let Ok(()) = fork.merge(numbers.clone(), object.clone()) {
                    return Ok(());
                }
            }
        }
        Err(())
    }

    /// Insert a segment, returning a diff of the canonical chain.
    /// Segments may be inserted in any order.
    pub fn insert(&mut self, segment: Segment) -> TableSegmentsDiff {
        #[cfg(any(test, debug_assertions))]
        self.check_invariants();

        let mut diff: TableSegmentsDiff = Default::default();
        match &mut self.canonical {
            None => {
                self.canonical = Some(SegmentGroup(vec![segment.clone()]));
                diff.add.push(segment);
                return diff;
            }
            Some(canonical) => match canonical.insert(&segment) {
                Ok(()) => {
                    for index in 0..self.forks.len() {
                        if !self.forks[index].adjacent_after(canonical.bounds().1) {
                            continue;
                        }
                        let mut fork = self.forks.remove(index);
                        diff.add.append(&mut fork.0.clone());
                        canonical.0.append(&mut fork.0);
                        break;
                    }
                    diff.add.push(segment);
                    return diff;
                }
                Err(()) => (),
            },
        };

        let fork_index = self.update_forks(segment);
        let canonical = self.canonical.as_ref().unwrap();
        let fork = &self.forks[fork_index];

        if fork.adjacent_after(canonical.bounds().1) {
            diff.add.append(&mut fork.0.clone());
            let canonical = self.canonical.as_mut().unwrap();
            canonical.0.append(&mut self.forks.remove(fork_index).0);
            return diff;
        }

        if (fork.start() < canonical.start())
            || (fork.start() == canonical.start() && fork.end() > canonical.end())
        {
            diff.add.append(&mut fork.0.clone());
            diff.sub.append(&mut canonical.0.clone());
            self.forks.push(self.canonical.take().unwrap());
            self.canonical = Some(self.forks.remove(fork_index));
        }
        diff
    }

    fn update_forks(&mut self, segment: Segment) -> usize {
        for index in 0..self.forks.len() {
            match self.forks[index].insert(&segment) {
                Ok(()) => return self.merge_forks(index),
                Err(()) => continue,
            };
        }
        self.forks.push(SegmentGroup(vec![segment]));
        self.forks.len() - 1
    }

    fn merge_forks(&mut self, mut updated_index: usize) -> usize {
        for mut index in 0..self.forks.len() {
            let (start, end) = self.forks[index].bounds();
            if self.forks[updated_index].adjacent_before(start) {
                let mut next = self.forks.remove(index);
                if index < updated_index {
                    updated_index -= 1;
                }
                self.forks[updated_index].0.append(&mut next.0);
                return updated_index;
            }
            if self.forks[updated_index].adjacent_after(end) {
                let mut next = self.forks.remove(updated_index);
                if updated_index < index {
                    index -= 1;
                }
                self.forks[index].0.append(&mut next.0);
                return index;
            }
        }
        updated_index
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SegmentGroup(Vec<Segment>);

impl SegmentGroup {
    #[cfg(any(test, debug_assertions))]
    fn check_invariants(&self) {
        use itertools::Itertools as _;
        assert!(!self.0.is_empty());
        for (a, b) in self.0.iter().tuple_windows() {
            assert!((*a.range.numbers.end() + 1) == *b.range.numbers.start());
            assert!(a.range.network == b.range.network);
            assert!(b.range.prev_hash.map(|h| h == a.range.hash).unwrap_or(true));
        }
    }

    fn bounds(&self) -> (&BlockRange, &BlockRange) {
        let first = &self.0.first().unwrap().range;
        let last = &self.0.last().unwrap().range;
        (first, last)
    }

    fn start(&self) -> BlockNum {
        *self.0.first().unwrap().range.numbers.start()
    }

    fn end(&self) -> BlockNum {
        *self.0.last().unwrap().range.numbers.end()
    }

    fn adjacent_before(&self, range: &BlockRange) -> bool {
        let (_, last) = self.bounds();
        if range.network != last.network {
            return false;
        }
        if *range.numbers.start() != (*last.numbers.end() + 1) {
            return false;
        }
        match &range.prev_hash {
            None => true,
            Some(prev_hash) => prev_hash == &last.hash,
        }
    }

    fn adjacent_after(&self, range: &BlockRange) -> bool {
        let (first, _) = self.bounds();
        if range.network != first.network {
            return false;
        }
        if (*range.numbers.end() + 1) != *first.numbers.start() {
            return false;
        }
        match &first.prev_hash {
            None => true,
            Some(prev_hash) => prev_hash == &range.hash,
        }
    }

    fn insert(&mut self, segment: &Segment) -> Result<(), ()> {
        if self.adjacent_before(&segment.range) {
            self.0.push(segment.clone());
            return Ok(());
        }
        if self.adjacent_after(&segment.range) {
            self.0.insert(0, segment.clone());
            return Ok(());
        }
        if let Some(index) = self.0.iter().position(|s| {
            (s.range.network == segment.range.network) && (s.range.numbers == segment.range.numbers)
        }) {
            if &self.0[index] == segment {
                return Ok(());
            }
            if self.0[index].range.prev_hash.is_none() && segment.range.prev_hash.is_none() {
                self.0[index] = segment.clone();
                return Ok(());
            }
        }
        Err(())
    }

    fn merge(&mut self, numbers: RangeInclusive<BlockNum>, object: ObjectMeta) -> Result<(), ()> {
        let end = self
            .0
            .iter()
            .position(|s| s.range.numbers.end() == numbers.end())
            .ok_or(())?;
        let end = self.0.remove(end).range;
        let index = self
            .0
            .iter()
            .position(|s| s.range.numbers.start() == numbers.start())
            .ok_or(())?;
        let start = self.0.remove(index).range;
        let segment = Segment {
            range: BlockRange {
                numbers: *start.numbers.start()..=*end.numbers.end(),
                network: start.network,
                hash: end.hash,
                prev_hash: start.prev_hash,
            },
            object,
        };
        self.0.insert(index, segment);
        Ok(())
    }
}

pub fn missing_block_ranges(
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

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        ops::{Range, RangeInclusive},
    };

    use alloy::primitives::BlockHash;
    use object_store::ObjectMeta;
    use rand::{Rng as _, RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};

    use super::{BlockRange, TableSegments};
    use crate::{BlockNum, metadata::segments::Segment};

    #[test]
    fn canonical_segments() {
        let seed = rand::rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let segments = gen_segments(&mut rng);
        println!(
            "{:?}",
            segments
                .iter()
                .map(|s| format!("({:?}, {})", s.range.numbers, s.range.hash.0[31]))
                .collect::<Vec<_>>()
        );
        let mut model: Vec<BlockRange> = Default::default();
        let mut table: TableSegments = Default::default();
        for segment in {
            let mut segments = segments.clone();
            segments.shuffle(&mut rng);
            segments
        } {
            println!(
                "insert ({:?}, {})",
                segment.range.numbers, segment.range.hash.0[31]
            );
            let diff = table.insert(segment);
            for segment in diff.sub {
                let index = model.iter().position(|r| r == &segment.range).unwrap();
                model.remove(index);
            }
            for segment in diff.add {
                model.push(segment.range);
            }
        }
        table.check_invariants();

        model.sort_by_key(|r| *r.numbers.start());
        let expected_model = segments
            .iter()
            .map(|s| &s.range)
            .filter(|r| r.hash[31] == 0)
            .cloned()
            .collect::<Vec<BlockRange>>();
        assert_eq!(model, expected_model);
    }

    #[test]
    fn canonical_segments_no_prev_hash() {
        let seed = rand::rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let mut segments = gen_segments(&mut rng);
        for segment in &mut segments {
            segment.range.prev_hash = None;
        }
        println!(
            "{:?}",
            segments
                .iter()
                .map(|s| format!("({:?}, {})", s.range.numbers, s.range.hash.0[31]))
                .collect::<Vec<_>>()
        );
        let mut model: BTreeMap<BlockNum, BlockRange> = Default::default();
        let mut expected: BTreeMap<BlockNum, BlockRange> = Default::default();
        let mut table: TableSegments = Default::default();
        for segment in {
            let mut segments = segments.clone();
            segments.shuffle(&mut rng);
            segments
        } {
            println!(
                "insert ({:?}, {})",
                segment.range.numbers, segment.range.hash.0[31]
            );
            expected.insert(*segment.range.numbers.start(), segment.range.clone());
            let diff = table.insert(segment);
            for segment in diff.sub {
                model.remove(segment.range.numbers.start());
            }
            for segment in diff.add {
                model.insert(*segment.range.numbers.start(), segment.range);
            }
        }
        table.check_invariants();

        assert_eq!(expected, model);
    }

    fn gen_segments(rng: &mut StdRng) -> Vec<Segment> {
        let hash = |number: u8, fork_index: u8| -> BlockHash {
            let mut hash: BlockHash = Default::default();
            hash.0[0] = number;
            hash.0[31] = fork_index;
            hash
        };
        let gen_ranges = |numbers: Range<u8>, fork_index: u8| -> Vec<BlockRange> {
            numbers
                .map(|n| BlockRange {
                    numbers: (n as u64)..=(n as u64),
                    network: "test".to_string(),
                    hash: hash(n, fork_index),
                    prev_hash: Some(hash(n.checked_sub(1).unwrap_or(0), fork_index)),
                })
                .collect()
        };

        let mut ranges: Vec<BlockRange> = Default::default();
        let canonical_chain_depth = 10;
        let max_fork_depth = 2;
        ranges.append(&mut gen_ranges(0..canonical_chain_depth, 0));
        // generate forks
        for fork_index in 1..(1 + rng.random_range(0..3)) {
            let start = rng.random_range(0..(canonical_chain_depth - max_fork_depth));
            let end = rng.random_range((start + 1)..(start + 3));
            ranges.append(&mut gen_ranges(start..end, fork_index));
        }
        assert!(
            ranges
                .iter()
                .all(|r| *r.numbers.end() < canonical_chain_depth as u64)
        );
        ranges
            .into_iter()
            .map(|range| Segment {
                object: ObjectMeta {
                    location: format!("{:?}", range.hash).into(),
                    last_modified: Default::default(),
                    size: 0,
                    e_tag: None,
                    version: None,
                },
                range,
            })
            .collect()
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
    fn missing_ranges() {
        fn missing_ranges(
            ranges: Vec<RangeInclusive<BlockNum>>,
            desired: RangeInclusive<BlockNum>,
        ) -> Vec<RangeInclusive<BlockNum>> {
            let mut table: TableSegments = Default::default();
            for numbers in ranges {
                table.insert(Segment {
                    range: BlockRange {
                        numbers,
                        network: "test".to_string(),
                        hash: Default::default(),
                        prev_hash: None,
                    },
                    object: ObjectMeta {
                        location: Default::default(),
                        last_modified: Default::default(),
                        size: 0,
                        e_tag: None,
                        version: None,
                    },
                });
            }
            table.missing_ranges(desired)
        }

        assert_eq!(missing_ranges(vec![], 0..=10), vec![0..=10]);
        assert_eq!(missing_ranges(vec![0..=10], 0..=10), vec![]);
        assert_eq!(missing_ranges(vec![0..=5], 10..=15), vec![10..=15]);
        assert_eq!(missing_ranges(vec![3..=7], 0..=10), vec![0..=2, 8..=10]);
        assert_eq!(missing_ranges(vec![0..=15], 5..=10), vec![]);
        assert_eq!(missing_ranges(vec![5..=15], 0..=10), vec![0..=4]);
        assert_eq!(missing_ranges(vec![0..=5], 0..=10), vec![6..=10]);
        assert_eq!(
            missing_ranges(vec![0..=3, 5..=7], 0..=10),
            vec![4..=4, 8..=10]
        );
        assert_eq!(
            missing_ranges(vec![0..=2, 5..=7, 1..=12], 0..=15),
            vec![13..=15]
        );
    }
}
