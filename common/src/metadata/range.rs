use std::ops::RangeInclusive;

use alloy::primitives::BlockHash;

use crate::BlockNum;

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
pub struct TableRangesDiff {
    /// block ranges added to the canonical chain
    pub add: Vec<BlockRange>,
    /// block ranges removed from the canonical chain
    pub sub: Vec<BlockRange>,
}

/// Block ranges associated with a table. This incrementally organizes block ranges into either
/// the canonical chain or a set of forks. The canonical chain is defined as the set of adjacent
/// block ranges with the greatest block height.
// We are assuming single block range per segment, for now.
#[derive(Default)]
pub struct TableRanges {
    canonical: Option<RangeGroup>,
    forks: Vec<RangeGroup>,
}

impl TableRanges {
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
    }

    /// Merge known block ranges. This fails if the given block numbers do not correspond to a set
    /// of adjacent and complete block ranges. This should be done after the associated files have
    /// been merged, and the merged files have been committed to the metadata DB.
    pub fn merge(&mut self, numbers: RangeInclusive<BlockNum>) -> Result<(), ()> {
        #[cfg(any(test, debug_assertions))]
        self.check_invariants();

        if let Some(canonical) = &mut self.canonical {
            if let Ok(()) = canonical.merge(numbers.clone()) {
                return Ok(());
            }
            for fork in &mut self.forks {
                if let Ok(()) = fork.merge(numbers.clone()) {
                    return Ok(());
                }
            }
        }
        Err(())
    }

    /// Insert a block range, returning a diff of the canonical chain.
    /// Block ranges may be inserted in any order.
    pub fn insert(&mut self, range: BlockRange) -> TableRangesDiff {
        #[cfg(any(test, debug_assertions))]
        self.check_invariants();

        let mut diff: TableRangesDiff = Default::default();
        match &mut self.canonical {
            None => {
                self.canonical = Some(RangeGroup(vec![range.clone()]));
                diff.add.push(range);
                return diff;
            }
            Some(canonical) => match canonical.insert(&range) {
                Ok(()) => {
                    for index in 0..self.forks.len() {
                        if !self.forks[index].adjacent_before(canonical.bounds().0) {
                            continue;
                        }
                        let mut fork = self.forks.remove(index);
                        diff.add.append(&mut fork.0.clone());
                        let mut canonical = self.canonical.take().unwrap();
                        fork.0.append(&mut canonical.0);
                        self.canonical = Some(fork);
                        break;
                    }
                    diff.add.push(range);
                    return diff;
                }
                Err(()) => (),
            },
        };
        let fork_index = self.update_forks(range);
        let canonical = self.canonical.as_ref().unwrap();
        let fork = &self.forks[fork_index];
        if fork.bounds().1.numbers.end() > canonical.bounds().1.numbers.end() {
            diff.add.append(&mut fork.0.clone());
            diff.sub.append(&mut canonical.0.clone());
            self.forks.push(self.canonical.take().unwrap());
            self.canonical = Some(self.forks.remove(fork_index));
        }
        diff
    }

    fn update_forks(&mut self, range: BlockRange) -> usize {
        for index in 0..self.forks.len() {
            match self.forks[index].insert(&range) {
                Ok(()) => return self.merge_forks(index),
                Err(()) => continue,
            };
        }
        self.forks.push(RangeGroup(vec![range]));
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
struct RangeGroup(Vec<BlockRange>);

impl RangeGroup {
    #[cfg(any(test, debug_assertions))]
    fn check_invariants(&self) {
        use itertools::Itertools as _;
        assert!(!self.0.is_empty());
        for (a, b) in self.0.iter().tuple_windows() {
            assert!((*a.numbers.end() + 1) == *b.numbers.start());
            assert!(a.network == b.network);
            assert!(b.prev_hash.map(|h| h == a.hash).unwrap_or(true));
        }
    }

    fn bounds(&self) -> (&BlockRange, &BlockRange) {
        (self.0.first().unwrap(), self.0.last().unwrap())
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

    fn insert(&mut self, range: &BlockRange) -> Result<(), ()> {
        if self.adjacent_before(range) {
            self.0.push(range.clone());
            return Ok(());
        }
        if self.adjacent_after(range) {
            self.0.insert(0, range.clone());
            return Ok(());
        }
        if let Some(index) = self
            .0
            .iter()
            .position(|r| (r.network == range.network) && (r.numbers == range.numbers))
        {
            if &self.0[index] == range {
                return Ok(());
            }
            if self.0[index].prev_hash.is_none() && range.prev_hash.is_none() {
                self.0[index] = range.clone();
                return Ok(());
            }
        }
        Err(())
    }

    fn merge(&mut self, numbers: RangeInclusive<BlockNum>) -> Result<(), ()> {
        let end = self
            .0
            .iter()
            .position(|r| r.numbers.end() == numbers.end())
            .ok_or(())?;
        let end = self.0.remove(end);
        let index = self
            .0
            .iter()
            .position(|r| r.numbers.start() == numbers.start())
            .ok_or(())?;
        let start = self.0.remove(index);
        let range = BlockRange {
            numbers: *start.numbers.start()..=*end.numbers.end(),
            network: start.network,
            hash: end.hash,
            prev_hash: start.prev_hash,
        };
        self.0.insert(index, range);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, ops::Range};

    use alloy::primitives::BlockHash;
    use rand::{Rng as _, RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};

    use super::{BlockRange, TableRanges};
    use crate::BlockNum;

    #[test]
    fn canonical_segments() {
        let seed = rand::rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);

        let ranges = gen_ranges(&mut rng);
        println!(
            "{:?}",
            ranges
                .iter()
                .map(|r| format!("({:?}, {})", r.numbers, r.hash.0[31]))
                .collect::<Vec<_>>()
        );
        let mut model: Vec<BlockRange> = Default::default();
        let mut table: TableRanges = Default::default();
        for range in {
            let mut ranges = ranges.clone();
            ranges.shuffle(&mut rng);
            ranges
        } {
            println!("insert ({:?}, {})", range.numbers, range.hash.0[31]);
            let diff = table.insert(range);
            for range in diff.sub {
                let index = model.iter().position(|r| r == &range).unwrap();
                model.remove(index);
            }
            for range in diff.add {
                model.push(range);
            }
        }
        table.check_invariants();

        model.sort_by_key(|r| *r.numbers.start());
        let expected_model = ranges
            .iter()
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

        let mut ranges = gen_ranges(&mut rng);
        for range in &mut ranges {
            range.prev_hash = None;
        }
        println!(
            "{:?}",
            ranges
                .iter()
                .map(|r| format!("({:?}, {})", r.numbers, r.hash.0[31]))
                .collect::<Vec<_>>()
        );
        let mut model: BTreeMap<BlockNum, BlockRange> = Default::default();
        let mut expected: BTreeMap<BlockNum, BlockRange> = Default::default();
        let mut table: TableRanges = Default::default();
        for range in {
            let mut ranges = ranges.clone();
            ranges.shuffle(&mut rng);
            ranges
        } {
            println!("insert ({:?}, {})", range.numbers, range.hash.0[31]);
            expected.insert(*range.numbers.start(), range.clone());
            let diff = table.insert(range);
            for range in diff.sub {
                model.remove(range.numbers.start());
            }
            for range in diff.add {
                model.insert(*range.numbers.start(), range);
            }
        }
        table.check_invariants();

        assert_eq!(expected, model);
    }

    fn gen_ranges(rng: &mut StdRng) -> Vec<BlockRange> {
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
    }
}
