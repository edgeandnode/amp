use std::{collections::BTreeMap, ops::RangeInclusive};

use alloy::primitives::BlockHash;
use metadata_db::FileId;
use object_store::ObjectMeta;

use crate::BlockNum;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Watermarks(pub BTreeMap<String, Watermark>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Watermark {
    pub number: BlockNum,
    pub hash: BlockHash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockRanges(pub BTreeMap<String, BlockRange>);

impl BlockRanges {
    fn adjacent(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && std::iter::zip(&self.0, &other.0)
                .all(|((n0, r0), (n1, r1))| n0 == n1 && BlockRange::adjacent(r0, r1))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BlockRange {
    pub numbers: RangeInclusive<BlockNum>,
    pub hash: BlockHash,
    pub prev_hash: Option<BlockHash>,
}

impl BlockRange {
    pub fn start(&self) -> BlockNum {
        *self.numbers.start()
    }

    pub fn end(&self) -> BlockNum {
        *self.numbers.end()
    }

    fn adjacent(&self, other: &Self) -> bool {
        (self.end() + 1) == other.start() && other.prev_hash.map(|h| h == self.hash).unwrap_or(true)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub id: FileId,
    pub object: ObjectMeta,
    pub ranges: BlockRanges,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chain(pub Vec<Segment>);

impl Chain {
    #[cfg(test)]
    fn check_invariants(&self) {
        assert!(!self.0.is_empty());
        for w in self.0.windows(2) {
            assert!(BlockRanges::adjacent(&w[0].ranges, &w[1].ranges));
        }
    }

    pub fn first(&self) -> &BlockRanges {
        &self.0.first().unwrap().ranges
    }

    pub fn last(&self) -> &BlockRanges {
        &self.0.last().unwrap().ranges
    }

    pub fn range(&self) -> BlockRanges {
        BlockRanges(
            std::iter::zip(&self.first().0, &self.last().0)
                .map(|((network, r0), (_, r1))| {
                    let range = BlockRange {
                        numbers: r0.start()..=r1.end(),
                        hash: r1.hash,
                        prev_hash: r0.prev_hash,
                    };
                    (network.clone(), range)
                })
                .collect(),
        )
    }
}

impl IntoIterator for Chain {
    type Item = Segment;
    type IntoIter = std::vec::IntoIter<Segment>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

struct Chains {
    canonical: Chain,
    fork: Option<Chain>,
}

fn chains(segments: Vec<Segment>) -> Option<Chain> {
    todo!()
}
