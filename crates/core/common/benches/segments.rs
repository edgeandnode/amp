use alloy::primitives::BlockHash;
use chrono::DateTime;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datasets_common::{block_num::BlockNum, block_range::BlockRange};
use metadata_db::files::FileId;
use object_store::ObjectMeta;

use common::metadata::segments::{Segment, canonical_chain};

fn test_hash(number: u64, fork: u8) -> BlockHash {
    let mut hash: BlockHash = Default::default();
    // Store lower 8 bytes of number in the hash
    hash.0[0..8].copy_from_slice(&number.to_le_bytes());
    hash.0[31] = fork;
    hash
}

fn test_range(numbers: std::ops::RangeInclusive<BlockNum>, fork: (u8, u8)) -> BlockRange {
    BlockRange {
        numbers: numbers.clone(),
        network: "test".parse().expect("valid network id"),
        hash: test_hash(*numbers.end(), fork.1),
        prev_hash: if *numbers.start() == 0 {
            Default::default()
        } else {
            test_hash(*numbers.start() - 1, fork.0)
        },
    }
}

fn test_segment(
    numbers: std::ops::RangeInclusive<BlockNum>,
    fork: (u8, u8),
    timestamp: i64,
) -> Segment {
    let range = test_range(numbers, fork);
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

fn build_segments() -> Vec<Segment> {
    let canonical_len = 100_000 as u64;
    let segments_len = canonical_len + (canonical_len / 1000) + 2;
    let mut segments = Vec::with_capacity(segments_len as usize);

    // Build 100,000 canonical chain segments (each covering 1 block)
    for i in 0..canonical_len {
        segments.push(test_segment(i..=i, (0, 0), 0));

        // Add 1 fork segment every 1000 canonical segments (hash mismatch)
        if i > 0 && i % 1000 == 0 {
            // Fork segment at this position with different fork identifier
            segments.push(test_segment(i..=i, (1, 1), 0));
        }
    }

    // Add final fork of 2 segments extending beyond canonical chain
    let fork_start = canonical_len - 1;
    segments.push(test_segment(fork_start..=(fork_start + 1), (1, 1), 0));
    segments.push(test_segment((fork_start + 2)..=(fork_start + 3), (1, 1), 0));

    segments
}

fn bench_chains(c: &mut Criterion) {
    c.bench_function("chains_100k_segments", |b| {
        b.iter(|| {
            let segments = build_segments();
            let chain = canonical_chain(segments);
            black_box(chain);
        })
    });
}

criterion_group!(benches, bench_chains);
criterion_main!(benches);
