//! Integration tests for RocksDB state store.
//!
//! These tests verify the RocksDB backend works correctly with the StateStore trait,
//! including persistence across restarts and multi-network scenarios.

use std::sync::Arc;

use amp_client::InvalidationRange;
use amp_debezium_client::{RocksDbStore, StateStore, StoredBatch};
use common::{
    arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    metadata::segments::BlockRange,
};
use tempfile::TempDir;

fn create_test_batch(block_nums: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]));
    let block_array = Int64Array::from(block_nums.clone());
    let data_array = StringArray::from(
        block_nums
            .iter()
            .map(|n| format!("data_{}", n))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(schema, vec![Arc::new(block_array), Arc::new(data_array)]).unwrap()
}

#[tokio::test]
async fn rocksdb_basic_operations() {
    //* Given - A RocksDB store with test data
    let temp_dir = TempDir::new().unwrap();
    let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();

    let batch = Arc::new(create_test_batch(vec![100, 101, 102]));
    let stored_batch = StoredBatch {
        batch: batch.clone(),
        ranges: vec![BlockRange {
            network: "ethereum".to_string(),
            numbers: 100..=102,
            hash: [1u8; 32].into(),
            prev_hash: Some([0u8; 32].into()),
        }],
    };

    //* When - Insert and retrieve
    store.insert(stored_batch).await.unwrap();

    let ranges = vec![InvalidationRange {
        network: "ethereum".to_string(),
        numbers: 100..=102,
    }];
    let batches = store.get_in_ranges(&ranges).await.unwrap();

    //* Then - Data is retrievable
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn rocksdb_multi_network_storage() {
    //* Given - A RocksDB store with data from multiple networks
    let temp_dir = TempDir::new().unwrap();
    let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();

    // Insert batch spanning multiple networks
    let batch = Arc::new(create_test_batch(vec![100, 200]));
    let stored_batch = StoredBatch {
        batch: batch.clone(),
        ranges: vec![
            BlockRange {
                network: "ethereum".to_string(),
                numbers: 100..=100,
                hash: [1u8; 32].into(),
                prev_hash: None,
            },
            BlockRange {
                network: "polygon".to_string(),
                numbers: 200..=200,
                hash: [2u8; 32].into(),
                prev_hash: None,
            },
        ],
    };

    store.insert(stored_batch).await.unwrap();

    //* When - Query by different networks
    let eth_ranges = vec![InvalidationRange {
        network: "ethereum".to_string(),
        numbers: 100..=100,
    }];
    let poly_ranges = vec![InvalidationRange {
        network: "polygon".to_string(),
        numbers: 200..=200,
    }];

    let eth_batches = store.get_in_ranges(&eth_ranges).await.unwrap();
    let poly_batches = store.get_in_ranges(&poly_ranges).await.unwrap();

    //* Then - Both queries return the same batch
    assert_eq!(eth_batches.len(), 1);
    assert_eq!(poly_batches.len(), 1);
    assert_eq!(eth_batches[0].num_rows(), 2);
    assert_eq!(poly_batches[0].num_rows(), 2);
}

#[tokio::test]
async fn rocksdb_persistence_with_multiple_batches() {
    //* Given - Multiple batches stored in RocksDB
    let temp_dir = TempDir::new().unwrap();

    {
        let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();

        for block_num in [100u64, 200, 300] {
            let batch = Arc::new(create_test_batch(vec![block_num as i64]));
            let stored_batch = StoredBatch {
                batch: batch.clone(),
                ranges: vec![BlockRange {
                    network: "ethereum".to_string(),
                    numbers: block_num..=block_num,
                    hash: [block_num as u8; 32].into(),
                    prev_hash: None,
                }],
            };
            store.insert(stored_batch).await.unwrap();
        }
    }

    //* When - Reopen database and query
    let store = RocksDbStore::new(temp_dir.path(), 64).unwrap();
    let ranges = vec![InvalidationRange {
        network: "ethereum".to_string(),
        numbers: 100..=300,
    }];
    let batches = store.get_in_ranges(&ranges).await.unwrap();

    //* Then - All batches are still there
    assert_eq!(batches.len(), 3);
    for batch in &batches {
        assert_eq!(batch.num_rows(), 1);
    }
}

#[tokio::test]
async fn rocksdb_prune_respects_multi_network_watermarks() {
    //* Given - Batches with ranges from multiple networks
    let temp_dir = TempDir::new().unwrap();
    let mut store = RocksDbStore::new(temp_dir.path(), 10).unwrap();

    // Insert a batch spanning two networks
    for block_num in 0u64..=20u64 {
        let batch = Arc::new(create_test_batch(vec![block_num as i64]));
        let stored_batch = StoredBatch {
            batch: batch.clone(),
            ranges: vec![
                BlockRange {
                    network: "fast_chain".to_string(),
                    numbers: block_num..=block_num,
                    hash: [block_num as u8; 32].into(),
                    prev_hash: None,
                },
                BlockRange {
                    network: "slow_chain".to_string(),
                    numbers: block_num..=block_num,
                    hash: [(block_num + 100) as u8; 32].into(),
                    prev_hash: None,
                },
            ],
        };
        store.insert(stored_batch).await.unwrap();
    }

    //* When - Prune with different watermarks
    let mut watermarks = std::collections::BTreeMap::new();
    watermarks.insert("fast_chain".to_string(), 20);
    watermarks.insert("slow_chain".to_string(), 15);
    store.prune(&watermarks).await.unwrap();

    //* Then - Only batches safe for both networks are pruned
    // prune_before for slow_chain = 15 - 10 = 5
    // So blocks 0-4 should be pruned, 5-20 should remain
    let all_ranges = vec![
        InvalidationRange {
            network: "fast_chain".to_string(),
            numbers: 0..=100,
        },
        InvalidationRange {
            network: "slow_chain".to_string(),
            numbers: 0..=100,
        },
    ];
    let remaining = store.get_in_ranges(&all_ranges).await.unwrap();

    // Should have 16 batches (blocks 5-20)
    assert_eq!(remaining.len(), 16);
}

#[tokio::test]
async fn rocksdb_range_overlap_detection() {
    //* Given - Multiple overlapping batches
    let temp_dir = TempDir::new().unwrap();
    let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();

    // Insert batches with overlapping ranges
    for start_block in [100u64, 105, 110, 115, 120] {
        let batch = Arc::new(create_test_batch(vec![start_block as i64]));
        let stored_batch = StoredBatch {
            batch: batch.clone(),
            ranges: vec![BlockRange {
                network: "ethereum".to_string(),
                numbers: start_block..=start_block + 2,
                hash: [start_block as u8; 32].into(),
                prev_hash: None,
            }],
        };
        store.insert(stored_batch).await.unwrap();
    }

    //* When - Query a range that overlaps multiple batches
    let ranges = vec![InvalidationRange {
        network: "ethereum".to_string(),
        numbers: 107..=113,
    }];
    let batches = store.get_in_ranges(&ranges).await.unwrap();

    //* Then - All overlapping batches are returned
    // Ranges: 100-102, 105-107, 110-112, 115-117, 120-122
    // Query: 107-113
    // Should match: 105-107 (overlap at 107), 110-112 (fully contained)
    assert_eq!(batches.len(), 2);
}
