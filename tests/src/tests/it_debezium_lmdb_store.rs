//! Integration tests for LMDB state store.
//!
//! These tests verify the LMDB backend works correctly with the StateStore trait,
//! including data persistence, handle_data, handle_watermark, and handle_invalidation.

use std::sync::Arc;

use amp_debezium_client::{LmdbStore, StateStore};
use common::arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
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
async fn lmdb_append_stores_batch() {
    //* Given - An LMDB store
    let temp_dir = TempDir::new().unwrap();
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();

    //* When - Store a batch
    let batch = create_test_batch(vec![100, 101, 102]);
    store.append(batch.clone(), 0).await.unwrap();

    //* Then - Batch is stored (verify by reopening and checking retraction)
    let retracted = store.retract(0..=0).await.unwrap();
    assert_eq!(retracted.len(), 1);
    assert_eq!(retracted[0].num_rows(), 3);
}

#[tokio::test]
async fn lmdb_handle_invalidation_retracts_batches() {
    //* Given - LMDB store with multiple batches
    let temp_dir = TempDir::new().unwrap();
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();

    let batch = create_test_batch(vec![100, 101, 102]);

    // Store batches with IDs 0, 1, 2, 3, 4
    for id in 0..=4 {
        store.append(batch.clone(), id).await.unwrap();
    }

    //* When - Invalidate IDs 1..=3
    let retracted = store.retract(1..=3).await.unwrap();

    //* Then - 3 batches retracted
    assert_eq!(retracted.len(), 3);
    for batch in &retracted {
        assert_eq!(batch.num_rows(), 3);
    }

    // Verify IDs 0 and 4 still exist
    let remaining = store.retract(0..=0).await.unwrap();
    assert_eq!(remaining.len(), 1);

    let remaining = store.retract(4..=4).await.unwrap();
    assert_eq!(remaining.len(), 1);
}

#[tokio::test]
async fn lmdb_handle_watermark_prunes_old_batches() {
    //* Given - LMDB store with batches 0..=20
    let temp_dir = TempDir::new().unwrap();
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();

    let batch = create_test_batch(vec![100]);

    for id in 0..=20 {
        store.append(batch.clone(), id).await.unwrap();
    }

    //* When - Prune batches with ID < 10
    store.prune(10).await.unwrap();

    //* Then - Only batches 10..=20 remain (11 batches)
    // Try to invalidate 0..=9 (should return empty)
    let pruned = store.retract(0..=9).await.unwrap();
    assert_eq!(pruned.len(), 0, "Batches 0-9 should be pruned");

    // Verify 10..=20 still exist
    let remaining = store.retract(10..=20).await.unwrap();
    assert_eq!(remaining.len(), 11, "Batches 10-20 should remain");
}

#[tokio::test]
async fn lmdb_persistence_across_restarts() {
    //* Given - Store batches then close
    let temp_dir = TempDir::new().unwrap();

    {
        let mut store = LmdbStore::new(temp_dir.path()).unwrap();
        let batch = create_test_batch(vec![100, 200, 300]);

        for id in 0..=4 {
            store.append(batch.clone(), id).await.unwrap();
        }
    }

    //* When - Reopen and query
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();
    let retracted = store.retract(0..=4).await.unwrap();

    //* Then - All 5 batches are still there
    assert_eq!(retracted.len(), 5);
    for batch in &retracted {
        assert_eq!(batch.num_rows(), 3);
    }
}

#[tokio::test]
async fn lmdb_empty_invalidation_range() {
    //* Given - LMDB store with some batches
    let temp_dir = TempDir::new().unwrap();
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();

    let batch = create_test_batch(vec![100]);
    store.append(batch.clone(), 5).await.unwrap();
    store.append(batch.clone(), 10).await.unwrap();

    //* When - Invalidate non-existent range
    let retracted = store.retract(0..=4).await.unwrap();

    //* Then - No batches retracted
    assert_eq!(retracted.len(), 0);
}

#[tokio::test]
async fn lmdb_watermark_with_zero_cutoff() {
    //* Given - LMDB store with batches
    let temp_dir = TempDir::new().unwrap();
    let mut store = LmdbStore::new(temp_dir.path()).unwrap();

    let batch = create_test_batch(vec![100]);
    for id in 0..=5 {
        store.append(batch.clone(), id).await.unwrap();
    }

    //* When - Prune with cutoff = 0 (should be a no-op)
    store.prune(0).await.unwrap();

    //* Then - All batches remain
    let remaining = store.retract(0..=5).await.unwrap();
    assert_eq!(remaining.len(), 6);
}
