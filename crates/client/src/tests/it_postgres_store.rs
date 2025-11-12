//! Integration tests for PostgresStateStore

use std::collections::VecDeque;

use alloy::primitives::BlockHash;
use common::metadata::segments::BlockRange;

use crate::{
    store::{PostgresStateStore, StateStore},
    transactional::Commit,
};

/// Helper function to create a test store with a temporary database.
async fn create_test_store(temp_db: &pgtemp::PgTempDB) -> PostgresStateStore {
    let stream_id = format!("test-stream-{}", uuid::Uuid::new_v4());

    // Create pool and run migrations
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to test database");

    PostgresStateStore::migrate(&pool)
        .await
        .expect("Failed to run migrations");

    // Create store
    PostgresStateStore::new(pool, &stream_id)
        .await
        .expect("Failed to create test store")
}

/// Helper to create test block ranges.
fn create_test_ranges(start: u64, end: u64, network: &str) -> Vec<BlockRange> {
    vec![BlockRange {
        numbers: start..=end,
        network: network.to_string(),
        hash: BlockHash::ZERO,
        prev_hash: None,
    }]
}

#[tokio::test]
async fn load_with_default_state_returns_empty_snapshot() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let store = create_test_store(&temp_db).await;

    //* When
    let snapshot = store
        .load()
        .await
        .expect("load should succeed with new store");

    //* Then
    assert_eq!(
        snapshot.next, 0,
        "next transaction id should be 0 for new store"
    );
    assert_eq!(
        snapshot.buffer,
        VecDeque::new(),
        "buffer should be empty for new store"
    );
}

#[tokio::test]
async fn advance_with_new_transaction_id_updates_next() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;

    //* When
    store
        .advance(10)
        .await
        .expect("advance should succeed with valid id");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.next, 10,
        "next transaction id should be updated to 10"
    );
}

#[tokio::test]
async fn advance_multiple_times_updates_to_latest() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;

    //* When
    store
        .advance(5)
        .await
        .expect("first advance should succeed");
    store
        .advance(10)
        .await
        .expect("second advance should succeed");
    store
        .advance(15)
        .await
        .expect("third advance should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.next, 15,
        "next transaction id should be updated to latest value"
    );
}

#[tokio::test]
async fn commit_with_insert_adds_watermark_to_buffer() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    let commit = Commit {
        insert: vec![(1, ranges.clone())],
        prune: None,
    };

    //* When
    store.commit(commit).await.expect("commit should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(snapshot.buffer.len(), 1, "buffer should have one entry");
    assert_eq!(
        snapshot.buffer[0].0, 1,
        "transaction id should match inserted watermark"
    );
    assert_eq!(
        snapshot.buffer[0].1, ranges,
        "ranges should match inserted watermark"
    );
}

#[tokio::test]
async fn commit_with_multiple_inserts_adds_all_watermarks() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges1 = create_test_ranges(0, 10, "eth");
    let ranges2 = create_test_ranges(11, 20, "eth");
    let ranges3 = create_test_ranges(21, 30, "eth");

    let commit = Commit {
        insert: vec![
            (1, ranges1.clone()),
            (2, ranges2.clone()),
            (3, ranges3.clone()),
        ],
        prune: None,
    };

    //* When
    store.commit(commit).await.expect("commit should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(snapshot.buffer.len(), 3, "buffer should have three entries");
    assert_eq!(snapshot.buffer[0].0, 1, "first entry should have id 1");
    assert_eq!(snapshot.buffer[1].0, 2, "second entry should have id 2");
    assert_eq!(snapshot.buffer[2].0, 3, "third entry should have id 3");
}

#[tokio::test]
async fn commit_with_prune_removes_watermarks() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    // First insert three watermarks
    let insert_commit = Commit {
        insert: vec![
            (1, ranges.clone()),
            (2, ranges.clone()),
            (3, ranges.clone()),
        ],
        prune: None,
    };
    store
        .commit(insert_commit)
        .await
        .expect("insert commit should succeed");

    // Then prune the first two (all entries <= 2)
    let prune_commit = Commit {
        insert: vec![],
        prune: Some(2),
    };

    //* When
    store
        .commit(prune_commit)
        .await
        .expect("prune commit should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.buffer.len(),
        1,
        "buffer should have one entry after pruning"
    );
    assert_eq!(
        snapshot.buffer[0].0, 3,
        "remaining entry should be the unpruned watermark"
    );
}

#[tokio::test]
async fn truncate_removes_watermarks() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    // First insert three watermarks
    let insert_commit = Commit {
        insert: vec![
            (1, ranges.clone()),
            (2, ranges.clone()),
            (3, ranges.clone()),
        ],
        prune: None,
    };
    store
        .commit(insert_commit)
        .await
        .expect("insert commit should succeed");

    //* When
    // Truncate from transaction id 2 (removes all entries >= 2)
    store.truncate(2).await.expect("truncate should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.buffer.len(),
        1,
        "buffer should have one entry after truncation"
    );
    assert_eq!(
        snapshot.buffer[0].0, 1,
        "remaining entry should be the non-truncated watermark"
    );
}

#[tokio::test]
async fn commit_with_insert_and_prune_applies_both_operations() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    // First insert two watermarks
    let insert_commit = Commit {
        insert: vec![(1, ranges.clone()), (2, ranges.clone())],
        prune: None,
    };
    store
        .commit(insert_commit)
        .await
        .expect("insert commit should succeed");

    // Then add a new one and prune the first (all entries <= 1)
    let combined_commit = Commit {
        insert: vec![(3, ranges.clone())],
        prune: Some(1),
    };

    //* When
    store
        .commit(combined_commit)
        .await
        .expect("combined commit should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.buffer.len(),
        2,
        "buffer should have two entries after combined operation"
    );
    assert_eq!(snapshot.buffer[0].0, 2, "first entry should be watermark 2");
    assert_eq!(
        snapshot.buffer[1].0, 3,
        "second entry should be new watermark 3"
    );
}

#[tokio::test]
async fn commit_and_truncate_applies_both_operations() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    // First insert four watermarks
    let insert_commit = Commit {
        insert: vec![
            (1, ranges.clone()),
            (2, ranges.clone()),
            (3, ranges.clone()),
            (4, ranges.clone()),
        ],
        prune: None,
    };
    store
        .commit(insert_commit)
        .await
        .expect("insert commit should succeed");

    // Then prune the first (all entries <= 1)
    let prune_commit = Commit {
        insert: vec![],
        prune: Some(1),
    };
    store
        .commit(prune_commit)
        .await
        .expect("prune commit should succeed");

    //* When
    // Then truncate from transaction id 4 (removes all entries >= 4)
    store.truncate(4).await.expect("truncate should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(
        snapshot.buffer.len(),
        2,
        "buffer should have two entries after combined operation"
    );
    assert_eq!(snapshot.buffer[0].0, 2, "first entry should be watermark 2");
    assert_eq!(
        snapshot.buffer[1].0, 3,
        "second entry should be watermark 3"
    );
}

#[tokio::test]
async fn load_from_different_store_instance_sees_committed_state() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let stream_id = format!("test-stream-{}", uuid::Uuid::new_v4());

    // Create pool and run migrations once
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to test database");

    PostgresStateStore::migrate(&pool)
        .await
        .expect("Failed to run migrations");

    // First store instance
    let mut store1 = PostgresStateStore::new(pool.clone(), &stream_id)
        .await
        .expect("Failed to create first store");

    let ranges = create_test_ranges(0, 10, "eth");

    // Commit data with first instance
    store1.advance(5).await.expect("advance should succeed");
    let commit = Commit {
        insert: vec![(1, ranges.clone())],
        prune: None,
    };
    store1.commit(commit).await.expect("commit should succeed");

    // Second store instance with same stream_id (reuses pool)
    let store2 = PostgresStateStore::new(pool.clone(), &stream_id)
        .await
        .expect("Failed to create second store");

    //* When
    let snapshot = store2.load().await.expect("load should succeed");

    //* Then
    assert_eq!(
        snapshot.next, 5,
        "second store should see committed next value"
    );
    assert_eq!(
        snapshot.buffer.len(),
        1,
        "second store should see committed buffer"
    );
    assert_eq!(
        snapshot.buffer[0].0, 1,
        "second store should see committed watermark id"
    );
}

#[tokio::test]
async fn different_stream_ids_maintain_independent_state() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let stream_id1 = "stream-1";
    let stream_id2 = "stream-2";

    // Create pool and run migrations once
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&temp_db.connection_uri())
        .await
        .expect("Failed to connect to test database");

    PostgresStateStore::migrate(&pool)
        .await
        .expect("Failed to run migrations");

    // Create two stores with different stream IDs (reuse pool)
    let mut store1 = PostgresStateStore::new(pool.clone(), stream_id1)
        .await
        .expect("Failed to create store 1");

    let store2 = PostgresStateStore::new(pool.clone(), stream_id2)
        .await
        .expect("Failed to create store 2");

    let ranges = create_test_ranges(0, 10, "eth");

    // Commit to store1
    store1
        .advance(10)
        .await
        .expect("advance on store1 should succeed");
    let commit = Commit {
        insert: vec![(1, ranges.clone())],
        prune: None,
    };
    store1
        .commit(commit)
        .await
        .expect("commit on store1 should succeed");

    //* When
    let snapshot1 = store1
        .load()
        .await
        .expect("load from store1 should succeed");
    let snapshot2 = store2
        .load()
        .await
        .expect("load from store2 should succeed");

    //* Then
    assert_eq!(snapshot1.next, 10, "store1 should have next = 10");
    assert_eq!(snapshot1.buffer.len(), 1, "store1 should have 1 watermark");
    assert_eq!(snapshot2.next, 0, "store2 should have default next = 0");
    assert_eq!(snapshot2.buffer.len(), 0, "store2 should have empty buffer");
}

#[tokio::test]
async fn commit_with_empty_operations_succeeds() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;

    let empty_commit = Commit {
        insert: vec![],
        prune: None,
    };

    //* When
    let result = store.commit(empty_commit).await;

    //* Then
    assert!(result.is_ok(), "empty commit should succeed");

    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(snapshot.buffer.len(), 0, "buffer should remain empty");
}

#[tokio::test]
async fn advance_and_commit_preserve_order() {
    //* Given
    let temp_db = pgtemp::PgTempDB::new();
    let mut store = create_test_store(&temp_db).await;
    let ranges = create_test_ranges(0, 10, "eth");

    //* When
    // Sequence of operations
    store.advance(1).await.expect("advance 1 should succeed");
    let commit1 = Commit {
        insert: vec![(1, ranges.clone())],
        prune: None,
    };
    store
        .commit(commit1)
        .await
        .expect("commit 1 should succeed");

    store.advance(2).await.expect("advance 2 should succeed");
    let commit2 = Commit {
        insert: vec![(2, ranges.clone())],
        prune: None,
    };
    store
        .commit(commit2)
        .await
        .expect("commit 2 should succeed");

    //* Then
    let snapshot = store.load().await.expect("load should succeed");
    assert_eq!(snapshot.next, 2, "next should be at latest value");
    assert_eq!(
        snapshot.buffer.len(),
        2,
        "buffer should have both watermarks"
    );
    assert_eq!(snapshot.buffer[0].0, 1, "first watermark should have id 1");
    assert_eq!(snapshot.buffer[1].0, 2, "second watermark should have id 2");
}
