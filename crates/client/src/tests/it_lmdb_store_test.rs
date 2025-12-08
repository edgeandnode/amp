//! Tests for LMDB state and batch store implementations.

use arrow::{
    array::{Int32Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use tempfile::TempDir;

use crate::{
    error::Error,
    store::{
        BatchStore, LmdbBatchStore, LmdbStateStore, StateStore, open_lmdb_env,
        open_lmdb_env_with_options,
    },
    transactional::Commit,
};

// Helper function to create a test RecordBatch
fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let ids: Vec<i32> = (0..rows as i32).collect();
    let names: Vec<String> = (0..rows).map(|i| format!("name_{}", i)).collect();

    RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(Int32Array::from(ids)),
            std::sync::Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

/// Test basic load operation returns empty state on first use.
#[tokio::test]
async fn load_returns_empty_state_on_first_use() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let store = LmdbStateStore::new(env)?;

    let state = store.load().await?;

    assert_eq!(state.next, 0);
    assert!(state.buffer.is_empty());

    Ok(())
}

/// Test creating both stores sharing the same LMDB environment.
#[tokio::test]
async fn shared_env_both_stores_work() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();

    let env = open_lmdb_env(dir.path())?;
    let mut state_store = LmdbStateStore::new(env.clone())?;
    let mut batch_store = LmdbBatchStore::new(env)?;

    // Use state store
    state_store.advance(42).await?;
    let state = state_store.load().await?;
    assert_eq!(state.next, 42);

    // Use batch store
    let batch = create_test_batch(5);
    batch_store.append(batch.clone(), 1).await?;
    let loaded = batch_store.load(1).await?.unwrap();
    assert_eq!(loaded.num_rows(), 5);

    Ok(())
}

/// Test that shared stores persist across recreation.
#[tokio::test]
async fn shared_env_persists_across_instances() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // First instance: write data
    {
        let env = open_lmdb_env(&path)?;
        let mut state_store = LmdbStateStore::new(env.clone())?;
        let mut batch_store = LmdbBatchStore::new(env)?;

        state_store.advance(100).await?;
        let commit = Commit {
            insert: vec![(10, vec![])],
            prune: None,
        };
        state_store.commit(commit).await?;

        batch_store.append(create_test_batch(7), 1).await?;
        batch_store.append(create_test_batch(3), 2).await?;
    }

    // Second instance: read data
    {
        let env = open_lmdb_env(&path)?;
        let state_store = LmdbStateStore::new(env.clone())?;
        let batch_store = LmdbBatchStore::new(env)?;

        let state = state_store.load().await?;
        assert_eq!(state.next, 100);
        assert_eq!(state.buffer.len(), 1);

        let batch1 = batch_store.load(1).await?.unwrap();
        assert_eq!(batch1.num_rows(), 7);

        let batch2 = batch_store.load(2).await?.unwrap();
        assert_eq!(batch2.num_rows(), 3);
    }

    Ok(())
}

/// Test commit operation adds watermarks to buffer.
#[tokio::test]
async fn commit_adds_watermarks_to_buffer() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbStateStore::new(env)?;

    // Create a commit with watermarks
    let commit = Commit {
        insert: vec![(1, vec![]), (2, vec![])],
        prune: None,
    };

    store.commit(commit).await?;

    // Load and verify
    let state = store.load().await?;
    assert_eq!(state.buffer.len(), 2);
    assert_eq!(state.buffer[0].0, 1);
    assert_eq!(state.buffer[1].0, 2);

    Ok(())
}

/// Test commit operation with pruning removes old watermarks.
#[tokio::test]
async fn commit_with_pruning_removes_old_watermarks() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbStateStore::new(env)?;

    // Add some watermarks
    let commit1 = Commit {
        insert: vec![(1, vec![]), (2, vec![]), (3, vec![])],
        prune: None,
    };
    store.commit(commit1).await?;

    // Prune watermarks <= 1
    let commit2 = Commit {
        insert: vec![(4, vec![])],
        prune: Some(1),
    };
    store.commit(commit2).await?;

    // Load and verify
    let state = store.load().await?;
    assert_eq!(state.buffer.len(), 3); // IDs 2, 3, 4
    assert_eq!(state.buffer[0].0, 2);
    assert_eq!(state.buffer[1].0, 3);
    assert_eq!(state.buffer[2].0, 4);

    Ok(())
}

/// Test truncate operation removes watermarks from a point onwards.
#[tokio::test]
async fn truncate_removes_watermarks_from_point_onwards() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbStateStore::new(env)?;

    // Add some watermarks
    let commit = Commit {
        insert: vec![(1, vec![]), (2, vec![]), (3, vec![]), (4, vec![])],
        prune: None,
    };
    store.commit(commit).await?;

    // Truncate from ID 3 onwards
    store.truncate(3).await?;

    // Load and verify
    let state = store.load().await?;
    assert_eq!(state.buffer.len(), 2); // IDs 1, 2
    assert_eq!(state.buffer[0].0, 1);
    assert_eq!(state.buffer[1].0, 2);

    Ok(())
}

/// Test that state persists across store instances (crash safety).
#[tokio::test]
async fn state_persists_across_store_instances() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // First store instance: write state
    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbStateStore::new(env)?;

        store.advance(100).await?;

        let commit = Commit {
            insert: vec![(10, vec![]), (20, vec![])],
            prune: None,
        };
        store.commit(commit).await?;
    }

    // Second store instance: read state
    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbStateStore::new(env)?;
        let state = store.load().await?;

        assert_eq!(state.next, 100);
        assert_eq!(state.buffer.len(), 2);
        assert_eq!(state.buffer[0].0, 10);
        assert_eq!(state.buffer[1].0, 20);
    }

    Ok(())
}

/// Test multiple advances persist correctly.
#[tokio::test]
async fn multiple_advances_persist_correctly() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbStateStore::new(env)?;
        store.advance(10).await?;
        store.advance(20).await?;
        store.advance(30).await?;
    }

    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbStateStore::new(env)?;
        let state = store.load().await?;
        assert_eq!(state.next, 30);
    }

    Ok(())
}

/// Test that commit with prune and insert in same operation is atomic.
#[tokio::test]
async fn commit_with_prune_and_insert_is_atomic() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbStateStore::new(env)?;

        // Initial watermarks
        let commit1 = Commit {
            insert: vec![(1, vec![]), (2, vec![]), (3, vec![])],
            prune: None,
        };
        store.commit(commit1).await?;

        // Atomic prune + insert
        let commit2 = Commit {
            insert: vec![(4, vec![]), (5, vec![])],
            prune: Some(2), // Prune 1, 2
        };
        store.commit(commit2).await?;
    }

    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbStateStore::new(env)?;
        let state = store.load().await?;

        // Should only have IDs 3, 4, 5
        assert_eq!(state.buffer.len(), 3);
        assert_eq!(state.buffer[0].0, 3);
        assert_eq!(state.buffer[1].0, 4);
        assert_eq!(state.buffer[2].0, 5);
    }

    Ok(())
}

/// Test crash recovery scenario: uncommitted data is lost, committed data persists.
#[tokio::test]
async fn crash_recovery_uncommitted_lost_committed_persists() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Simulate first session
    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbStateStore::new(env)?;

        // Committed work
        store.advance(10).await?;
        let commit = Commit {
            insert: vec![(5, vec![])],
            prune: None,
        };
        store.commit(commit).await?;

        // Uncommitted work (advance but no commit)
        store.advance(20).await?;

        // Simulated crash here (store dropped without committing)
    }

    // Recover from crash
    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbStateStore::new(env)?;
        let state = store.load().await?;

        // Last committed advance persists
        assert_eq!(state.next, 20);

        // Watermark persists
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].0, 5);
    }

    Ok(())
}

/// Test basic append and load operations for a single batch.
#[tokio::test]
async fn batch_store_append_and_load_single_batch() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    let batch = create_test_batch(5);
    let id = 42;

    // Append batch
    store.append(batch.clone(), id).await?;

    // Load and verify
    let loaded = store.load(id).await?;
    assert!(loaded.is_some());
    let loaded_batch = loaded.unwrap();
    assert_eq!(loaded_batch.num_rows(), batch.num_rows());
    assert_eq!(loaded_batch.num_columns(), batch.num_columns());

    Ok(())
}

/// Test that loading a nonexistent batch returns None.
#[tokio::test]
async fn batch_store_load_nonexistent_returns_none() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let store = LmdbBatchStore::new(env)?;

    let loaded = store.load(999).await?;
    assert!(loaded.is_none());

    Ok(())
}

/// Test seek operation with empty range.
#[tokio::test]
async fn batch_store_seek_empty_range() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let store = LmdbBatchStore::new(env)?;

    let ids = store.seek(10..=20).await?;
    assert!(ids.is_empty());

    Ok(())
}

/// Test seek operation with sparse IDs.
#[tokio::test]
async fn batch_store_seek_with_sparse_ids() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Store batches at IDs 5, 10, 15, 20
    for id in [5, 10, 15, 20] {
        let batch = create_test_batch(3);
        store.append(batch, id).await?;
    }

    // Seek range 8..=18 should find IDs 10, 15
    let ids = store.seek(8..=18).await?;
    assert_eq!(ids, vec![10, 15]);

    // Seek range 1..=25 should find all
    let ids = store.seek(1..=25).await?;
    assert_eq!(ids, vec![5, 10, 15, 20]);

    // Seek range 12..=14 should find nothing
    let ids = store.seek(12..=14).await?;
    assert!(ids.is_empty());

    Ok(())
}

/// Test that prune removes correct batches.
#[tokio::test]
async fn batch_store_prune_removes_correct_batches() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Store batches at IDs 1, 2, 3, 4, 5
    for id in 1..=5 {
        let batch = create_test_batch(2);
        store.append(batch, id).await?;
    }

    // Prune up to and including ID 3
    store.prune(3).await?;

    // IDs 1, 2, 3 should be gone; 4, 5 should remain
    assert!(store.load(1).await?.is_none());
    assert!(store.load(2).await?.is_none());
    assert!(store.load(3).await?.is_none());
    assert!(store.load(4).await?.is_some());
    assert!(store.load(5).await?.is_some());

    Ok(())
}

/// Test that prune is idempotent.
#[tokio::test]
async fn batch_store_prune_is_idempotent() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Store batches
    for id in 1..=5 {
        let batch = create_test_batch(2);
        store.append(batch, id).await?;
    }

    // Prune twice with same cutoff
    store.prune(2).await?;
    store.prune(2).await?; // Should not error

    // Verify state
    assert!(store.load(1).await?.is_none());
    assert!(store.load(2).await?.is_none());
    assert!(store.load(3).await?.is_some());

    Ok(())
}

/// Test that batches persist across store instances.
#[tokio::test]
async fn batch_store_persists_across_instances() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let batch = create_test_batch(10);

    // First instance: write batches
    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbBatchStore::new(env)?;
        store.append(batch.clone(), 1).await?;
        store.append(create_test_batch(5), 2).await?;
        store.append(create_test_batch(3), 3).await?;
    }

    // Second instance: read batches
    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbBatchStore::new(env)?;

        let loaded1 = store.load(1).await?.unwrap();
        assert_eq!(loaded1.num_rows(), 10);

        let loaded2 = store.load(2).await?.unwrap();
        assert_eq!(loaded2.num_rows(), 5);

        let loaded3 = store.load(3).await?.unwrap();
        assert_eq!(loaded3.num_rows(), 3);

        let ids = store.seek(1..=3).await?;
        assert_eq!(ids, vec![1, 2, 3]);
    }

    Ok(())
}

/// Test seek with boundary conditions.
#[tokio::test]
async fn batch_store_seek_boundary_conditions() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Store batches at IDs 5, 10, 15
    for id in [5, 10, 15] {
        store.append(create_test_batch(1), id).await?;
    }

    // Exact boundary matches
    let ids = store.seek(5..=5).await?;
    assert_eq!(ids, vec![5]);

    let ids = store.seek(10..=10).await?;
    assert_eq!(ids, vec![10]);

    // Start before first, end at first
    let ids = store.seek(0..=5).await?;
    assert_eq!(ids, vec![5]);

    // Start at last, end after last
    let ids = store.seek(15..=100).await?;
    assert_eq!(ids, vec![15]);

    // Range completely outside
    let ids = store.seek(100..=200).await?;
    assert!(ids.is_empty());

    Ok(())
}

/// Test large batch handling.
#[tokio::test]
async fn batch_store_handles_large_batches() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Create a large batch (1000 rows)
    let large_batch = create_test_batch(1000);
    store.append(large_batch.clone(), 1).await?;

    let loaded = store.load(1).await?.unwrap();
    assert_eq!(loaded.num_rows(), 1000);
    assert_eq!(loaded.num_columns(), large_batch.num_columns());

    Ok(())
}

/// Test multiple appends and selective pruning.
#[tokio::test]
async fn batch_store_multiple_appends_and_pruning() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let env = open_lmdb_env(dir.path())?;
    let mut store = LmdbBatchStore::new(env)?;

    // Append 20 batches
    for id in 1..=20 {
        store.append(create_test_batch(id as usize), id).await?;
    }

    // Verify all exist
    let all_ids = store.seek(1..=20).await?;
    assert_eq!(all_ids.len(), 20);

    // Prune first 10
    store.prune(10).await?;

    // Verify pruning worked
    let remaining = store.seek(1..=20).await?;
    assert_eq!(remaining, (11..=20).collect::<Vec<_>>());

    Ok(())
}

/// Test custom map size configuration.
#[tokio::test]
async fn batch_store_with_custom_map_size() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();

    // Create with 100MB map size
    let env = open_lmdb_env_with_options(dir.path(), 100 * 1024 * 1024, 2)?;
    let mut store = LmdbBatchStore::new(env)?;

    let batch = create_test_batch(5);
    store.append(batch, 1).await?;

    let loaded = store.load(1).await?;
    assert!(loaded.is_some());

    Ok(())
}

/// Test crash recovery: committed data persists.
#[tokio::test]
async fn batch_store_crash_recovery() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Simulate first session
    {
        let env = open_lmdb_env(&path)?;
        let mut store = LmdbBatchStore::new(env)?;

        // Commit some batches
        store.append(create_test_batch(5), 1).await?;
        store.append(create_test_batch(10), 2).await?;

        // Store dropped here (simulated crash)
    }

    // Recover from crash
    {
        let env = open_lmdb_env(&path)?;
        let store = LmdbBatchStore::new(env)?;

        // Verify committed data persists
        let batch1 = store.load(1).await?.unwrap();
        assert_eq!(batch1.num_rows(), 5);

        let batch2 = store.load(2).await?.unwrap();
        assert_eq!(batch2.num_rows(), 10);

        let ids = store.seek(1..=2).await?;
        assert_eq!(ids, vec![1, 2]);
    }

    Ok(())
}

/// Test that reopening existing databases works correctly (application restart scenario).
#[tokio::test]
async fn test_reopen_existing_databases() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // First session: create stores and write data
    {
        let env = open_lmdb_env(&path)?;
        let mut state_store = LmdbStateStore::new(env.clone())?;
        let mut batch_store = LmdbBatchStore::new(env)?;

        state_store.advance(42).await?;
        batch_store.append(create_test_batch(5), 1).await?;
    }

    // Second session: reopen the SAME environment and create stores again
    // This simulates an application restart
    {
        let env = open_lmdb_env(&path)?;
        let state_store = LmdbStateStore::new(env.clone())?; // Should open existing DB
        let batch_store = LmdbBatchStore::new(env)?; // Should open existing DB

        // Verify data persists
        let state = state_store.load().await?;
        assert_eq!(state.next, 42);

        let batch = batch_store.load(1).await?.unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    // Third session: create stores in different order
    {
        let env = open_lmdb_env(&path)?;
        let batch_store = LmdbBatchStore::new(env.clone())?; // Batch first
        let state_store = LmdbStateStore::new(env)?; // State second

        // Data should still be there
        let state = state_store.load().await?;
        assert_eq!(state.next, 42);

        let batch = batch_store.load(1).await?.unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    Ok(())
}
