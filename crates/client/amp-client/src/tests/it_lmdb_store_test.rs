//! Tests for LMDB state store implementation.

use tempfile::TempDir;

use crate::{
    error::Error,
    store::{LmdbStateStore, StateStore},
    transactional::Commit,
};

/// Test basic load operation returns empty state on first use.
#[tokio::test]
async fn load_returns_empty_state_on_first_use() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let store = LmdbStateStore::new(dir.path())?;

    let state = store.load().await?;

    assert_eq!(state.next, 0);
    assert!(state.buffer.is_empty());

    Ok(())
}

/// Test advance operation updates the next transaction ID.
#[tokio::test]
async fn advance_updates_next_transaction_id() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let mut store = LmdbStateStore::new(dir.path())?;

    // Advance to ID 42
    store.advance(42).await?;

    // Load and verify
    let state = store.load().await?;
    assert_eq!(state.next, 42);

    Ok(())
}

/// Test commit operation adds watermarks to buffer.
#[tokio::test]
async fn commit_adds_watermarks_to_buffer() -> Result<(), Error> {
    let dir = TempDir::new().unwrap();
    let mut store = LmdbStateStore::new(dir.path())?;

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
    let mut store = LmdbStateStore::new(dir.path())?;

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
    let mut store = LmdbStateStore::new(dir.path())?;

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
        let mut store = LmdbStateStore::new(&path)?;

        store.advance(100).await?;

        let commit = Commit {
            insert: vec![(10, vec![]), (20, vec![])],
            prune: None,
        };
        store.commit(commit).await?;
    }

    // Second store instance: read state
    {
        let store = LmdbStateStore::new(&path)?;
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
        let mut store = LmdbStateStore::new(&path)?;
        store.advance(10).await?;
        store.advance(20).await?;
        store.advance(30).await?;
    }

    {
        let store = LmdbStateStore::new(&path)?;
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
        let mut store = LmdbStateStore::new(&path)?;

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
        let store = LmdbStateStore::new(&path)?;
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
        let mut store = LmdbStateStore::new(&path)?;

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
        let store = LmdbStateStore::new(&path)?;
        let state = store.load().await?;

        // Last committed advance persists
        assert_eq!(state.next, 20);

        // Watermark persists
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].0, 5);
    }

    Ok(())
}
