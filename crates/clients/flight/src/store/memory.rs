//! In-memory state and batch store implementations (not crash-safe)

use std::{collections::BTreeMap, ops::RangeInclusive};

use arrow::array::RecordBatch;

use super::{BatchStore, StateSnapshot, StateStore};
use crate::{
    error::Error,
    transactional::{Commit, TransactionId},
};

/// In-memory implementation of StateStore (not crash-safe).
///
/// State is lost on process restart, so this is suitable for:
/// - Development and testing
/// - Scenarios where crash recovery is not required
/// - As a fallback when no durable store is configured
///
/// # Example
/// ```rust,ignore
/// let store = InMemoryStateStore::new();
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .transactional(store, 128)
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryStateStore {
    state: StateSnapshot,
}

impl InMemoryStateStore {
    /// Create a new in-memory state store.
    pub fn new() -> Self {
        Self {
            state: StateSnapshot::default(),
        }
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StateStore for InMemoryStateStore {
    async fn advance(&mut self, next: TransactionId) -> Result<(), Error> {
        self.state.next = next;
        Ok(())
    }

    async fn commit(&mut self, commit: Commit) -> Result<(), Error> {
        // Remove pruned watermarks (all IDs <= prune)
        if let Some(prune) = commit.prune {
            self.state.buffer.retain(|(id, _)| *id > prune);
        }

        // Add new watermarks
        for (id, ranges) in commit.insert {
            self.state.buffer.push_back((id, ranges));
        }

        Ok(())
    }

    async fn truncate(&mut self, from: TransactionId) -> Result<(), Error> {
        // Remove all watermarks with IDs >= from
        self.state.buffer.retain(|(id, _)| *id < from);
        Ok(())
    }

    async fn load(&self) -> Result<StateSnapshot, Error> {
        Ok(self.state.clone())
    }
}

/// In-memory implementation of BatchStore (not crash-safe).
///
/// Batches are lost on process restart, so this is suitable for:
/// - Development and testing
/// - Scenarios where crash recovery is not required
///
/// # Example
/// ```rust,ignore
/// let batch_store = InMemoryBatchStore::new();
/// let state_store = InMemoryStateStore::new();
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .cdc(state_store, batch_store, 128)
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryBatchStore {
    batches: BTreeMap<TransactionId, RecordBatch>,
}

impl InMemoryBatchStore {
    /// Create a new in-memory batch store.
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
        }
    }
}

impl Default for InMemoryBatchStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl BatchStore for InMemoryBatchStore {
    async fn append(&mut self, batch: RecordBatch, id: TransactionId) -> Result<(), Error> {
        self.batches.insert(id, batch);
        Ok(())
    }

    async fn seek(
        &self,
        range: RangeInclusive<TransactionId>,
    ) -> Result<Vec<TransactionId>, Error> {
        Ok(self.batches.range(range).map(|(id, _)| *id).collect())
    }

    async fn load(&self, id: TransactionId) -> Result<Option<RecordBatch>, Error> {
        Ok(self.batches.get(&id).cloned())
    }

    async fn prune(&mut self, cutoff: TransactionId) -> Result<(), Error> {
        self.batches = self.batches.split_off(&(cutoff + 1));
        Ok(())
    }
}
