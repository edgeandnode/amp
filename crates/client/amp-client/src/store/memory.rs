//! In-memory state store implementation (not crash-safe)

use super::{StateSnapshot, StateStore};
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
        // Remove pruned and invalidated watermarks
        self.state.buffer.retain(|(id, _)| {
            let invalidated = commit
                .invalidate
                .as_ref()
                .map(|r| r.contains(id))
                .unwrap_or(false);
            let pruned = commit
                .prune
                .as_ref()
                .map(|r| r.contains(id))
                .unwrap_or(false);
            !invalidated && !pruned
        });

        // Add new watermarks
        for (id, ranges) in commit.insert {
            self.state.buffer.push_back((id, ranges));
        }

        Ok(())
    }

    async fn load(&self) -> Result<StateSnapshot, Error> {
        Ok(self.state.clone())
    }
}
