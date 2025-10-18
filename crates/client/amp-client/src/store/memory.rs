//! In-memory state store implementation (not crash-safe)

use std::sync::Arc;

use tokio::sync::RwLock;

use super::{StateStore, StreamState};
use crate::{error::Error, state::CompressedCommit};

/// In-memory implementation of StateStore (not crash-safe).
///
/// Stores stream state in process memory using an Arc<RwLock<StreamState>>.
/// State is lost on process restart, so this is suitable for:
/// - Development and testing
/// - Scenarios where crash recovery is not required
/// - As a fallback when no durable store is configured
///
/// For production deployments requiring crash recovery, use a persistent
/// implementation like SQLite or LMDB.
///
/// # Example
/// ```rust,ignore
/// let store = InMemoryStateStore::new();
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .with_state_store(store)
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryStateStore {
    state: Arc<RwLock<StreamState>>,
}

impl InMemoryStateStore {
    /// Create a new in-memory state store.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(StreamState::default())),
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
    async fn advance(&mut self, next: u64) -> Result<(), Error> {
        let mut state = self.state.write().await;
        state.next = next;
        Ok(())
    }

    async fn commit(&mut self, commit: CompressedCommit) -> Result<(), Error> {
        let mut state = self.state.write().await;
        state
            .buffer
            .retain(|id, _| !commit.delete.iter().any(|range| range.contains(id)));
        state.buffer.extend(commit.insert);
        if let Some(watermark) = commit.watermark {
            state.watermark = Some(watermark);
        }
        Ok(())
    }

    async fn load(&self) -> Result<StreamState, Error> {
        let state = self.state.read().await;
        Ok(state.clone())
    }
}
