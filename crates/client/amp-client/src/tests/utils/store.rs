//! Shared state store for testing crash recovery scenarios.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    error::Error,
    store::{InMemoryStateStore, StateSnapshot, StateStore},
    transactional::{Commit, TransactionId},
};

/// Wrapper around InMemoryStateStore that shares state across clones.
///
/// This is necessary for testing crash recovery scenarios where multiple
/// stream instances need to see each other's state changes. Without shared
/// state, each clone would have an independent copy of the store.
///
/// # Example
/// ```ignore
/// let store = SharedStore::new();
///
/// // First scenario commits some data
/// let first = scenario! {
///     store: store.clone();
///     Step::data(0..=10),
///     Step::watermark(0..=10),
/// }.await;
///
/// // Second scenario sees the first's committed state
/// let second = scenario! {
///     store: store.clone();
///     Step::data(11..=20),
/// }.await;
/// ```
#[derive(Clone)]
pub struct SharedStore {
    inner: Arc<Mutex<InMemoryStateStore>>,
}

impl SharedStore {
    /// Create a new shared store.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryStateStore::new())),
        }
    }
}

impl Default for SharedStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StateStore for SharedStore {
    async fn advance(&mut self, next: TransactionId) -> Result<(), Error> {
        self.inner.lock().await.advance(next).await
    }

    async fn commit(&mut self, commit: Commit) -> Result<(), Error> {
        self.inner.lock().await.commit(commit).await
    }

    async fn truncate(&mut self, up_to: TransactionId) -> Result<(), Error> {
        self.inner.lock().await.truncate(up_to).await
    }

    async fn load(&self) -> Result<StateSnapshot, Error> {
        self.inner.lock().await.load().await
    }
}
