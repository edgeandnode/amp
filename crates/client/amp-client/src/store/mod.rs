//! State persistence for durable streaming
//!
//! This module provides the `StateStore` trait for pluggable state persistence,
//! enabling crash recovery and watermark-based resumption.

mod memory;

#[cfg(feature = "lmdb")]
mod lmdb;

use std::collections::VecDeque;

use common::metadata::segments::BlockRange;
#[cfg(feature = "lmdb")]
pub use lmdb::LmdbStateStore;
pub use memory::InMemoryStateStore;
use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    transactional::{Commit, TransactionId},
};

/// Persisted state for stream resumption and crash recovery.
///
/// Contains everything needed to resume a stream:
/// - `buffer`: Watermarks within retention window (oldest to newest)
/// - `next`: Monotonic transaction ID counter for uniqueness
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// Watermarks buffer for reorg recovery (oldest to newest)
    pub buffer: VecDeque<(TransactionId, Vec<BlockRange>)>,

    /// Next transaction ID to assign (persisted for uniqueness guarantee)
    pub next: TransactionId,
}

/// Trait for pluggable state persistence.
///
/// Implementors provide durable storage for stream state, enabling:
/// - Crash recovery by resuming from last watermark
/// - Watermark buffering for reorg transaction id range computation
/// - Monotonic transaction id counter persistence
/// - Rewind detection on reconnection
///
/// # Atomic Commits
///
/// All state changes are applied atomically via the `commit` method. This ensures
/// that each event's state changes (which may involve multiple fields) are either
/// fully applied or not applied at all, preventing partial state corruption.
///
/// # Watermark-Based Resumption with Rewind
///
/// On reconnection, the stream checks if we need to rewind to the previous watermark.
/// If `next` is greater than the watermark transaction id, we need to rewind. In this
/// case, we emit an `Undo` event with the invalidated transaction id range. The stream
/// then resumes from the watermark with fresh transaction ids.
///
/// # Watermark Buffering for Reorg Handling
///
/// The buffer stores only watermark events (data events are not buffered). This enables
/// computing which transaction ids are invalidated when a reorg is detected.
///
/// On reorg at block N, we walk backwards through watermarks to find the last good
/// watermark before the reorg point. All transaction IDs after that watermark (including
/// both watermarks and data events) are invalidated. An `Undo` event is emitted with
/// `cause` set to `Reorg` and the invalidation range.
///
/// The buffer is pruned when watermarks arrive, removing watermarks outside the retention
/// window (e.g., keep only watermarks covering the last 128 blocks). Data events are never
/// buffered, so they don't contribute to memory usage.
#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    /// Persist the next transaction id.
    ///
    /// Called after incrementing the in-memory counter to pre-allocate an ID.
    /// This guarantees that transaction IDs are monotonically increasing even
    /// across crashes.
    ///
    /// # Arguments
    /// - `next`: The new value of the next transaction id to persist
    async fn advance(&mut self, next: TransactionId) -> Result<(), Error>;

    /// Persist a compressed commit of watermark events.
    ///
    /// Called after a sequence of commits has been compressed into a single
    /// atomic update. This ensures that all watermark commits are applied
    /// atomically.
    ///
    /// # Arguments
    /// - `commit`: The compressed commit to persist
    async fn commit(&mut self, commit: Commit) -> Result<(), Error>;

    /// Truncate buffer by removing all watermarks beyond a certain point.
    ///
    /// Called during reorg handling to immediately cut the buffer to the recovery point.
    /// This ensures both in-memory and persisted state are consistent.
    ///
    /// # Arguments
    /// - `from`: Remove all watermarks with transaction ID >= this value
    async fn truncate(&mut self, from: TransactionId) -> Result<(), Error>;

    /// Load initial state from persistent storage (called once on startup for rehydration).
    ///
    /// This method is called exactly once when creating a `StateManager` to load
    /// the persisted state into memory. After initialization, all state access is via
    /// the in-memory copy in `StateManager`, not via this method.
    async fn load(&self) -> Result<StateSnapshot, Error>;
}
