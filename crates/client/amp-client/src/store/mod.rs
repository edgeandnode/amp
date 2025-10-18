//! State persistence for durable streaming
//!
//! This module provides the `StateStore` trait for pluggable state persistence,
//! enabling crash recovery and checkpoint-based resumption.

mod memory;

use std::collections::BTreeMap;

use common::metadata::segments::BlockRange;
pub use memory::InMemoryStateStore;
use serde::{Deserialize, Serialize};

use crate::{error::Error, state::CompressedCommit, stream::WatermarkCheckpoint};

/// Persisted state for stream resumption and crash recovery.
///
/// Contains everything needed to resume a stream from a previous checkpoint:
/// - Watermark checkpoint (ranges + sequence at last safe point)
/// - Block ranges from most recent event (for reorg detection within connection)
/// - Consumed batches buffer (for computing invalidated sequences on reorg)
/// - Monotonic sequence counter (for unique batch identification)
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamState {
    /// Last committed watermark checkpoint.
    ///
    /// Contains both the block ranges and the highest sequence number at the time
    /// of the watermark. This represents the last safe checkpoint where all prior
    /// data has been committed and can be used for crash recovery.
    pub watermark: Option<WatermarkCheckpoint>,

    /// Consumed batches buffer for computing invalidated sequences on reorg.
    ///
    /// Maps sequence number -> block ranges for that batch.
    /// Pruned on watermark based on retention window to bound memory usage.
    ///
    /// Used to compute which sequences are invalidated when a reorg is detected:
    /// Walk back through buffer to find all sequences that covered the reorg'd blocks.
    pub buffer: BTreeMap<u64, Vec<BlockRange>>,

    /// Next sequence number to assign.
    ///
    /// Monotonically increasing counter for assigning unique sequence numbers to batches.
    /// Never decreases, never resets. Persisted across restarts to ensure uniqueness.
    pub next: u64,
}

/// Trait for pluggable state persistence.
///
/// Implementors provide durable storage for stream state, enabling:
/// - Crash recovery by resuming from last watermark
/// - Persistent watermark checkpoints with sequence tracking
/// - Batch buffering for reorg sequence computation
/// - Monotonic sequence counter persistence
/// - Rewind detection on reconnection
///
/// # Atomic Commits
///
/// All state changes are applied atomically via the `commit` method. This ensures
/// that each event's state changes (which may involve multiple fields) are either
/// fully applied or not applied at all, preventing partial state corruption.
///
/// # Checkpoint-Based Resumption with Rewind
///
/// On reconnection, the stream checks if there were uncommitted batches:
/// - If `next_sequence > watermark.sequence + 1`: Uncommitted batches exist
/// - Emit `Rewind` event with invalidated sequence range
/// - Consumer deletes data with those sequences
/// - Resume from watermark with fresh sequences
///
/// # Batch Buffering for Reorg Handling
///
/// The `consumed_batches` buffer enables computing which sequences are invalidated
/// when a reorg is detected. On reorg at block N, walk back through buffer to find
/// all batches that covered blocks >= N, emit their sequences as invalidated.
///
/// Buffer is pruned on watermark based on retention window (e.g., keep last 10,000 blocks).
#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    /// Persist the next transaction id.
    ///
    /// Called by StateManager after incrementing the in-memory counter to pre-allocate
    /// an ID. This guarantees ID uniqueness even across crashes.
    ///
    /// # Arguments
    /// - `next`: The new value of the next transaction id to persist
    async fn advance(&mut self, next: u64) -> Result<(), Error>;

    /// Persist a compressed commit.
    ///
    /// Called by StateManager after a sequence of commits has been compressed into a single atomic update.
    /// This ensures that all state changes are applied atomically.
    ///
    /// # Arguments
    /// - `commit`: The compressed commit to persist
    async fn commit(&mut self, commit: CompressedCommit) -> Result<(), Error>;

    /// Load initial state from persistent storage (called once on startup for rehydration).
    ///
    /// This method is called exactly once when creating a `StateManager` to load
    /// the persisted state into memory. After initialization, all state access is via
    /// the in-memory copy in `StateManager`, not via this method.
    async fn load(&self) -> Result<StreamState, Error>;
}
