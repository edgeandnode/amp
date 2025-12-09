//! State persistence for durable streaming

mod memory;
#[cfg(feature = "postgres")]
mod postgres;

#[cfg(feature = "lmdb")]
mod lmdb;

use std::{collections::VecDeque, ops::RangeInclusive};

use arrow::array::RecordBatch;
#[cfg(feature = "lmdb")]
pub use lmdb::{LmdbBatchStore, LmdbStateStore, open_lmdb_env, open_lmdb_env_with_options};
pub use memory::{InMemoryBatchStore, InMemoryStateStore};
#[cfg(feature = "postgres")]
pub use postgres::PostgresStateStore;
use serde::{Deserialize, Serialize};

use crate::{
    BlockRange,
    error::{Error, SerializationError},
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

/// Trait for CDC streams with batch content persistence.
///
/// CDC (Change Data Capture) streams require storing the actual batch content to
/// enable generating Delete events with the original data.
///
/// # Persistence Strategy
///
/// Batches are stored **before** emitting Insert events. This ensures:
/// - Delete events always have access to original batch content
/// - At-least-once delivery semantics are maintained
/// - Consumer crashes can be recovered via rewind
///
/// # Storage Lifecycle
///
/// 1. **Append**: Batch stored immediately when received (before advance)
/// 2. **Load**: Batches loaded during rewind to emit Delete events
/// 3. **Prune**: Batches cleaned up when retention window expires
#[async_trait::async_trait]
pub trait BatchStore: Send + Sync {
    /// Store a single record batch.
    ///
    /// # Arguments
    /// - `batch`: The RecordBatch to store
    /// - `id`: Transaction ID for this batch
    async fn append(&mut self, batch: RecordBatch, id: TransactionId) -> Result<(), Error>;

    /// Load all batch IDs in a transaction ID range (sparse).
    ///
    /// This is a lightweight operation that returns only the transaction IDs of batches
    /// that exist in the range, without loading the actual batch data. This enables
    /// efficient iteration: load IDs first, then load batches one by one.
    ///
    /// # Arguments
    /// - `range`: Inclusive range of transaction IDs to scan
    ///
    /// # Returns
    /// Vector of transaction IDs for batches that exist in the range.
    /// May be empty if no batches exist in range.
    async fn seek(&self, range: RangeInclusive<TransactionId>)
    -> Result<Vec<TransactionId>, Error>;

    /// Load a single batch by transaction ID.
    ///
    /// Returns `None` if no batch exists for this ID (e.g., it was a watermark or undo event).
    ///
    /// # Arguments
    /// - `id`: Transaction ID to load
    ///
    /// # Returns
    /// - `Some(batch)` if the batch exists
    /// - `None` if no batch for this ID
    async fn load(&self, id: TransactionId) -> Result<Option<RecordBatch>, Error>;

    /// Prune batches up to cutoff transaction ID (inclusive).
    ///
    /// Deletes all batches with IDs in range 0..=cutoff. Must be idempotent -
    /// calling multiple times with same cutoff is safe.
    ///
    /// # Best-Effort
    ///
    /// Callers should treat pruning as best-effort cleanup. Failures should be logged
    /// but not fatal. The system will retry on next watermark or startup.
    ///
    /// # Arguments
    /// - `cutoff`: Last transaction ID to prune (inclusive). All IDs 0..=cutoff are deleted.
    async fn prune(&mut self, cutoff: TransactionId) -> Result<(), Error>;
}

/// Serialize a RecordBatch to Arrow IPC format.
///
/// Uses Arrow IPC streaming format which includes the schema and is self-contained.
/// This format is:
/// - Efficient (binary, columnar)
/// - Schema-preserving (can deserialize without external schema)
/// - Standard across Arrow ecosystem
///
/// # Example
/// ```rust,ignore
/// let bytes = serialize_batch(&batch)?;
/// store.put(id, &bytes)?;
/// ```
pub fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    use arrow::ipc::writer::StreamWriter;

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|err| Error::Serialization(SerializationError::WriterCreation(err)))?;
    writer
        .write(batch)
        .map_err(|err| Error::Serialization(SerializationError::BatchWrite(err)))?;
    writer
        .finish()
        .map_err(|err| Error::Serialization(SerializationError::WriterFinish(err)))?;
    Ok(buf)
}

/// Deserialize a RecordBatch from Arrow IPC format.
///
/// Reads Arrow IPC streaming format produced by `serialize_batch`.
///
/// # Example
/// ```rust,ignore
/// let bytes = store.get(id)?;
/// let batch = deserialize_batch(&bytes)?;
/// ```
pub fn deserialize_batch(bytes: &[u8]) -> Result<RecordBatch, Error> {
    use arrow::ipc::reader::StreamReader;

    let mut reader = StreamReader::try_new(bytes, None)
        .map_err(|err| Error::Serialization(SerializationError::ReaderCreation(err)))?;
    reader
        .next()
        .ok_or(Error::Serialization(SerializationError::EmptyBatchStream))?
        .map_err(|err| Error::Serialization(SerializationError::BatchRead(err)))
}
