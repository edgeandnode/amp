use std::ops::RangeInclusive;

use async_trait::async_trait;
use common::arrow::array::RecordBatch;

use crate::error::Result;

/// Trait for storing and managing batches.
///
/// Implementations track emitted batches by sequence ID and handle
/// all stream event types (Data, Watermark, Reorg, Rewind).
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Append a batch to the store.
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch data
    /// * `id` - Unique identifier
    async fn append(&mut self, batch: RecordBatch, id: u64) -> Result<()>;

    /// Prune old batches from the store.
    ///
    /// # Arguments
    /// * `cutoff` - Prune all batches before this id
    async fn prune(&mut self, cutoff: u64) -> Result<()>;

    /// Retract invalidated batches from the store.
    ///
    /// Returns the batches that were retracted (for emitting delete records).
    ///
    /// # Arguments
    /// * `ids` - The unique identifier range that was invalidated
    ///
    /// # Returns
    /// A vector of RecordBatch references for all retracted batches
    async fn retract(&mut self, ids: RangeInclusive<u64>) -> Result<Vec<RecordBatch>>;
}

mod memory;

#[cfg(feature = "lmdb")]
mod lmdb;

#[cfg(feature = "lmdb")]
pub use lmdb::LmdbStore;
pub use memory::InMemoryStore;
