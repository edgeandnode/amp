use std::sync::Arc;

use amp_client::InvalidationRange;
use async_trait::async_trait;
use common::{BlockNum, arrow::array::RecordBatch};

use crate::{error::Result, types::StoredBatch};

/// Trait for storing and retrieving batches to support reorg handling.
///
/// Implementations track emitted batches with their associated block ranges.
/// A single batch can span multiple networks. When a reorg occurs on any network,
/// all batches whose ranges intersect with that network's invalidation range are retracted.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Insert a batch with all its ranges and records.
    ///
    /// # Arguments
    /// * `batch` - The stored batch with ranges and all records
    async fn insert(&mut self, batch: StoredBatch) -> Result<()>;

    /// Retrieve all batches whose ranges intersect with any of the given invalidation ranges.
    ///
    /// Used during reorg handling to find batches that need to be retracted.
    /// Returns batches where ANY range overlaps with ANY invalidation range.
    /// ALL records from matching batches should be retracted (batch-level granularity).
    ///
    /// # Arguments
    /// * `ranges` - The invalidation ranges (network + block number ranges)
    ///
    /// # Returns
    /// A vector of RecordBatch references for all batches that overlap
    async fn get_in_ranges(&self, ranges: &[InvalidationRange]) -> Result<Vec<Arc<RecordBatch>>>;

    /// Remove batches based on multi-network watermarks.
    ///
    /// Used to maintain a sliding window of recent batches and prevent
    /// unbounded memory growth. A batch is only deleted when ALL its ranges
    /// are beyond their respective network's reorg window (conservative approach).
    ///
    /// # Arguments
    /// * `watermarks` - Map of network name to block number watermark
    async fn prune(
        &mut self,
        watermarks: &std::collections::BTreeMap<String, BlockNum>,
    ) -> Result<()>;
}

mod memory;

#[cfg(feature = "rocksdb")]
mod rocksdb;

pub use memory::InMemoryStore;
#[cfg(feature = "rocksdb")]
pub use rocksdb::RocksDbStore;
