use amp_client::InvalidationRange;
use async_trait::async_trait;
use common::BlockNum;

use crate::{
    error::Result,
    types::{StoredBatch, StoredRecord},
};

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

    /// Retrieve all records from batches whose ranges intersect with the given invalidation range.
    ///
    /// Used during reorg handling to find records that need to be retracted.
    /// Returns ALL records from any batch that has a range overlapping the invalidation range.
    ///
    /// # Arguments
    /// * `range` - The invalidation range (network + block number range)
    ///
    /// # Returns
    /// A vector of all stored records from batches that overlap with the invalidation range
    async fn get_in_range(&self, range: &InvalidationRange) -> Result<Vec<StoredRecord>>;

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

/// In-memory implementation of StateStore using Vec.
///
/// Stores batches in a vector, appended as they arrive.
/// Batches are mostly-ordered but may have rewinds due to reorgs.
/// Suitable for most use cases but does not persist across restarts.
pub struct InMemoryStore {
    /// All stored batches, appended in order as they arrive from the stream
    batches: Vec<StoredBatch>,

    /// Maximum number of blocks to retain in memory (reorg window)
    reorg_window: u64,
}

impl InMemoryStore {
    /// Create a new in-memory state store.
    ///
    /// # Arguments
    /// * `reorg_window` - Number of blocks to retain (e.g., 64 for Ethereum finality)
    ///
    /// # Example
    /// ```
    /// use amp_debezium::InMemoryStore;
    ///
    /// let store = InMemoryStore::new(64);
    /// ```
    pub fn new(reorg_window: u64) -> Self {
        Self {
            batches: Vec::new(),
            reorg_window,
        }
    }

    /// Get the current number of batches stored in memory.
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
    async fn insert(&mut self, batch: StoredBatch) -> Result<()> {
        // Append batch as it arrives
        self.batches.push(batch);

        Ok(())
    }

    async fn get_in_range(&self, range: &InvalidationRange) -> Result<Vec<StoredRecord>> {
        Ok(self
            .batches
            .iter()
            .filter(|batch| {
                batch.ranges.iter().any(|block_range| {
                    block_range.network == range.network
                        && block_range.numbers.start() <= range.numbers.end()
                        && block_range.numbers.end() >= range.numbers.start()
                })
            })
            .flat_map(|batch| {
                (0..batch.batch.num_rows()).map(|row_idx| StoredRecord {
                    batch: batch.batch.clone(),
                    row_idx,
                })
            })
            .collect())
    }

    async fn prune(
        &mut self,
        watermarks: &std::collections::BTreeMap<String, BlockNum>,
    ) -> Result<()> {
        // Use retain to efficiently remove batches that should be pruned
        // Batches are mostly-ordered but may have rewinds due to reorgs
        self.batches.retain(|batch| {
            // Check if this batch should be retained
            for block_range in &batch.ranges {
                if let Some(&watermark_block) = watermarks.get(&block_range.network) {
                    // Calculate prune threshold: keep batches within reorg_window of watermark
                    let prune_before = watermark_block.saturating_sub(self.reorg_window);

                    // If this range ends after the prune threshold, keep this batch
                    if *block_range.numbers.end() >= prune_before {
                        return true;
                    }
                } else {
                    // No watermark for this network, keep this batch
                    return true;
                }
            }
            // All ranges are prunable, don't keep this batch
            false
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{
        arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        metadata::segments::BlockRange,
    };

    use super::*;
    use crate::types::StoredBatch;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::Int64,
            false,
        )]));
        let array = Int64Array::from(vec![1, 2, 3]);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn insert_and_retrieve_records() {
        //* Given
        let mut store = InMemoryStore::new(64);
        let batch = Arc::new(create_test_batch());
        let stored_batch = StoredBatch {
            batch: batch.clone(),
            ranges: vec![BlockRange {
                network: "test".to_string(),
                numbers: 100..=100,
                hash: [0u8; 32].into(),
                prev_hash: None,
            }],
        };

        //* When
        store
            .insert(stored_batch)
            .await
            .expect("insert should succeed");

        //* Then
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    #[tokio::test]
    async fn get_records_in_range() {
        //* Given
        let mut store = InMemoryStore::new(64);
        let batch = Arc::new(create_test_batch());

        // Insert batches with different block ranges
        for block_num in [100, 105, 110, 115, 120] {
            let stored_batch = StoredBatch {
                batch: batch.clone(),
                ranges: vec![BlockRange {
                    network: "test".to_string(),
                    numbers: block_num..=block_num,
                    hash: [0u8; 32].into(),
                    prev_hash: None,
                }],
            };
            store
                .insert(stored_batch)
                .await
                .expect("insert should succeed");
        }

        //* When
        let range = InvalidationRange {
            network: "test".to_string(),
            numbers: 105..=115,
        };
        let records = store
            .get_in_range(&range)
            .await
            .expect("get_in_range should succeed");

        //* Then
        // Each batch has 3 rows, and we have 3 batches (105, 110, 115) overlapping
        assert_eq!(records.len(), 9); // 3 batches * 3 rows each
    }

    #[tokio::test]
    async fn prune_removes_old_records() {
        //* Given
        let mut store = InMemoryStore::new(10); // Small window for testing
        let batch = Arc::new(create_test_batch());

        // Insert batches at blocks 0-20
        for block_num in 0..=20 {
            let stored_batch = StoredBatch {
                batch: batch.clone(),
                ranges: vec![BlockRange {
                    network: "test".to_string(),
                    numbers: block_num..=block_num,
                    hash: [0u8; 32].into(),
                    prev_hash: None,
                }],
            };
            store
                .insert(stored_batch)
                .await
                .expect("insert should succeed");
        }

        //* When
        let mut watermarks = std::collections::BTreeMap::new();
        watermarks.insert("test".to_string(), 15);
        store
            .prune(&watermarks)
            .await
            .expect("prune should succeed");

        //* Then
        // Should keep blocks within reorg_window (10) of watermark (15)
        // prune_before = 15 - 10 = 5, so blocks 5-20 should remain (16 batches)
        assert_eq!(store.len(), 16);

        // Verify all remaining batch ranges end >= block 5
        let all_range = InvalidationRange {
            network: "test".to_string(),
            numbers: 0..=100,
        };
        let remaining = store
            .get_in_range(&all_range)
            .await
            .expect("get_in_range should succeed");

        // Should have 16 batches * 3 rows each = 48 records from batch ranges 5..=20
        assert_eq!(remaining.len(), 48);
    }
}
