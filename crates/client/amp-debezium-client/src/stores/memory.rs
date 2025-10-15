use std::sync::Arc;

use amp_client::InvalidationRange;
use async_trait::async_trait;
use common::{BlockNum, arrow::array::RecordBatch};

use super::StateStore;
use crate::{error::Result, types::StoredBatch};

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
    /// * `reorg_window` - Number of blocks to retain
    ///
    /// # Example
    /// ```
    /// use amp_debezium_client::InMemoryStore;
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

    async fn get_in_ranges(&self, ranges: &[InvalidationRange]) -> Result<Vec<Arc<RecordBatch>>> {
        Ok(self
            .batches
            .iter()
            .filter(|batch| {
                // Check if any range in this batch overlaps with any invalidation range
                batch.ranges.iter().any(|block_range| {
                    ranges.iter().any(|inv_range| {
                        block_range.network == inv_range.network
                            && block_range.numbers.start() <= inv_range.numbers.end()
                            && block_range.numbers.end() >= inv_range.numbers.start()
                    })
                })
            })
            .map(|batch| batch.batch.clone())
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
        let ranges = vec![InvalidationRange {
            network: "test".to_string(),
            numbers: 105..=115,
        }];
        let batches = store
            .get_in_ranges(&ranges)
            .await
            .expect("get_in_ranges should succeed");

        //* Then
        // We have 3 batches (105, 110, 115) overlapping with range 105..=115
        assert_eq!(batches.len(), 3);
        // Each batch has 3 rows
        for batch in batches {
            assert_eq!(batch.num_rows(), 3);
        }
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

        // Verify all remaining batches
        let all_ranges = vec![InvalidationRange {
            network: "test".to_string(),
            numbers: 0..=100,
        }];
        let remaining_batches = store
            .get_in_ranges(&all_ranges)
            .await
            .expect("get_in_ranges should succeed");

        // Should have 16 batches from block ranges 5..=20
        assert_eq!(remaining_batches.len(), 16);
    }
}
