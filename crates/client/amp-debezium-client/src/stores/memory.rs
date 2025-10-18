use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

use async_trait::async_trait;
use common::arrow::array::RecordBatch;

use super::StateStore;
use crate::error::Result;

/// In-memory implementation of StateStore using BTreeMap.
///
/// Stores batches indexed by sequence ID in a sorted map.
/// Supports efficient range-based operations for pruning and reorgs.
/// Suitable for most use cases but does not persist across restarts.
pub struct InMemoryStore {
    /// All stored batches indexed by sequence ID
    batches: BTreeMap<u64, Arc<RecordBatch>>,
}

impl InMemoryStore {
    /// Create a new in-memory state store.
    ///
    /// # Example
    /// ```
    /// use amp_debezium_client::InMemoryStore;
    ///
    /// let store = InMemoryStore::new();
    /// ```
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
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
    async fn append(&mut self, batch: RecordBatch, id: u64) -> Result<()> {
        self.batches.insert(id, Arc::new(batch));
        Ok(())
    }

    async fn prune(&mut self, cutoff: u64) -> Result<()> {
        self.batches = self.batches.split_off(&cutoff);
        Ok(())
    }

    async fn retract(&mut self, ids: RangeInclusive<u64>) -> Result<Vec<RecordBatch>> {
        // Collect and remove batches in the invalidated range using functional pattern
        let retracted = ids
            .filter_map(|id| self.batches.remove(&id).map(Arc::unwrap_or_clone))
            .collect();

        Ok(retracted)
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

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
    async fn append_stores_batch() {
        //* Given
        let mut store = InMemoryStore::new();
        let batch = Arc::new(create_test_batch());

        //* When
        store
            .append((*batch).clone(), 0)
            .await
            .expect("append should succeed");

        //* Then
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    #[tokio::test]
    async fn retract_retracts_batches() {
        //* Given
        let mut store = InMemoryStore::new();
        let batch = Arc::new(create_test_batch());

        // Insert batches with different IDs
        for (idx, _) in [100, 105, 110, 115, 120].iter().enumerate() {
            store
                .append((*batch).clone(), idx as u64)
                .await
                .expect("append should succeed");
        }

        //* When
        // Invalidate IDs 1..=4 (batches at blocks 105, 110, 115, 120)
        let retracted_batches = store.retract(1..=4).await.expect("retract should succeed");

        //* Then
        // We retracted 4 batches (IDs 1, 2, 3, 4)
        assert_eq!(retracted_batches.len(), 4);
        // Each batch has 3 rows
        for batch in &retracted_batches {
            assert_eq!(batch.num_rows(), 3);
        }
        // Store should have 1 remaining batch (100)
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn prune_prunes_old_records() {
        //* Given
        let mut store = InMemoryStore::new();
        let batch = Arc::new(create_test_batch());

        // Insert batches with IDs 0-20
        for id in 0..=20 {
            store
                .append((*batch).clone(), id)
                .await
                .expect("append should succeed");
        }

        //* When
        // Prune batches with ID < 10
        store.prune(10).await.expect("prune should succeed");

        //* Then
        // Should keep batches with ID >= 10 (IDs 10-20, so 11 batches)
        assert_eq!(store.len(), 11);
    }
}
