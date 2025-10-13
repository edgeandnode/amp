use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
};

use amp_client::InvalidationRange;
use async_trait::async_trait;
use common::BlockNum;

use crate::{
    error::Result,
    types::{RecordKey, StoredRecord},
};

/// Identity hasher for RecordKey.
///
/// Since RecordKey is already a hash (u128 from xxhash), we use an identity
/// function as the hash function to avoid double-hashing.
#[derive(Default)]
struct RecordKeyHasher(u64);

impl Hasher for RecordKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!()
    }

    fn write_u128(&mut self, i: u128) {
        // Use the lower 64 bits of the u128 hash as the HashMap hash
        self.0 = i as u64;
    }
}

/// Type alias for HashMap with identity hasher for RecordKey
type RecordKeyMap<V> = HashMap<RecordKey, V, BuildHasherDefault<RecordKeyHasher>>;

/// Trait for storing and retrieving records to support reorg handling.
///
/// Implementations track emitted records so that when a reorg occurs,
/// they can be retracted (emitted as delete operations in Debezium format).
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Insert a record into the state store.
    ///
    /// # Arguments
    /// * `record` - The stored record with key, batch, row index, and block number
    async fn insert(&mut self, record: StoredRecord) -> Result<()>;

    /// Retrieve all records within the given invalidation range.
    ///
    /// Used during reorg handling to find records that need to be retracted.
    ///
    /// # Arguments
    /// * `range` - The invalidation range (network + block number range)
    ///
    /// # Returns
    /// A vector of all stored records that fall within the range
    async fn get_in_range(&self, range: &InvalidationRange) -> Result<Vec<StoredRecord>>;

    /// Remove records with block numbers before the specified block.
    ///
    /// Used to maintain a sliding window of recent records and prevent
    /// unbounded memory growth.
    ///
    /// # Arguments
    /// * `before_block` - Remove records with block_num < this value
    async fn prune(&mut self, before_block: BlockNum) -> Result<()>;
}

/// In-memory implementation of StateStore using HashMap.
///
/// Stores records in memory with a configurable block window for retention.
/// Suitable for most use cases but does not persist across restarts.
pub struct InMemoryStore {
    /// Map from RecordKey to stored record (using identity hasher)
    records: RecordKeyMap<StoredRecord>,

    /// Maximum number of blocks to retain in memory (reorg window)
    reorg_window: u64,

    /// The highest block number seen so far
    max_block: BlockNum,
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
            records: HashMap::default(),
            reorg_window,
            max_block: 0,
        }
    }

    /// Get the current number of records stored in memory.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
    async fn insert(&mut self, record: StoredRecord) -> Result<()> {
        // Update max block
        self.max_block = self.max_block.max(record.block_num);

        // Insert the record
        self.records.insert(record.key, record);

        Ok(())
    }

    async fn get_in_range(&self, range: &InvalidationRange) -> Result<Vec<StoredRecord>> {
        let records = self
            .records
            .values()
            .filter(|record| {
                record.block_num >= *range.numbers.start()
                    && record.block_num <= *range.numbers.end()
            })
            .cloned()
            .collect();

        Ok(records)
    }

    async fn prune(&mut self, before_block: BlockNum) -> Result<()> {
        // Calculate prune threshold based on reorg window
        let prune_before = self.max_block.saturating_sub(self.reorg_window);

        // Only prune if the requested block is older than our window
        let prune_block = prune_before.min(before_block);

        // Remove all records before the prune block
        self.records
            .retain(|_, record| record.block_num >= prune_block);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::arrow::{
        array::{Int64Array, RecordBatch},
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
    async fn insert_and_retrieve_records() {
        //* Given
        let mut store = InMemoryStore::new(64);
        let batch = Arc::new(create_test_batch());
        let record = StoredRecord {
            key: RecordKey::new(123),
            batch: batch.clone(),
            row_idx: 0,
            block_num: 100,
        };

        //* When
        store
            .insert(record.clone())
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

        // Insert records at different blocks
        for block_num in [100, 105, 110, 115, 120] {
            let record = StoredRecord {
                key: RecordKey::new(block_num as u128),
                batch: batch.clone(),
                row_idx: 0,
                block_num,
            };
            store.insert(record).await.expect("insert should succeed");
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
        assert_eq!(records.len(), 3); // blocks 105, 110, 115
        for record in records {
            assert!(record.block_num >= 105 && record.block_num <= 115);
        }
    }

    #[tokio::test]
    async fn prune_removes_old_records() {
        //* Given
        let mut store = InMemoryStore::new(10); // Small window for testing
        let batch = Arc::new(create_test_batch());

        // Insert records at blocks 0-20
        for block_num in 0..=20 {
            let record = StoredRecord {
                key: RecordKey::new(block_num as u128),
                batch: batch.clone(),
                row_idx: 0,
                block_num,
            };
            store.insert(record).await.expect("insert should succeed");
        }

        //* When
        store.prune(15).await.expect("prune should succeed");

        //* Then
        // Should only keep blocks within reorg_window (10) of max_block (20)
        // So blocks 10-20 should remain (11 records)
        assert_eq!(store.len(), 11);

        // Verify all remaining records are >= block 10
        let all_range = InvalidationRange {
            network: "test".to_string(),
            numbers: 0..=100,
        };
        let remaining = store
            .get_in_range(&all_range)
            .await
            .expect("get_in_range should succeed");

        for record in remaining {
            assert!(record.block_num >= 10);
        }
    }
}
