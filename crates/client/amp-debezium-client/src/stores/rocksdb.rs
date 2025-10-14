use std::{path::Path, sync::Arc};

use amp_client::InvalidationRange;
use async_trait::async_trait;
use bincode::{config, decode_from_slice, encode_to_vec};
use common::{BlockNum, arrow::array::RecordBatch};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteBatch};

use super::StateStore;
use crate::{
    error::{Error, Result},
    types::StoredBatch,
};

/// RocksDB-backed persistent implementation of StateStore.
///
/// Stores batches durably on disk for reorg handling that survives restarts.
/// Uses two column families:
/// - `batches`: Stores serialized StoredBatch data keyed by batch_id
/// - `index`: Maps (network, block_num) -> Vec<batch_id> for fast range queries
pub struct RocksDbStore {
    db: Arc<DB>,
    reorg_window: u64,
    next_batch_id: u64,
}

const CF_BATCHES: &str = "batches";
const CF_INDEX: &str = "index";

impl RocksDbStore {
    /// Create a new RocksDB-backed state store.
    ///
    /// # Arguments
    /// * `path` - Directory path for RocksDB storage
    /// * `reorg_window` - Number of blocks to retain
    ///
    /// # Example
    /// ```no_run
    /// use amp_debezium_client::RocksDbStore;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = RocksDbStore::new("/path/to/db", 64)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(path: impl AsRef<Path>, reorg_window: u64) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let batches_cf = ColumnFamilyDescriptor::new(CF_BATCHES, Options::default());
        let index_cf = ColumnFamilyDescriptor::new(CF_INDEX, Options::default());

        let db = DB::open_cf_descriptors(&opts, path, vec![batches_cf, index_cf])?;

        // Determine next_batch_id by finding the highest existing ID
        let next_batch_id = {
            let batches_cf_handle = db.cf_handle(CF_BATCHES).unwrap();
            let mut iter = db.raw_iterator_cf(batches_cf_handle);
            iter.seek_to_last();

            if iter.valid() {
                let key = iter.key().unwrap();
                let last_id = u64::from_be_bytes(key.try_into().unwrap());
                last_id + 1
            } else {
                0
            }
        };

        Ok(Self {
            db: Arc::new(db),
            reorg_window,
            next_batch_id,
        })
    }

    /// Serialize a block range into an index key.
    ///
    /// Format: network_name + block_number (big-endian u64)
    /// This enables range scans by network and block number.
    fn range_to_index_key(network: &str, block_num: BlockNum) -> Vec<u8> {
        let mut key = network.as_bytes().to_vec();
        key.push(0); // Separator
        key.extend_from_slice(&block_num.to_be_bytes());
        key
    }
}

#[async_trait]
impl StateStore for RocksDbStore {
    async fn insert(&mut self, batch: StoredBatch) -> Result<()> {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch_key = batch_id.to_be_bytes();
        let batch_value = batch
            .to_bytes()
            .map_err(|e| Error::StateStore(format!("Failed to serialize batch: {}", e)))?;

        let batches_cf = self.db.cf_handle(CF_BATCHES).unwrap();
        let index_cf = self.db.cf_handle(CF_INDEX).unwrap();

        let mut write_batch = WriteBatch::default();

        // Store the batch
        write_batch.put_cf(batches_cf, batch_key, batch_value);

        // Update index for all block ranges in this batch
        for block_range in &batch.ranges {
            // Index each block number in the range
            for block_num in *block_range.numbers.start()..=*block_range.numbers.end() {
                let index_key = Self::range_to_index_key(&block_range.network, block_num);

                // Read existing batch IDs for this key
                let mut batch_ids: Vec<u64> = self
                    .db
                    .get_cf(index_cf, &index_key)?
                    .map(|bytes| {
                        decode_from_slice(&bytes, config::standard())
                            .map(|(v, _)| v)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                // Add new batch ID
                batch_ids.push(batch_id);

                // Write updated list
                let index_value = encode_to_vec(&batch_ids, config::standard())?;
                write_batch.put_cf(index_cf, index_key, index_value);
            }
        }

        self.db.write(write_batch)?;

        Ok(())
    }

    async fn get_in_ranges(&self, ranges: &[InvalidationRange]) -> Result<Vec<Arc<RecordBatch>>> {
        let batches_cf = self.db.cf_handle(CF_BATCHES).unwrap();
        let index_cf = self.db.cf_handle(CF_INDEX).unwrap();

        let mut batch_ids = std::collections::HashSet::new();

        // Collect all batch IDs that overlap with any invalidation range
        for inv_range in ranges {
            for block_num in *inv_range.numbers.start()..=*inv_range.numbers.end() {
                let index_key = Self::range_to_index_key(&inv_range.network, block_num);

                if let Some(bytes) = self.db.get_cf(index_cf, &index_key)? {
                    let (ids, _): (Vec<u64>, _) = decode_from_slice(&bytes, config::standard())?;
                    batch_ids.extend(ids);
                }
            }
        }

        // Fetch all unique batches
        let mut result = Vec::new();
        for batch_id in batch_ids {
            let batch_key = batch_id.to_be_bytes();
            if let Some(batch_bytes) = self.db.get_cf(batches_cf, batch_key)? {
                let stored_batch = StoredBatch::from_bytes(&batch_bytes).map_err(|e| {
                    Error::StateStore(format!("Failed to deserialize batch: {}", e))
                })?;
                result.push(stored_batch.batch);
            }
        }

        Ok(result)
    }

    async fn prune(
        &mut self,
        watermarks: &std::collections::BTreeMap<String, BlockNum>,
    ) -> Result<()> {
        let batches_cf = self.db.cf_handle(CF_BATCHES).unwrap();
        let index_cf = self.db.cf_handle(CF_INDEX).unwrap();

        let mut write_batch = WriteBatch::default();
        let mut batch_ids_to_delete = Vec::new();

        // Iterate through all batches
        let iter = self
            .db
            .iterator_cf(batches_cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item?;
            let batch_id = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            let stored_batch = StoredBatch::from_bytes(&value)
                .map_err(|e| Error::StateStore(format!("Failed to deserialize batch: {}", e)))?;

            // Check if all ranges in this batch are prunable
            let mut all_prunable = true;

            for block_range in &stored_batch.ranges {
                if let Some(&watermark_block) = watermarks.get(&block_range.network) {
                    let prune_before = watermark_block.saturating_sub(self.reorg_window);

                    // If this range ends after the prune threshold, keep this batch
                    if *block_range.numbers.end() >= prune_before {
                        all_prunable = false;
                        break;
                    }
                } else {
                    // No watermark for this network, keep this batch
                    all_prunable = false;
                    break;
                }
            }

            if all_prunable {
                batch_ids_to_delete.push((batch_id, stored_batch));
            }
        }

        // Delete prunable batches and their index entries
        for (batch_id, stored_batch) in batch_ids_to_delete {
            let batch_key = batch_id.to_be_bytes();
            write_batch.delete_cf(batches_cf, batch_key);

            // Remove from index
            for block_range in &stored_batch.ranges {
                for block_num in *block_range.numbers.start()..=*block_range.numbers.end() {
                    let index_key = Self::range_to_index_key(&block_range.network, block_num);

                    if let Some(bytes) = self.db.get_cf(index_cf, &index_key)? {
                        let (mut batch_ids, _): (Vec<u64>, _) =
                            decode_from_slice(&bytes, config::standard())?;
                        batch_ids.retain(|&id| id != batch_id);

                        if batch_ids.is_empty() {
                            write_batch.delete_cf(index_cf, &index_key);
                        } else {
                            let index_value = encode_to_vec(&batch_ids, config::standard())?;
                            write_batch.put_cf(index_cf, index_key, index_value);
                        }
                    }
                }
            }
        }

        self.db.write(write_batch)?;

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
    use tempfile::TempDir;

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
        let temp_dir = TempDir::new().unwrap();
        let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();
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
        let ranges = vec![InvalidationRange {
            network: "test".to_string(),
            numbers: 100..=100,
        }];
        let batches = store.get_in_ranges(&ranges).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn get_records_in_range() {
        //* Given
        let temp_dir = TempDir::new().unwrap();
        let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();
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
        let temp_dir = TempDir::new().unwrap();
        let mut store = RocksDbStore::new(temp_dir.path(), 10).unwrap();
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
        // Verify remaining batches are within the window
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

    #[tokio::test]
    async fn persistence_across_restarts() {
        //* Given
        let temp_dir = TempDir::new().unwrap();
        let batch = Arc::new(create_test_batch());

        // Insert data and close
        {
            let mut store = RocksDbStore::new(temp_dir.path(), 64).unwrap();
            let stored_batch = StoredBatch {
                batch: batch.clone(),
                ranges: vec![BlockRange {
                    network: "test".to_string(),
                    numbers: 100..=100,
                    hash: [0u8; 32].into(),
                    prev_hash: None,
                }],
            };
            store.insert(stored_batch).await.unwrap();
        }

        //* When - Reopen the database
        let store = RocksDbStore::new(temp_dir.path(), 64).unwrap();

        //* Then - Data should still be accessible
        let ranges = vec![InvalidationRange {
            network: "test".to_string(),
            numbers: 100..=100,
        }];
        let batches = store.get_in_ranges(&ranges).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }
}
