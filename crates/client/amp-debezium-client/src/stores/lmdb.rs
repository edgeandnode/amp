use std::{path::Path, sync::Arc};

use amp_client::InvalidationRange;
use async_trait::async_trait;
use bincode::{config, decode_from_slice, encode_to_vec};
use common::{BlockNum, arrow::array::RecordBatch};
use heed::{
    Database, Env, EnvOpenOptions,
    byteorder::BigEndian,
    types::{Bytes, U64},
};

use super::StateStore;
use crate::{
    error::{Error, Result},
    types::StoredBatch,
};

/// LMDB-backed persistent implementation of StateStore.
///
/// Stores batches durably on disk for reorg handling that survives restarts.
/// Uses two databases:
/// - `batches`: Stores serialized StoredBatch data keyed by batch_id
/// - `index`: Maps (network, block_num) -> Vec<batch_id> for fast range queries
pub struct LmdbStore {
    env: Arc<Env>,
    batches_db: Database<U64<BigEndian>, Bytes>,
    index_db: Database<Bytes, Bytes>,
    reorg_window: u64,
    next_batch_id: u64,
}

impl LmdbStore {
    /// Create a new LMDB-backed state store.
    ///
    /// # Arguments
    /// * `path` - Directory path for LMDB storage
    /// * `reorg_window` - Number of blocks to retain
    ///
    /// # Example
    /// ```no_run
    /// use amp_debezium_client::LmdbStore;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = LmdbStore::new("/path/to/db", 64)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(path: impl AsRef<Path>, reorg_window: u64) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::StateStore(format!("Failed to create directory: {}", e)))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024 * 1024) // 10 GB
                .max_dbs(2)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let batches_db = env.create_database(&mut wtxn, Some("batches"))?;
        let index_db = env.create_database(&mut wtxn, Some("index"))?;
        wtxn.commit()?;

        // Determine next_batch_id by finding the highest existing ID
        let next_batch_id = {
            let rtxn = env.read_txn()?;
            let mut iter = batches_db.rev_iter(&rtxn)?;

            if let Some(result) = iter.next() {
                let (last_id, _) = result?;
                last_id + 1
            } else {
                0
            }
        };

        Ok(Self {
            env: Arc::new(env),
            batches_db,
            index_db,
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
impl StateStore for LmdbStore {
    async fn insert(&mut self, batch: StoredBatch) -> Result<()> {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;

        let batch_value = batch
            .to_bytes()
            .map_err(|e| Error::StateStore(format!("Failed to serialize batch: {}", e)))?;

        let mut wtxn = self.env.write_txn()?;

        // Store the batch
        self.batches_db.put(&mut wtxn, &batch_id, &batch_value)?;

        // Update index for all block ranges in this batch
        for block_range in &batch.ranges {
            // Index each block number in the range
            for block_num in *block_range.numbers.start()..=*block_range.numbers.end() {
                let index_key = Self::range_to_index_key(&block_range.network, block_num);

                // Read existing batch IDs for this key
                let mut batch_ids: Vec<u64> = self
                    .index_db
                    .get(&wtxn, &index_key)?
                    .map(|bytes| {
                        decode_from_slice(bytes, config::standard())
                            .map(|(v, _)| v)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                // Add new batch ID
                batch_ids.push(batch_id);

                // Write updated list
                let index_value = encode_to_vec(&batch_ids, config::standard())?;
                self.index_db.put(&mut wtxn, &index_key, &index_value)?;
            }
        }

        wtxn.commit()?;

        Ok(())
    }

    async fn get_in_ranges(&self, ranges: &[InvalidationRange]) -> Result<Vec<Arc<RecordBatch>>> {
        let rtxn = self.env.read_txn()?;

        let mut batch_ids = std::collections::HashSet::new();

        // Collect all batch IDs that overlap with any invalidation range
        for inv_range in ranges {
            for block_num in *inv_range.numbers.start()..=*inv_range.numbers.end() {
                let index_key = Self::range_to_index_key(&inv_range.network, block_num);

                if let Some(bytes) = self.index_db.get(&rtxn, &index_key)? {
                    let (ids, _): (Vec<u64>, _) = decode_from_slice(bytes, config::standard())?;
                    batch_ids.extend(ids);
                }
            }
        }

        // Fetch all unique batches
        let mut result = Vec::new();
        for batch_id in batch_ids {
            if let Some(batch_bytes) = self.batches_db.get(&rtxn, &batch_id)? {
                let stored_batch = StoredBatch::from_bytes(batch_bytes).map_err(|e| {
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
        let mut batch_ids_to_delete = Vec::new();

        // Iterate through all batches in a read transaction
        {
            let rtxn = self.env.read_txn()?;
            let iter = self.batches_db.iter(&rtxn)?;

            for item in iter {
                let (batch_id, value) = item?;
                let stored_batch = StoredBatch::from_bytes(value).map_err(|e| {
                    Error::StateStore(format!("Failed to deserialize batch: {}", e))
                })?;

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
        }

        // Delete prunable batches and their index entries in a write transaction
        let mut wtxn = self.env.write_txn()?;

        for (batch_id, stored_batch) in batch_ids_to_delete {
            self.batches_db.delete(&mut wtxn, &batch_id)?;

            // Remove from index
            for block_range in &stored_batch.ranges {
                for block_num in *block_range.numbers.start()..=*block_range.numbers.end() {
                    let index_key = Self::range_to_index_key(&block_range.network, block_num);

                    if let Some(bytes) = self.index_db.get(&wtxn, &index_key)? {
                        let (mut batch_ids, _): (Vec<u64>, _) =
                            decode_from_slice(bytes, config::standard())?;
                        batch_ids.retain(|&id| id != batch_id);

                        if batch_ids.is_empty() {
                            self.index_db.delete(&mut wtxn, &index_key)?;
                        } else {
                            let index_value = encode_to_vec(&batch_ids, config::standard())?;
                            self.index_db.put(&mut wtxn, &index_key, &index_value)?;
                        }
                    }
                }
            }
        }

        wtxn.commit()?;

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
        let mut store = LmdbStore::new(temp_dir.path(), 64).unwrap();
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
        let mut store = LmdbStore::new(temp_dir.path(), 64).unwrap();
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
        let mut store = LmdbStore::new(temp_dir.path(), 10).unwrap();
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
            let mut store = LmdbStore::new(temp_dir.path(), 64).unwrap();
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
        let store = LmdbStore::new(temp_dir.path(), 64).unwrap();

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
