use std::{ops::RangeInclusive, path::Path, sync::Arc};

use async_trait::async_trait;
use common::arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use heed::{
    Database, Env, EnvOpenOptions,
    byteorder::BigEndian,
    types::{Bytes, U64},
};

use super::StateStore;
use crate::error::{Error, Result};

/// LMDB-backed persistent implementation of StateStore.
pub struct LmdbStore {
    env: Arc<Env>,
    db: Database<U64<BigEndian>, Bytes>,
}

impl LmdbStore {
    /// Create a new LMDB-backed state store.
    ///
    /// # Arguments
    /// * `path` - Directory path for LMDB storage
    ///
    /// # Example
    /// ```no_run
    /// use amp_debezium_client::LmdbStore;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = LmdbStore::new("/path/to/db")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::StateStore(format!("Failed to create directory: {}", e)))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024 * 1024) // 10 GB
                .max_dbs(1)
                .open(path)?
        };

        let mut wtxn = env.write_txn()?;
        let db = env.create_database(&mut wtxn, Some("batches"))?;
        wtxn.commit()?;

        Ok(Self {
            env: Arc::new(env),
            db,
        })
    }
}

#[async_trait]
impl StateStore for LmdbStore {
    async fn append(&mut self, batch: RecordBatch, id: u64) -> Result<()> {
        // Serialize RecordBatch to Arrow IPC format
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| Error::StateStore(format!("Failed to create writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| Error::StateStore(format!("Failed to write batch: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::StateStore(format!("Failed to finish writer: {}", e)))?;

        let mut wtxn = self.env.write_txn()?;
        self.db.put(&mut wtxn, &id, &buf)?;
        wtxn.commit()?;

        Ok(())
    }

    async fn prune(&mut self, cutoff: u64) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        self.db.delete_range(&mut wtxn, &(0..cutoff))?;
        wtxn.commit()?;

        Ok(())
    }

    async fn retract(&mut self, ids: RangeInclusive<u64>) -> Result<Vec<RecordBatch>> {
        let mut retracted = Vec::new();
        let mut wtxn = self.env.write_txn()?;

        // Fetch all batches in the range first
        let start = *ids.start();
        let end = *ids.end();

        {
            let range_iter = self.db.range(&wtxn, &(start..=end))?;
            for result in range_iter {
                let (_, batch_bytes) = result?;

                // Deserialize RecordBatch from Arrow IPC format
                let mut reader = StreamReader::try_new(batch_bytes, None)
                    .map_err(|e| Error::StateStore(format!("Failed to create reader: {}", e)))?;
                let batch = reader
                    .next()
                    .ok_or_else(|| Error::StateStore("No batch found in stream".to_string()))?
                    .map_err(|e| Error::StateStore(format!("Failed to read batch: {}", e)))?;

                retracted.push(batch);
            }
        }

        // Delete entire range in one operation
        self.db.delete_range(&mut wtxn, &(start..=end))?;
        wtxn.commit()?;

        Ok(retracted)
    }
}
