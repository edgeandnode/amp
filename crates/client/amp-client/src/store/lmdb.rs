//! LMDB-backed persistent state and batch store implementations

use std::{ops::RangeInclusive, path::Path, sync::Arc};

use common::arrow::array::RecordBatch;
use heed::{
    Database, Env, EnvOpenOptions,
    types::{Bytes, Str},
};

use super::{BatchStore, StateSnapshot, StateStore, deserialize_batch, serialize_batch};
use crate::{
    error::{Error, LmdbError, StateStoreError},
    transactional::{Commit, TransactionId},
};

/// Open a shared LMDB environment with default settings (10GB, 2 databases).
///
/// This is a convenience function for creating an LMDB environment suitable
/// for both state and batch stores. For more control, create your own `Env`.
///
/// # Arguments
/// * `path` - Directory path for LMDB storage
pub fn open_lmdb_env(path: impl AsRef<Path>) -> Result<Arc<Env>, Error> {
    open_lmdb_env_with_options(path, 10 * 1024 * 1024 * 1024, 2)
}

/// Open an LMDB environment with custom settings.
///
/// # Arguments
/// * `path` - Directory path for LMDB storage
/// * `map_size` - Maximum size of the LMDB database in bytes
/// * `max_dbs` - Maximum number of named databases (use 2 for state + batch)
pub fn open_lmdb_env_with_options(
    path: impl AsRef<Path>,
    map_size: usize,
    max_dbs: u32,
) -> Result<Arc<Env>, Error> {
    std::fs::create_dir_all(&path).map_err(|err| {
        Error::StateStore(StateStoreError::Lmdb(LmdbError::DirectoryCreation(err)))
    })?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(map_size)
            .max_dbs(max_dbs)
            .open(path)
            .map_err(|err| Error::StateStore(StateStoreError::Lmdb(LmdbError::EnvOpen(err))))?
    };

    Ok(Arc::new(env))
}

/// LMDB-backed persistent implementation of StateStore.
///
/// Accepts a shared LMDB environment, allowing you to control the configuration
/// and share the environment with other stores (e.g., `LmdbBatchStore`).
///
/// # Example
/// ```rust,ignore
/// use amp_client::store::{open_lmdb_env, LmdbStateStore, LmdbBatchStore};
///
/// let env = open_lmdb_env("/path/to/db")?;
/// let state_store = LmdbStateStore::new(env.clone())?;
/// let batch_store = LmdbBatchStore::new(env)?;
///
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .cdc(state_store, batch_store, 128)
///     .await?;
/// ```
pub struct LmdbStateStore {
    env: Arc<Env>,
    db: Database<Str, Bytes>,
}

impl LmdbStateStore {
    /// Create a new LMDB-backed state store.
    ///
    /// # Arguments
    /// * `env` - Shared LMDB environment
    pub fn new(env: Arc<Env>) -> Result<Self, Error> {
        let mut wtxn = env.write_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::WriteTransactionBegin(err)))
        })?;
        let db = env
            .create_database(&mut wtxn, Some("state"))
            .map_err(|err| {
                Error::StateStore(StateStoreError::Lmdb(LmdbError::DatabaseCreation(err)))
            })?;
        wtxn.commit().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::TransactionCommit(err)))
        })?;

        Ok(Self { env, db })
    }

    /// Read the current state snapshot from storage.
    fn read_state(&self) -> Result<StateSnapshot, Error> {
        let rtxn = self.env.read_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::ReadTransactionBegin(err)))
        })?;

        match self
            .db
            .get(&rtxn, "snapshot")
            .map_err(|err| Error::StateStore(StateStoreError::Lmdb(LmdbError::StateRead(err))))?
        {
            Some(bytes) => serde_json::from_slice(bytes).map_err(Error::Json),
            None => Ok(StateSnapshot::default()),
        }
    }

    /// Write the state snapshot to storage.
    fn write_state(&mut self, state: &StateSnapshot) -> Result<(), Error> {
        let bytes = serde_json::to_vec(state).map_err(Error::Json)?;

        let mut wtxn = self.env.write_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::WriteTransactionBegin(err)))
        })?;
        self.db
            .put(&mut wtxn, "snapshot", &bytes)
            .map_err(|err| Error::StateStore(StateStoreError::Lmdb(LmdbError::StateWrite(err))))?;
        wtxn.commit().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::TransactionCommit(err)))
        })?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StateStore for LmdbStateStore {
    async fn advance(&mut self, next: TransactionId) -> Result<(), Error> {
        let mut state = self.read_state()?;
        state.next = next;
        self.write_state(&state)
    }

    async fn commit(&mut self, commit: Commit) -> Result<(), Error> {
        let mut state = self.read_state()?;

        // Remove pruned watermarks (all IDs <= prune)
        if let Some(prune) = commit.prune {
            state.buffer.retain(|(id, _)| *id > prune);
        }

        // Add new watermarks
        for (id, ranges) in commit.insert {
            state.buffer.push_back((id, ranges));
        }

        self.write_state(&state)
    }

    async fn truncate(&mut self, from: TransactionId) -> Result<(), Error> {
        let mut state = self.read_state()?;
        state.buffer.retain(|(id, _)| *id < from);
        self.write_state(&state)
    }

    async fn load(&self) -> Result<StateSnapshot, Error> {
        self.read_state()
    }
}

/// LMDB-backed persistent implementation of BatchStore.
///
/// Accepts a shared LMDB environment, allowing you to control the configuration
/// and share the environment with other stores (e.g., `LmdbStateStore`).
///
/// # Storage Format
///
/// - **Key**: Transaction ID encoded as 8-byte big-endian u64
/// - **Value**: RecordBatch serialized using Arrow IPC format
///
/// # Example
/// ```rust,ignore
/// use amp_client::store::{open_lmdb_env, LmdbStateStore, LmdbBatchStore};
///
/// let env = open_lmdb_env("/path/to/db")?;
/// let state_store = LmdbStateStore::new(env.clone())?;
/// let batch_store = LmdbBatchStore::new(env)?;
///
/// let stream = client.stream("SELECT * FROM eth.logs")
///     .cdc(state_store, batch_store, 128)
///     .await?;
/// ```
pub struct LmdbBatchStore {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
}

impl LmdbBatchStore {
    /// Create a new LMDB-backed batch store from an existing environment.
    ///
    /// # Arguments
    /// * `env` - Shared LMDB environment
    pub fn new(env: Arc<Env>) -> Result<Self, Error> {
        let mut wtxn = env.write_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::WriteTransactionBegin(err)))
        })?;
        let db = env
            .create_database(&mut wtxn, Some("batches"))
            .map_err(|err| {
                Error::StateStore(StateStoreError::Lmdb(LmdbError::DatabaseCreation(err)))
            })?;
        wtxn.commit().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::TransactionCommit(err)))
        })?;

        Ok(Self { env, db })
    }
}

/// Encode TransactionId as 8-byte big-endian for LMDB key.
///
/// Big-endian ensures lexicographic ordering matches numeric ordering,
/// which is essential for efficient range queries in LMDB.
fn encode_id(id: TransactionId) -> [u8; 8] {
    id.to_be_bytes()
}

/// Decode 8-byte big-endian key back to TransactionId.
fn decode_id(bytes: &[u8]) -> Result<TransactionId, Error> {
    bytes.try_into().map(u64::from_be_bytes).map_err(|_| {
        Error::StateStore(StateStoreError::Lmdb(
            LmdbError::InvalidTransactionIdKeyLength,
        ))
    })
}

#[async_trait::async_trait]
impl BatchStore for LmdbBatchStore {
    async fn append(&mut self, batch: RecordBatch, id: TransactionId) -> Result<(), Error> {
        let serialized = serialize_batch(&batch)?;
        let key = encode_id(id);

        let mut wtxn = self.env.write_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::WriteTransactionBegin(err)))
        })?;
        self.db
            .put(&mut wtxn, &key, &serialized)
            .map_err(|err| Error::StateStore(StateStoreError::Lmdb(LmdbError::BatchWrite(err))))?;
        wtxn.commit().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::TransactionCommit(err)))
        })?;

        Ok(())
    }

    async fn seek(
        &self,
        range: RangeInclusive<TransactionId>,
    ) -> Result<Vec<TransactionId>, Error> {
        let rtxn = self.env.read_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::ReadTransactionBegin(err)))
        })?;

        let start_key = encode_id(*range.start());
        let end_key = encode_id(*range.end());

        let mut ids = Vec::new();
        let iter = self.db.iter(&rtxn).map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::IteratorCreation(err)))
        })?;

        // Seek to start of range
        for result in iter {
            let (key, _) = result.map_err(|err| {
                Error::StateStore(StateStoreError::Lmdb(LmdbError::BatchIteration(err)))
            })?;

            // Skip keys before range start
            if key < start_key.as_slice() {
                continue;
            }

            // Stop if we've passed the range end
            if key > end_key.as_slice() {
                break;
            }

            ids.push(decode_id(key)?);
        }

        Ok(ids)
    }

    async fn load(&self, id: TransactionId) -> Result<Option<RecordBatch>, Error> {
        let rtxn = self.env.read_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::ReadTransactionBegin(err)))
        })?;

        let key = encode_id(id);
        match self
            .db
            .get(&rtxn, &key)
            .map_err(|err| Error::StateStore(StateStoreError::Lmdb(LmdbError::BatchRead(err))))?
        {
            Some(bytes) => Ok(Some(deserialize_batch(bytes)?)),
            None => Ok(None), // Not an error - batch might not exist (e.g., watermark event)
        }
    }

    async fn prune(&mut self, cutoff: TransactionId) -> Result<(), Error> {
        let mut wtxn = self.env.write_txn().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::WriteTransactionBegin(err)))
        })?;

        let cutoff_key = encode_id(cutoff);

        // Collect keys to delete first to avoid iterator invalidation
        let mut keys_to_delete = Vec::new();
        {
            let iter = self.db.iter(&wtxn).map_err(|err| {
                Error::StateStore(StateStoreError::Lmdb(LmdbError::IteratorCreation(err)))
            })?;

            for result in iter {
                let (key, _) = result.map_err(|err| {
                    Error::StateStore(StateStoreError::Lmdb(LmdbError::BatchIteration(err)))
                })?;

                if key <= cutoff_key.as_slice() {
                    keys_to_delete.push(key.to_vec());
                } else {
                    break; // Keys are ordered, so we can stop
                }
            }
        }

        // Delete collected keys
        for key in keys_to_delete {
            self.db.delete(&mut wtxn, &key).map_err(|err| {
                Error::StateStore(StateStoreError::Lmdb(LmdbError::BatchDelete(err)))
            })?;
        }

        wtxn.commit().map_err(|err| {
            Error::StateStore(StateStoreError::Lmdb(LmdbError::TransactionCommit(err)))
        })?;

        Ok(())
    }
}
