//! LMDB-backed persistent state store implementation

use std::{path::Path, sync::Arc};

use heed::{
    Database, Env, EnvOpenOptions,
    types::{Bytes, Str},
};

use super::{StateSnapshot, StateStore};
use crate::{
    error::Error,
    transactional::{Commit, TransactionId},
};

/// LMDB-backed persistent implementation of StateStore.
///
/// Provides crash-safe state persistence using LMDB. State is stored as a single
/// serialized `StateSnapshot` under a fixed key, ensuring atomic updates.
///
/// # Example
/// ```rust,ignore
/// use amp_client::{AmpClient, LmdbStateStore};
///
/// let store = LmdbStateStore::new("/path/to/db")?;
/// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
///     .transactional(store, 128)
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
    /// * `path` - Directory path for LMDB storage
    ///
    /// # Example
    /// ```rust,ignore
    /// use amp_client::LmdbStateStore;
    ///
    /// let store = LmdbStateStore::new("/path/to/db")?;
    /// ```
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::Store(format!("Failed to create LMDB directory: {}", e)))?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1 GB
                .max_dbs(1)
                .open(path)
                .map_err(|e| Error::Store(format!("Failed to open LMDB environment: {}", e)))?
        };

        let mut wtxn = env
            .write_txn()
            .map_err(|e| Error::Store(format!("Failed to create write transaction: {}", e)))?;
        let db = env
            .create_database(&mut wtxn, Some("state"))
            .map_err(|e| Error::Store(format!("Failed to create database: {}", e)))?;
        wtxn.commit()
            .map_err(|e| Error::Store(format!("Failed to commit transaction: {}", e)))?;

        Ok(Self {
            env: Arc::new(env),
            db,
        })
    }

    /// Read the current state snapshot from storage.
    fn read_state(&self) -> Result<StateSnapshot, Error> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|e| Error::Store(format!("Failed to create read transaction: {}", e)))?;

        match self
            .db
            .get(&rtxn, "snapshot")
            .map_err(|e| Error::Store(format!("Failed to read state: {}", e)))?
        {
            Some(bytes) => serde_json::from_slice(bytes).map_err(Error::Json),
            None => Ok(StateSnapshot::default()),
        }
    }

    /// Write the state snapshot to storage.
    fn write_state(&mut self, state: &StateSnapshot) -> Result<(), Error> {
        let bytes = serde_json::to_vec(state).map_err(Error::Json)?;

        let mut wtxn = self
            .env
            .write_txn()
            .map_err(|e| Error::Store(format!("Failed to create write transaction: {}", e)))?;
        self.db
            .put(&mut wtxn, "snapshot", &bytes)
            .map_err(|e| Error::Store(format!("Failed to write state: {}", e)))?;
        wtxn.commit()
            .map_err(|e| Error::Store(format!("Failed to commit transaction: {}", e)))?;

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
