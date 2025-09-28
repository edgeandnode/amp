//! Establishes the connection to the postgres db where amp datasets will be synced to

use std::time::Duration;

use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tracing::instrument;

pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Errors that can occur when connecting to the syncing layer DB.
#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    /// Error connecting to the DB.
    #[error("Error connecting to db: {0}")]
    ConnectionError(#[source] sqlx::Error),
}

/// A connection pool to the metadata DB.
#[derive(Debug, Clone)]
pub struct DbConnPool(Pool<Postgres>);

impl DbConnPool {
    /// Set up a connection pool to the syncing layer DB.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self, ConnError> {
        PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map(Self)
            .map_err(ConnError::ConnectionError)
    }
}

impl std::ops::Deref for DbConnPool {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DbConnPool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
