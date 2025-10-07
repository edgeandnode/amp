//! Establishes the connection to the postgres db where amp datasets will be synced to

use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tracing::{instrument, warn};

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
    /// Set up a connection pool to the syncing layer DB with exponential backoff retry.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self, ConnError> {
        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(30))
            .with_max_times(10);

        fn is_connection_error(err: &sqlx::Error) -> bool {
            match err {
                sqlx::Error::Database(db_err) => {
                    // PostgreSQL error codes for connection issues
                    db_err.code().map_or(false, |code| {
                        matches!(
                            code.as_ref(),
                            "57P03" | // The database system is starting up
                            "53300" | // Too many connections
                            "08006" | // Connection failure
                            "08001" | // Unable to connect to server
                            "08004" // Server rejected the connection
                        )
                    })
                }
                sqlx::Error::Io(_) => true,  // I/O errors are retryable
                sqlx::Error::Tls(_) => true, // TLS errors might be temporary
                sqlx::Error::PoolClosed => true, // Pool closed, might recover
                sqlx::Error::PoolTimedOut => true, // Pool timeout, retry
                _ => false,
            }
        }

        fn notify_retry(err: &sqlx::Error, dur: Duration) {
            warn!(
                error = %err,
                retry_delay_secs = dur.as_secs_f32(),
                "db_connection_retry"
            );
        }

        let pool_options = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(5));

        let url = url.to_string();

        (|| {
            let pool_options = pool_options.clone();
            let url = url.clone();
            async move { pool_options.connect(&url).await }
        })
        .retry(retry_policy)
        .when(is_connection_error)
        .notify(notify_retry)
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
