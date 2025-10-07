//! Establishes the connection to the postgres db where amp datasets will be synced to

use std::time::{Duration, Instant};

use backon::{ExponentialBuilder, Retryable};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tracing::{error, instrument, warn};

pub const DEFAULT_POOL_SIZE: u32 = 10;
/// Default maximum duration for connection retries (5 minutes)
pub const DEFAULT_MAX_RETRY_DURATION_SECS: u64 = 300;

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
    /// Connects to PostgreSQL with exponential backoff and circuit breaker.
    ///
    /// Respects `DB_MAX_RETRY_DURATION_SECS` environment variable (default: 300 seconds).
    /// Stops retrying after this duration to prevent indefinite hangs.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self, ConnError> {
        Self::connect_with_max_duration(url, pool_size, None).await
    }

    /// Connects with explicit max retry duration for testing.
    pub async fn connect_with_max_duration(
        url: &str,
        pool_size: u32,
        max_duration: Option<Duration>,
    ) -> Result<Self, ConnError> {
        let max_retry_duration = max_duration.unwrap_or_else(|| {
            let secs = std::env::var("DB_MAX_RETRY_DURATION_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_MAX_RETRY_DURATION_SECS);
            Duration::from_secs(secs)
        });

        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(30))
            .with_max_times(100); // High limit, circuit breaker will stop us first

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

        let pool_options = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(5));

        let url = url.to_string();

        // Circuit breaker: track when we started retrying
        let start_time = Instant::now();

        let notify_retry = move |err: &sqlx::Error, dur: Duration| {
            let elapsed = start_time.elapsed();

            if elapsed >= max_retry_duration {
                error!(
                    error = %err,
                    elapsed_secs = elapsed.as_secs(),
                    max_duration_secs = max_retry_duration.as_secs(),
                    "db_connection_circuit_breaker_triggered"
                );
            } else {
                warn!(
                    error = %err,
                    retry_delay_secs = dur.as_secs_f32(),
                    elapsed_secs = elapsed.as_secs(),
                    "db_connection_retry"
                );
            }
        };

        // Circuit breaker condition: stop if we've been retrying too long
        let circuit_breaker_start = Instant::now();
        let is_retryable = move |err: &sqlx::Error| -> bool {
            if circuit_breaker_start.elapsed() >= max_retry_duration {
                return false; // Circuit breaker: stop retrying
            }
            is_connection_error(err)
        };

        (|| {
            let pool_options = pool_options.clone();
            let url = url.clone();
            async move { pool_options.connect(&url).await }
        })
        .retry(retry_policy)
        .when(is_retryable)
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
