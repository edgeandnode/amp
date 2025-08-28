//! Test utilities for database connections and testing

use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use tracing::warn;

use crate::{
    Error, MetadataDb,
    conn::{ConnError, DbConn},
};

/// Connect to the database with retry logic to handle startup timing issues
///
/// This is specifically designed for tests using pgtemp where PostgreSQL
/// may not be immediately ready to accept connections after startup.
pub async fn connect_with_retry(url: &str) -> Result<DbConn, ConnError> {
    (|| DbConn::connect(url))
        .retry(test_retry_policy())
        .when(is_database_starting_up)
        .notify(|err, dur| {
            warn!(
                error = %err,
                "Database still starting up during test connection. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// Connect to MetadataDb with retry logic to handle startup timing issues
///
/// This is specifically designed for tests using pgtemp where PostgreSQL
/// may not be immediately ready to accept connections after startup.
pub async fn connect_metadata_db_with_retry(
    url: &str,
    pool_size: u32,
) -> Result<MetadataDb, Error> {
    (|| MetadataDb::connect(url, pool_size))
        .retry(test_retry_policy())
        .when(is_metadata_db_starting_up)
        .notify(|err, dur| {
            warn!(
                error = %err,
                "Database still starting up during test MetadataDb connection. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// Test retry policy matching the pattern used in worker metadata operations
///
/// Uses exponential backoff optimized for test environments:
/// - Faster initial retries (10ms vs 1s)
/// - Shorter max delay (100ms vs default)
/// - More attempts (20 vs 3) since we expect quick startup
fn test_retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_millis(100))
        .with_max_times(20)
}

/// Check if a ConnError is due to database startup
fn is_database_starting_up(err: &ConnError) -> bool {
    match err {
        ConnError::ConnectionError(sqlx::Error::Database(db_err)) => db_err
            .to_string()
            .contains("the database system is starting up"),
        _ => false,
    }
}

/// Check if a MetadataDb Error is due to database startup
fn is_metadata_db_starting_up(err: &Error) -> bool {
    match err {
        Error::ConnectionError(sqlx::Error::Database(db_err)) => db_err
            .to_string()
            .contains("the database system is starting up"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use pgtemp::PgTempDB;

    use super::*;

    #[tokio::test]
    async fn test_connect_with_retry_success() {
        let temp_db = PgTempDB::new();
        let mut conn = connect_with_retry(&temp_db.connection_uri())
            .await
            .expect("Should connect successfully");

        conn.run_migrations().await.expect("Should run migrations");
    }

    #[tokio::test]
    async fn test_connect_metadata_db_with_retry_success() {
        let temp_db = PgTempDB::new();
        let _metadata_db = connect_metadata_db_with_retry(&temp_db.connection_uri(), 1)
            .await
            .expect("Should connect successfully");
    }
}
