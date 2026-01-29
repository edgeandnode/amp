//! Metadata database fixture for isolated test environments.
//!
//! This fixture module provides the `MetadataDb` type for managing temporary metadata
//! databases in test environments. It handles the lifecycle of temporary PostgreSQL instances
//! and connection pool management, ensuring proper cleanup and isolation between tests.

use std::path::PathBuf;

use metadata_db::{DEFAULT_POOL_SIZE, MetadataDb as MetadataDbConnPool};
use metadata_db_postgres::service::Handle;
use tokio::task::JoinHandle;

/// Fixture for managing temporary metadata databases in tests.
///
/// This fixture wraps a running temporary PostgreSQL instance and provides convenient access
/// to the connection pool. The fixture automatically handles cleanup by aborting the database
/// service task when dropped.
pub struct MetadataDb {
    conn_pool: MetadataDbConnPool,
    service_handle: Handle,
    _task: JoinHandle<Result<(), metadata_db_postgres::PostgresError>>,
}

impl MetadataDb {
    /// Create and start a new temporary metadata database for testing.
    ///
    /// Starts a temporary PostgreSQL instance with the default connection pool size.
    /// The database will be automatically shut down when the fixture is dropped.
    /// Uses a unique temporary directory for each test to ensure isolation.
    pub async fn new() -> Self {
        Self::with_pool_size(DEFAULT_POOL_SIZE).await
    }

    /// Create and start a new temporary metadata database with custom pool size.
    ///
    /// Starts a temporary PostgreSQL instance with the specified connection pool size.
    /// The database will be automatically shut down when the fixture is dropped.
    /// Uses a unique temporary directory for each test to ensure isolation.
    ///
    /// NOTE: This method creates a process-wide temp directory. For test isolation,
    /// prefer `with_data_dir()` with a test-specific path.
    pub async fn with_pool_size(pool_size: u32) -> Self {
        // Create a unique temp directory for this test's database
        let temp_dir = std::env::temp_dir().join(format!("amp-test-metadb-{}", std::process::id()));
        Self::with_data_dir_and_pool_size(temp_dir, pool_size).await
    }

    /// Create and start a new temporary metadata database with a specific data directory.
    ///
    /// This allows tests to specify where the PostgreSQL data will be stored.
    /// Useful for testing persistence or sharing databases between tests.
    /// Uses the default connection pool size.
    pub async fn with_data_dir(data_dir: PathBuf) -> Self {
        Self::with_data_dir_and_pool_size(data_dir, DEFAULT_POOL_SIZE).await
    }

    /// Create and start a new temporary metadata database with a specific data directory
    /// and custom pool size.
    ///
    /// This allows tests to specify where the PostgreSQL data will be stored
    /// and customize the connection pool size.
    pub async fn with_data_dir_and_pool_size(data_dir: PathBuf, pool_size: u32) -> Self {
        let (postgres_handle, service) = metadata_db_postgres::service::new(data_dir)
            .await
            .expect("failed to start PostgreSQL for test");

        // Spawn the service to keep the database alive
        let task = tokio::spawn(service);

        let conn_pool = metadata_db::connect_pool_with_retry(postgres_handle.url(), pool_size)
            .await
            .expect("failed to connect to temp metadata-db");

        Self {
            conn_pool,
            service_handle: postgres_handle,
            _task: task,
        }
    }

    /// Gets the database connection URL for use in configuration files
    pub fn connection_url(&self) -> String {
        self.service_handle.url().to_string()
    }

    /// Gets the metadata database connection pool for performing database operations
    pub fn conn_pool(&self) -> &MetadataDbConnPool {
        &self.conn_pool
    }
}

impl Drop for MetadataDb {
    fn drop(&mut self) {
        tracing::debug!("Aborting metadata database service task");
        self._task.abort();
    }
}
