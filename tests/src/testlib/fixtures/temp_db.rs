//! Metadata database fixture for isolated test environments.
//!
//! This fixture module provides the `TempMetadataDb` type for managing temporary metadata
//! databases in test environments. It handles the lifecycle of temporary PostgreSQL instances
//! and MetadataDb connections, ensuring proper cleanup and isolation between tests.

use metadata_db::{DEFAULT_POOL_SIZE, MetadataDb};

use crate::testlib::debug::TESTS_KEEP_TEMP_DIRS;

/// Fixture for managing temporary metadata databases in tests.
///
/// This fixture wraps a `TempDbHandle` and a `MetadataDb` connection, providing a convenient interface
/// for creating and managing temporary metadata databases with proper cleanup behavior. The fixture
/// automatically handles the `TESTS_KEEP_TEMP_DIRS` environment variable for debugging.
pub struct TempMetadataDb {
    metadata_db: MetadataDb,
    _temp_db: tempdb::TempDbHandle,
}

impl std::ops::Deref for TempMetadataDb {
    type Target = MetadataDb;

    fn deref(&self) -> &Self::Target {
        &self.metadata_db
    }
}

impl TempMetadataDb {
    /// Create a new temporary metadata database for testing.
    ///
    /// Creates a new `TempMetadataDb` instance with default pool size and proper cleanup
    /// behavior based on the `TESTS_KEEP_TEMP_DIRS` environment variable.
    pub async fn new() -> Self {
        Self::with_pool_size(DEFAULT_POOL_SIZE).await
    }

    /// Create a new temporary metadata database with custom pool size.
    ///
    /// Creates a new `TempMetadataDb` instance with the specified pool size and proper cleanup
    /// behavior based on the `TESTS_KEEP_TEMP_DIRS` environment variable.
    pub async fn with_pool_size(pool_size: u32) -> Self {
        let (temp_db, service) = tempdb::service::new(*TESTS_KEEP_TEMP_DIRS);

        // Spawn the service to keep the database alive
        tokio::spawn(service);

        let metadata_db = MetadataDb::connect_with_retry(temp_db.url(), pool_size)
            .await
            .expect("failed to connect to temp metadata-db");

        Self {
            metadata_db,
            _temp_db: temp_db,
        }
    }

    /// Get the database connection URL.
    ///
    /// Returns the connection URL for the temporary database, which can be used
    /// in configuration files or for direct database connections.
    pub fn connection_url(&self) -> String {
        self._temp_db.url().to_string()
    }

    /// Get a reference to the metadata database.
    ///
    /// Returns a reference to the `MetadataDb` instance that can be used for database
    /// operations in tests.
    pub fn metadata_db(&self) -> &MetadataDb {
        &self.metadata_db
    }
}
