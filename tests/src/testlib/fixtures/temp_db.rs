//! Metadata database fixture for isolated test environments.
//!
//! This fixture module provides the `MetadataDbFixture` type for managing temporary metadata
//! databases in test environments. It handles the lifecycle of `TempMetadataDb` instances,
//! ensuring proper cleanup and isolation between tests.

use metadata_db::DEFAULT_POOL_SIZE;

use crate::testlib::debug::TESTS_KEEP_TEMP_DIRS;

/// Fixture for managing temporary metadata databases in tests.
///
/// This fixture wraps a `TempMetadataDb` and provides a convenient interface for creating
/// and managing temporary metadata databases with proper cleanup behavior. The fixture
/// automatically handles the `TESTS_KEEP_TEMP_DIRS` environment variable for debugging.
pub struct TempMetadataDb {
    inner: metadata_db::temp::TempMetadataDb,
}

impl std::ops::Deref for TempMetadataDb {
    type Target = metadata_db::MetadataDb;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TempMetadataDb {
    /// Create a new temporary metadata database for testing.
    ///
    /// Creates a new `TempMetadataDb` instance with default pool size and proper cleanup
    /// behavior based on the `TESTS_KEEP_TEMP_DIRS` environment variable.
    pub async fn new() -> Self {
        let inner =
            metadata_db::temp::TempMetadataDb::new(*TESTS_KEEP_TEMP_DIRS, DEFAULT_POOL_SIZE).await;
        Self { inner }
    }

    /// Create a new temporary metadata database with custom pool size.
    ///
    /// Creates a new `TempMetadataDb` instance with the specified pool size and proper cleanup
    /// behavior based on the `TESTS_KEEP_TEMP_DIRS` environment variable.
    pub async fn with_pool_size(pool_size: u32) -> Self {
        let inner = metadata_db::temp::TempMetadataDb::new(*TESTS_KEEP_TEMP_DIRS, pool_size).await;
        Self { inner }
    }

    /// Get the database connection URL.
    ///
    /// Returns the connection URL for the temporary database, which can be used
    /// in configuration files or for direct database connections.
    pub fn connection_url(&self) -> &str {
        self.inner.url()
    }

    /// Get a reference to the metadata database.
    ///
    /// Returns a reference to the `MetadataDb` instance that can be used for database
    /// operations in tests.
    pub fn metadata_db(&self) -> &metadata_db::MetadataDb {
        &self.inner
    }
}
