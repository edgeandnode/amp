//! Test environment directory management.
//!
//! This module provides low-level temporary directory management for isolated test environments.
//! It handles the creation of temporary directories with test-specific naming and automatic cleanup.
//!
//! # Core Functionality
//!
//! ## Directory Management
//! - Creates temporary directories with test-specific naming
//! - Handles automatic cleanup with optional debug retention
//!
//! ## Integration Points
//!
//! This module is primarily used by:
//! - [`super::ctx::TestCtxBuilder`] - High-level test environment construction
//!
//! Most test authors should use the [`super::ctx`] module's [`TestCtxBuilder`](super::ctx::TestCtxBuilder) rather
//! than working directly with [`TestEnvDir`], as it provides a more convenient API.

use std::path::PathBuf;

use tempfile::TempDir;

use super::debug;
use crate::BoxError;

/// Wrapper around [`TempDir`] that provides a temporary test environment directory.
///
/// This struct wraps a `TempDir` and provides a convenient way to create
/// temporary test environment directories with test-specific naming and cleanup behavior.
/// It automatically handles the `TESTS_KEEP_TEMP_DIRS` environment variable
/// for debugging purposes.
pub struct TestEnvDir {
    temp_dir: TempDir,
}

impl TestEnvDir {
    /// Creates a new temporary test environment directory with test-specific naming.
    ///
    /// The directory will be created with a prefix based on the test name,
    /// making it easier to identify which test created which temporary directory.
    /// If the `TESTS_KEEP_TEMP_DIRS` environment variable is set, the directory
    /// will not be automatically cleaned up on drop, which is useful for debugging.
    pub fn new(test_name: &str) -> Result<Self, BoxError> {
        let prefix = format!("tests__{}__", test_name);

        let mut builder = tempfile::Builder::new();
        builder.prefix(&prefix);

        // Check for debug environment variable to keep temp directories
        if *debug::TESTS_KEEP_TEMP_DIRS {
            builder.disable_cleanup(true);
            tracing::info!("TESTS_KEEP_TEMP_DIRS set - temporary directory will not be cleaned up");
        }

        let temp_dir = builder.tempdir()?;

        Ok(TestEnvDir { temp_dir })
    }

    /// Get the path to a file in the test environment.
    ///
    /// Returns the full path to the specified file in the test environment's directory.
    /// This is useful for getting paths to any files that may be written to the test environment.
    pub fn file_path(&self, filename: &str) -> PathBuf {
        self.temp_dir.path().join(filename)
    }
}

impl std::ops::Deref for TestEnvDir {
    type Target = TempDir;

    /// Provides transparent access to the underlying `TempDir`.
    ///
    /// This allows `TestEnvDir` to be used anywhere a `TempDir` is expected,
    /// providing access to methods like `path()` and other `TempDir` functionality.
    fn deref(&self) -> &Self::Target {
        &self.temp_dir
    }
}
