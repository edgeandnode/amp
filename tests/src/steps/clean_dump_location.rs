//! Test step for cleaning dump location directories.

use std::path::PathBuf;

use anyhow::Result;
use fs_err as fs;

use crate::testlib::ctx::TestCtx;

/// Test step that cleans up dump location directories.
///
/// This step removes directories created during dump operations to ensure
/// clean test environments between test runs.
#[derive(Debug, serde::Deserialize)]
pub struct Step {
    /// The location path to clean up relative to the data store root.
    pub clean_dump_location: String,
}

impl Step {
    /// Removes the specified dump location directory if it exists.
    ///
    /// The method constructs the full path by combining the data store URL path
    /// with the specified clean_dump_location. If the directory exists, it removes
    /// it recursively. If it doesn't exist, the operation is silently skipped.
    pub async fn run(&self, ctx: &TestCtx) -> Result<()> {
        tracing::debug!("Cleaning dump location '{}'", self.clean_dump_location);

        let mut path = PathBuf::from(ctx.daemon_worker().data_store().url().path());
        path.push(&self.clean_dump_location);

        if path.exists() {
            tracing::trace!("Removing directory: {}", path.display());
            fs::remove_dir_all(path)?;
        } else {
            tracing::trace!(
                "Directory does not exist, skipping cleanup: {}",
                path.display()
            );
        }

        Ok(())
    }
}
