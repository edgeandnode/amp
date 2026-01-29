//! Amp dir management for test environments.
//!
//! This module provides a standalone way to manage amp dir structures
//! including config files, manifests, providers, and data directories.
//! It is completely independent and can be used with any directory path.
//!
//! # Directory Structure
//!
//! The amp dir typically contains:
//! ```text
//! <root>/
//! ├── config.toml       # Generated daemon configuration
//! ├── manifests/        # Dataset manifest files (.json)
//! ├── providers/        # Provider configuration files (.toml)
//! ├── data/             # Dataset storage and dataset snapshot reference data
//! └── metadb/           # PostgreSQL metadata database files
//! ```

use std::path::{Path, PathBuf};

use common::BoxError;
use fs_err as fs;

use crate::testlib::config;

/// Default amp dir name
const AMP_DIR_NAME: &str = ".amp";

/// Default config file name
const DEFAULT_CONFIG_FILENAME: &str = "config.toml";

/// Default manifests directory name
const DEFAULT_MANIFESTS_DIRNAME: &str = "manifests";

/// Default providers directory name
const DEFAULT_PROVIDERS_DIRNAME: &str = "providers";

/// Default data directory name
const DEFAULT_DATA_DIRNAME: &str = "data";

/// Default metadata database directory name
const DEFAULT_METADB_DIRNAME: &str = "metadb";

/// Create a builder for configuring [`DaemonAmpDir`] with custom paths.
///
/// Use this function when you need to customize directory names or config file name.
/// This is a convenience function that creates a new [`DaemonAmpDirBuilder`] instance.
///
/// The builder automatically appends the default amp dir name (`.amp`) to the provided parent path.
pub fn builder(parent: impl AsRef<Path>) -> DaemonAmpDirBuilder {
    DaemonAmpDirBuilder::new(parent)
}

/// Manages an amp dir with all daemon-related files and configurations.
///
/// This struct provides a standalone way to manage amp dir structures
/// independent of any temporary directory management. It can be used with any
/// root directory path to create and manage the amp dir structure.
pub struct DaemonAmpDir {
    root: PathBuf,
    config_file_path: PathBuf,
    manifests_dir_path: PathBuf,
    providers_dir_path: PathBuf,
    data_dir_path: PathBuf,
    metadb_dir_path: PathBuf,
}

impl DaemonAmpDir {
    /// Create a new amp dir manager with the given parent directory.
    ///
    /// Creates a new `DaemonAmpDir` instance that manages amp dir files within
    /// a `.amp/` subdirectory of the provided parent path. This is the standard constructor
    /// for test environments that need amp dir management.
    /// # Directory Structure
    ///
    /// The created amp dir will have this structure:
    /// ```text
    /// parent/
    /// └── .amp/                    # Amp dir root directory
    ///     ├── config.toml          # Main daemon configuration file
    ///     ├── manifests/           # Dataset manifest files (.json) and SQL files
    ///     ├── providers/           # Provider configuration files (.toml)
    ///     ├── data/                # Dataset storage and blessed reference data
    ///     └── metadb/              # PostgreSQL metadata database files
    /// ```
    ///
    /// For custom directory names or paths, use [`builder()`] instead.
    pub fn new(parent: impl AsRef<Path>) -> Self {
        let root = parent.as_ref().join(AMP_DIR_NAME);
        Self {
            config_file_path: root.join(DEFAULT_CONFIG_FILENAME),
            manifests_dir_path: root.join(DEFAULT_MANIFESTS_DIRNAME),
            providers_dir_path: root.join(DEFAULT_PROVIDERS_DIRNAME),
            data_dir_path: root.join(DEFAULT_DATA_DIRNAME),
            metadb_dir_path: root.join(DEFAULT_METADB_DIRNAME),
            root,
        }
    }

    /// Get the root directory path for this amp dir.
    ///
    /// Returns the path where all amp dir files and directories are located.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Get the path to the config file.
    ///
    /// Returns the full path to the config file within the amp dir.
    pub fn config_file(&self) -> &Path {
        &self.config_file_path
    }

    /// Get the manifests directory path.
    ///
    /// Returns the full path to the manifests directory within the amp dir.
    pub fn manifests_dir(&self) -> &Path {
        &self.manifests_dir_path
    }

    /// Get the providers directory path.
    ///
    /// Returns the full path to the providers directory within the amp dir.
    pub fn providers_dir(&self) -> &Path {
        &self.providers_dir_path
    }

    /// Get the data directory path.
    ///
    /// Returns the full path to the data directory within the amp dir.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir_path
    }

    /// Get the metadata database directory path.
    ///
    /// Returns the full path to the metadb directory within the amp dir.
    /// This directory stores the PostgreSQL data files for the test's metadata database.
    pub fn metadb_dir(&self) -> &Path {
        &self.metadb_dir_path
    }

    /// Write config file with the provided content.
    ///
    /// Writes the given content to the config file in the amp dir.
    /// This method automatically creates the root directory if it doesn't exist.
    /// This method is type agnostic - it accepts any string content without handling serialization.
    pub fn write_config_file(&self, content: &str) -> Result<(), BoxError> {
        let config_file_path = &self.config_file_path;

        // Ensure the root directory exists
        if let Some(parent) = config_file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(config_file_path, content)?;

        tracing::debug!("Generated config file: {}", config_file_path.display());
        Ok(())
    }

    /// Create the dataset manifests directory with the specified name.
    ///
    /// Creates the directory for storing dataset manifest files (.json) within the amp dir.
    /// Returns the path to the created directory.
    pub fn create_manifests_dir(&self) -> Result<(), BoxError> {
        let dir_path = &self.manifests_dir_path;

        fs::create_dir_all(dir_path)?;

        tracing::debug!(
            "Created dataset manifests directory: {}",
            dir_path.display()
        );
        Ok(())
    }

    /// Create the providers directory with the specified name.
    ///
    /// Creates the directory for storing provider configuration files (.toml) within the amp dir.
    /// Returns the path to the created directory.
    pub fn create_providers_dir(&self) -> Result<(), BoxError> {
        let dir_path = &self.providers_dir_path;

        fs::create_dir_all(dir_path)?;

        tracing::debug!("Created providers directory: {}", dir_path.display());
        Ok(())
    }

    /// Create the data directory with the specified name.
    ///
    /// Creates the directory for storing dataset data and dataset snapshot reference data within the amp dir.
    /// Returns the path to the created directory.
    pub fn create_data_dir(&self) -> Result<(), BoxError> {
        let dir_path = &self.data_dir_path;

        fs::create_dir_all(dir_path)?;

        tracing::debug!("Created data directory: {}", dir_path.display());
        Ok(())
    }

    /// Copy dataset snapshots to the amp dir's data directory.
    ///
    /// This copies pre-validated reference datasets from the `tests/config/snapshots` directory
    /// to the amp dir's data directory. Only the specifically requested datasets
    /// are copied, maintaining the complete directory structure including revision folders.
    ///
    /// Dataset snapshots are used as baseline reference data for test comparisons.
    pub async fn preload_dataset_snapshots(
        &self,
        datasets_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(), BoxError> {
        let target_dir = &self.data_dir_path;

        // Create directory lazily if it doesn't exist
        if !target_dir.exists() {
            tracing::debug!("Data directory doesn't exist, creating it lazily");
            self.create_data_dir()?;
        }

        for dataset in datasets_data {
            let name = dataset.as_ref();
            let path = PathBuf::from(name);

            // Resolve source directory by searching known fixture locations
            let source_dir_path = config::resolve_snapshot_source_dir(&path).ok_or_else(|| {
                format!("Could not find dataset snapshot '{name}' source directory",)
            })?;
            let target_dir_path = target_dir.join(&path);

            tracing::debug!(
                "Copying dataset snapshots: {} -> {}",
                source_dir_path.display(),
                target_dir_path.display()
            );

            copy_dir_recursive(&source_dir_path, &target_dir_path)?;

            tracing::trace!(
                "Copied dataset snapshots: {} -> {}",
                source_dir_path.display(),
                target_dir_path.display()
            );
        }

        Ok(())
    }
}

/// Builder for configuring [`DaemonAmpDir`] with custom directory paths.
///
/// Allows flexible configuration of amp dir structure including
/// custom paths for config file, manifests, providers, and data directories.
pub struct DaemonAmpDirBuilder {
    root: PathBuf,
    config_file: Option<String>,
    manifests_dir: Option<String>,
    providers_dir: Option<String>,
    data_dir: Option<String>,
    metadb_dir: Option<String>,
}

impl DaemonAmpDirBuilder {
    /// Create a new builder with the specified parent directory.
    ///
    /// The builder automatically appends the default amp dir name (`.amp`) to the provided parent path.
    pub fn new(parent: impl AsRef<Path>) -> Self {
        Self {
            root: parent.as_ref().join(AMP_DIR_NAME),
            config_file: None,
            manifests_dir: None,
            providers_dir: None,
            data_dir: None,
            metadb_dir: None,
        }
    }

    /// Set the config file name (defaults to "config.toml").
    pub fn config_file(mut self, name: impl Into<String>) -> Self {
        self.config_file = Some(name.into());
        self
    }

    /// Set the manifests directory sub-path (defaults to "manifests").
    pub fn manifests_dir(mut self, path: impl Into<String>) -> Self {
        self.manifests_dir = Some(path.into());
        self
    }

    /// Set the providers directory sub-path (defaults to "providers").
    pub fn providers_dir(mut self, path: impl Into<String>) -> Self {
        self.providers_dir = Some(path.into());
        self
    }

    /// Set the data directory sub-path (defaults to "data").
    pub fn data_dir(mut self, path: impl Into<String>) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Set the metadb directory sub-path (defaults to "metadb").
    pub fn metadb_dir(mut self, path: impl Into<String>) -> Self {
        self.metadb_dir = Some(path.into());
        self
    }

    /// Build the [`DaemonAmpDir`] with the configured settings.
    pub fn build(self) -> DaemonAmpDir {
        let config_file = self
            .config_file
            .unwrap_or_else(|| DEFAULT_CONFIG_FILENAME.to_string());
        let manifests_dir = self
            .manifests_dir
            .unwrap_or_else(|| DEFAULT_MANIFESTS_DIRNAME.to_string());
        let providers_dir = self
            .providers_dir
            .unwrap_or_else(|| DEFAULT_PROVIDERS_DIRNAME.to_string());
        let data_dir = self
            .data_dir
            .unwrap_or_else(|| DEFAULT_DATA_DIRNAME.to_string());
        let metadb_dir = self
            .metadb_dir
            .unwrap_or_else(|| DEFAULT_METADB_DIRNAME.to_string());

        DaemonAmpDir {
            config_file_path: self.root.join(&config_file),
            manifests_dir_path: self.root.join(&manifests_dir),
            providers_dir_path: self.root.join(&providers_dir),
            data_dir_path: self.root.join(&data_dir),
            metadb_dir_path: self.root.join(&metadb_dir),
            root: self.root,
        }
    }
}

/// Recursively copy a directory and all its contents.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), BoxError> {
    fs::create_dir_all(dst)?;

    let entries = fs::read_dir(src)?;
    for entry in entries {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }

    Ok(())
}
