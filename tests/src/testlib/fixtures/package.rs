//! Dataset package fixture for managing dataset operations in tests.
//!
//! This fixture provides a convenient interface for managing dataset packages
//! in test environments. It wraps dataset metadata and provides methods for
//! installing dependencies, building, and registering datasets using the NozzlCli.

use std::{path::PathBuf, str::FromStr};

use common::BoxError;

use super::NozzlCli;

/// Dataset package fixture for managing dataset operations.
///
/// This fixture represents a dataset package with its metadata and provides
/// convenient methods for common dataset operations like installing dependencies,
/// building, and registering. It delegates CLI operations to a NozzlCli instance.
#[derive(Clone, Debug)]
pub struct DatasetPackage {
    pub name: String,
    // Relative to crate root
    pub path: PathBuf,
    // Relative config path
    pub config: Option<String>,
}

impl DatasetPackage {
    /// Create a new DatasetPackage instance.
    ///
    /// Takes a dataset name and optional config file path. The dataset path
    /// is automatically constructed as `datasets/{name}` relative to the crate root.
    pub fn new(name: &str, config: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            path: PathBuf::from_str(&format!("datasets/{}", name)).unwrap(),
            config: config.map(|s| s.to_string()),
        }
    }

    /// Build the dataset manifest using nozzl build command.
    ///
    /// Runs `pnpm nozzl build` in the dataset directory using the provided CLI.
    #[tracing::instrument(skip_all, err)]
    pub async fn build(&self, cli: &NozzlCli) -> Result<(), BoxError> {
        cli.build(&self.path, self.config.as_deref()).await
    }

    /// Register the dataset using nozzl register command.
    ///
    /// Runs `pnpm nozzl register` in the dataset directory using the provided CLI.
    #[tracing::instrument(skip_all, err)]
    pub async fn register(&self, cli: &NozzlCli) -> Result<(), BoxError> {
        cli.register(&self.path, self.config.as_deref()).await
    }
}
