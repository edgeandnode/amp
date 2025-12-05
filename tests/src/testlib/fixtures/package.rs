//! Dataset package fixture for managing dataset operations in tests.
//!
//! This fixture provides a convenient interface for managing dataset packages
//! in test environments. It wraps dataset metadata and provides methods for
//! installing dependencies, building, and registering datasets using the AmpCli.

use std::path::{Path, PathBuf};

use common::BoxError;

use super::{AmpCli, ContractArtifact};
use crate::testlib::helpers::{forge, git};

/// Dataset package fixture for managing dataset operations.
///
/// This fixture represents a dataset package with its metadata and provides
/// convenient methods for common dataset operations like installing dependencies,
/// building, and registering. It delegates CLI operations to a AmpCli instance.
#[derive(Clone, Debug)]
pub struct DatasetPackage {
    pub name: String,
    // Path to dataset directory
    path: PathBuf,
    // Relative config path
    pub config: Option<String>,
    // Path to contracts directory (if set via builder)
    pub contracts_dir: Option<PathBuf>,
}

impl DatasetPackage {
    /// Create a new DatasetPackage instance.
    ///
    /// Takes a dataset name and optional config file path. The dataset path
    /// is automatically constructed as absolute path using CARGO_MANIFEST_DIR.
    pub fn new(name: &str, config: Option<&str>) -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("datasets")
            .join(name);
        Self {
            name: name.to_string(),
            path,
            config: config.map(|s| s.to_string()),
            contracts_dir: None,
        }
    }

    /// Create a builder for constructing a DatasetPackage with additional options.
    pub fn builder(name: &str) -> DatasetPackageBuilder {
        DatasetPackageBuilder::new(name)
    }

    /// Get the root path to the dataset directory.
    pub fn root(&self) -> &PathBuf {
        &self.path
    }

    /// Get the path to the contracts directory.
    ///
    /// Panics if `contracts_dir` was not set via the builder.
    pub fn contracts_dir(&self) -> &Path {
        self.contracts_dir.as_deref().expect(
            "contracts_dir not set - use DatasetPackage::builder().contracts_dir() to set it",
        )
    }

    /// Build the dataset manifest using amp build command.
    ///
    /// Runs `pnpm amp build` in the dataset directory using the provided CLI.
    #[tracing::instrument(skip_all, err)]
    pub async fn build(&self, cli: &AmpCli) -> Result<(), BoxError> {
        cli.build(&self.path, self.config.as_deref()).await
    }

    /// Register the dataset using amp register command.
    ///
    /// Runs `pnpm amp register` in the dataset directory using the provided CLI.
    /// Optionally accepts a version tag (semantic version or "dev").
    #[tracing::instrument(skip_all, err)]
    pub async fn register(
        &self,
        cli: &AmpCli,
        tag: impl Into<Option<&str>>,
    ) -> Result<(), BoxError> {
        cli.register(&self.path, tag.into(), self.config.as_deref())
            .await
    }

    /// Deploy the dataset using amp deploy command.
    ///
    /// Runs `pnpm amp deploy` in the dataset directory using the provided CLI.
    /// Optionally accepts a reference and end block.
    #[tracing::instrument(skip_all, err)]
    pub async fn deploy(
        &self,
        cli: &AmpCli,
        reference: Option<&str>,
        end_block: Option<u64>,
    ) -> Result<(), BoxError> {
        cli.deploy(&self.path, reference, end_block, self.config.as_deref())
            .await
    }

    /// Build smart contracts and load specified artifacts.
    ///
    /// Initializes git submodules, runs forge build, and loads the specified
    /// contract artifacts. Panics if `contracts_dir` was not set via the builder.
    pub async fn build_contracts<const N: usize>(
        &self,
        contract_names: [&str; N],
    ) -> Result<Vec<ContractArtifact>, BoxError> {
        let contracts_dir = self.contracts_dir.as_ref().expect(
            "contracts_dir not set - use DatasetPackage::builder().contracts_dir() to set it",
        );

        // Initialize git submodules for forge-std dependencies
        git::submodules_init(contracts_dir).await?;

        // Build contracts with forge
        let build_output = forge::build(contracts_dir).await?;

        // Load all requested artifacts
        contract_names
            .iter()
            .map(|name| {
                let artifact_path = build_output.artifact_path(name);
                ContractArtifact::load(&artifact_path)
            })
            .collect()
    }
}

/// Builder for constructing DatasetPackage with additional options.
pub struct DatasetPackageBuilder {
    name: String,
    config: Option<String>,
    contracts_subdir: Option<String>,
}

impl DatasetPackageBuilder {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            config: None,
            contracts_subdir: None,
        }
    }

    /// Set a custom config file path.
    pub fn config(mut self, config: &str) -> Self {
        self.config = Some(config.to_string());
        self
    }

    /// Set the contracts subdirectory (relative to dataset path).
    pub fn contracts_dir(mut self, subdir: &str) -> Self {
        self.contracts_subdir = Some(subdir.to_string());
        self
    }

    /// Build the DatasetPackage.
    pub fn build(self) -> DatasetPackage {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("datasets")
            .join(&self.name);
        let contracts_dir = self.contracts_subdir.map(|subdir| path.join(subdir));
        DatasetPackage {
            name: self.name,
            path,
            config: self.config,
            contracts_dir,
        }
    }
}
