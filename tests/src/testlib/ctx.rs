//! Isolated test environment creation for End-to-end tests.
//!
//! This module provides utilities for creating completely isolated test environments using
//! temporary directories. Each test environment gets its own configuration files, dataset
//! definitions, and provider configurations, eliminating any coupling between tests.
//!
//! # Required Files Structure
//!
//! Each isolated test environment creates the following directory structure:
//!
//! ```text
//! test__<test-name>__<rand-id>/
//! └── .amp/                         # Daemon state directory
//!     ├── config.toml               # Main config (dynamically generated)
//!     ├── manifests/                # Dataset manifests (only copied if explicitly requested)
//!     │   ├── eth_rpc.json
//!     │   └── anvil_rpc.json
//!     ├── providers/                # Provider configs (only copied if explicitly requested)
//!     │   ├── rpc_eth_mainnet.toml
//!     │   ├── firehose_eth_mainnet.toml
//!     │   └── rpc_anvil.toml        # Note: Static provider configs are rarely used; Anvil uses dynamic IPC configs
//!     └── data/                     # Output directory (dataset snapshots copied if requested)
//!         └── eth_mainnet/          # Dataset snapshot reference data with complete structure
//!             └── revisions/
//! ```
//!
//! # Key Features
//!
//! - **Complete isolation**: Each test gets its own temporary directory
//! - **Selective resource loading**: Only copy explicitly requested datasets and providers
//! - **Dynamic configuration**: Generate test-specific config.toml files
//! - **Builder pattern**: Fluent API for environment customization
//! - **Automatic cleanup**: Temporary directories cleaned up on drop (unless `TESTS_KEEP_TEMP_DIRS=1` is set)

use std::{collections::BTreeSet, path::Path, str::FromStr as _, sync::Arc};

use common::{BoxError, config::Config};
use metadata_db::WorkerNodeId;

use super::fixtures::{
    Anvil, DaemonConfig, DaemonConfigBuilder, DaemonServer, DaemonStateDir, DaemonWorker,
    FlightClient, JsonlClient, NozzlCli, TempMetadataDb as MetadataDbFixture,
    builder as daemon_state_dir_builder,
};
use crate::testlib::env_dir::TestEnvDir;

enum AnvilMode {
    Ipc,
    Http,
}

pub struct TestCtxBuilder {
    test_name: String,
    daemon_config: DaemonConfig,
    anvil_fixture: Option<AnvilMode>,
    dataset_manifests_to_preload: BTreeSet<String>,
    provider_configs_to_preload: BTreeSet<String>,
    dataset_snapshots_to_preload: BTreeSet<String>,
}

impl TestCtxBuilder {
    /// Create a new test environment builder.
    pub fn new(test_name: impl Into<String>) -> Self {
        Self {
            test_name: test_name.into(),
            daemon_config: Default::default(),
            anvil_fixture: None,
            dataset_manifests_to_preload: Default::default(),
            provider_configs_to_preload: Default::default(),
            dataset_snapshots_to_preload: Default::default(),
        }
    }

    /// Add configuration overrides.
    ///
    /// Replaces the default configuration with the provided overrides. These values will be
    /// written to the generated config.toml file in the test environment.
    pub fn with_config(mut self, config: DaemonConfig) -> Self {
        self.daemon_config = config;
        self
    }

    /// Add a single dataset manifest that this test environment needs.
    ///
    /// Only the specified dataset manifest will be copied from the source directory to the test environment.
    /// This is a convenience method for adding one dataset at a time.
    ///
    /// # Panics
    ///
    /// Panics if the manifest name is an absolute path or contains '..' segments.
    pub fn with_dataset_manifest(mut self, manifest: impl Into<String>) -> Self {
        let manifest = manifest.into();

        if manifest.starts_with("/") {
            panic!("The manifest name cannot be an absolute path: {}", manifest);
        }

        if manifest.contains("..") {
            panic!(
                "The manifest name cannot contain '..' path segments: {}",
                manifest
            );
        }

        self.dataset_manifests_to_preload.insert(manifest);
        self
    }

    /// Add dataset manifests that this test environment needs.
    ///
    /// Only the specified dataset manifests will be copied from the source directory to the test environment.
    /// If no dataset manifests are specified, the manifests directory will remain empty.
    ///
    /// # Panics
    ///
    /// Panics if any manifest name is an absolute path or contains '..' segments.
    pub fn with_dataset_manifests(
        mut self,
        manifests: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let manifests = manifests.into_iter().map(Into::into).collect::<Vec<_>>();

        for manifest in &manifests {
            if manifest.starts_with("/") {
                panic!("The manifest name cannot be an absolute path: {}", manifest);
            }

            if manifest.contains("..") {
                panic!(
                    "The manifest name cannot contain '..' path segments: {}",
                    manifest
                );
            }
        }

        self.dataset_manifests_to_preload
            .extend(manifests.into_iter());
        self
    }

    /// Add a single provider config that this test environment needs.
    ///
    /// Only the specified provider config will be copied from the source directory to the test environment.
    /// This is a convenience method for adding one provider at a time.
    ///
    /// # Panics
    ///
    /// Panics if the provider name is an absolute path or contains '..' segments.
    pub fn with_provider_config(mut self, provider: impl Into<String>) -> Self {
        let provider = provider.into();

        if provider.starts_with("/") {
            panic!("The provider name cannot be an absolute path: {}", provider);
        }

        if provider.contains("..") {
            panic!(
                "The provider name cannot contain '..' path segments: {}",
                provider
            );
        }

        self.provider_configs_to_preload.insert(provider);
        self
    }

    /// Add provider configs that this test environment needs.
    ///
    /// Only the specified provider configs will be copied from the source directory to the test environment.
    /// If no provider configs are specified, the providers directory will remain empty.
    ///
    /// # Panics
    ///
    /// Panics if any provider name is an absolute path or contains '..' segments.
    pub fn with_provider_configs(
        mut self,
        providers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let providers = providers.into_iter().map(Into::into).collect::<Vec<_>>();

        for provider in &providers {
            if provider.starts_with("/") {
                panic!("The provider name cannot be an absolute path: {}", provider);
            }

            if provider.contains("..") {
                panic!(
                    "The provider name cannot contain '..' path segments: {}",
                    provider
                );
            }
        }

        self.provider_configs_to_preload
            .extend(providers.into_iter().map(Into::into));
        self
    }

    /// Add a single dataset snapshot that this test environment needs.
    ///
    /// Dataset snapshots are pre-validated reference datasets used for test comparisons.
    /// Only the specified dataset snapshot will be copied from the tests/data/snapshots directory
    /// to the test environment's data directory, maintaining the complete directory structure
    /// including revision folders. This is a convenience method for adding one dataset at a time.
    ///
    /// # Panics
    ///
    /// Panics if the dataset name is an absolute path or contains '..' segments.
    pub fn with_dataset_snapshot(mut self, dataset: impl Into<String>) -> Self {
        let dataset = dataset.into();

        if dataset.starts_with("/") {
            panic!("The dataset name cannot be an absolute path: {}", dataset);
        }

        if dataset.contains("..") {
            panic!(
                "The dataset name cannot contain '..' path segments: {}",
                dataset
            );
        }

        self.dataset_snapshots_to_preload.insert(dataset);
        self
    }

    /// Add dataset snapshots that this test environment needs.
    ///
    /// Dataset snapshots are pre-validated reference datasets used for test comparisons.
    /// Only the specified dataset snapshots will be copied from the tests/data/snapshots directory
    /// to the test environment's data directory, maintaining the complete directory structure
    /// including revision folders.
    ///
    /// # Panics
    ///
    /// Panics if any dataset name is an absolute path or contains '..' segments.
    pub fn with_dataset_snapshots(
        mut self,
        datasets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let datasets = datasets.into_iter().map(Into::into).collect::<Vec<_>>();

        for dataset in &datasets {
            if dataset.starts_with("/") {
                panic!("The dataset name cannot be an absolute path: {}", dataset);
            }

            if dataset.contains("..") {
                panic!(
                    "The dataset name cannot contain '..' path segments: {}",
                    dataset
                );
            }
        }

        self.dataset_snapshots_to_preload
            .extend(datasets.into_iter());
        self
    }

    /// Enable Anvil fixture for blockchain testing with IPC connection.
    ///
    /// An Anvil instance will be created with IPC connection and configured automatically.
    /// The provider will be named "anvil_rpc" and available for dataset connections.
    pub fn with_anvil_ipc(mut self) -> Self {
        self.anvil_fixture = Some(AnvilMode::Ipc);
        self
    }

    /// Enable Anvil fixture for blockchain testing with HTTP connection.
    ///
    /// An Anvil instance will be created with HTTP connection on an automatically allocated port.
    /// This is useful for avoiding port conflicts and allows forge scripts to work (forge doesn't support IPC).
    /// The provider will be named "anvil_rpc" and available for dataset connections.
    pub fn with_anvil_http(mut self) -> Self {
        self.anvil_fixture = Some(AnvilMode::Http);
        self
    }

    /// Build the isolated test environment.
    ///
    /// Creates a temporary directory structure, generates the configuration file,
    /// copies requested datasets and providers, and returns a ready-to-use test environment.
    pub async fn build(self) -> Result<TestCtx, BoxError> {
        // Load environment variables from .env file (if present)
        let _ = dotenvy::dotenv_override();

        // Create temporary metadata database fixture
        let temp_db = MetadataDbFixture::new().await;

        // Update daemon config with the temp database URL
        let daemon_config = DaemonConfigBuilder::from_config(&self.daemon_config)
            .metadata_db_url(temp_db.connection_url().to_string())
            .build();

        // Serialize config content before moving daemon_config
        let config_content = daemon_config.serialize_to_toml();

        // Create temporary test directory
        let test_dir = TestEnvDir::new(&self.test_name)?;
        tracing::info!(
            "Creating isolated test environment at: {}",
            test_dir.path().display()
        );

        let daemon_state_dir = test_dir.path();

        // Build DaemonStateDir with extracted configuration
        let daemon_state_dir = daemon_state_dir_builder(daemon_state_dir)
            .manifests_dir(daemon_config.dataset_manifests_path())
            .providers_dir(daemon_config.provider_configs_path())
            .data_dir(daemon_config.data_path())
            .build();

        // Write config file to the daemon state dir (i.e., `.amp/` dir)
        // Create required daemon subdirectories
        daemon_state_dir.write_config_file(&config_content)?;
        daemon_state_dir.create_providers_dir()?;
        daemon_state_dir.create_manifests_dir()?;
        daemon_state_dir.create_data_dir()?;

        // Preload test resources in the daemon state dir as requested
        if !self.dataset_manifests_to_preload.is_empty() {
            tracing::info!(
                "Preloading dataset manifests: {:?}",
                self.dataset_manifests_to_preload
            );
            daemon_state_dir
                .preload_dataset_manifests(&self.dataset_manifests_to_preload)
                .await?;
        }
        if !self.provider_configs_to_preload.is_empty() {
            tracing::info!(
                "Preloading provider configs: {:?}",
                self.provider_configs_to_preload
            );
            daemon_state_dir
                .preload_provider_configs(&self.provider_configs_to_preload)
                .await?;
        }
        if !self.dataset_snapshots_to_preload.is_empty() {
            tracing::info!(
                "Preloading dataset snapshots: {:?}",
                self.dataset_snapshots_to_preload
            );
            daemon_state_dir
                .preload_dataset_snapshots(&self.dataset_snapshots_to_preload)
                .await?;
        }

        // Load the config from our temporary directory
        let config =
            Arc::new(Config::load(daemon_state_dir.config_file(), false, None, true).await?);

        // Create Anvil fixture (if enabled)
        let anvil = match self.anvil_fixture {
            Some(AnvilMode::Ipc) => {
                let fixture = Anvil::new_ipc().await?;

                fixture
                    .wait_for_ready(std::time::Duration::from_secs(30))
                    .await?;

                // Preload Anvil provider config into the daemon state dir
                let anvil_provider = fixture.new_provider_config();
                daemon_state_dir.create_provider_config("anvil_rpc", &anvil_provider)?;
                Some(fixture)
            }
            Some(AnvilMode::Http) => {
                let fixture = Anvil::new_http(0).await?;

                fixture
                    .wait_for_ready(std::time::Duration::from_secs(30))
                    .await?;

                // Preload Anvil provider config into the daemon state dir
                let anvil_provider = fixture.new_provider_config();
                daemon_state_dir.create_provider_config("anvil_rpc", &anvil_provider)?;
                Some(fixture)
            }
            None => None,
        };

        // Start nozzle server using the fixture
        let server = DaemonServer::new(
            config.clone(),
            temp_db.metadata_db().clone(),
            true, // enable_flight
            true, // enable_jsonl
            true, // enable_admin_api
        )
        .await?;

        // Start worker using the fixture
        let worker = DaemonWorker::new(
            WorkerNodeId::from_str(&self.test_name)
                .expect("test name should be a valid WorkerNodeId"),
            config,
            temp_db.metadata_db().clone(),
            None,
            None,
        )
        .await?;

        // Wait for Anvil service to be ready (if enabled)
        Ok(TestCtx {
            test_name: self.test_name,
            test_dir,
            daemon_state_dir,
            tempdb_fixture: temp_db,
            daemon_server_fixture: server,
            daemon_worker_fixture: worker,
            anvil_fixture: anvil,
        })
    }
}

/// An isolated test environment with its own temporary directory and configuration.
///
/// This struct represents a fully configured test environment that is ready for use.
/// It contains the loaded configuration, temporary directory, and convenience methods
/// for interacting with the environment.
pub struct TestCtx {
    test_name: String,
    test_dir: TestEnvDir,

    daemon_state_dir: DaemonStateDir,

    tempdb_fixture: MetadataDbFixture,
    daemon_server_fixture: DaemonServer,
    daemon_worker_fixture: DaemonWorker,
    anvil_fixture: Option<Anvil>,
}

impl TestCtx {
    /// Get the test name.
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Get the path to the config.toml file.
    pub fn config_file(&self) -> &Path {
        self.daemon_state_dir.config_file()
    }

    /// Get a reference to the temporary config directory.
    pub fn config_dir(&self) -> &TestEnvDir {
        &self.test_dir
    }

    /// Get a reference to the daemon state directory.
    pub fn daemon_state_dir(&self) -> &DaemonStateDir {
        &self.daemon_state_dir
    }

    /// Get a reference to the metadata database.
    pub fn metadata_db(&self) -> &metadata_db::MetadataDb {
        self.tempdb_fixture.metadata_db()
    }

    /// Get a reference to the [`DaemonServer`] fixture.
    pub fn daemon_server(&self) -> &DaemonServer {
        &self.daemon_server_fixture
    }

    /// Get a reference to the daemon worker fixture.
    pub fn daemon_worker(&self) -> &DaemonWorker {
        &self.daemon_worker_fixture
    }

    /// Get a reference to the [`Anvil`] fixture.
    ///
    /// # Panics
    ///
    /// Panics if Anvil was not enabled during environment creation with `with_anvil_ipc()`.
    pub fn anvil(&self) -> &Anvil {
        self.anvil_fixture.as_ref().unwrap_or_else(|| {
            panic!("Anvil fixture not enabled - call with_anvil_ipc() on TestCtxBuilder")
        })
    }

    /// Create a new Flight client connected to this test environment's server.
    ///
    /// This convenience method creates a new FlightClient instance connected to the
    /// daemon server's Flight endpoint.
    ///
    /// Returns a new FlightClient instance ready to execute SQL queries.
    pub async fn new_flight_client(&self) -> Result<FlightClient, BoxError> {
        FlightClient::new(self.daemon_server_fixture.flight_server_url()).await
    }

    /// Create a new JSONL client connected to this test environment's server.
    ///
    /// This convenience method creates a new JsonlClient instance connected to the
    /// daemon server's JSON Lines endpoint.
    ///
    /// Returns a new JsonlClient instance ready to execute SQL queries.
    pub fn new_jsonl_client(&self) -> super::fixtures::JsonlClient {
        JsonlClient::new(self.daemon_server_fixture.jsonl_server_url())
    }

    /// Create a new `nozzl` CLI fixture connected to this test environment's server.
    ///
    /// This convenience method creates a new [`NozzlCli`] instance connected to the
    /// daemon server's admin API endpoint for executing `nozzl` CLI commands in tests.
    ///
    /// Returns a new [`NozzlCli`] instance ready to execute CLI commands.
    pub fn new_nozzl_cli(&self) -> NozzlCli {
        NozzlCli::new(self.daemon_server_fixture.admin_api_server_url())
    }
}
