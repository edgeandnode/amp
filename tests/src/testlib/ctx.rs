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
//! └── .amp/                         # Amp dir
//!     ├── config.toml               # Main config (dynamically generated)
//!     ├── manifests/                # Dataset manifests directory (initially empty, registered via Admin API)
//!     ├── providers/                # Provider configs directory (initially empty, registered via Admin API)
//!     ├── data/                     # Output directory (dataset snapshots copied if requested)
//!     │   └── eth_mainnet/          # Dataset snapshot reference data with complete structure
//!     └── metadb/                   # PostgreSQL metadata database files (isolated per test)
//! ```
//!
//! # Key Features
//!
//! - **Complete isolation**: Each test gets its own temporary directory
//! - **Admin API registration**: Manifests and providers registered via Admin API after controller starts
//! - **Dynamic configuration**: Generate test-specific config.toml files (including dynamic Anvil providers)
//! - **Selective resource loading**: Only copy explicitly requested dataset snapshots
//! - **Builder pattern**: Fluent API for environment customization
//! - **Automatic cleanup**: Temporary directories cleaned up on drop (unless `TESTS_KEEP_TEMP_DIRS=1` is set)

use std::{collections::BTreeSet, path::Path, sync::Arc};

use amp_config::Config;
use amp_data_store::DataStore;
use amp_dataset_store::DatasetStore;
use common::BoxError;
use datasets_common::reference::Reference;
use worker::node_id::NodeId;

use super::fixtures::{
    AmpCli, Ampctl, Anvil, DaemonAmpDir, DaemonConfig, DaemonConfigBuilder, DaemonController,
    DaemonServer, DaemonWorker, FlightClient, JsonlClient, MetadataDb as MetadataDbFixture,
    builder as daemon_amp_dir_builder,
};
use crate::testlib::{
    config::{read_manifest_fixture, read_provider_fixture},
    env_dir::TestEnvDir,
};

enum AnvilMode {
    Ipc,
    Http,
}

pub struct TestCtxBuilder {
    test_name: String,
    daemon_config: DaemonConfig,
    anvil_fixture: Option<AnvilMode>,
    manifests_to_register: Vec<ManifestRegistration>,
    providers_to_register: Vec<ProviderRegistration>,
    dataset_snapshots_to_preload: BTreeSet<String>,
    meter: Option<monitoring::telemetry::metrics::Meter>,
}

impl TestCtxBuilder {
    /// Create a new test environment builder.
    pub fn new(test_name: impl Into<String>) -> Self {
        Self {
            test_name: test_name.into(),
            daemon_config: Default::default(),
            anvil_fixture: None,
            manifests_to_register: Default::default(),
            providers_to_register: Default::default(),
            dataset_snapshots_to_preload: Default::default(),
            meter: None,
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

    /// Enable metrics collection for this test environment.
    ///
    /// Provides a meter that will be passed to the daemon server and worker for metrics collection.
    /// Use with `monitoring::test_utils::TestMetricsContext` to collect and validate metrics.
    pub fn with_meter(mut self, meter: monitoring::telemetry::metrics::Meter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Add a single dataset manifest that this test environment needs.
    ///
    /// The manifest will be registered with the Admin API after the controller starts.
    /// This is a convenience method for adding one manifest at a time.
    ///
    /// Accepts either a simple string for auto-inferred reference, or a tuple
    /// for explicit reference specification:
    /// - `"eth_rpc"` → registers as `_/eth_rpc@0.0.0`
    /// - `("eth_rpc", "_/eth_rpc@1.0.0")` → registers with explicit version
    ///
    /// # Panics
    ///
    /// Panics if the manifest name is an absolute path or contains '..' segments.
    pub fn with_dataset_manifest(mut self, manifest: impl Into<ManifestRegistration>) -> Self {
        let registration = manifest.into();

        // Validate file name
        if registration.manifest_file.starts_with("/") {
            panic!(
                "The manifest name cannot be an absolute path: {}",
                registration.manifest_file
            );
        }

        if registration.manifest_file.contains("..") {
            panic!(
                "The manifest name cannot contain '..' path segments: {}",
                registration.manifest_file
            );
        }

        self.manifests_to_register.push(registration);
        self
    }

    /// Add dataset manifests that this test environment needs.
    ///
    /// The manifests will be registered with the Admin API after the controller starts.
    ///
    /// Each item can be either a simple string for auto-inferred reference, or a tuple
    /// for explicit reference specification:
    /// - `"eth_rpc"` → registers as `_/eth_rpc@0.0.0`
    /// - `("eth_rpc", "_/eth_rpc@1.0.0")` → registers with explicit version
    ///
    /// # Panics
    ///
    /// Panics if any manifest name is an absolute path or contains '..' segments.
    pub fn with_dataset_manifests(
        mut self,
        manifests: impl IntoIterator<Item = impl Into<ManifestRegistration>>,
    ) -> Self {
        for manifest in manifests {
            self = self.with_dataset_manifest(manifest);
        }
        self
    }

    /// Add a single provider config that this test environment needs.
    ///
    /// The provider will be registered with the Admin API after the controller starts.
    /// This is a convenience method for adding one provider at a time.
    ///
    /// Accepts either a simple string for auto-inferred name, or a tuple
    /// for explicit name specification:
    /// - `"rpc_eth_mainnet"` → registers with name `rpc_eth_mainnet`
    /// - `("rpc_eth_mainnet", "custom_name")` → registers with name `custom_name`
    ///
    /// # Panics
    ///
    /// Panics if the provider name is an absolute path or contains '..' segments.
    pub fn with_provider_config(mut self, provider: impl Into<ProviderRegistration>) -> Self {
        let registration = provider.into();

        // Validate file name
        if registration.provider_file.starts_with("/") {
            panic!(
                "The provider name cannot be an absolute path: {}",
                registration.provider_file
            );
        }

        if registration.provider_file.contains("..") {
            panic!(
                "The provider name cannot contain '..' path segments: {}",
                registration.provider_file
            );
        }

        self.providers_to_register.push(registration);
        self
    }

    /// Add provider configs that this test environment needs.
    ///
    /// The providers will be registered with the Admin API after the controller starts.
    ///
    /// Each item can be either a simple string for auto-inferred name, or a tuple
    /// for explicit name specification:
    /// - `"rpc_eth_mainnet"` → registers with name `rpc_eth_mainnet`
    /// - `("rpc_eth_mainnet", "custom_name")` → registers with name `custom_name`
    ///
    /// # Panics
    ///
    /// Panics if any provider name is an absolute path or contains '..' segments.
    pub fn with_provider_configs(
        mut self,
        providers: impl IntoIterator<Item = impl Into<ProviderRegistration>>,
    ) -> Self {
        for provider in providers {
            self = self.with_provider_config(provider);
        }
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

        self.dataset_snapshots_to_preload.extend(datasets);
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

        // Create temporary test directory first (needed for metadb path)
        let test_dir = TestEnvDir::new(&self.test_name)?;
        tracing::info!(
            "Creating isolated test environment at: {}",
            test_dir.path().display()
        );

        // Build DaemonAmpDir with default paths (we'll use this for metadb path)
        let daemon_amp_dir_path = test_dir.path();
        let daemon_amp_dir_temp = daemon_amp_dir_builder(daemon_amp_dir_path).build();

        // Create temporary metadata database fixture using the test's isolated metadb directory
        let metadata_db =
            MetadataDbFixture::with_data_dir(daemon_amp_dir_temp.metadb_dir().to_path_buf()).await;

        // Update daemon config with the temp database URL
        let daemon_config = DaemonConfigBuilder::from_config(&self.daemon_config)
            .metadata_db_url(metadata_db.connection_url().to_string())
            .build();

        // Serialize config content before moving daemon_config
        let config_content = daemon_config.serialize_to_toml();

        // Build final DaemonAmpDir with extracted configuration from daemon_config
        let daemon_amp_dir = daemon_amp_dir_builder(daemon_amp_dir_path)
            .manifests_dir(daemon_config.dataset_manifests_path())
            .providers_dir(daemon_config.provider_configs_path())
            .data_dir(daemon_config.data_path())
            .build();

        // Write config file to the amp dir (i.e., `.amp/` dir)
        // Create required daemon subdirectories
        daemon_amp_dir.write_config_file(&config_content)?;
        daemon_amp_dir.create_providers_dir()?;
        daemon_amp_dir.create_manifests_dir()?;
        daemon_amp_dir.create_data_dir()?;

        // Preload test resources in the amp dir as requested
        if !self.dataset_snapshots_to_preload.is_empty() {
            tracing::info!(
                "Preloading dataset snapshots: {:?}",
                self.dataset_snapshots_to_preload
            );
            daemon_amp_dir
                .preload_dataset_snapshots(&self.dataset_snapshots_to_preload)
                .await?;
        }

        // Load the config from our temporary directory.
        // The config file contains the metadata DB URL from the test fixture,
        // so no managed_db_url fallback is needed.
        let config = Arc::new(Config::load(
            daemon_amp_dir.config_file(),
            false,
            None,
            None,
            None,
        )?);

        let data_store = DataStore::new(
            metadata_db.conn_pool().clone(),
            config.data_store_url.clone(),
            config.parquet.cache_size_mb,
        )?;

        // Create shared DatasetStore instance (used by both server and worker)
        let (datasets_registry, providers_registry) = {
            use amp_datasets_registry::{DatasetsRegistry, manifests::DatasetManifestsStore};
            use amp_providers_registry::{ProviderConfigsStore, ProvidersRegistry};

            let provider_configs_store =
                ProviderConfigsStore::new(amp_object_store::new_with_prefix(
                    &config.providers_store_url,
                    config.providers_store_url.path(),
                )?);
            let providers_registry = ProvidersRegistry::new(provider_configs_store);

            let dataset_manifests_store =
                DatasetManifestsStore::new(amp_object_store::new_with_prefix(
                    &config.manifests_store_url,
                    config.manifests_store_url.path(),
                )?);
            let datasets_registry =
                DatasetsRegistry::new(metadata_db.conn_pool().clone(), dataset_manifests_store);
            (datasets_registry, providers_registry)
        };
        let dataset_store =
            DatasetStore::new(datasets_registry.clone(), providers_registry.clone());

        // Create Anvil fixture (if enabled) and capture provider config for later registration
        let (anvil, anvil_provider_config) = match self.anvil_fixture {
            Some(AnvilMode::Ipc) => {
                let fixture = Anvil::new_ipc().await?;

                fixture
                    .wait_for_ready(std::time::Duration::from_secs(30))
                    .await?;

                // Capture Anvil provider config for registration via Admin API
                let anvil_provider = fixture.new_provider_config();
                (Some(fixture), Some(anvil_provider))
            }
            Some(AnvilMode::Http) => {
                let fixture = Anvil::new_http(0).await?;

                fixture
                    .wait_for_ready(std::time::Duration::from_secs(30))
                    .await?;

                // Capture Anvil provider config for registration via Admin API
                let anvil_provider = fixture.new_provider_config();
                (Some(fixture), Some(anvil_provider))
            }
            None => (None, None),
        };

        // Clone meter for worker and controller before server consumes it
        let worker_meter = self.meter.clone();
        let controller_meter = self.meter.clone();

        // Start query server (pass shared dataset_store)
        let server = DaemonServer::new(
            config.clone(),
            metadata_db.conn_pool().clone(),
            data_store.clone(),
            dataset_store.clone(),
            self.meter,
            true, // enable_flight
            true, // enable_jsonl
        )
        .await?;

        // Start controller (Admin API) with shared dataset_store
        let controller = DaemonController::new(
            config.clone(),
            metadata_db.conn_pool().clone(),
            datasets_registry,
            providers_registry,
            data_store.clone(),
            dataset_store.clone(),
            controller_meter,
        )
        .await?;

        // Register manifests with the Admin API (after controller is running)
        if !self.manifests_to_register.is_empty() {
            tracing::info!(
                "Registering {} dataset manifests with Admin API",
                self.manifests_to_register.len()
            );

            let ampctl = Ampctl::new(controller.admin_api_url());

            for registration in &self.manifests_to_register {
                tracing::debug!(
                    file_name = %registration.manifest_file,
                    reference = %registration.dataset_ref,
                    "Registering manifest"
                );

                // Read manifest content from fixtures
                let manifest_content = read_manifest_fixture(&registration.manifest_file).await?;

                // Register with Admin API
                ampctl
                    .register_manifest(&registration.dataset_ref, &manifest_content)
                    .await
                    .map_err(|err| {
                        format!(
                            "Failed to register manifest '{}' as '{}': {}",
                            registration.manifest_file, registration.dataset_ref, err
                        )
                    })?;

                tracing::info!(
                    file_name = %registration.manifest_file,
                    reference = %registration.dataset_ref,
                    "Successfully registered manifest"
                );
            }
        }

        // Register providers with Admin API (if any)
        if !self.providers_to_register.is_empty() || anvil_provider_config.is_some() {
            let ampctl = Ampctl::new(controller.admin_api_url());

            // Register static providers from fixtures
            if !self.providers_to_register.is_empty() {
                tracing::info!(
                    "Registering {} provider configurations with Admin API",
                    self.providers_to_register.len()
                );

                for registration in &self.providers_to_register {
                    tracing::debug!(
                        file_name = %registration.provider_file,
                        name = %registration.provider_name,
                        "Registering provider"
                    );

                    // Read provider content from fixtures
                    let provider_toml = read_provider_fixture(&registration.provider_file).await?;

                    // Register with Admin API
                    ampctl
                        .register_provider(&registration.provider_name, &provider_toml)
                        .await
                        .map_err(|err| {
                            format!(
                                "Failed to register provider '{}' as '{}': {}",
                                registration.provider_file, registration.provider_name, err
                            )
                        })?;

                    tracing::info!(
                        file_name = %registration.provider_file,
                        name = %registration.provider_name,
                        "Successfully registered provider"
                    );
                }
            }

            // Register dynamic Anvil provider (if present)
            if let Some(anvil_config) = anvil_provider_config {
                tracing::info!("Registering dynamic Anvil provider with Admin API");

                ampctl
                    .register_provider("anvil_rpc", &anvil_config)
                    .await
                    .map_err(|err| format!("Failed to register dynamic Anvil provider: {}", err))?;

                tracing::info!("Successfully registered dynamic Anvil provider");
            }
        }

        // Start worker using the fixture with shared dataset_store
        let node_id: NodeId = self
            .test_name
            .parse()
            .expect("test name should be a valid WorkerNodeId");
        let worker = DaemonWorker::new(
            config,
            metadata_db.conn_pool().clone(),
            data_store,
            dataset_store,
            worker_meter,
            node_id,
        )
        .await?;

        // Wait for Anvil service to be ready (if enabled)
        Ok(TestCtx {
            test_name: self.test_name,
            test_dir,
            daemon_amp_dir,
            _metadata_db_fixture: metadata_db,
            daemon_server_fixture: server,
            daemon_controller_fixture: controller,
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

    daemon_amp_dir: DaemonAmpDir,

    _metadata_db_fixture: MetadataDbFixture,
    daemon_server_fixture: DaemonServer,
    daemon_controller_fixture: DaemonController,
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
        self.daemon_amp_dir.config_file()
    }

    /// Get a reference to the temporary config directory.
    pub fn config_dir(&self) -> &TestEnvDir {
        &self.test_dir
    }

    /// Get a reference to the daemon amp dir.
    pub fn daemon_amp_dir(&self) -> &DaemonAmpDir {
        &self.daemon_amp_dir
    }

    /// Get a reference to the [`DaemonServer`] fixture.
    pub fn daemon_server(&self) -> &DaemonServer {
        &self.daemon_server_fixture
    }

    /// Get a reference to the [`DaemonController`] fixture.
    pub fn daemon_controller(&self) -> &DaemonController {
        &self.daemon_controller_fixture
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

    /// Create a new `amp` CLI fixture connected to this test environment's controller.
    ///
    /// This convenience method creates a new [`AmpCli`] instance connected to the
    /// daemon controller's admin API endpoint for executing `amp` CLI commands in tests.
    ///
    /// Returns a new [`AmpCli`] instance ready to execute CLI commands.
    pub fn new_amp_cli(&self) -> AmpCli {
        AmpCli::new(self.daemon_controller_fixture.admin_api_url())
    }

    /// Create a new `ampctl` fixture connected to this test environment's controller.
    ///
    /// This convenience method creates a new [`Ampctl`] instance connected to the
    /// daemon controller's admin API endpoint for registering dataset manifests in tests.
    ///
    /// Returns a new [`Ampctl`] instance ready to register manifests.
    pub fn new_ampctl(&self) -> Ampctl {
        Ampctl::new(self.daemon_controller_fixture.admin_api_url())
    }
}

/// Helper type for specifying dataset manifest registrations.
///
/// This type encapsulates the information needed to register a dataset manifest:
/// - The filename (without extension) of the manifest in the fixtures directory
/// - The dataset reference (namespace/name@version) to use for registration
///
/// It provides convenient `From` implementations to support both explicit
/// and inferred reference strings.
#[derive(Debug, Clone)]
pub struct ManifestRegistration {
    manifest_file: String,
    dataset_ref: Reference,
}

impl From<&str> for ManifestRegistration {
    /// Create a manifest registration with auto-inferred reference.
    ///
    /// The reference is inferred as `_/{file_name}@0.0.0` where `_` is the default namespace.
    /// If the file name ends with `.json`, the extension is stripped before creating the reference.
    fn from(file_name: &str) -> Self {
        let name_without_ext = file_name.strip_suffix(".json").unwrap_or(file_name);
        let dataset_ref = format!("_/{}@0.0.0", name_without_ext)
            .parse()
            .expect("auto-inferred reference should be valid");

        Self {
            manifest_file: file_name.to_string(),
            dataset_ref,
        }
    }
}

impl From<String> for ManifestRegistration {
    fn from(file_name: String) -> Self {
        Self::from(file_name.as_str())
    }
}

impl<M, R> From<(M, R)> for ManifestRegistration
where
    M: AsRef<str>,
    R: AsRef<str>,
{
    /// Create a manifest registration with an explicit reference string.
    fn from((file_name, dataset_ref): (M, R)) -> Self {
        let dataset_ref = dataset_ref
            .as_ref()
            .parse()
            .unwrap_or_else(|_| panic!("Invalid reference string: {}", dataset_ref.as_ref()));

        Self {
            manifest_file: file_name.as_ref().to_string(),
            dataset_ref,
        }
    }
}

/// Helper type for specifying provider configuration registrations.
///
/// This type encapsulates the information needed to register a provider configuration:
/// - The filename (without extension) of the provider config in the fixtures directory
/// - The provider name to use for registration (typically matches the filename)
///
/// It provides convenient `From` implementations to support both simple strings
/// and tuples for explicit name specification.
#[derive(Debug, Clone)]
pub struct ProviderRegistration {
    provider_file: String,
    provider_name: String,
}

impl From<&str> for ProviderRegistration {
    /// Create a provider registration where the provider name matches the file name.
    ///
    /// If the file name ends with `.toml`, the extension is stripped before using as the provider name.
    fn from(file_name: &str) -> Self {
        let name_without_ext = file_name.strip_suffix(".toml").unwrap_or(file_name);

        Self {
            provider_file: file_name.to_string(),
            provider_name: name_without_ext.to_string(),
        }
    }
}

impl From<String> for ProviderRegistration {
    fn from(file_name: String) -> Self {
        Self::from(file_name.as_str())
    }
}

impl<F, N> From<(F, N)> for ProviderRegistration
where
    F: AsRef<str>,
    N: AsRef<str>,
{
    /// Create a provider registration with an explicit provider name.
    ///
    /// This allows the provider name used for registration to differ from the file name.
    fn from((file_name, provider_name): (F, N)) -> Self {
        Self {
            provider_file: file_name.as_ref().to_string(),
            provider_name: provider_name.as_ref().to_string(),
        }
    }
}
