//! Test support CLI for the Amp project.
//!
//! This module provides a command-line interface for test-related operations,
//! particularly for creating and managing blessed dataset snapshots used in
//! integration tests.
//!
//! ## Overview
//!
//! The primary functionality is the "bless" command, which creates reproducible
//! dataset snapshots for use in integration tests. This ensures tests have
//! consistent, known-good data to work with.
//!
//! ## Blessing Workflow
//!
//! The blessing process follows these steps:
//! 1. **Environment Setup**: Creates isolated temporary metadata database and configuration
//! 2. **Dependency Resolution**: Identifies and restores required dataset dependencies
//! 3. **Data Cleanup**: Removes any existing data for the target dataset
//! 4. **Data Extraction**: Dumps fresh data from the source up to the specified block
//! 5. **Validation**: Runs consistency checks on all dumped tables
//! 6. **Blessing**: Marks the snapshot as blessed for use in tests
//!
//! ## Usage
//!
//! ```bash
//! # Bless a dataset up to block 1000000
//! cargo run -p tests -- bless ethereum_mainnet 1000000
//! ```
//!
//! ## Test Data Directory Structure
//!
//! The tool expects a test data directory with the following structure:
//! ```
//! tests/data/
//! ├── manifests/     # Dataset manifest files
//! ├── providers/     # Provider configuration files
//! └── snapshots/     # Blessed dataset snapshots (.parquet files)
//! ```
//!
//! ## Key Features
//!
//! - **Isolated Execution**: Uses temporary metadata database to avoid conflicts
//! - **Dependency Management**: Automatically resolves and restores dataset dependencies
//! - **Deterministic Output**: Single-threaded processing ensures reproducible snapshots
//! - **Data Validation**: Comprehensive consistency checks ensure data integrity
//! - **Error Recovery**: Detailed error messages for troubleshooting

use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use common::{BoxError, Store};
use config::Config;
use dataset_store::{
    DatasetStore, dataset_and_dependencies, manifests::DatasetManifestsStore,
    providers::ProviderConfigsStore,
};
use datasets_common::reference::Reference;
use dump::consistency_check;
use fs_err as fs;
use futures::{StreamExt as _, TryStreamExt as _};
use metadata_db::MetadataDb;
use monitoring::logging;
use tests::testlib::{
    fixtures::{Ampctl, DaemonConfigBuilder, DaemonController, TempMetadataDb},
    helpers as test_helpers,
};

/// Command-line interface for test support operations.
///
/// This CLI provides utilities for managing test data and creating reproducible
/// dataset snapshots that can be used in integration tests.
#[derive(Parser, Debug)]
#[command(name = "tests")]
#[command(about = "Test support utilities for the Amp project")]
#[command(
    long_about = "Provides commands for creating and managing blessed dataset snapshots used in integration tests"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

/// Available test support commands.
#[derive(Parser, Debug)]
enum Command {
    /// Create a blessed dataset snapshot for testing.
    ///
    /// This command extracts data from the specified dataset up to the given
    /// block number, validates the data integrity, and creates a blessed snapshot
    /// that can be used as a test fixture. The process includes dependency
    /// resolution, data cleanup, extraction, and validation.
    Bless {
        /// Name of the dataset to create a blessed snapshot for.
        ///
        /// This should match a dataset name defined in the `manifests` directory.
        /// The dataset and all its dependencies will be processed.
        dataset: Reference,

        /// End block number (inclusive) for the dataset snapshot.
        ///
        /// The blessing process will extract data from the dataset's start block
        /// up to and including this block number. Choose a block that provides
        /// sufficient test data while keeping snapshot size manageable.
        end_block: u64,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    logging::init();

    match args.command {
        Command::Bless {
            dataset: dataset_ref,
            end_block,
        } => {
            let _ = dotenvy::dotenv_override();

            // Create temporary metadata database
            let temp_db = TempMetadataDb::new().await;

            // Get test data directory and create absolute paths for subdirectories
            let test_data_dir =
                resolve_test_data_dir().expect("Failed to resolve 'tests/data' directory");
            let manifests_dir = test_data_dir.join("manifests");
            let providers_dir = test_data_dir.join("providers");
            let snapshots_dir = test_data_dir.join("snapshots");

            // Create daemon config with absolute paths
            let daemon_config = DaemonConfigBuilder::new()
                .manifests_dir(manifests_dir.to_string_lossy().to_string())
                .providers_dir(providers_dir.to_string_lossy().to_string())
                .data_dir(snapshots_dir.to_string_lossy().to_string())
                .metadata_db_url(temp_db.connection_url())
                .build();

            // Write config to temporary file
            let temp_config_file = tempfile::Builder::new()
                .suffix(".toml")
                .tempfile_in(test_data_dir)
                .expect("Failed to create temp config file");
            fs::write(temp_config_file.path(), daemon_config.serialize_to_toml())
                .expect("Failed to write daemon config to temporary file");

            tracing::debug!(
                "Using temporary config file at: {}",
                temp_config_file.path().display()
            );

            // Load configuration and create necessary components
            let config = Arc::new(
                Config::load(temp_config_file.path(), false, None, false, None)
                    .await
                    .expect("Failed to load config"),
            );
            let data_store = Arc::new(
                Store::new(config.data_store_url.clone()).expect("Failed to create data store"),
            );
            let providers_store = Store::new(config.providers_store_url.clone())
                .expect("Failed to create providers store");
            let manifests_store = Store::new(config.manifests_store_url.clone())
                .expect("Failed to create manifests store");

            let dataset_store = {
                let provider_configs_store =
                    ProviderConfigsStore::new(providers_store.prefixed_store());
                let dataset_manifests_store =
                    DatasetManifestsStore::new(manifests_store.prefixed_store());
                DatasetStore::new(
                    temp_db.metadata_db().clone(),
                    provider_configs_store,
                    dataset_manifests_store,
                )
            };

            // Start controller for Admin API access during dependency restoration
            let controller = DaemonController::new(
                config.clone(),
                temp_db.metadata_db().clone(),
                data_store.clone(),
                dataset_store.clone(),
                None,
            )
            .await
            .expect("Failed to start controller for dependency restoration");

            // Register manifest with the Admin API (after controller is running).
            let ampctl = Ampctl::new(controller.admin_api_url());

            let dataset_name = dataset_ref.name().to_string();

            tracing::info!(
                manifest = %dataset_name,
                "Registering manifest"
            );

            let manifest_path = format!(
                "{}/{}.json",
                daemon_config.dataset_manifests_path(),
                dataset_name,
            );

            let manifest_content = tokio::fs::read_to_string(manifest_path)
                .await
                .expect("Failed to read manifest file");

            ampctl
                .register_manifest(&dataset_ref, &manifest_content)
                .await
                .expect("Failed to register manifest");

            tracing::info!(
                manifest = %dataset_name,
                "Successfully registered manifest"
            );

            // Run blessing procedure
            bless(
                config.clone(),
                &ampctl,
                dataset_store.clone(),
                temp_db.metadata_db().clone(),
                data_store.clone(),
                dataset_ref.clone(),
                end_block,
            )
            .await
            .expect("Failed to bless dataset");

            eprintln!("✅ Successfully blessed dataset '{dataset_ref}' up to block {end_block}");
        }
    }
}

/// Create a blessed dataset snapshot for testing.
///
/// This function implements the core blessing workflow that creates reproducible
/// dataset snapshots for use in integration tests. The process ensures data
/// consistency and validates integrity through multiple steps.
///
/// # Workflow
///
/// 1. **Dependency Resolution**: Resolves all dataset dependencies and restores them
///    from existing snapshots to ensure a complete dependency chain.
///
/// 2. **Data Cleanup**: Removes any existing data for the target dataset to ensure
///    a clean state before dumping fresh data.
///
/// 3. **Data Extraction**: Dumps data from the dataset source up to the specified
///    end block using single-threaded processing for deterministic output.
///
/// 4. **Validation**: Runs comprehensive consistency checks on all dumped tables
///    to ensure data integrity and correctness.
///
/// # Error Handling
///
/// The function provides detailed error messages for troubleshooting:
/// - Dependency resolution failures
/// - Data cleanup errors
/// - Dump operation failures
/// - Consistency check failures
///
/// All errors include context about the specific dataset and operation that failed.
async fn bless(
    config: Arc<Config>,
    ampctl: &Ampctl,
    dataset_store: DatasetStore,
    metadata_db: MetadataDb,
    data_store: Arc<Store>,
    dataset: Reference,
    end: u64,
) -> Result<(), BoxError> {
    // Resolve dataset dependencies and restore them first
    tracing::debug!(%dataset, "Resolving dataset dependencies");
    let deps = {
        let mut ds_and_deps = dataset_and_dependencies(&dataset_store, dataset.clone())
            .await
            .map_err(|err| {
                format!(
                    "Failed to resolve dependencies for dataset '{}': {}",
                    dataset, err
                )
            })?;

        // Remove the dataset itself from the list, leaving only dependencies
        if ds_and_deps.pop() != Some(dataset.clone()) {
            return Err(format!(
                "Dataset '{}' not found in resolved dependencies list",
                dataset
            )
            .into());
        }

        ds_and_deps
    };

    tracing::debug!(%dataset, ?deps, "Restoring dataset dependencies");
    for dep in deps {
        test_helpers::restore_dataset_snapshot(ampctl, &dataset_store, &metadata_db, &dep)
            .await
            .map_err(|err| {
                format!(
                    "Failed to restore dependency '{}' for dataset '{}': {}",
                    dep, dataset, err
                )
            })?;
    }

    // Clear existing dataset data if it exists
    let store = data_store.prefixed_store();
    let path = object_store::path::Path::parse(dataset.name())
        .map_err(|err| format!("Invalid dataset name '{}': {}", dataset.name(), err))?;

    // Check if dataset exists using head operation
    match store.head(&path).await {
        Ok(_) => {
            tracing::debug!(%dataset, "Dataset exists, clearing existing data");
            let path_stream = store.list(Some(&path)).map_ok(|obj| obj.location).boxed();
            store
                .delete_stream(path_stream)
                .try_collect::<Vec<_>>()
                .await
                .map_err(|err| {
                    format!(
                        "Failed to clear existing data for dataset '{}': {}",
                        dataset, err
                    )
                })?;
        }
        Err(_) => {
            tracing::debug!(%dataset, "No existing data found, skipping cleanup");
        }
    }

    // Dump the dataset
    tracing::debug!(%dataset, end_block=end, "Dumping dataset");
    let worker_config = worker_config_from_common(&config);
    let physical_tables = test_helpers::dump_dataset(
        worker_config,
        metadata_db,
        data_store.clone(),
        dataset_store,
        dataset.clone(),
        end,
    )
    .await
    .map_err(|err| {
        format!(
            "Failed to dump dataset '{}' to block {}: {}",
            dataset, end, err
        )
    })?;

    // Run consistency check on all tables after dump
    tracing::debug!(%dataset, "Running consistency checks on dumped tables");
    for physical_table in physical_tables {
        consistency_check(&physical_table).await.map_err(|err| {
            format!(
                "Consistency check failed for dataset '{}': {}",
                dataset, err
            )
        })?;
    }

    Ok(())
}

/// Convert config::Config to worker::config::Config
///
/// Creates a worker configuration with a dummy WorkerInfo since the blessing
/// process doesn't need real build information.
fn worker_config_from_common(config: &Config) -> worker::config::Config {
    worker::config::Config {
        microbatch_max_interval: config.microbatch_max_interval,
        poll_interval: config.poll_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet: config.parquet.clone(),
        worker_info: worker::info::WorkerInfo {
            version: Some("test".to_string()),
            commit_sha: None,
            commit_timestamp: None,
            build_date: None,
        },
    }
}

/// Resolve test data directory from known standard locations.
///
/// This function searches for the test data directory in a predefined list of
/// standard locations, returning the canonicalized absolute path to the first
/// directory that exists and is accessible.
///
/// # Search Locations
///
/// The function searches in the following order:
/// 1. `tests/data` - Standard location when running from workspace root
/// 2. `data` - Fallback location when running from the tests crate directory
///
/// # Returns
///
/// Returns the canonicalized absolute path to the test data directory if found,
/// or an error with helpful guidance if no valid directory is located.
///
/// # Errors
///
/// Returns an error if:
/// - None of the standard locations exist
/// - The found directory is not accessible
/// - Path canonicalization fails
///
/// The error message includes the searched locations and usage instructions.
///
/// # Usage Context
///
/// This function ensures the CLI works correctly regardless of whether it's
/// executed from:
/// - The workspace root: `cargo run -p tests`
/// - The tests crate directory: `cargo run`
fn resolve_test_data_dir() -> Result<PathBuf, BoxError> {
    const TEST_DATA_BASE_DIRS: [&str; 2] = ["tests/config", "config"];
    TEST_DATA_BASE_DIRS
        .iter()
        .filter_map(|dir| std::path::Path::new(dir).canonicalize().ok())
        .find(|path| path.exists() && path.is_dir())
        .ok_or_else(|| -> BoxError {
            format!(
                "Couldn't find test data directory in locations: {:?}. \
                The `cargo run -p tests` command must be run from the workspace root or the tests crate root",
                TEST_DATA_BASE_DIRS
            )
            .into()
        })
}
