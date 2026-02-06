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

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use amp_data_store::DataStore;
use amp_dataset_store::{DatasetStore, GetDatasetError};
use amp_datasets_registry::{
    DatasetsRegistry, error::ResolveRevisionError, manifests::DatasetManifestsStore,
};
use amp_providers_registry::{ProviderConfigsStore, ProvidersRegistry};
use anyhow::{Result, anyhow};
use clap::Parser;
use datasets_common::reference::Reference;
use datasets_derived::{
    Dataset as DerivedDataset,
    deps::{DfsError, dfs},
};
use dump::consistency_check;
use fs_err as fs;
use futures::{StreamExt as _, TryStreamExt as _};
use monitoring::logging;
use tests::testlib::{
    build_info,
    fixtures::{
        Ampctl, DaemonConfigBuilder, DaemonController, DaemonWorker,
        MetadataDb as MetadataDbFixture,
    },
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
            let sysdb = MetadataDbFixture::new().await;

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
                .metadata_db_url(sysdb.connection_url())
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
            let config = amp_config::load_config(
                temp_config_file.path(),
                amp_config::no_defaults_override(),
            )
            .expect("Failed to load config");
            let config = Arc::new(config);
            let data_store = DataStore::new(
                sysdb.conn_pool().clone(),
                config.data_store_url.clone(),
                config.parquet.cache_size_mb,
            )
            .expect("Failed to create data store");

            let (dataset_store, datasets_registry, providers_registry) = {
                let provider_configs_store = ProviderConfigsStore::new(
                    amp_object_store::new_with_prefix(
                        &config.providers_store_url,
                        config.providers_store_url.path(),
                    )
                    .expect("Failed to create provider configs store"),
                );
                let providers_registry = ProvidersRegistry::new(provider_configs_store);
                let dataset_manifests_store = DatasetManifestsStore::new(
                    amp_object_store::new_with_prefix(
                        &config.manifests_store_url,
                        config.manifests_store_url.path(),
                    )
                    .expect("Failed to create manifests store"),
                );
                let datasets_registry =
                    DatasetsRegistry::new(sysdb.conn_pool().clone(), dataset_manifests_store);
                let dataset_store =
                    DatasetStore::new(datasets_registry.clone(), providers_registry.clone());
                (dataset_store, datasets_registry, providers_registry)
            };

            // Start controller for Admin API access during dependency restoration
            let build_info = build_info::load();
            let controller = DaemonController::new(
                build_info.clone(),
                config.clone(),
                sysdb.conn_pool().clone(),
                datasets_registry,
                providers_registry,
                data_store.clone(),
                dataset_store.clone(),
                None,
            )
            .await
            .expect("Failed to start controller for dependency restoration");

            // Start a worker to handle dump jobs
            let _worker = DaemonWorker::new(
                build_info,
                config.clone(),
                sysdb.conn_pool().clone(),
                data_store.clone(),
                dataset_store.clone(),
                None,
                "bless".parse().expect("valid worker node id"),
            )
            .await
            .expect("Failed to start worker");

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
                &ampctl,
                dataset_store.clone(),
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
    ampctl: &Ampctl,
    dataset_store: DatasetStore,
    data_store: DataStore,
    dataset: Reference,
    end: u64,
) -> Result<()> {
    // Resolve dataset dependencies and restore them first
    tracing::debug!(%dataset, "Resolving dataset dependencies");
    let deps = {
        let mut ds_and_deps = dataset_and_dependencies(&dataset_store, dataset.clone())
            .await
            .map_err(|err| {
                anyhow!(
                    "Failed to resolve dependencies for dataset '{}': {}",
                    dataset,
                    err
                )
            })?;

        // Remove the dataset itself from the list, leaving only dependencies
        if ds_and_deps.pop() != Some(dataset.clone()) {
            return Err(anyhow!(
                "Dataset '{}' not found in resolved dependencies list",
                dataset
            ));
        }

        ds_and_deps
    };

    tracing::debug!(%dataset, ?deps, "Restoring dataset dependencies");
    for dep in deps {
        test_helpers::restore_dataset_snapshot(ampctl, &dataset_store, &data_store, &dep)
            .await
            .map_err(|err| {
                anyhow!(
                    "Failed to restore dependency '{}' for dataset '{}': {}",
                    dep,
                    dataset,
                    err
                )
            })?;
    }

    // Clear existing dataset data if it exists
    let store = data_store.as_datafusion_object_store();
    let path = object_store::path::Path::parse(dataset.name())
        .map_err(|err| anyhow!("Invalid dataset name '{}': {}", dataset.name(), err))?;

    // Check if dataset exists using head operation
    match store.head(&path).await {
        Ok(_) => {
            tracing::debug!(%dataset, "Dataset exists, clearing existing data");
            let path_stream = store.list(Some(&path)).map_ok(|obj| obj.location).boxed();
            store
                .delete_stream(path_stream)
                .try_collect::<Vec<_>>()
                .await
                .map_err(|err: object_store::Error| {
                    anyhow!(
                        "Failed to clear existing data for dataset '{}': {}",
                        dataset,
                        err
                    )
                })?;
        }
        Err(_) => {
            tracing::debug!(%dataset, "No existing data found, skipping cleanup");
        }
    }

    // Dump the dataset
    tracing::debug!(%dataset, end_block=end, "Dumping dataset");
    test_helpers::deploy_and_wait(ampctl, &dataset, Some(end), Duration::from_secs(30))
        .await
        .map_err(|err| {
            anyhow!(
                "Failed to dump dataset '{}' to block {}: {}",
                dataset,
                end,
                err
            )
        })?;

    // Run consistency check on all tables after dump
    tracing::debug!(%dataset, "Running consistency checks on dumped tables");
    let physical_tables =
        test_helpers::load_physical_tables(&dataset_store, &data_store, &dataset).await?;
    for physical_table in physical_tables {
        consistency_check(&physical_table, &data_store)
            .await
            .map_err(|err| {
                anyhow!(
                    "Consistency check failed for dataset '{}': {}",
                    dataset,
                    err
                )
            })?;
    }

    Ok(())
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
fn resolve_test_data_dir() -> Result<PathBuf> {
    const TEST_DATA_BASE_DIRS: [&str; 2] = ["tests/config", "config"];
    TEST_DATA_BASE_DIRS
        .iter()
        .filter_map(|dir| std::path::Path::new(dir).canonicalize().ok())
        .find(|path| path.exists() && path.is_dir())
        .ok_or_else(|| {
            anyhow!(
                "Couldn't find test data directory in locations: {:?}. \
                The `cargo run -p tests` command must be run from the workspace root or the tests crate root",
                TEST_DATA_BASE_DIRS
            )
        })
}

/// Return the input datasets and their dataset dependencies. The output set is ordered such that
/// each dataset comes after all datasets it depends on.
async fn dataset_and_dependencies(
    store: &DatasetStore,
    dataset: Reference,
) -> Result<Vec<Reference>, DatasetDependencyError> {
    let mut datasets = vec![dataset];
    let mut deps: BTreeMap<Reference, Vec<Reference>> = Default::default();
    while let Some(dataset_ref) = datasets.pop() {
        // Resolve the reference to a hash reference first
        let hash_ref = store
            .resolve_revision(&dataset_ref)
            .await
            .map_err(DatasetDependencyError::ResolveRevision)?
            .ok_or_else(|| DatasetDependencyError::DatasetNotFound(dataset_ref.clone()))?;
        let dataset = store
            .get_dataset(&hash_ref)
            .await
            .map_err(DatasetDependencyError::GetDataset)?;

        // Only derived datasets have dependencies; raw datasets are leaf nodes
        let Some(dataset) = dataset.downcast_ref::<DerivedDataset>() else {
            deps.insert(dataset_ref, vec![]);
            continue;
        };

        let refs: Vec<Reference> = dataset
            .dependencies()
            .values()
            .map(|dep| dep.to_reference())
            .collect();
        let mut untracked_refs = refs
            .iter()
            .filter(|r| deps.keys().all(|d| d != *r))
            .cloned()
            .collect();
        datasets.append(&mut untracked_refs);
        deps.insert(dataset_ref, refs);
    }

    dependency_sort(deps).map_err(DatasetDependencyError::CycleDetected)
}

/// Errors that occur when resolving dataset dependencies.
///
/// Derived datasets can depend on other datasets (raw or derived). These errors
/// cover failures during the dependency resolution process, including missing
/// datasets, revision lookups, and cycle detection in the dependency graph.
#[derive(Debug, thiserror::Error)]
enum DatasetDependencyError {
    /// A referenced dataset was not found.
    ///
    /// This occurs when a dataset references another dataset by name or hash
    /// that does not exist in the registry. The dependency cannot be resolved
    /// because the target dataset is missing.
    #[error("dataset '{0}' not found")]
    DatasetNotFound(Reference),

    /// Failed to resolve the revision for a dataset reference.
    ///
    /// When a dataset is referenced by name (without a specific hash), the system
    /// must resolve it to a specific revision. This error occurs when that resolution
    /// fails, typically due to registry lookup issues.
    #[error("failed to resolve dataset revision")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Failed to retrieve a dataset from the store.
    ///
    /// After resolving a reference, the actual dataset must be fetched. This error
    /// occurs when the dataset retrieval fails, which may be due to manifest parsing
    /// issues or object store access problems.
    #[error("failed to get dataset")]
    GetDataset(#[source] GetDatasetError),

    /// A circular dependency was detected in the dataset graph.
    ///
    /// Datasets cannot depend on themselves directly or transitively. This error
    /// occurs when traversing the dependency graph reveals a cycle, which would
    /// cause infinite recursion during query execution.
    #[error("dependency cycle detected")]
    CycleDetected(#[source] datasets_derived::deps::DfsError<Reference>),
}

/// Given a map of values to their dependencies, return a set where each value is ordered after
/// all of its dependencies. An error is returned if a cycle is detected.
fn dependency_sort(
    deps: BTreeMap<Reference, Vec<Reference>>,
) -> Result<Vec<Reference>, DfsError<Reference>> {
    let nodes: BTreeSet<&Reference> = deps
        .iter()
        .flat_map(|(ds, deps)| std::iter::once(ds).chain(deps))
        .collect();
    let mut ordered: Vec<Reference> = Default::default();
    let mut visited: BTreeSet<&Reference> = Default::default();
    let mut visited_cycle: BTreeSet<&Reference> = Default::default();
    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visited_cycle)?;
        }
    }
    Ok(ordered)
}
