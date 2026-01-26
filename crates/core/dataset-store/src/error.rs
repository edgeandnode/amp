use amp_datasets_registry::{error::ResolveRevisionError, manifests::ManifestParseError};
use datasets_common::{
    hash::Hash, hash_reference::HashReference, raw_dataset_kind::RawDatasetKind,
    reference::Reference,
};

/// Errors specific to getting dataset operations
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    /// Dataset not found.
    ///
    /// This occurs when the manifest hash in the hash reference does not exist in the
    /// manifest store, or when the manifest file cannot be retrieved.
    #[error("Dataset '{0}' not found")]
    DatasetNotFound(HashReference),

    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when attempting to retrieve the manifest file path associated
    /// with the given hash from the metadata database.
    #[error("Failed to query manifest path from metadata database for dataset '{reference}'")]
    QueryManifestPath {
        reference: HashReference,
        #[source]
        source: metadata_db::Error,
    },

    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store for dataset '{reference}'")]
    LoadManifestContent {
        reference: HashReference,
        #[source]
        source: amp_datasets_registry::error::GetManifestError,
    },

    /// Failed to parse manifest to extract kind field
    ///
    /// This occurs when parsing the manifest file to extract the `kind` field.
    /// The manifest may contain invalid JSON/TOML syntax or be missing the kind field.
    #[error("Failed to parse manifest to extract kind field for dataset '{reference}'")]
    ParseManifestForKind {
        reference: HashReference,
        #[source]
        source: ManifestParseError,
    },

    /// Dataset kind is not supported
    ///
    /// This occurs when the `kind` field in the manifest contains a value that
    /// doesn't match any supported dataset type (evm-rpc, firehose, derived).
    #[error("Unsupported dataset kind '{kind}' for dataset '{reference}'")]
    UnsupportedKind {
        reference: HashReference,
        kind: String,
    },

    /// Failed to parse kind-specific manifest
    ///
    /// This occurs when parsing the kind-specific manifest (EvmRpc, EthBeacon, Firehose, or Derived).
    /// The manifest structure may not match the expected schema for the dataset kind.
    #[error("Failed to parse {kind} manifest for dataset '{reference}'")]
    ParseManifest {
        reference: HashReference,
        kind: RawDatasetKind,
        #[source]
        source: ManifestParseError,
    },

    /// Failed to create derived dataset instance
    ///
    /// This occurs when creating a derived dataset from its manifest, which may fail due to:
    /// - Invalid SQL queries in the dataset definition
    /// - Dependency resolution issues (referenced datasets not found)
    /// - Logical errors in the dataset definition
    #[error("Failed to create derived dataset for '{reference}'")]
    CreateDerivedDataset {
        reference: HashReference,
        #[source]
        source: datasets_derived::DatasetError,
    },
}

/// Errors that occur when getting all datasets from the dataset store
#[derive(Debug, thiserror::Error)]
pub enum GetAllDatasetsError {
    /// Failed to list datasets from the metadata database
    ///
    /// This occurs when the database query to retrieve all dataset tags fails,
    /// typically due to connection issues, query timeouts, or database errors.
    #[error("Failed to list datasets from metadata database: {0}")]
    ListDatasetsFromDb(#[source] metadata_db::Error),

    /// Failed to load a specific dataset
    ///
    /// This occurs when loading an individual dataset from the list fails.
    /// The dataset exists in the metadata database but cannot be loaded,
    /// typically due to manifest retrieval/parse errors or missing manifest files.
    #[error("Failed to load dataset '{namespace}/{name}@{version}'")]
    LoadDataset {
        namespace: String,
        name: String,
        version: String,
        #[source]
        source: GetDatasetError,
    },
}

/// Errors specific to getting derived dataset manifest operations
#[derive(Debug, thiserror::Error)]
pub enum GetDerivedManifestError {
    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when attempting to retrieve the manifest file path associated
    /// with the given hash from the metadata database.
    #[error("Failed to query manifest path from metadata database")]
    QueryManifestPath(#[source] metadata_db::Error),

    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store")]
    LoadManifestContent(#[source] amp_datasets_registry::error::GetManifestError),

    /// Failed to parse the manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected schema
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse manifest")]
    ManifestParseError(#[source] ManifestParseError),

    /// The dataset kind is not SQL or Derived.
    ///
    /// This occurs when trying to get a dataset as a SQL dataset but the manifest
    /// indicates a different kind (e.g., evm-rpc, firehose).
    /// Only SQL and Derived dataset kinds can be retrieved as SQL datasets.
    #[error(
        "Dataset has unsupported kind '{kind}' for SQL dataset retrieval (expected 'sql' or 'derived')"
    )]
    UnsupportedKind { kind: String },

    #[error("Manifest {0} is not registered")]
    ManifestNotRegistered(Hash),

    #[error("Manifest {0} not found in the manifest store")]
    ManifestNotFound(Hash),
}

/// Errors specific to getting client operations for raw datasets
#[derive(Debug, thiserror::Error)]
pub enum GetClientError {
    /// Failed to load manifest content from object store
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to load manifest content from object store")]
    LoadManifestContent(#[source] amp_datasets_registry::error::GetManifestError),

    /// Failed to parse the common manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected schema
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse common manifest for dataset")]
    CommonManifestParseError(#[source] ManifestParseError),

    /// The dataset kind is not a raw dataset type.
    ///
    /// This occurs when trying to get a client for a dataset that is not a raw data source.
    /// Only raw dataset kinds (evm-rpc, firehose) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error(
        "Dataset has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc or firehose)"
    )]
    UnsupportedKind { kind: String },

    /// Dataset is missing the required 'network' field.
    ///
    /// This occurs when a raw dataset definition (evm-rpc or firehose)
    /// does not include the network field, which is required to determine the appropriate provider configuration.
    #[error("Dataset is missing required 'network' field for raw dataset kind")]
    MissingNetwork,

    /// Failed to create client for the dataset.
    ///
    /// This occurs during client creation for the provider, which may fail due to:
    /// - No provider found for the kind-network combination
    /// - Invalid provider configuration
    /// - Client initialization failures (connection issues, invalid URLs, etc.)
    #[error("Failed to create client")]
    ClientCreation(#[source] amp_providers_registry::CreateClientError),

    #[error("Manifest {0} not found in the manifest store")]
    ManifestNotFound(Hash),
}

/// Errors that occur when creating an ETH call UDF for a dataset.
///
/// This error type is used by the `eth_call_for_dataset` method when setting up
/// the eth_call user-defined function for EVM RPC datasets.
#[derive(Debug, thiserror::Error)]
pub enum EthCallForDatasetError {
    /// Dataset is missing the required 'network' field.
    ///
    /// This occurs when an EVM RPC dataset definition does not include the network
    /// field, which is required to determine the appropriate provider configuration.
    #[error("Dataset '{reference}' is missing required 'network' field for EvmRpc kind")]
    MissingNetwork { reference: HashReference },

    /// No provider configuration found for the dataset kind and network combination.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled or failed environment variable substitution
    /// - Provider configuration files are missing or invalid
    #[error("No provider found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: RawDatasetKind,
        network: String,
    },

    /// Failed to create the EVM RPC provider.
    ///
    /// This occurs when provider configuration parsing fails or when
    /// establishing an IPC connection fails.
    #[error("Failed to create EVM RPC provider")]
    ProviderCreation(#[source] amp_providers_registry::CreateEvmRpcClientError),
}

/// Errors that occur when resolving dataset dependencies.
///
/// Derived datasets can depend on other datasets (raw or derived). These errors
/// cover failures during the dependency resolution process, including missing
/// datasets, revision lookups, and cycle detection in the dependency graph.
#[derive(Debug, thiserror::Error)]
pub enum DatasetDependencyError {
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
