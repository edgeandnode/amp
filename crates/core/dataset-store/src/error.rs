use amp_datasets_registry::manifests::ManifestParseError;
use amp_providers_registry::ParseConfigError;
use common::BoxError;
use datasets_common::{hash::Hash, hash_reference::HashReference};

use crate::dataset_kind::DatasetKind;

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
    /// doesn't match any supported dataset type (evm-rpc, eth-beacon, firehose, derived).
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
        kind: DatasetKind,
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
    /// Only raw dataset kinds (evm-rpc, eth-beacon, firehose) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error(
        "Dataset has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc, eth-beacon, or firehose)"
    )]
    UnsupportedKind { kind: String },

    /// Dataset is missing the required 'network' field.
    ///
    /// This occurs when a raw dataset definition (evm-rpc, eth-beacon, or firehose)
    /// does not include the network field, which is required to determine the appropriate provider configuration.
    #[error("Dataset is missing required 'network' field for raw dataset kind")]
    MissingNetwork,

    /// No provider configuration found for the dataset kind and network combination.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled
    /// - Provider configuration files are missing or invalid
    #[error("No provider found with kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKind,
        network: String,
    },

    /// Failed to parse the provider configuration.
    ///
    /// This occurs when the provider configuration cannot be deserialized into the
    /// expected type for the dataset kind (EvmRpc, EthBeacon, or Firehose).
    #[error("Failed to parse provider configuration for provider '{name}': {source}")]
    ProviderConfigParseError {
        name: String,
        source: ParseConfigError,
    },

    /// Failed to create an EVM RPC client.
    ///
    /// This occurs during initialization of the EVM RPC client, which may fail due to
    /// invalid RPC URLs, connection issues, or authentication failures.
    #[error("Failed to create EVM RPC client for dataset '{name}': {source}")]
    EvmRpcClientError {
        name: String,
        source: evm_rpc_datasets::Error,
    },

    /// Failed to create a Solana extractor.
    ///
    /// This occurs during initialization of the Solana extractor, which may fail due to
    /// invalid URLs, connection issues, or authentication failures.
    #[error("Failed to create Solana extractor for dataset '{name}': {source}")]
    SolanaExtractorError {
        name: String,
        source: solana_datasets::Error,
    },

    /// Failed to create a Firehose client.
    ///
    /// This occurs during initialization of the Firehose client, which may fail due to
    /// invalid gRPC endpoints, connection issues, or authentication failures.
    #[error("Failed to create Firehose client for dataset '{name}': {source}")]
    FirehoseClientError {
        name: String,
        source: firehose_datasets::Error,
    },

    #[error("Manifest {0} is not registered")]
    ManifestNotRegistered(Hash),

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
        dataset_kind: DatasetKind,
        network: String,
    },

    /// Failed to parse the provider configuration.
    ///
    /// This occurs when the provider configuration cannot be deserialized into the
    /// expected EvmRpcProviderConfig type, typically due to missing required fields
    /// or invalid field values.
    #[error("Failed to parse provider configuration: {0}")]
    ProviderConfigParse(#[source] ParseConfigError),

    /// Failed to establish an IPC connection to the EVM provider.
    ///
    /// This occurs specifically when using IPC (Inter-Process Communication) as the
    /// transport protocol, and the connection to the provider socket fails due to
    /// socket unavailability, permission issues, or other IPC-specific problems.
    #[error("Failed to establish IPC connection: {0}")]
    IpcConnection(#[source] BoxError),
}
