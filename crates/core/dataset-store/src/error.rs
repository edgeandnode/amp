use amp_datasets_registry::manifests::ManifestParseError;
use datasets_common::{
    dataset_kind_str::DatasetKindStr, hash::Hash, hash_reference::HashReference,
    network_id::NetworkId,
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
        kind: DatasetKindStr,
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
        source: common::datasets_derived::DatasetError,
    },
}

/// Errors specific to getting derived dataset manifest operations
#[derive(Debug, thiserror::Error)]
pub enum GetDerivedManifestError {
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
    /// - The manifest structure doesn't match the expected derived dataset schema
    /// - The dataset kind is not 'manifest' (for derived datasets)
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse manifest")]
    ManifestParseError(#[source] ManifestParseError),

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

    /// Failed to parse the raw manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest is missing the required 'network' field
    /// - The manifest structure doesn't match the expected raw dataset schema
    #[error("Failed to parse manifest for raw dataset")]
    RawManifestParseError(#[source] ManifestParseError),

    /// The dataset kind is not a raw dataset type.
    ///
    /// This occurs when trying to get a client for a dataset that is not a raw data source.
    /// Only raw dataset kinds (evm-rpc, firehose) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error(
        "Dataset has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc or firehose)"
    )]
    UnsupportedKind { kind: String },

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
    /// No provider configuration found for the dataset kind and network combination.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled or failed environment variable substitution
    /// - Provider configuration files are missing or invalid
    #[error("No provider found for dataset kind '{dataset_kind}' and network '{network}'")]
    ProviderNotFound {
        dataset_kind: DatasetKindStr,
        network: NetworkId,
    },

    /// Failed to create the EVM RPC provider.
    ///
    /// This occurs when provider configuration parsing fails or when
    /// establishing an IPC connection fails.
    #[error("Failed to create EVM RPC provider")]
    ProviderCreation(#[source] amp_providers_registry::CreateEvmRpcClientError),
}
