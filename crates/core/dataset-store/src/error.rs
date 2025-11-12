use common::BoxError;
use datasets_common::{hash::Hash, name::NameError, reference::Reference};

use crate::{
    DatasetKind,
    manifests::{ManifestParseError, StoreError},
    providers::ParseConfigError,
};

/// Errors specific to manifest registration operations
///
/// This error type is used by `DatasetStore::register_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    /// Failed to store manifest in dataset definitions store
    ///
    /// This occurs when the object store operation to save the manifest file fails.
    /// The manifest file is stored in the dataset definitions object store before being
    /// registered in the metadata database.
    ///
    /// Common causes:
    /// - Object store connection failures (S3, GCS, local filesystem, etc.)
    /// - Network issues when writing to remote storage
    /// - Permission problems (filesystem permissions, cloud IAM roles)
    /// - Disk full on local storage
    /// - Invalid storage path or bucket configuration
    ///
    /// The operation can be retried as no partial state is persisted if storage fails.
    #[error("Failed to store manifest in dataset definitions store")]
    ManifestStorage(#[source] StoreError),

    /// Failed to register manifest in metadata database
    ///
    /// This occurs when the database operation to register the manifest metadata fails.
    /// This happens after the manifest file has been successfully stored in the object store.
    ///
    /// Common causes:
    /// - Database connection issues
    /// - Database unavailability or timeouts
    /// - Permission problems (database user lacks INSERT privileges)
    /// - Database constraint violations (e.g., duplicate manifest hash)
    /// - Transaction conflicts with concurrent operations
    ///
    /// If this error occurs, the manifest file exists in the object store but is not
    /// registered in the metadata database. The operation can be retried - duplicate
    /// manifest hashes will be handled by database constraints.
    #[error("Failed to register manifest in metadata database")]
    MetadataRegistration(#[source] metadata_db::Error),
}

/// Errors specific to manifest linking operations
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    /// Manifest does not exist in the system
    ///
    /// This occurs when attempting to link a manifest hash that hasn't been registered.
    /// The manifest must be registered first via `register_manifest` before it can be
    /// linked to a dataset.
    ///
    /// This error is detected via foreign key constraint violation (PostgreSQL error code 23503)
    /// when the database rejects the link operation due to the missing manifest.
    #[error("Manifest with hash '{0}' does not exist")]
    ManifestNotFound(Hash),

    /// Failed to begin transaction
    ///
    /// This occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to link manifest to dataset in metadata database
    ///
    /// This occurs when the database operation to create the manifest-dataset link fails,
    /// typically due to:
    /// - Database connection issues during the operation
    /// - Permission problems
    /// - Other database errors
    ///
    /// Note: Foreign key constraint violations (manifest doesn't exist) are handled separately
    /// as `ManifestNotFound` errors.
    #[error("Failed to link manifest to dataset in metadata database")]
    LinkManifestToDataset(#[source] metadata_db::Error),

    /// Failed to set dev tag for dataset
    ///
    /// This occurs when the database operation to update the dev tag fails,
    /// typically due to:
    /// - Database connection issues during the operation
    /// - Permission problems
    /// - Constraint violations
    #[error("Failed to set dev tag for dataset")]
    SetDevTag(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (linking manifest and updating dev tag)
    /// were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Errors specific to setting semantic version tags for dataset manifests
///
/// These errors occur during the `set_dataset_version_tag` operation, which creates
/// or updates a semantic version tag and automatically updates the "latest" tag if needed.
#[derive(Debug, thiserror::Error)]
pub enum SetVersionTagError {
    /// Failed to begin transaction or execute database operations
    ///
    /// This error occurs when:
    /// - The database connection is unavailable or times out
    /// - The manifest hash does not exist (foreign key constraint violation)
    /// - The dataset namespace/name combination doesn't exist
    /// - Database permissions prevent the upsert operation
    /// - Failed to begin the transaction
    /// - Failed to register version tag or query latest tag
    ///
    /// The operation may be retried as it's idempotent. If the manifest hash
    /// is invalid, ensure the manifest is registered first via `register_manifest`.
    #[error("Failed to execute database operations")]
    MetadataDb(#[source] metadata_db::Error),

    /// Failed to update the "latest" tag to point to the highest version
    ///
    /// This error occurs when:
    /// - The database connection fails during the latest tag update
    /// - A transaction conflict occurs if another process updates latest simultaneously
    /// - Database permissions prevent updating the latest tag
    /// - The manifest hash resolved as highest no longer exists (rare edge case)
    ///
    /// This happens after successfully registering the version tag and determining
    /// that the "latest" tag needs to be updated. The error occurs before commit,
    /// so no changes have been persisted yet.
    #[error("Failed to update latest tag in metadata database")]
    UpdateLatestTag(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (version tag registration and latest
    /// tag update) were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Error when resolving revision references to manifest hashes
///
/// This error occurs during the `resolve_revision` operation, which converts
/// revision references (version tags, hashes, or special tags) into concrete manifest hashes.
///
/// This occurs when:
/// - The database connection is unavailable or times out
/// - Database permissions prevent the query operation
/// - Database query syntax or logic error (should be rare)
///
/// The operation may be retried as it's read-only.
#[derive(Debug, thiserror::Error)]
#[error("Failed to query metadata database")]
pub struct ResolveRevisionError(#[source] pub metadata_db::Error);

/// Errors that occur when loading a dataset by its manifest hash
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetByHashError {
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
    LoadManifestContent(#[source] crate::manifests::GetError),

    /// Failed to parse manifest to extract kind field
    ///
    /// This occurs when parsing the manifest file to extract the `kind` field.
    /// The manifest may contain invalid JSON/TOML syntax or be missing the kind field.
    #[error("Failed to parse manifest to extract kind field")]
    ParseManifestForKind(#[source] ManifestParseError),

    /// Dataset kind is not supported
    ///
    /// This occurs when the `kind` field in the manifest contains a value that
    /// doesn't match any supported dataset type (evm-rpc, eth-beacon, firehose, derived).
    #[error("Unsupported dataset kind: {0}")]
    UnsupportedKind(String),

    /// Failed to parse kind-specific manifest
    ///
    /// This occurs when parsing the kind-specific manifest (EvmRpc, EthBeacon, Firehose, or Derived).
    /// The manifest structure may not match the expected schema for the dataset kind.
    #[error("Failed to parse {kind} manifest")]
    ParseManifest {
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
    #[error("Failed to create derived dataset")]
    CreateDerivedDataset(#[source] BoxError),
}

/// Errors specific to getting dataset operations
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    /// The provided dataset name failed to parse according to the naming rules.
    ///
    /// This occurs during the initial validation of the dataset name string before
    /// any database or manifest operations are performed.
    #[error("Invalid dataset name '{name}': {source}")]
    InvalidDatasetName { name: String, source: NameError },

    /// Failed to retrieve the latest version for a dataset from the metadata database.
    ///
    /// This occurs when no specific version is provided and the system attempts to
    /// query the metadata database for the most recent version of the dataset.
    #[error("Failed to get latest version for dataset '{namespace}/{name}': {source}")]
    GetLatestVersion {
        namespace: String,
        name: String,
        source: metadata_db::Error,
    },

    /// Failed to resolve dataset revision to manifest hash.
    ///
    /// This occurs when attempting to resolve a revision (version tag, hash, or special tags
    /// like "latest" or "dev") to a concrete manifest hash. The resolution involves querying
    /// the metadata database and may fail due to connection issues, database unavailability,
    /// or permission problems.
    #[error("Failed to resolve revision for dataset '{reference}': {source}")]
    ResolveRevision {
        reference: Reference,
        #[source]
        source: ResolveRevisionError,
    },

    /// Failed to load dataset by manifest hash.
    ///
    /// This occurs when attempting to load a dataset from the manifest store and may fail due to:
    /// - Database query failures when retrieving the manifest path
    /// - Object store failures when fetching the manifest content
    /// - Manifest parsing errors (invalid JSON/TOML, schema mismatches)
    /// - Dataset kind parsing errors
    /// - Derived dataset creation errors
    #[error("Failed to load dataset '{reference}' by manifest hash: {source}")]
    LoadDatasetByHash {
        reference: Reference,
        #[source]
        source: GetDatasetByHashError,
    },

    /// Failed to parse the manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected schema
    /// - Required fields are missing or have incorrect types
    ///
    /// Can happen during parsing of the common manifest or any dataset-specific
    /// manifest type (EVM RPC, Firehose, Derived, SQL).
    #[error("Failed to parse manifest for dataset '{namespace}/{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestParseError {
        namespace: String,
        name: String,
        version: Option<String>,
        source: ManifestParseError,
    },

    /// The dataset kind specified in the manifest is not supported.
    ///
    /// This occurs when the `kind` field in the manifest contains a value that
    /// doesn't match any of the supported dataset types (evm-rpc, eth-beacon,
    /// firehose, derived, sql).
    #[error("Unsupported dataset kind '{kind}' for dataset '{namespace}/{name}' version '{}'", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        namespace: String,
        name: String,
        version: Option<String>,
        kind: String,
    },

    /// Failed to create a Derived dataset instance.
    ///
    /// This occurs when processing a derived dataset manifest, which may fail due to
    /// invalid SQL queries, dependency resolution issues, or logical errors in the
    /// dataset definition.
    #[error("Failed to create Derived dataset '{namespace}/{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    DerivedCreationError {
        namespace: String,
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Dataset not found.
    #[error("Dataset '{0}' not found")]
    DatasetNotFound(Reference),
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
    #[error("Failed to load dataset '{namespace}/{name}@{version}': {source}")]
    LoadDataset {
        namespace: String,
        name: String,
        version: String,
        source: GetDatasetError,
    },
}

/// Errors specific to getting derived dataset manifest operations
#[derive(Debug, thiserror::Error)]
pub enum GetDerivedManifestError {
    /// The provided dataset name failed to parse according to the naming rules.
    ///
    /// This occurs during the initial validation of the dataset name string before
    /// any database or manifest operations are performed.
    #[error("Invalid dataset name '{name}': {source}")]
    InvalidDatasetName { name: String, source: NameError },

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
    LoadManifestContent(#[source] crate::manifests::GetError),

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
    LoadManifestContent(#[source] crate::manifests::GetError),

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
    #[error("Dataset '{manifest_hash}' is missing required 'network' field for EvmRpc kind")]
    MissingNetwork { manifest_hash: String },

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

/// Errors that can occur when retrieving a manifest by hash
///
/// This error type is used by `DatasetStore::get_manifest_by_hash()`.
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    /// Failed to query manifest path from metadata database
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    /// Failed to retrieve manifest from object store
    #[error("Failed to retrieve manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::GetError),
}

/// Errors that can occur when deleting a manifest
///
/// This error type is used by `DatasetStore::delete_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum DeleteManifestError {
    /// Manifest is linked to one or more datasets and cannot be deleted
    ///
    /// Manifests must be unlinked from all datasets before deletion.
    #[error("Manifest is linked to datasets and cannot be deleted")]
    ManifestLinked,

    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to check if manifest is linked to datasets
    #[error("Failed to check if manifest is linked to datasets")]
    MetadataDbCheckLinks(#[source] metadata_db::Error),

    /// Failed to delete manifest from metadata database
    #[error("Failed to delete manifest from metadata database")]
    MetadataDbDelete(#[source] metadata_db::Error),

    /// Failed to delete manifest from object store
    #[error("Failed to delete manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::DeleteError),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// The manifest deletion was not persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Error when unlinking dataset manifests
///
/// This error type is used by `DatasetStore::unlink_dataset_manifests()`.
///
/// This occurs when failing to delete dataset manifest links from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete dataset manifest links from metadata database")]
pub struct UnlinkDatasetManifestsError(#[source] pub metadata_db::Error);

/// Error when listing version tags for a dataset
///
/// This error type is used by `DatasetStore::list_version_tags()`.
///
/// This occurs when failing to query version tags from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list version tags from metadata database")]
pub struct ListVersionTagsError(#[source] pub metadata_db::Error);

/// Error when listing all datasets
///
/// This error type is used by `DatasetStore::list_all_datasets()`.
///
/// This occurs when failing to query all datasets from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list all datasets from metadata database")]
pub struct ListAllDatasetsError(#[source] pub metadata_db::Error);

/// Error when deleting a version tag
///
/// This error type is used by `DatasetStore::delete_version_tag()`.
///
/// This occurs when failing to delete a version tag from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete version tag from metadata database")]
pub struct DeleteVersionTagError(#[source] pub metadata_db::Error);

/// Error when listing orphaned manifests
///
/// This error type is used by `DatasetStore::list_orphaned_manifests()`.
///
/// This occurs when failing to query orphaned manifests from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list orphaned manifests from metadata database")]
pub struct ListOrphanedManifestsError(#[source] pub metadata_db::Error);

/// Error when listing all registered manifests
///
/// This error type is used by `DatasetStore::list_all_manifests()`.
///
/// This occurs when failing to query all manifests from the metadata database,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to list all manifests from metadata database")]
pub struct ListAllManifestsError(#[source] pub metadata_db::Error);

/// Errors that occur when listing datasets that use a specific manifest
///
/// This error type is used by `DatasetStore::list_manifest_linked_datasets()`.
#[derive(Debug, thiserror::Error)]
pub enum ListDatasetsUsingManifestError {
    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to resolve manifest hash to file path fails
    /// - Database schema inconsistencies prevent path lookup
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    /// Failed to list dataset tags from metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to retrieve dataset tags fails
    /// - Database schema inconsistencies prevent tag retrieval
    #[error("Failed to list dataset tags from metadata database")]
    MetadataDbListTags(#[source] metadata_db::Error),
}

/// Error when checking if a manifest is linked to a dataset
///
/// This error type is used by `DatasetStore::is_manifest_linked()`.
///
/// This occurs when the database query to check manifest linkage fails,
/// typically due to database connection issues, unavailability, or permission problems.
#[derive(Debug, thiserror::Error)]
#[error("Failed to check if manifest is linked to dataset")]
pub struct IsManifestLinkedError(#[source] pub metadata_db::Error);
