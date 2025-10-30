use common::BoxError;
use datasets_common::{
    name::{Name, NameError},
    partial_reference::PartialReferenceError,
    version::Version,
};

use crate::{
    DatasetKind,
    manifests::{ManifestParseError, StoreError},
    providers::ParseConfigError,
};

/// Errors specific to manifest registration operations
#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    /// Failed to store manifest in dataset definitions store
    #[error("Failed to store manifest in dataset definitions store")]
    ManifestStorage(#[from] StoreError),

    /// Failed to register dataset in metadata database
    #[error("Failed to register dataset in metadata database")]
    MetadataRegistration(#[source] metadata_db::Error),

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

    /// Failed to retrieve the manifest file from the manifest store.
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to retrieve manifest for dataset '{namespace}/{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestRetrievalError {
        namespace: String,
        name: String,
        version: Option<String>,
        source: BoxError,
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

    /// Failed to retrieve the manifest file from the manifest store.
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to retrieve manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestRetrievalError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to parse the manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected schema
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestParseError {
        name: String,
        version: Option<String>,
        source: ManifestParseError,
    },

    /// The dataset kind is not SQL or Derived.
    ///
    /// This occurs when trying to get a dataset as a SQL dataset but the manifest
    /// indicates a different kind (e.g., evm-rpc, firehose).
    /// Only SQL and Derived dataset kinds can be retrieved as SQL datasets.
    #[error("Dataset '{name}' version '{}' has unsupported kind '{kind}' for SQL dataset retrieval (expected 'sql' or 'derived')", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        name: String,
        version: Option<String>,
        kind: String,
    },
}

/// Errors specific to getting client operations for raw datasets
#[derive(Debug, thiserror::Error)]
pub enum GetClientError {
    /// The provided dataset name failed to parse according to the naming rules.
    ///
    /// This occurs during the initial validation of the dataset name string before
    /// any database or manifest operations are performed.
    #[error("Invalid dataset name '{name}': {source}")]
    InvalidDatasetName { name: String, source: NameError },

    /// Failed to retrieve the manifest file from the manifest store.
    ///
    /// This occurs when the object store operation to fetch the manifest file fails,
    /// which could be due to network issues, permissions, or storage backend problems.
    #[error("Failed to retrieve manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestRetrievalError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to parse the common manifest file content.
    ///
    /// This occurs when:
    /// - The manifest file contains invalid JSON or TOML syntax
    /// - The manifest structure doesn't match the expected schema
    /// - Required fields are missing or have incorrect types
    #[error("Failed to parse common manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    CommonManifestParseError {
        name: String,
        version: Option<String>,
        source: ManifestParseError,
    },

    /// The dataset kind is not a raw dataset type.
    ///
    /// This occurs when trying to get a client for a dataset that is not a raw data source.
    /// Only raw dataset kinds (evm-rpc, eth-beacon, firehose) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error("Dataset '{name}' version '{}' has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc, eth-beacon, or firehose)", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        name: String,
        version: Option<String>,
        kind: String,
    },

    /// Dataset is missing the required 'network' field.
    ///
    /// This occurs when a raw dataset definition (evm-rpc, eth-beacon, or firehose)
    /// does not include the network field, which is required to determine the appropriate provider configuration.
    #[error("Dataset '{name}' version '{}' is missing required 'network' field for raw dataset kind", version.as_deref().unwrap_or("latest"))]
    MissingNetwork {
        name: String,
        version: Option<String>,
    },

    /// No provider configuration found for the dataset kind and network combination.
    ///
    /// This occurs when:
    /// - No provider is configured for the specific kind-network pair
    /// - All providers for this kind-network are disabled
    /// - Provider configuration files are missing or invalid
    #[error(
        "No provider found for dataset '{name}' with kind '{dataset_kind}' and network '{network}'"
    )]
    ProviderNotFound {
        name: String,
        dataset_kind: DatasetKind,
        network: String,
    },

    /// Failed to parse the provider configuration.
    ///
    /// This occurs when the provider configuration cannot be deserialized into the
    /// expected type for the dataset kind (EvmRpc, EthBeacon, or Firehose).
    #[error("Failed to parse provider configuration for dataset '{name}': {source}")]
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
}

/// Errors specific to get_logical_catalog operations
#[derive(Debug, thiserror::Error)]
pub enum GetLogicalCatalogError {
    /// Failed to extract dataset names and versions from table references.
    ///
    /// This occurs when parsing table references to extract dataset information fails,
    /// typically due to invalid table reference formats or naming convention violations.
    #[error("Failed to extract datasets from table references: {0}")]
    ExtractDatasetFromTableRefs(#[source] ExtractDatasetFromTableRefsError),

    /// Failed to extract dataset names and versions from function names.
    ///
    /// This occurs when parsing qualified function names to extract dataset information fails,
    /// typically due to invalid function name formats or naming convention violations.
    #[error("Failed to extract datasets from function names: {0}")]
    ExtractDatasetFromFunctionNames(#[source] ExtractDatasetFromFunctionNamesError),

    /// Dataset not found.
    ///
    /// This occurs when a dataset referenced in the query does not exist in the dataset store.
    #[error("Dataset '{name}' version '{}' not found", version.as_ref().map(|v| v.to_string()).unwrap_or_else(|| "latest".to_string()))]
    DatasetNotFound {
        name: String,
        version: Option<Version>,
    },

    /// Failed to get a dataset.
    ///
    /// This wraps errors from `get_dataset`, which can occur when:
    /// - The manifest is invalid
    /// - The dataset kind is unsupported
    #[error("Failed to get dataset: {0}")]
    GetDataset(#[source] GetDatasetError),

    /// Failed to create ETH call UDF for a dataset.
    ///
    /// This occurs when creating the eth_call user-defined function for an EVM RPC dataset
    /// fails, typically due to invalid provider configuration or connection issues.
    #[error("Failed to create ETH call UDF for dataset '{dataset}': {source}")]
    EthCallUdfCreation {
        dataset: String,
        source: EthCallForDatasetError,
    },
}

/// Errors specific to get_physical_catalog operations
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum GetPhysicalCatalogError {
    /// Failed to get the logical catalog.
    ///
    /// This wraps errors from `get_logical_catalog`, which can occur when:
    /// - Dataset names cannot be extracted from table references or function names
    /// - Dataset retrieval fails
    /// - UDF creation fails
    #[error("Failed to get logical catalog: {0}")]
    GetLogicalCatalog(#[source] GetLogicalCatalogError),

    /// Failed to retrieve physical table metadata from the metadata database.
    ///
    /// This occurs when querying the metadata database for the active physical
    /// location of a table fails due to database connection issues, query errors,
    /// or other database-related problems.
    #[error("Failed to retrieve physical table metadata for table '{table}': {source}")]
    PhysicalTableRetrieval { table: String, source: BoxError },

    /// Table has not been synced and no physical location exists.
    ///
    /// This occurs when attempting to load a physical catalog for a table that
    /// has been defined but has not yet been dumped/synced to storage. The table
    /// exists in the dataset definition but has no physical parquet files.
    #[error("Table '{table}' has not been synced")]
    TableNotSynced { table: String },
}

/// Errors specific to catalog_for_sql operations
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum CatalogForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains invalid table references
    /// - Table names cannot be parsed or extracted
    /// - The SQL syntax is malformed for table resolution
    #[error("Failed to resolve table references from SQL: {source}")]
    TableReferenceResolution { source: BoxError },

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains invalid function calls
    /// - Function names cannot be parsed or extracted
    /// - The SQL syntax is malformed for function analysis
    #[error("Failed to extract function names from SQL: {source}")]
    FunctionNameExtraction { source: BoxError },

    /// Failed to get the physical catalog for the resolved tables and functions.
    ///
    /// This wraps errors from `get_physical_catalog`, which can occur when:
    /// - Dataset retrieval fails
    /// - Physical table metadata cannot be retrieved
    /// - Tables have not been synced
    #[error("Failed to get physical catalog: {0}")]
    GetPhysicalCatalog(#[from] GetPhysicalCatalogError),
}

/// Errors specific to planning_ctx_for_sql operations
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum PlanningCtxForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains invalid table references
    /// - Table names cannot be parsed or extracted
    /// - The SQL syntax is malformed for table resolution
    #[error("Failed to resolve table references from SQL: {source}")]
    TableReferenceResolution { source: BoxError },

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains invalid function calls
    /// - Function names cannot be parsed or extracted
    /// - The SQL syntax is malformed for function analysis
    #[error("Failed to extract function names from SQL: {source}")]
    FunctionNameExtraction { source: BoxError },

    /// Failed to get the logical catalog for the resolved tables and functions.
    ///
    /// This wraps errors from `get_logical_catalog`, which can occur when:
    /// - Dataset names cannot be extracted from table references or function names
    /// - Dataset retrieval fails
    /// - UDF creation fails
    #[error("Failed to get logical catalog: {0}")]
    GetLogicalCatalog(#[from] GetLogicalCatalogError),
}

/// Errors that occur when extracting dataset names and versions from table references.
///
/// This error type is used by the `dataset_versions_from_table_refs` function when
/// parsing table references from SQL queries to extract dataset information.
#[derive(Debug, thiserror::Error)]
pub enum ExtractDatasetFromTableRefsError {
    /// Table is qualified with a catalog, which is not supported.
    ///
    /// Only dataset-qualified tables are allowed (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error(
        "Found table qualified with catalog '{table}', tables must only be qualified with a dataset name"
    )]
    CatalogQualifiedTable { table: String },

    /// Table is not qualified with a dataset name.
    ///
    /// All tables must be qualified with a dataset name (e.g., `dataset.table`).
    /// Unqualified tables (e.g., just `table`) are not allowed.
    #[error("Found unqualified table '{table}', all tables must be qualified with a dataset name")]
    UnqualifiedTable { table: String },

    /// Failed to parse the dataset reference from the table schema.
    ///
    /// This occurs when the schema portion contains an invalid reference format.
    /// Expected format: `namespace/name@version` or `namespace/name`
    #[error("Failed to parse dataset reference '{schema}': {source}")]
    ReferenceParse {
        schema: String,
        #[source]
        source: PartialReferenceError,
    },

    /// The version string in the dataset schema is invalid.
    ///
    /// This occurs when the revision is not a semantic version (e.g., hash, latest, dev).
    #[error(
        "Invalid version '{version}' in dataset schema '{schema}', only semantic versions are supported"
    )]
    InvalidVersion { version: String, schema: String },
}

/// Errors that occur when extracting dataset names and versions from function names.
///
/// This error type is used by the `dataset_versions_from_function_names` function when
/// parsing qualified function names from SQL queries to extract dataset information.
#[derive(Debug, thiserror::Error)]
pub enum ExtractDatasetFromFunctionNamesError {
    /// Function name has an invalid format.
    ///
    /// Function names can be:
    /// - Simple names (no qualifier) - assumed to be built-in DataFusion functions
    /// - Two-part names: `dataset.function` or `dataset__x_y_z.function`
    ///
    /// This error occurs when a function has more than two parts (e.g., `a.b.c.function`).
    #[error(
        "Invalid function format '{function}', expected either 'function' or 'dataset.function'"
    )]
    InvalidFunctionFormat { function: String },

    /// Failed to parse the dataset reference from the function qualifier.
    ///
    /// This occurs when the qualifier portion contains an invalid reference format.
    /// Expected format: `namespace/name@version` or `namespace/name`
    #[error("Failed to parse dataset reference from function '{function}': {source}")]
    ReferenceParse {
        function: String,
        source: PartialReferenceError,
    },

    /// The version string in the function qualifier is invalid.
    ///
    /// This occurs when the revision is not a semantic version (e.g., hash, latest, dev).
    #[error(
        "Invalid version '{version}' in function '{function}', only semantic versions are supported"
    )]
    InvalidVersion { version: String, function: String },
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
    #[error(
        "Dataset '{dataset_name}' version '{dataset_version}' is missing required 'network' field for EvmRpc kind"
    )]
    MissingNetwork {
        dataset_name: Name,
        dataset_version: Version,
    },

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
