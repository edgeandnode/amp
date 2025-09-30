use common::{BoxError, query_context};
use datasets_common::{
    name::{Name, NameError},
    version::Version,
};

use crate::{
    DatasetKind,
    manifests::{ManifestParseError, StoreError},
    providers::ParseConfigError,
};

/// Error for dataset registration check operations
#[derive(Debug, thiserror::Error)]
#[error("Failed to check dataset registration in metadata database: {0}")]
pub struct IsRegisteredError(#[source] pub metadata_db::Error);

/// Errors specific to manifest registration operations
#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    /// Dataset already exists in the registry
    #[error("Dataset '{name}' version '{version}' already registered")]
    DatasetExists { name: Name, version: Version },

    /// Failed to serialize manifest to JSON
    #[error("Failed to serialize manifest to JSON: {0}")]
    ManifestSerialization(#[source] serde_json::Error),

    /// Failed to store manifest in dataset definitions store
    #[error("Failed to store manifest in dataset definitions store: {0}")]
    ManifestStorage(#[source] object_store::Error),

    /// Failed to register dataset in metadata database
    #[error("Failed to register dataset in metadata database: {0}")]
    MetadataRegistration(#[source] metadata_db::Error),

    /// Failed to check if dataset is registered in metadata database
    #[error("Failed to check dataset registration in metadata database: {0}")]
    ExistenceCheck(#[source] metadata_db::Error),
}

impl From<IsRegisteredError> for RegisterManifestError {
    fn from(IsRegisteredError(err): IsRegisteredError) -> Self {
        RegisterManifestError::ExistenceCheck(err)
    }
}

impl From<StoreError> for RegisterManifestError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::ManifestSerialization(e) => RegisterManifestError::ManifestSerialization(e),
            StoreError::ManifestStorage(e) => RegisterManifestError::ManifestStorage(e),
        }
    }
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
    #[error("Failed to get latest version for dataset '{name}': {source}")]
    GetLatestVersion {
        name: String,
        source: metadata_db::Error,
    },

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
    ///
    /// Can happen during parsing of the common manifest or any dataset-specific
    /// manifest type (EVM RPC, Firehose, Substreams, Derived, SQL).
    #[error("Failed to parse manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    ManifestParseError {
        name: String,
        version: Option<String>,
        source: ManifestParseError,
    },

    /// The dataset kind specified in the manifest is not supported.
    ///
    /// This occurs when the `kind` field in the manifest contains a value that
    /// doesn't match any of the supported dataset types (evm-rpc, eth-beacon,
    /// firehose, substreams, derived, sql).
    #[error("Unsupported dataset kind '{kind}' for dataset '{name}' version '{}'", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        name: String,
        version: Option<String>,
        kind: String,
    },

    /// The schema in the Substreams manifest doesn't match the inferred schema.
    ///
    /// This occurs specifically for Substreams datasets when the schema defined in
    /// the manifest differs from the schema automatically inferred from the dataset's
    /// table definitions.
    #[error("Schema mismatch for Substreams dataset '{name}' version '{}'", version.as_deref().unwrap_or("latest"))]
    SchemaMismatch {
        name: String,
        version: Option<String>,
    },

    /// Failed to create a Substreams dataset instance.
    ///
    /// This occurs during the asynchronous initialization of a Substreams dataset,
    /// which may fail due to issues with the Substreams package, connection problems,
    /// or invalid configuration.
    #[error("Failed to create Substreams dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SubstreamsCreationError {
        name: String,
        version: Option<String>,
        source: firehose_datasets::Error,
    },

    /// Failed to create a Derived dataset instance.
    ///
    /// This occurs when processing a derived dataset manifest, which may fail due to
    /// invalid SQL queries, dependency resolution issues, or logical errors in the
    /// dataset definition.
    #[error("Failed to create Derived dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    DerivedCreationError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to retrieve SQL files for a SQL dataset.
    ///
    /// This occurs specifically for SQL datasets when fetching the associated SQL
    /// query files from the manifest store fails, which could be due to missing files,
    /// permissions, or storage issues.
    #[error("Failed to get SQL files for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SqlFilesError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to create a SQL dataset instance.
    ///
    /// This occurs during SQL dataset initialization, which may fail due to invalid
    /// SQL syntax, missing dependencies, circular references, or other SQL processing
    /// errors.
    #[error("Failed to create SQL dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SqlDatasetError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },
}

/// Error that occurs when getting all datasets from the manifest store.
///
/// This error wraps a `GetDatasetError` that occurred while getting one of the
/// datasets during the iteration through all registered datasets.
#[derive(Debug, thiserror::Error)]
#[error("Failed to get dataset: {0}")]
pub struct GetAllDatasetsError(#[from] pub GetDatasetError);

/// Errors specific to getting SQL dataset operations (SQL and Derived dataset kinds)
#[derive(Debug, thiserror::Error)]
pub enum GetSqlDatasetError {
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
    /// indicates a different kind (e.g., evm-rpc, firehose, substreams).
    /// Only SQL and Derived dataset kinds can be retrieved as SQL datasets.
    #[error("Dataset '{name}' version '{}' has unsupported kind '{kind}' for SQL dataset retrieval (expected 'sql' or 'derived')", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        name: String,
        version: Option<String>,
        kind: String,
    },

    /// Failed to parse SQL queries in a Derived dataset.
    ///
    /// This occurs specifically for Derived datasets when the SQL queries defined
    /// in the manifest contain syntax errors or are otherwise invalid.
    #[error("Failed to parse SQL queries for Derived dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SqlParseError {
        name: String,
        version: Option<String>,
        source: query_context::Error,
    },

    /// Failed to retrieve SQL files for a SQL dataset.
    ///
    /// This occurs specifically for SQL datasets when fetching the associated SQL
    /// query files from the manifest store fails, which could be due to missing files,
    /// permissions, or storage issues.
    #[error("Failed to get SQL files for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SqlFilesError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to create a SQL dataset instance.
    ///
    /// This occurs during SQL dataset initialization, which may fail due to invalid
    /// SQL syntax, missing dependencies, circular references, or other SQL processing
    /// errors.
    #[error("Failed to create SQL dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SqlDatasetError {
        name: String,
        version: Option<String>,
        source: BoxError,
    },

    /// Failed to create a Derived dataset instance.
    ///
    /// This occurs when processing a derived dataset manifest, which may fail due to
    /// invalid configuration, dependency resolution issues, or logical errors in the
    /// dataset definition.
    #[error("Failed to create Derived dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    DerivedCreationError {
        name: String,
        version: Option<String>,
        source: BoxError,
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

    /// Failed to parse the dataset-specific manifest file content.
    ///
    /// This occurs when parsing the specific manifest for Substreams datasets fails.
    #[error("Failed to parse Substreams manifest for dataset '{name}' version '{}': {source}", version.as_deref().unwrap_or("latest"))]
    SubstreamsManifestParseError {
        name: String,
        version: Option<String>,
        source: ManifestParseError,
    },

    /// The dataset kind is not a raw dataset type.
    ///
    /// This occurs when trying to get a client for a dataset that is not a raw data source.
    /// Only raw dataset kinds (evm-rpc, eth-beacon, firehose, substreams) can have clients retrieved.
    /// SQL and Derived datasets cannot have clients as they are views over other datasets.
    #[error("Dataset '{name}' version '{}' has unsupported kind '{kind}' for client retrieval (expected raw dataset: evm-rpc, eth-beacon, firehose, or substreams)", version.as_deref().unwrap_or("latest"))]
    UnsupportedKind {
        name: String,
        version: Option<String>,
        kind: String,
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
    /// expected type for the dataset kind (EvmRpc, EthBeacon, Firehose, or Substreams).
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

    /// Failed to create a Substreams client.
    ///
    /// This occurs during initialization of the Substreams client, which may fail due to
    /// invalid package references, connection issues, or authentication failures.
    #[error("Failed to create Substreams client for dataset '{name}': {source}")]
    SubstreamsClientError { name: String, source: BoxError },
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

    /// The dataset name extracted from the table reference is invalid.
    ///
    /// This occurs when the schema portion of the table reference contains an invalid
    /// dataset name according to the dataset naming rules.
    #[error("Found invalid dataset name '{name}' in dataset schema '{schema}': {source}")]
    InvalidDatasetName {
        name: String,
        schema: String,
        #[source]
        source: NameError,
    },

    /// The version string in the dataset schema is invalid.
    ///
    /// Version strings must follow the format `x_y_z` where x, y, and z are integers.
    #[error(
        "Found invalid version '{version}' in dataset schema '{schema}', version must be in the form 'x_y_z' where x, y, z are integers"
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

    /// The dataset name extracted from the function qualifier is invalid.
    ///
    /// This occurs when the qualifier portion of the function name contains an invalid
    /// dataset name according to the dataset naming rules.
    #[error("Found invalid dataset name '{name}' in function dataset name '{function}': {source}")]
    InvalidDatasetName {
        name: String,
        function: String,
        #[source]
        source: NameError,
    },

    /// The version string in the function qualifier is invalid.
    ///
    /// Version strings must follow the format `x_y_z` where x, y, and z are integers.
    #[error(
        "Found invalid version '{version}' in function dataset name '{function}', version must be in the form 'x_y_z' where x, y, z are integers"
    )]
    InvalidVersion { version: String, function: String },
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
