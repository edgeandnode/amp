use datasets_common::name::Name;

use crate::BoxError;

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
    GetPhysicalCatalog(#[source] GetPhysicalCatalogError),
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
    GetLogicalCatalog(#[source] GetLogicalCatalogError),
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

    /// Failed to get a dataset.
    ///
    /// This wraps errors from `get_dataset`, which can occur when:
    /// - The manifest is invalid
    /// - The dataset kind is unsupported
    #[error("Failed to get dataset: {0}")]
    GetDataset(#[source] BoxError),

    /// Failed to create ETH call UDF for a dataset.
    ///
    /// This occurs when creating the eth_call user-defined function for an EVM RPC dataset
    /// fails, typically due to invalid provider configuration or connection issues.
    #[error("Failed to create ETH call UDF for dataset '{dataset}': {source}")]
    EthCallUdfCreation { dataset: Name, source: BoxError },
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
        source: datasets_common::partial_reference::PartialReferenceError,
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
        source: datasets_common::partial_reference::PartialReferenceError,
    },

    /// The version string in the function qualifier is invalid.
    ///
    /// This occurs when the revision is not a semantic version (e.g., hash, latest, dev).
    #[error(
        "Invalid version '{version}' in function '{function}', only semantic versions are supported"
    )]
    InvalidVersion { version: String, function: String },
}
