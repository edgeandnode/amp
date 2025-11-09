use datafusion::error::DataFusionError;
use datasets_common::{reference::Reference, table_name::TableName};

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
///
/// This error type is used exclusively by `planning_ctx_for_sql()` to create
/// a planning context for SQL queries without requiring physical data to exist.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum PlanningCtxForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when DataFusion's table reference resolver encounters issues:
    /// - The SQL statement contains invalid table references
    /// - Table names cannot be parsed or extracted
    /// - The SQL syntax is malformed for table resolution
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] DataFusionError),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when analyzing the SQL AST to find function calls:
    /// - The SQL statement contains invalid function syntax
    /// - Function names cannot be traversed from the AST
    /// - DML statements are encountered (which are not supported)
    #[error("Failed to extract function names from SQL")]
    FunctionNameExtraction(#[source] BoxError),

    /// Table reference is catalog-qualified.
    ///
    /// This occurs when a table reference includes a catalog qualifier.
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error(
        "Catalog-qualified table '{table_ref}' not supported, tables must only be qualified with a dataset"
    )]
    CatalogQualifiedTable { table_ref: String },

    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable { table_ref: String },

    /// Table name is invalid.
    ///
    /// This occurs when the table name portion of a table reference does not
    /// conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name '{table_name}' in table reference '{table_ref}'")]
    InvalidTableName {
        table_name: String,
        table_ref: String,
        #[source]
        source: datasets_common::table_name::TableNameError,
    },

    /// Failed to parse schema portion of table reference as PartialReference.
    ///
    /// This occurs when the schema portion of a table reference cannot be parsed
    /// as a valid dataset reference (namespace/name@version format).
    #[error("Invalid schema reference '{schema}' in table reference")]
    InvalidSchemaReference {
        schema: String,
        #[source]
        source: datasets_common::partial_reference::PartialReferenceError,
    },

    /// Failed to resolve dataset reference to a hash.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash, typically due to:
    /// - Storage backend errors
    /// - Invalid reference format
    #[error("Failed to resolve dataset reference '{reference}' to hash")]
    ResolveHash {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Dataset reference could not be found.
    ///
    /// This occurs when the dataset store successfully processed the reference
    /// but no matching dataset exists:
    /// - Dataset does not exist in the store
    /// - Version not found
    #[error("Dataset reference '{reference}' not found")]
    DatasetNotFound { reference: Reference },

    /// Failed to retrieve a dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}'")]
    GetDataset {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for an EVM RPC dataset.
    ///
    /// This occurs when creating the eth_call user-defined function for a dataset:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error("Failed to create ETH call UDF for dataset '{reference}'")]
    EthCallUdfCreation {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to parse dataset qualifier in function name.
    ///
    /// This occurs when a qualified function name's dataset portion cannot be parsed
    /// as a valid dataset reference.
    #[error("Invalid function reference '{function}', could not parse dataset qualifier")]
    InvalidFunctionReference {
        function: String,
        #[source]
        source: datasets_common::partial_reference::PartialReferenceError,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        table_name: TableName,
        reference: Reference,
    },

    /// Function name has invalid format.
    ///
    /// Function names must be either:
    /// - Unqualified (assumed to be DataFusion built-in): `function_name`
    /// - Dataset-qualified: `dataset.function_name`
    ///
    /// This error occurs when a function has more than two parts.
    #[error("Invalid function format '{function}', expected 'dataset.function' or 'function'")]
    InvalidFunctionFormat { function: String },
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
#[allow(clippy::large_enum_variant)]
pub enum GetLogicalCatalogError {
    /// Table reference is catalog-qualified.
    ///
    /// This occurs when a table reference includes a catalog qualifier.
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error(
        "Catalog-qualified table '{table_ref}' not supported, tables must only be qualified with a dataset"
    )]
    CatalogQualifiedTable { table_ref: String },

    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable { table_ref: String },

    /// Table name is invalid.
    ///
    /// This occurs when the table name portion of a table reference does not
    /// conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name '{table_name}' in table reference '{table_ref}'")]
    InvalidTableName {
        table_name: String,
        table_ref: String,
        #[source]
        source: datasets_common::table_name::TableNameError,
    },

    /// Failed to parse schema portion of table reference as PartialReference.
    ///
    /// This occurs when the schema portion of a table reference cannot be parsed
    /// as a valid dataset reference (namespace/name@version format).
    #[error("Invalid schema reference '{schema}' in table reference")]
    InvalidSchemaReference {
        schema: String,
        #[source]
        source: datasets_common::partial_reference::PartialReferenceError,
    },

    /// Failed to resolve dataset reference to a hash.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash, typically due to:
    /// - Storage backend errors
    /// - Invalid reference format
    #[error("Failed to resolve dataset reference '{reference}' to hash")]
    ResolveHash {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Dataset reference could not be found.
    ///
    /// This occurs when the dataset store successfully processed the reference
    /// but no matching dataset exists:
    /// - Dataset does not exist in the store
    /// - Version not found
    #[error("Dataset reference '{reference}' not found")]
    DatasetNotFound { reference: Reference },

    /// Failed to retrieve a dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}'")]
    GetDataset {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for an EVM RPC dataset.
    ///
    /// This occurs when creating the eth_call user-defined function for a dataset:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error("Failed to create ETH call UDF for dataset '{reference}'")]
    EthCallUdfCreation {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to parse dataset qualifier in function name.
    ///
    /// This occurs when a qualified function name's dataset portion cannot be parsed
    /// as a valid dataset reference.
    #[error("Invalid function reference '{function}', could not parse dataset qualifier")]
    InvalidFunctionReference {
        function: String,
        #[source]
        source: datasets_common::partial_reference::PartialReferenceError,
    },

    /// Function name has invalid format.
    ///
    /// Function names must be either:
    /// - Unqualified (assumed to be DataFusion built-in): `function_name`
    /// - Dataset-qualified: `dataset.function_name`
    ///
    /// This error occurs when a function has more than two parts.
    #[error("Invalid function format '{function}', expected 'dataset.function' or 'function'")]
    InvalidFunctionFormat { function: String },
}
