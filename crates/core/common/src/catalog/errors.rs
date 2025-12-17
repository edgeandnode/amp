use datasets_common::{
    hash_reference::HashReference, partial_reference::PartialReferenceError, reference::Reference,
    table_name::TableName,
};

use crate::{
    BoxError,
    sql::{ResolveFunctionReferencesError, ResolveTableReferencesError},
};

#[derive(Debug, thiserror::Error)]
pub enum CatalogForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    /// - Schema portion fails to parse as PartialReference
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError<PartialReferenceError>),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    /// - Schema portion fails to parse as PartialReference
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError<PartialReferenceError>),

    /// Failed to get the logical catalog.
    ///
    /// This wraps errors from `get_logical_catalog`, which can occur when:
    /// - Dataset names cannot be extracted from table references or function names
    /// - Dataset retrieval fails
    /// - UDF creation fails
    #[error("Failed to get logical catalog")]
    GetLogicalCatalog(#[source] GetLogicalCatalogError),

    /// Failed to retrieve physical table metadata from the metadata database.
    ///
    /// This occurs when querying the metadata database for the active physical
    /// location of a table fails due to database connection issues, query errors,
    /// or other database-related problems.
    #[error("Failed to retrieve physical table metadata for table '{table}'")]
    PhysicalTableRetrieval {
        table: String,
        #[source]
        source: BoxError,
    },

    /// Table has not been synced and no physical location exists.
    ///
    /// This occurs when attempting to load a physical catalog for a table that
    /// has been defined but has not yet been dumped/synced to storage. The table
    /// exists in the dataset definition but has no physical parquet files.
    #[error("Table '{table}' has not been synced")]
    TableNotSynced { table: String },
}

impl CatalogForSqlError {
    /// Returns true if this error is due to a table not being found in a dataset.
    pub fn is_table_not_found(&self) -> bool {
        matches!(
            self,
            CatalogForSqlError::GetLogicalCatalog(
                GetLogicalCatalogError::TableNotFoundInDataset { .. }
            )
        )
    }
}

/// Errors specific to planning_ctx_for_sql_tables_with_deps operations
///
/// This error type is used exclusively by `planning_ctx_for_sql_tables_with_deps()` to create
/// a planning context for SQL tables with external dependencies.
#[derive(Debug, thiserror::Error)]
pub enum PlanningCtxForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    /// - Schema portion fails to parse as PartialReference
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError<PartialReferenceError>),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when analyzing the SQL AST to find function calls:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    /// - A function name has an invalid format (more than 2 parts)
    /// - Schema portion fails to parse as PartialReference
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError<PartialReferenceError>),

    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable { table_ref: String },

    /// Failed to resolve dataset reference to a hash reference.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash. Common causes include:
    /// - Dataset does not exist in the store
    /// - Version tag not found
    /// - Storage backend errors
    /// - Invalid reference format
    /// - Database connection issues
    #[error("Failed to resolve dataset reference '{reference}'")]
    ResolveDatasetReference {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to load dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails. Common causes include:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    /// - Manifest file not found in object store
    #[error("Failed to load dataset '{reference}'")]
    LoadDataset {
        reference: HashReference,
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
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        table_name: TableName,
        reference: HashReference,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error("Function '{function_name}' not found in dataset '{reference}'")]
    FunctionNotFoundInDataset {
        function_name: String,
        reference: HashReference,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable { reference: HashReference },
}

/// Errors specific to catalog_for_sql_with_deps operations
///
/// This error type is used exclusively by `catalog_for_sql_with_deps()` to create
/// a physical catalog for SQL query execution with pre-resolved dependencies.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum GetLogicalCatalogError {
    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable { table_ref: String },

    /// Failed to resolve dataset reference to a hash reference.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash. Common causes include:
    /// - Dataset does not exist in the store
    /// - Version tag not found
    /// - Storage backend errors
    /// - Invalid reference format
    /// - Database connection issues
    #[error("Failed to resolve dataset reference '{reference}'")]
    ResolveDatasetReference {
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to load dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails. Common causes include:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    /// - Manifest file not found in object store
    #[error("Failed to load dataset '{reference}'")]
    LoadDataset {
        reference: HashReference,
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
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error("Function '{function_name}' not found in dataset '{reference}'")]
    FunctionNotFoundInDataset {
        function_name: String,
        reference: HashReference,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable { reference: HashReference },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        table_name: TableName,
        reference: HashReference,
    },
}
