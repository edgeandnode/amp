use datasets_common::{hash_reference::HashReference, reference::Reference, table_name::TableName};
use datasets_derived::dep_alias::DepAlias;

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
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError),

    /// Failed to get the physical catalog for the resolved tables and functions.
    ///
    /// This wraps errors from `get_physical_catalog`, which can occur when:
    /// - Dataset retrieval fails
    /// - Physical table metadata cannot be retrieved
    /// - Tables have not been synced
    #[error("Failed to get physical catalog: {0}")]
    GetPhysicalCatalog(#[source] GetPhysicalCatalogError),
}

/// Errors specific to planning_ctx_for_sql_tables_with_deps operations
///
/// This error type is used exclusively by `planning_ctx_for_sql_tables_with_deps()` to create
/// a planning context for SQL tables with external dependencies.
#[derive(Debug, thiserror::Error)]
pub enum PlanningCtxForSqlTablesWithDepsError {
    /// Table reference is catalog-qualified.
    ///
    /// This occurs when a table reference includes a catalog qualifier.
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error(
        "In table '{table_name}': Catalog-qualified table '{table_ref}' not supported, tables must only be qualified with a dataset"
    )]
    CatalogQualifiedTable {
        table_name: TableName,
        table_ref: String,
    },

    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error(
        "In table '{table_name}': Unqualified table '{table_ref}', all tables must be qualified with a dataset"
    )]
    UnqualifiedTable {
        table_name: TableName,
        table_ref: String,
    },

    /// Dataset reference could not be found when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset referenced in a table reference fails
    /// because the dataset does not exist in the store.
    #[error("In table '{table_name}': Dataset reference '{reference}' not found")]
    DatasetNotFoundForTableRef {
        table_name: TableName,
        reference: HashReference,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}'")]
    GetDatasetForTableRef {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Schema portion of table reference is not a valid dependency alias.
    ///
    /// This occurs when the schema/dataset portion of a table reference does not
    /// conform to dependency alias rules (must start with letter, contain only
    /// alphanumeric/underscore, and be <= 63 bytes).
    #[error(
        "In table '{table_name}': Invalid dependency alias '{invalid_alias}' in table reference '{table_ref}'"
    )]
    InvalidDependencyAliasForTableRef {
        table_name: TableName,
        invalid_alias: String,
        table_ref: String,
        #[source]
        source: datasets_derived::dep_alias::DepAliasError,
    },

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in table but not provided in dependencies"
    )]
    DependencyAliasNotFoundForTableRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Dataset reference could not be found when loading dataset for function.
    ///
    /// This occurs when loading a dataset referenced in a function name fails
    /// because the dataset does not exist in the store.
    #[error("In table '{table_name}': Dataset reference '{reference}' not found for function")]
    DatasetNotFoundForFunction {
        table_name: TableName,
        reference: HashReference,
    },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}' for function")]
    GetDatasetForFunction {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for dataset referenced in function name.
    ///
    /// This occurs when creating the eth_call user-defined function for a function fails:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error(
        "In table '{table_name}': Failed to create ETH call UDF for dataset '{reference}' for function"
    )]
    EthCallUdfCreationForFunction {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error(
        "In table '{table_name}': Function '{function_name}' not found in dataset '{reference}'"
    )]
    FunctionNotFoundInDataset {
        table_name: TableName,
        function_name: String,
        reference: HashReference,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("In table '{table_name}': Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable {
        table_name: TableName,
        reference: HashReference,
    },

    /// Schema portion of function reference is not a valid dependency alias.
    ///
    /// This occurs when the schema/dataset portion of a function reference does not
    /// conform to dependency alias rules (must start with letter, contain only
    /// alphanumeric/underscore, and be <= 63 bytes).
    #[error(
        "In table '{table_name}': Invalid dependency alias '{invalid_alias}' in function reference '{func_ref}'"
    )]
    InvalidDependencyAliasForFunctionRef {
        table_name: TableName,
        invalid_alias: String,
        func_ref: String,
        #[source]
        source: datasets_derived::dep_alias::DepAliasError,
    },

    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in function but not provided in dependencies"
    )]
    DependencyAliasNotFoundForFunctionRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error(
        "In table '{table_name}': Table '{referenced_table_name}' not found in dataset '{reference}'"
    )]
    TableNotFoundInDataset {
        table_name: TableName,
        referenced_table_name: TableName,
        reference: HashReference,
    },
}

/// Errors specific to planning_ctx_for_sql operations
///
/// This error type is used exclusively by `planning_ctx_for_sql()` to create
/// a planning context for SQL queries without requiring physical data to exist.
#[derive(Debug, thiserror::Error)]
pub enum PlanningCtxForSqlError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when analyzing the SQL AST to find function calls:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    /// - A function name has an invalid format (more than 2 parts)
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError),

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

    /// Unknown dataset reference during resolution.
    ///
    /// This occurs when the dataset store successfully processed the reference
    /// but no matching dataset exists:
    /// - Dataset does not exist in the store
    /// - Version not found
    #[error("Unknown dataset reference '{reference}'")]
    UnknownDatasetReference { reference: Reference },

    /// Dataset not found after hash resolution.
    ///
    /// This occurs when a dataset reference was successfully resolved to a content hash,
    /// but the dataset with that hash does not exist in the store:
    /// - Dataset manifest was deleted after reference resolution
    /// - Race condition between resolution and retrieval
    /// - Dataset store inconsistency (hash exists in metadata but not in storage)
    ///
    /// This error is returned after successful hash resolution when attempting to
    /// load the dataset by its resolved hash.
    #[error("Dataset '{reference}' not found")]
    DatasetNotFound { reference: HashReference },

    /// Failed to retrieve a dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}'")]
    GetDataset {
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

    /// Failed to parse dataset qualifier in function reference.
    ///
    /// This occurs when a qualified function reference's dataset portion cannot be parsed
    /// as a valid dataset reference.
    #[error("Invalid function reference '{func_ref}', could not parse dataset qualifier")]
    InvalidFunctionReference {
        func_ref: String,
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

/// Errors specific to catalog_for_sql_with_deps operations
///
/// This error type is used exclusively by `catalog_for_sql_with_deps()` to create
/// a physical catalog for SQL query execution with pre-resolved dependencies.
#[derive(Debug, thiserror::Error)]
pub enum CatalogForSqlWithDepsError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError),

    /// Failed to get the physical catalog for the resolved tables and functions.
    ///
    /// This wraps errors from `get_physical_catalog_with_deps`, which can occur when:
    /// - Dataset retrieval fails
    /// - Physical table metadata cannot be retrieved
    /// - Tables have not been synced
    /// - Dependency aliases are invalid or not found
    #[error("Failed to get physical catalog with dependencies: {0}")]
    GetPhysicalCatalogWithDeps(#[source] GetPhysicalCatalogWithDepsError),
}

/// Errors specific to get_physical_catalog_with_deps operations
#[derive(Debug, thiserror::Error)]
pub enum GetPhysicalCatalogWithDepsError {
    /// Failed to get the logical catalog with dependencies.
    ///
    /// This wraps errors from `get_logical_catalog_with_deps`, which can occur when:
    /// - Dataset names cannot be extracted from table references or function names
    /// - Dataset retrieval fails
    /// - UDF creation fails
    /// - Dependency aliases are invalid or not found
    #[error("Failed to get logical catalog with dependencies: {0}")]
    GetLogicalCatalogWithDeps(#[source] GetLogicalCatalogWithDepsError),

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

/// Errors specific to get_logical_catalog_with_deps operations
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum GetLogicalCatalogWithDepsError {
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

    /// Schema portion of table reference is not a valid dependency alias.
    ///
    /// This occurs when the schema/dataset portion of a table reference does not
    /// conform to dependency alias rules (must start with letter, contain only
    /// alphanumeric/underscore, and be <= 63 bytes).
    #[error("Invalid dependency alias '{invalid_alias}' in table reference '{table_ref}'")]
    InvalidDependencyAliasForTableRef {
        invalid_alias: String,
        table_ref: String,
        #[source]
        source: datasets_derived::dep_alias::DepAliasError,
    },

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in table reference but not provided in dependencies"
    )]
    DependencyAliasNotFoundForTableRef { alias: DepAlias },

    /// Dataset reference could not be found when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset referenced in a table reference fails
    /// because the dataset does not exist in the store.
    #[error("Dataset reference '{reference}' not found for table reference")]
    DatasetNotFoundForTableRef { reference: HashReference },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for table reference")]
    GetDatasetForTableRef {
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Schema portion of function reference is not a valid dependency alias.
    ///
    /// This occurs when the schema/dataset portion of a function reference does not
    /// conform to dependency alias rules (must start with letter, contain only
    /// alphanumeric/underscore, and be <= 63 bytes).
    #[error("Invalid dependency alias '{invalid_alias}' in function reference '{func_ref}'")]
    InvalidDependencyAliasForFunctionRef {
        invalid_alias: String,
        func_ref: String,
        #[source]
        source: datasets_derived::dep_alias::DepAliasError,
    },

    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in function reference but not provided in dependencies"
    )]
    DependencyAliasNotFoundForFunctionRef { alias: DepAlias },

    /// Dataset reference could not be found when loading dataset for function.
    ///
    /// This occurs when loading a dataset referenced in a function name fails
    /// because the dataset does not exist in the store.
    #[error("Dataset reference '{reference}' not found for function reference")]
    DatasetNotFoundForFunction { reference: HashReference },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for function reference")]
    GetDatasetForFunction {
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for dataset referenced in function name.
    ///
    /// This occurs when creating the eth_call user-defined function for a function fails:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error("Failed to create ETH call UDF for dataset '{reference}' for function reference")]
    EthCallUdfCreationForFunction {
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable { reference: HashReference },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error("Function '{function_name}' not found in dataset '{reference}'")]
    FunctionNotFoundInDataset {
        function_name: String,
        reference: HashReference,
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

    /// Unknown dataset reference during resolution.
    ///
    /// This occurs when the dataset store successfully processed the reference
    /// but no matching dataset exists:
    /// - Dataset does not exist in the store
    /// - Version not found
    #[error("Unknown dataset reference '{reference}'")]
    UnknownDatasetReference { reference: Reference },

    /// Dataset not found after hash resolution.
    ///
    /// This occurs when a dataset reference was successfully resolved to a content hash,
    /// but the dataset with that hash does not exist in the store:
    /// - Dataset manifest was deleted after reference resolution
    /// - Race condition between resolution and retrieval
    /// - Dataset store inconsistency (hash exists in metadata but not in storage)
    ///
    /// This error is returned after successful hash resolution when attempting to
    /// load the dataset by its resolved hash.
    #[error("Dataset '{reference}' not found")]
    DatasetNotFound { reference: HashReference },

    /// Failed to retrieve a dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}'")]
    GetDataset {
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

    /// Failed to parse dataset qualifier in function reference.
    ///
    /// This occurs when a qualified function reference's dataset portion cannot be parsed
    /// as a valid dataset reference.
    #[error("Invalid function reference '{func_ref}', could not parse dataset qualifier")]
    InvalidFunctionReference {
        func_ref: String,
        #[source]
        source: datasets_common::partial_reference::PartialReferenceError,
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
