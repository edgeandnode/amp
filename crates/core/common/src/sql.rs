//! SQL parsing utilities.

use std::sync::Arc;

use datafusion::{
    common::utils::quote_identifier,
    error::DataFusionError,
    sql::{parser, parser::Statement},
};
use datasets_common::table_name::TableName;
use datasets_derived::{func_name::FuncName, sql_str::SqlStr};

/// Parses a SQL string into a single DataFusion statement.
///
/// Accepts `SqlStr` or any type implementing `AsRef<SqlStr>`. The function parses the input
/// into exactly one SQL statement, enforcing that the input contains exactly one statement -
/// no more, no less.
///
/// If the SQL has invalid syntax, the underlying DataFusion parser error is returned with
/// details about the syntax issue. If the input is empty, contains only whitespace, or only
/// semicolons, a `NoStatements` error is returned. If multiple SQL statements are provided
/// (separated by semicolons), a `MultipleStatements` error is returned with the count.
pub fn parse(sql: impl AsRef<SqlStr>) -> Result<Statement, ParseSqlError> {
    let mut statements =
        parser::DFParser::parse_sql(sql.as_ref()).map_err(ParseSqlError::InvalidSyntax)?;

    let count = statements.len();
    if count > 1 {
        return Err(ParseSqlError::MultipleStatements { count });
    }

    statements.pop_back().ok_or(ParseSqlError::NoStatements)
}

/// Error when parsing SQL strings
///
/// This error type is used by `parse()`.
#[derive(Debug, thiserror::Error)]
pub enum ParseSqlError {
    /// SQL syntax error
    ///
    /// This occurs when the provided SQL query has invalid syntax or uses
    /// unsupported SQL features. The underlying DataFusion parser error
    /// provides details about the syntax issue.
    #[error("Invalid SQL syntax")]
    InvalidSyntax(#[source] DataFusionError),

    /// No SQL statements provided
    ///
    /// This occurs when the input is empty or contains only whitespace
    /// after parsing.
    #[error("No SQL statement found")]
    NoStatements,

    /// Multiple SQL statements provided
    ///
    /// This occurs when the input contains more than one SQL statement.
    /// Only a single SQL statement is allowed per parse operation.
    #[error("Expected a single SQL statement, found {count}")]
    MultipleStatements { count: usize },
}

/// Resolves all table references from a SQL statement into structured [`TableReference`]s.
///
/// This is a wrapper around DataFusion's `resolve_table_references` that validates table names
/// and wraps them in type-safe [`TableReference`] variants.
///
/// Extracts table references and validates table names using [`datasets_common::table_name::TableName`].
/// Schema names are parsed using the generic type `T`.
/// Fails fast if any table name or schema name has an invalid format.
///
/// Note: This function does not return CTEs (Common Table Expressions). CTEs are internal to
/// the query and don't reference external tables.
///
/// Supported formats:
/// - `table` → [`TableReference::Bare`] (unqualified table)
/// - `schema.table` → [`TableReference::Partial`] (schema-qualified table)
/// - `catalog.schema.table` → [`TableReference::Full`] (fully-qualified table)
pub fn resolve_table_references<T>(
    stmt: &Statement,
) -> Result<Vec<TableReference<T>>, ResolveTableReferencesError<T::Err>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error,
{
    // Call DataFusion's resolve_table_references with normalization enabled
    let (df_table_refs, _df_cte_refs) =
        datafusion::sql::resolve::resolve_table_references(stmt, true).map_err(|err| {
            // NOTE: DataFusion's Plan variant only contains a String, not structured errors.
            // We match on message patterns for better error reporting. Unknown patterns
            // fall back to ResolveTableReferencesError::InvalidIdentifier.
            match &err {
                DataFusionError::Plan(msg) => {
                    if msg.contains("Expected identifier") {
                        ResolveTableReferencesError::InvalidIdentifier(err)
                    } else if msg.contains("Unsupported compound identifier")
                        || msg.contains("Expected 1, 2 or 3 parts")
                    {
                        ResolveTableReferencesError::UnsupportedTableReferenceFormat(err)
                    } else {
                        // Fallback for unknown Plan error patterns
                        ResolveTableReferencesError::InvalidIdentifier(err)
                    }
                }
                // Handle any unexpected error types from DataFusion
                _ => ResolveTableReferencesError::InvalidIdentifier(err),
            }
        })?;

    // Convert DataFusion TableReferences to our validated TableReferences
    let mut result = Vec::with_capacity(df_table_refs.len());
    for df_ref in df_table_refs {
        // Validate the table name
        let table = df_ref.table().parse().map_err(|err| {
            ResolveTableReferencesError::InvalidTableName {
                table_ref: df_ref.to_string(),
                source: err,
            }
        })?;

        // Construct our TableReference based on DataFusion's reference type
        let table_ref = match &df_ref {
            datafusion::sql::TableReference::Bare { .. } => TableReference::Bare {
                table: Arc::new(table),
            },
            datafusion::sql::TableReference::Partial { schema, .. } => {
                // Parse schema string into generic type T
                let parsed_schema = schema.parse::<T>().map_err(|err| {
                    ResolveTableReferencesError::InvalidSchemaFormat {
                        table_ref: df_ref.to_string(),
                        source: err,
                    }
                })?;
                TableReference::Partial {
                    schema: Arc::new(parsed_schema),
                    table: Arc::new(table),
                }
            }
            datafusion::sql::TableReference::Full { .. } => {
                // Catalog-qualified table references are not supported
                return Err(ResolveTableReferencesError::CatalogQualifiedTable {
                    table_ref: df_ref.to_string(),
                });
            }
        };

        result.push(table_ref);
    }

    Ok(result)
}

/// A reference to a table that may be bare, partial, or fully qualified.
///
/// This enum provides a type-safe representation of table references extracted from SQL queries,
/// similar to DataFusion's [`datafusion::sql::TableReference`] but with validated table names.
///
/// Table names are validated using [`datasets_common::table_name::TableName`] to ensure they
/// conform to identifier rules. The validated names are stored in `Arc` for efficient cloning.
///
/// Schema names are generic over type `T` which defaults to `String`. Custom types implementing
/// `FromStr` can be used for validated schema names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableReference<T = String> {
    /// An unqualified table reference, e.g., "orders", "customers"
    ///
    /// Corresponds to SQL: `SELECT * FROM table`
    Bare {
        /// The validated table name wrapped in Arc for efficient cloning
        table: Arc<TableName>,
    },
    /// A partially resolved table reference, e.g., "schema.table"
    ///
    /// Corresponds to SQL: `SELECT * FROM schema.table`
    Partial {
        /// The schema name containing the table (generic type T)
        schema: Arc<T>,
        /// The validated table name wrapped in Arc for efficient cloning
        table: Arc<TableName>,
    },
}

impl<T> TableReference<T> {
    /// Creates a partially qualified table reference (schema.table)
    pub fn partial(schema: impl Into<Arc<T>>, table: TableName) -> Self {
        Self::Partial {
            schema: schema.into(),
            table: Arc::new(table),
        }
    }

    /// Returns the table name, regardless of qualification.
    pub fn table(&self) -> &TableName {
        match self {
            Self::Bare { table } => table,
            Self::Partial { table, .. } => table,
        }
    }

    /// Returns the schema name if qualified, `None` otherwise.
    pub fn schema(&self) -> Option<&T> {
        match self {
            Self::Bare { .. } => None,
            Self::Partial { schema, .. } => Some(schema),
        }
    }
}

impl<T> TableReference<T>
where
    T: std::fmt::Display,
{
    /// Converts this table reference to a `TableReference<String>`
    pub fn to_string_reference(&self) -> TableReference<String> {
        match self {
            Self::Bare { table } => TableReference::Bare {
                table: Arc::clone(table),
            },
            Self::Partial { schema, table } => TableReference::Partial {
                schema: Arc::new(schema.to_string()),
                table: Arc::clone(table),
            },
        }
    }

    /// Converts this table reference into a `TableReference<String>` by consuming it
    pub fn into_string_reference(self) -> TableReference<String> {
        match self {
            Self::Bare { table } => TableReference::Bare { table },
            Self::Partial { schema, table } => TableReference::Partial {
                schema: Arc::new(schema.to_string()),
                table,
            },
        }
    }
}

impl<T> TableReference<T>
where
    T: AsRef<str>,
{
    /// Returns a properly quoted string representation suitable for SQL
    ///
    /// Uses DataFusion's `quote_identifier` to ensure identifiers are properly escaped.
    pub fn to_quoted_string(&self) -> String {
        match self {
            Self::Bare { table } => quote_identifier(table.as_str()).to_string(),
            Self::Partial { schema, table } => {
                format!(
                    "{}.{}",
                    quote_identifier(schema.as_ref().as_ref()),
                    quote_identifier(table.as_str())
                )
            }
        }
    }
}

impl<T> std::fmt::Display for TableReference<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { table } => write!(f, "{}", table),
            Self::Partial { schema, table } => write!(f, "{}.{}", schema, table),
        }
    }
}

impl<T> From<TableReference<T>> for datafusion::sql::TableReference
where
    T: AsRef<str>,
{
    fn from(value: TableReference<T>) -> Self {
        match value {
            TableReference::Bare { table } => datafusion::sql::TableReference::bare(table.as_str()),
            TableReference::Partial { schema, table } => {
                datafusion::sql::TableReference::partial(schema.as_ref().as_ref(), table.as_str())
            }
        }
    }
}

impl<T> TryFrom<datafusion::sql::TableReference> for TableReference<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error,
{
    type Error = TableReferenceConversionError<T::Err>;

    fn try_from(value: datafusion::sql::TableReference) -> Result<Self, Self::Error> {
        match value {
            datafusion::sql::TableReference::Bare { table } => {
                let table_name = table
                    .parse::<TableName>()
                    .map_err(TableReferenceConversionError::InvalidTableName)?;
                Ok(TableReference::Bare {
                    table: Arc::new(table_name),
                })
            }
            datafusion::sql::TableReference::Partial { schema, table } => {
                let table_name = table
                    .parse::<TableName>()
                    .map_err(TableReferenceConversionError::InvalidTableName)?;
                let parsed_schema = schema
                    .parse::<T>()
                    .map_err(TableReferenceConversionError::InvalidSchemaFormat)?;
                Ok(TableReference::Partial {
                    schema: Arc::new(parsed_schema),
                    table: Arc::new(table_name),
                })
            }
            datafusion::sql::TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                // Catalog-qualified table references are not supported
                Err(TableReferenceConversionError::CatalogQualifiedTable {
                    table_ref: format!("{}.{}.{}", catalog, schema, table),
                })
            }
        }
    }
}

/// Errors that occur when converting DataFusion table references
///
/// This error type is used by the `TryFrom<datafusion::sql::TableReference>` implementation.
#[derive(Debug, thiserror::Error)]
pub enum TableReferenceConversionError<E = std::convert::Infallible> {
    /// Table name has invalid format
    ///
    /// This occurs when a table name extracted from a DataFusion table reference
    /// does not conform to identifier rules.
    #[error("Invalid table name: {0}")]
    InvalidTableName(#[source] datasets_common::table_name::TableNameError),

    /// Schema name has invalid format
    ///
    /// This occurs when a schema name cannot be parsed into the target type `T`.
    /// The underlying error from `T::FromStr` provides the specific validation failure.
    #[error("Invalid schema format: {0}")]
    InvalidSchemaFormat(#[source] E),

    /// Table reference is catalog-qualified (not supported)
    ///
    /// This occurs when attempting to convert a DataFusion table reference that contains
    /// a catalog qualifier (3 parts). Catalog-qualified references are not supported -
    /// only bare table names (1 part) and schema-qualified names (2 parts) are allowed.
    #[error("Catalog-qualified table references are not supported: {table_ref}")]
    CatalogQualifiedTable { table_ref: String },
}

/// Errors that occur when resolving table references from SQL statements
///
/// This error type is used by [`resolve_table_references`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveTableReferencesError<E = std::convert::Infallible> {
    /// Table reference contains an invalid identifier
    ///
    /// This occurs when a table reference contains identifiers that are not valid
    /// SQL identifiers. DataFusion's parser expects simple identifiers or quoted
    /// identifiers, and this error is returned when parsing fails.
    ///
    /// Common causes:
    /// - Complex object name formats that couldn't be parsed
    /// - Special characters that require escaping but aren't properly quoted
    /// - Malformed identifier syntax
    ///
    /// This error originates from DataFusion's `object_name_to_table_reference`
    /// function when it encounters an identifier that doesn't match expected patterns.
    ///
    /// # Example
    /// ```sql
    /// -- May fail if parser encounters unexpected identifier format
    /// SELECT * FROM `invalid~identifier`
    /// ```
    #[error("Invalid identifier in table reference: {0}")]
    InvalidIdentifier(#[source] DataFusionError),

    /// Table reference has unsupported format
    ///
    /// This occurs when a table reference has more or fewer than the supported
    /// number of parts. DataFusion supports:
    /// - 1 part: bare table name (e.g., `orders`)
    /// - 2 parts: schema-qualified (e.g., `public.orders`)
    /// - 3 parts: fully-qualified (e.g., `catalog.public.orders`)
    ///
    /// Any other number of parts (0, 4, or more) will result in this error.
    ///
    /// This error originates from DataFusion's `idents_to_table_reference`
    /// function when the identifier parts count doesn't match 1, 2, or 3.
    ///
    /// # Example
    /// ```sql
    /// SELECT * FROM a.b.c.d  -- 4 parts - unsupported
    /// ```
    #[error("Unsupported table reference format (expected 1-3 parts): {0}")]
    UnsupportedTableReferenceFormat(#[source] DataFusionError),

    /// Table name has invalid format
    ///
    /// This occurs when a table name extracted from SQL does not conform to
    /// identifier rules. The table name must start with a letter or underscore,
    /// contain only alphanumeric characters, underscores, or dollar signs,
    /// and be no longer than 255 bytes.
    ///
    /// Common causes:
    /// - Table name starts with a digit (e.g., `"1table"`)
    /// - Table name contains invalid characters (e.g., `"my-table"`, `"my.table"`)
    /// - Table name exceeds 255 bytes
    /// - Table name is empty
    ///
    /// Valid table name format: `[a-zA-Z_][a-zA-Z0-9_$]*` (max 255 bytes)
    #[error("Invalid table name in reference '{table_ref}'")]
    InvalidTableName {
        table_ref: String,
        #[source]
        source: datasets_common::table_name::TableNameError,
    },

    /// Schema name has invalid format
    ///
    /// This occurs when a schema name extracted from SQL cannot be parsed into
    /// the target schema type. The schema type is determined by the generic parameter
    /// `T` in `resolve_table_references<T>()`.
    ///
    /// When using the default `String` type, this error will never occur as the conversion
    /// is infallible. When using a custom validated schema type, this error occurs if the
    /// schema name fails validation according to the type's `FromStr` implementation.
    #[error("Invalid schema format in reference '{table_ref}': {source}")]
    InvalidSchemaFormat {
        table_ref: String,
        #[source]
        source: E,
    },

    /// Table reference is catalog-qualified (not supported)
    ///
    /// This occurs when a table reference contains a catalog qualifier (3 parts).
    /// Catalog-qualified references are not supported - only bare table names
    /// (1 part) and schema-qualified names (2 parts) are allowed.
    #[error("Catalog-qualified table references are not supported: {table_ref}")]
    CatalogQualifiedTable { table_ref: String },
}

/// Resolves all function names from a SQL statement into structured [`FunctionReference`]s.
///
/// Extracts function calls and resolves them into typed [`FunctionReference`] variants.
/// Schema names are parsed using the generic type `T`.
/// Fails fast if any function name has an invalid format (more than 2 parts) or if
/// schema name parsing fails.
///
/// Supported formats:
/// - `function` → [`FunctionReference::Bare`] (built-in DataFusion functions)
/// - `schema.function` → [`FunctionReference::Qualified`] (dataset UDFs)
/// - `catalog.schema.function` → Error (not supported)
pub fn resolve_function_references<T>(
    stmt: &Statement,
) -> Result<Vec<FunctionReference<T>>, ResolveFunctionReferencesError<T::Err>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error,
{
    let func_refs = all_function_refs(stmt)?;

    let mut result = Vec::with_capacity(func_refs.len());
    for parts in func_refs {
        // Reconstruct dotted string only for error messages
        let func_ref_str = parts.join(".");

        let func_ref = match parts.as_slice() {
            [function] => {
                // Validate function name (one-time validation)
                let validated = function.parse::<FuncName>().map_err(|err| {
                    ResolveFunctionReferencesError::InvalidFunctionName {
                        function: function.to_string(),
                        source: err,
                    }
                })?;
                FunctionReference::bare(validated)
            }
            [schema, function] => {
                // Validate function name (one-time validation)
                let validated_func = function.parse::<FuncName>().map_err(|err| {
                    ResolveFunctionReferencesError::InvalidFunctionName {
                        function: function.to_string(),
                        source: err,
                    }
                })?;

                // Parse schema string into generic type T
                let validated_schema = schema.parse::<T>().map_err(|err| {
                    ResolveFunctionReferencesError::InvalidSchemaFormat {
                        function_ref: func_ref_str.clone(),
                        source: err,
                    }
                })?;

                FunctionReference::Qualified {
                    schema: Arc::new(validated_schema),
                    function: Arc::new(validated_func),
                }
            }
            [_catalog, _schema, _function] => {
                // Catalog-qualified function references are not supported
                return Err(ResolveFunctionReferencesError::CatalogQualifiedFunction {
                    function_ref: func_ref_str,
                });
            }
            _ => {
                // 4 or more parts - invalid format
                return Err(ResolveFunctionReferencesError::InvalidFunctionFormat {
                    function_ref: func_ref_str,
                });
            }
        };
        result.push(func_ref);
    }

    Ok(result)
}

/// A reference to a function that may be bare (unqualified) or qualified with a schema.
///
/// This enum provides a type-safe representation of function references extracted from SQL queries,
/// similar to DataFusion's [`TableReference`].
///
/// Function names are validated using [`datasets_derived::func_name::FuncName`] to ensure they conform to
/// DataFusion UDF identifier rules. The validated names are stored in `Arc` for efficient cloning.
///
/// Schema names are generic over type `T` which defaults to `String`. Custom types implementing
/// `FromStr` can be used for validated schema names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FunctionReference<T = String> {
    /// An unqualified function reference, e.g., "count", "sum"
    ///
    /// These typically refer to built-in DataFusion functions.
    Bare {
        /// The validated function name wrapped in Arc for efficient cloning
        function: Arc<FuncName>,
    },
    /// A schema-qualified function reference, e.g., "schema.function"
    ///
    /// These refer to user-defined functions (UDFs) from specific datasets.
    Qualified {
        /// The schema (dataset reference) containing the function (generic type T)
        schema: Arc<T>,
        /// The validated function name wrapped in Arc for efficient cloning
        function: Arc<FuncName>,
    },
}

impl<T> FunctionReference<T> {
    /// Creates a bare (unqualified) function reference from a validated function name.
    ///
    /// The function name must be validated before calling this method (via `FuncName::from_str`).
    /// The validated name is wrapped in `Arc` for efficient cloning.
    pub fn bare(function: FuncName) -> Self {
        Self::Bare {
            function: Arc::new(function),
        }
    }

    /// Creates a qualified function reference from a validated function name.
    ///
    /// The function name must be validated before calling this method (via `FuncName::from_str`).
    /// The validated name is wrapped in `Arc` for efficient cloning. The schema is assumed valid.
    pub fn qualified(schema: impl Into<Arc<T>>, function: FuncName) -> Self {
        Self::Qualified {
            schema: schema.into(),
            function: Arc::new(function),
        }
    }

    /// Returns the function name, regardless of qualification.
    pub fn function(&self) -> &str {
        match self {
            Self::Bare { function } => function,
            Self::Qualified { function, .. } => function,
        }
    }

    /// Returns the schema name if qualified, `None` otherwise.
    pub fn schema(&self) -> Option<&T> {
        match self {
            Self::Bare { .. } => None,
            Self::Qualified { schema, .. } => Some(schema),
        }
    }
}

impl<T> FunctionReference<T>
where
    T: std::fmt::Display,
{
    /// Converts this function reference to a `FunctionReference<String>`
    ///
    /// This is useful when you need to work with string-typed function references
    /// regardless of the original schema type `T`.
    pub fn to_string_reference(&self) -> FunctionReference<String> {
        match self {
            Self::Bare { function } => FunctionReference::Bare {
                function: Arc::clone(function),
            },
            Self::Qualified { schema, function } => FunctionReference::Qualified {
                schema: Arc::new(schema.to_string()),
                function: Arc::clone(function),
            },
        }
    }

    /// Converts this function reference into a `FunctionReference<String>` by consuming it
    ///
    /// This is more efficient than `to_string_reference()` when the original value
    /// is no longer needed, as it can reuse the function `Arc` without cloning.
    pub fn into_string_reference(self) -> FunctionReference<String> {
        match self {
            Self::Bare { function } => FunctionReference::Bare { function },
            Self::Qualified { schema, function } => FunctionReference::Qualified {
                schema: Arc::new(schema.to_string()),
                function,
            },
        }
    }
}

impl<T> std::fmt::Display for FunctionReference<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { function } => write!(f, "{}", function),
            Self::Qualified { schema, function } => write!(f, "{}.{}", schema, function),
        }
    }
}

/// Errors that occur when resolving function references from SQL statements
///
/// This error type is used by [`resolve_function_references`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveFunctionReferencesError<E = std::convert::Infallible> {
    /// Failed to extract function names from SQL statement
    ///
    /// This occurs when the underlying function name extraction fails,
    /// typically due to unsupported statement types.
    #[error("Failed to resolve function references: {0}")]
    FunctionReferenceResolution(#[from] AllFunctionNamesError),

    /// Function reference is catalog-qualified (not supported)
    ///
    /// This occurs when a function name contains a catalog qualifier (3 parts).
    /// Catalog-qualified references are not supported - only bare function names
    /// (1 part) and schema-qualified names (2 parts) are allowed.
    ///
    /// # Examples
    ///
    /// - Valid: `"count"`, `"eth_mainnet.decode_log"`
    /// - Invalid: `"catalog.schema.function"`, `"a.b.c.d"`
    #[error("Catalog-qualified function references are not supported: {function_ref}")]
    CatalogQualifiedFunction { function_ref: String },

    /// Function reference has invalid format (wrong number of parts)
    ///
    /// This occurs when a function name contains 4 or more dot-separated parts.
    /// Only bare functions (1 part), qualified functions (2 parts), and catalog-qualified
    /// functions (3 parts - rejected separately) are recognized.
    #[error("Invalid function format (expected 1-3 parts, got more): {function_ref}")]
    InvalidFunctionFormat { function_ref: String },

    /// Function name has invalid format
    ///
    /// This occurs when a function name extracted from SQL does not conform to
    /// DataFusion UDF identifier rules. The function name must start with a letter
    /// or underscore, contain only alphanumeric characters, underscores, or dollar signs,
    /// and be no longer than 255 bytes.
    ///
    /// Common causes:
    /// - Function name starts with a digit (e.g., `"1function"`)
    /// - Function name contains invalid characters (e.g., `"my-function"`, `"my.function"`)
    /// - Function name exceeds 255 bytes
    ///
    /// Valid function name format: `[a-zA-Z_][a-zA-Z0-9_$]*` (max 255 bytes)
    #[error("Invalid function name '{function}': {source}")]
    InvalidFunctionName {
        function: String,
        #[source]
        source: datasets_derived::func_name::FuncNameError,
    },

    /// Schema name has invalid format
    ///
    /// This occurs when a schema name in a qualified function reference cannot be parsed into
    /// the target schema type. The schema type is determined by the generic parameter
    /// `T` in `resolve_function_references<T>()`.
    ///
    /// When using the default `String` type, this error will never occur as the conversion
    /// is infallible. When using a custom validated schema type, this error occurs if the
    /// schema name fails validation according to the type's `FromStr` implementation.
    #[error("Invalid schema format in function reference '{function_ref}': {source}")]
    InvalidSchemaFormat {
        function_ref: String,
        #[source]
        source: E,
    },
}

/// Returns a list of all function names in the SQL statement as structured parts.
///
/// Each function is returned as a Vec<String> where each element is a part of the
/// qualified name (e.g., ["schema", "function"] for schema.function).
///
/// This preserves the original identifier boundaries, which is crucial for identifiers
/// that contain special characters like dots (e.g., "namespace/name@0.0.0").
///
/// Errors in case of some DML statements.
///
/// ## Note
///
/// This is an internal helper function. Use [`resolve_function_references`] instead,
/// which provides structured [`FunctionReference`] types.
fn all_function_refs(stmt: &Statement) -> Result<Vec<Vec<String>>, AllFunctionNamesError> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr, Function, ObjectNamePart, Visit, Visitor};

    struct FunctionCollector {
        functions: Vec<Function>,
    }

    impl Visitor for FunctionCollector {
        type Break = ();

        fn pre_visit_expr(&mut self, function: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = function {
                self.functions.push(f.clone());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = FunctionCollector {
        functions: Vec::new(),
    };
    let stmt = match stmt {
        Statement::Statement(statement) => statement,
        Statement::CreateExternalTable(_) | Statement::CopyTo(_) => {
            return Err(AllFunctionNamesError::DmlNotSupported);
        }
        Statement::Explain(explain) => match explain.statement.as_ref() {
            Statement::Statement(statement) => statement,
            _ => return Err(AllFunctionNamesError::UnsupportedStatementInExplain),
        },
    };

    let c = stmt.visit(&mut collector);
    assert!(c.is_continue());

    Ok(collector
        .functions
        .into_iter()
        .map(|f| {
            f.name
                .0
                .into_iter()
                .filter_map(|s| match s {
                    ObjectNamePart::Identifier(ident) => Some(ident.value),
                    ObjectNamePart::Function(_) => None,
                })
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Errors that occur when extracting function names from SQL statements
///
/// This error type is used by [`all_function_refs`].
#[derive(Debug, thiserror::Error)]
pub enum AllFunctionNamesError {
    /// DML statements are not supported for function name extraction
    ///
    /// This occurs when attempting to extract function names from DML statements
    /// like `CreateExternalTable` or `CopyTo`. These statement types are not
    /// supported because they represent data manipulation operations rather than
    /// queryable SQL statements.
    ///
    /// Function name extraction is only meaningful for query statements (SELECT)
    /// and their variants (e.g., within EXPLAIN).
    #[error("DML statements (CreateExternalTable, CopyTo) are not supported")]
    DmlNotSupported,

    /// Unsupported statement type within EXPLAIN
    ///
    /// This occurs when an EXPLAIN statement contains a nested statement type
    /// that is not supported for function name extraction. Only regular SQL
    /// statements (SELECT, etc.) are supported within EXPLAIN.
    ///
    /// Common causes:
    /// - EXPLAIN wrapping a DML statement (CreateExternalTable, CopyTo)
    /// - EXPLAIN wrapping another EXPLAIN statement
    #[error("Unsupported statement type in EXPLAIN")]
    UnsupportedStatementInExplain,
}

#[cfg(test)]
mod tests {
    use super::*;

    mod all_function_refs_tests {
        use super::*;

        /// Helper to parse SQL string into a Statement
        fn parse_sql(sql: &str) -> Statement {
            let sql_str = sql.parse::<SqlStr>().expect("SQL string should be valid");
            parse(&sql_str).expect("SQL should parse successfully")
        }

        #[test]
        fn basic_function_with_namespace_and_version() {
            //* Given
            let stmt = parse_sql(r#"SELECT "_/basic_function@0.0.0".testString()"#);

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "_/basic_function@0.0.0",
                "first part should be namespace with version"
            );
            assert_eq!(
                functions[0][1], "testString",
                "second part should be function name"
            );
        }

        #[test]
        fn basic_function_simple_qualified() {
            //* Given
            let stmt = parse_sql("SELECT basic_function.testString()");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "basic_function",
                "first part should be schema"
            );
            assert_eq!(
                functions[0][1], "testString",
                "second part should be function name"
            );
        }

        #[test]
        fn basic_function_partial_ref_underscore() {
            //* Given
            let stmt = parse_sql(r#"SELECT "_/basic_function".testString()"#);

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "_/basic_function",
                "first part should be namespace with underscore prefix"
            );
            assert_eq!(
                functions[0][1], "testString",
                "second part should be function name"
            );
        }

        #[test]
        fn basic_function_with_version() {
            //* Given
            let stmt = parse_sql(r#"SELECT "basic_function@0.0.0".testString()"#);

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "basic_function@0.0.0",
                "first part should be schema with version"
            );
            assert_eq!(
                functions[0][1], "testString",
                "second part should be function name"
            );
        }

        #[test]
        fn function_in_table_fqn_namespace() {
            //* Given
            let stmt = parse_sql(
                r#"SELECT "freecandylabs/function_in_table".addSuffix(miner_tagged) as double_tagged FROM "freecandylabs/function_in_table".blocks_with_suffix LIMIT 1"#,
            );

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "freecandylabs/function_in_table",
                "first part should be namespace with slash"
            );
            assert_eq!(
                functions[0][1], "addSuffix",
                "second part should be function name"
            );
        }

        #[test]
        fn function_in_table_fqn_with_version() {
            //* Given
            let stmt = parse_sql(
                r#"SELECT "freecandylabs/function_in_table@0.0.0".addSuffix(miner_tagged) as triple_tagged FROM "freecandylabs/function_in_table".blocks_with_suffix LIMIT 1"#,
            );

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "freecandylabs/function_in_table@0.0.0",
                "first part should be full namespace with version"
            );
            assert_eq!(
                functions[0][1], "addSuffix",
                "second part should be function name"
            );
        }

        #[test]
        fn bare_function_no_schema() {
            //* Given
            let stmt = parse_sql("SELECT count(*) FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 1, "should have 1 part");
            assert_eq!(functions[0][0], "count", "should be bare function name");
        }

        #[test]
        fn multiple_functions_in_query() {
            //* Given
            let stmt =
                parse_sql("SELECT count(*), sum(value), schema.custom_func(id) FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 3, "should extract all three functions");
            assert_eq!(functions[0].len(), 1, "first function should have 1 part");
            assert_eq!(functions[0][0], "count", "first function should be count");
            assert_eq!(functions[1].len(), 1, "second function should have 1 part");
            assert_eq!(functions[1][0], "sum", "second function should be sum");
            assert_eq!(functions[2].len(), 2, "third function should have 2 parts");
            assert_eq!(functions[2][0], "schema", "third function schema part");
            assert_eq!(functions[2][1], "custom_func", "third function name part");
        }

        #[test]
        fn nested_function_calls() {
            //* Given
            let stmt = parse_sql("SELECT upper(lower(name)) FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 2, "should extract both nested functions");
            assert_eq!(functions[0].len(), 1, "first function should have 1 part");
            assert_eq!(functions[0][0], "upper", "should extract outer function");
            assert_eq!(functions[1].len(), 1, "second function should have 1 part");
            assert_eq!(functions[1][0], "lower", "should extract inner function");
        }

        #[test]
        fn function_in_where_clause() {
            //* Given
            let stmt = parse_sql("SELECT * FROM my_table WHERE custom.validate(field) = true");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(
                functions.len(),
                1,
                "should extract function from WHERE clause"
            );
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(functions[0][0], "custom", "first part should be schema");
            assert_eq!(
                functions[0][1], "validate",
                "second part should be function name"
            );
        }

        #[test]
        fn function_in_join_condition() {
            //* Given
            let stmt = parse_sql("SELECT * FROM t1 JOIN t2 ON schema.compare(t1.id, t2.id)");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(
                functions.len(),
                1,
                "should extract function from JOIN condition"
            );
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(functions[0][0], "schema", "first part should be schema");
            assert_eq!(
                functions[0][1], "compare",
                "second part should be function name"
            );
        }

        #[test]
        fn no_functions_in_simple_select() {
            //* Given
            let stmt = parse_sql("SELECT id, name FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert!(
                functions.is_empty(),
                "should return empty list when no functions present"
            );
        }

        #[test]
        fn explain_statement_with_functions() {
            //* Given
            let stmt = parse_sql("EXPLAIN SELECT count(*), schema.func() FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(
                functions.len(),
                2,
                "should extract functions from EXPLAIN statement"
            );
        }

        #[test]
        fn create_external_table_returns_error() {
            //* Given
            let stmt = parse_sql("CREATE EXTERNAL TABLE test STORED AS CSV LOCATION 'file.csv'");

            //* When
            let result = all_function_refs(&stmt);

            //* Then
            assert!(result.is_err(), "should return error for DML statement");
            let error = result.expect_err("should be DmlNotSupported error");
            assert!(
                matches!(error, AllFunctionNamesError::DmlNotSupported),
                "should return DmlNotSupported error, got {:?}",
                error
            );
        }

        #[test]
        fn special_characters_in_namespace() {
            //* Given
            let stmt = parse_sql(r#"SELECT "org/dataset@1.2.3".process_data(field) FROM my_table"#);

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "org/dataset@1.2.3",
                "first part should preserve special characters in namespace"
            );
            assert_eq!(
                functions[0][1], "process_data",
                "second part should be function name"
            );
        }

        #[test]
        fn double_quoted_identifiers() {
            //* Given
            let stmt = parse_sql(r#"SELECT "my-schema"."my-function"() FROM my_table"#);

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract exactly one function");
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(
                functions[0][0], "my-schema",
                "first part should preserve double-quoted identifier"
            );
            assert_eq!(
                functions[0][1], "my-function",
                "second part should preserve double-quoted identifier"
            );
        }

        #[test]
        fn aggregate_function_with_distinct() {
            //* Given
            let stmt = parse_sql("SELECT count(DISTINCT id) FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract aggregate function");
            assert_eq!(functions[0].len(), 1, "should have 1 part");
            assert_eq!(
                functions[0][0], "count",
                "should extract function with DISTINCT modifier"
            );
        }

        #[test]
        fn window_function() {
            //* Given
            let stmt = parse_sql("SELECT row_number() OVER (ORDER BY id) FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(functions.len(), 1, "should extract window function");
            assert_eq!(functions[0].len(), 1, "should have 1 part");
            assert_eq!(
                functions[0][0], "row_number",
                "should extract window function name"
            );
        }

        #[test]
        fn function_in_case_expression() {
            //* Given
            let stmt =
                parse_sql("SELECT CASE WHEN schema.validate(x) THEN 1 ELSE 0 END FROM my_table");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(
                functions.len(),
                1,
                "should extract function from CASE expression"
            );
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(functions[0][0], "schema", "first part should be schema");
            assert_eq!(
                functions[0][1], "validate",
                "second part should be function name"
            );
        }

        #[test]
        fn function_with_subquery() {
            //* Given
            let stmt = parse_sql("SELECT schema.transform((SELECT value FROM t2)) FROM t1");

            //* When
            let functions = all_function_refs(&stmt).expect("function extraction should succeed");

            //* Then
            assert_eq!(
                functions.len(),
                1,
                "should extract function with subquery argument"
            );
            assert_eq!(functions[0].len(), 2, "should have 2 parts");
            assert_eq!(functions[0][0], "schema", "first part should be schema");
            assert_eq!(
                functions[0][1], "transform",
                "second part should be function name"
            );
        }
    }
}
