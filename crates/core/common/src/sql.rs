//! SQL parsing utilities.

use std::sync::Arc;

use datafusion::{
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
pub fn parse(sql: impl AsRef<SqlStr>) -> Result<parser::Statement, ParseSqlError> {
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
/// Fails fast if any table name has an invalid format.
///
/// Note: This function does not return CTEs (Common Table Expressions). CTEs are internal to
/// the query and don't reference external tables.
///
/// Supported formats:
/// - `table` → [`TableReference::Bare`] (unqualified table)
/// - `schema.table` → [`TableReference::Partial`] (schema-qualified table)
/// - `catalog.schema.table` → [`TableReference::Full`] (fully-qualified table)
pub fn resolve_table_references(
    stmt: &Statement,
) -> Result<Vec<TableReference>, ResolveTableReferencesError> {
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
        let table_ref = match df_ref {
            datafusion::sql::TableReference::Bare { .. } => TableReference::Bare {
                table: Arc::new(table),
            },
            datafusion::sql::TableReference::Partial { schema, .. } => TableReference::Partial {
                schema,
                table: Arc::new(table),
            },
            datafusion::sql::TableReference::Full {
                catalog, schema, ..
            } => TableReference::Full {
                catalog,
                schema,
                table: Arc::new(table),
            },
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableReference {
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
        /// The schema name containing the table
        schema: Arc<str>,
        /// The validated table name wrapped in Arc for efficient cloning
        table: Arc<TableName>,
    },
    /// A fully resolved table reference, e.g., "catalog.schema.table"
    ///
    /// Corresponds to SQL: `SELECT * FROM catalog.schema.table`
    Full {
        /// The catalog name
        catalog: Arc<str>,
        /// The schema name containing the table
        schema: Arc<str>,
        /// The validated table name wrapped in Arc for efficient cloning
        table: Arc<TableName>,
    },
}

impl TableReference {
    /// Creates a partially qualified table reference (schema.table)
    ///
    /// # Arguments
    /// * `schema` - The schema name
    /// * `table_name` - The table name as a string slice
    ///
    /// # Panics
    /// Panics if `table_name` is not a valid table name according to identifier rules.
    /// For fallible construction, parse the table name first using `.parse()`.
    pub fn partial(schema: impl Into<Arc<str>>, table_name: &str) -> Self {
        let table = table_name
            .parse()
            .expect("table_name must be a valid identifier");
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
            Self::Full { table, .. } => table,
        }
    }

    /// Returns the schema name if qualified, `None` otherwise.
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Bare { .. } => None,
            Self::Partial { schema, .. } => Some(schema),
            Self::Full { schema, .. } => Some(schema),
        }
    }

    /// Returns the catalog name if fully qualified, `None` otherwise.
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Bare { .. } | Self::Partial { .. } => None,
            Self::Full { catalog, .. } => Some(catalog),
        }
    }
}

impl std::fmt::Display for TableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { table } => write!(f, "{}", table),
            Self::Partial { schema, table } => write!(f, "{}.{}", schema, table),
            Self::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{}.{}.{}", catalog, schema, table),
        }
    }
}

impl From<TableReference> for datafusion::sql::TableReference {
    fn from(value: TableReference) -> Self {
        match value {
            TableReference::Bare { table } => datafusion::sql::TableReference::bare(table.as_str()),
            TableReference::Partial { schema, table } => {
                datafusion::sql::TableReference::partial(schema, table.as_str())
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => datafusion::sql::TableReference::full(catalog, schema, table.as_str()),
        }
    }
}

/// Errors that occur when resolving table references from SQL statements
///
/// This error type is used by [`resolve_table_references`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveTableReferencesError {
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
    #[error("Invalid table name in reference '{table_ref}': {source}")]
    InvalidTableName {
        table_ref: String,
        #[source]
        source: datasets_common::table_name::TableNameError,
    },
}

/// Resolves all function names from a SQL statement into structured [`FunctionReference`]s.
///
/// Extracts function calls and resolves them into typed [`FunctionReference`] variants.
/// Fails fast if any function name has an invalid format (more than 2 parts).
///
/// Supported formats:
/// - `function` → [`FunctionReference::Bare`] (built-in DataFusion functions)
/// - `schema.function` → [`FunctionReference::Qualified`] (dataset UDFs)
/// - `catalog.schema.function` → Error (not supported)
pub fn resolve_function_references(
    stmt: &Statement,
) -> Result<Vec<FunctionReference>, ResolveFunctionReferencesError> {
    let function_names = all_function_names(stmt)?;

    let mut result = Vec::with_capacity(function_names.len());
    for func_name in function_names {
        let parts: Vec<_> = func_name.split('.').collect();
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
                let validated = function.parse::<FuncName>().map_err(|err| {
                    ResolveFunctionReferencesError::InvalidFunctionName {
                        function: function.to_string(),
                        source: err,
                    }
                })?;
                FunctionReference::qualified(schema.to_string(), validated)
            }
            _ => {
                return Err(ResolveFunctionReferencesError::InvalidFunctionFormat {
                    function: func_name,
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionReference {
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
        /// The schema (dataset reference) containing the function
        schema: Arc<str>,
        /// The validated function name wrapped in Arc for efficient cloning
        function: Arc<FuncName>,
    },
}

impl FunctionReference {
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
    pub fn qualified(schema: impl Into<Arc<str>>, function: FuncName) -> Self {
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
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Bare { .. } => None,
            Self::Qualified { schema, .. } => Some(schema),
        }
    }
}

impl std::fmt::Display for FunctionReference {
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
pub enum ResolveFunctionReferencesError {
    /// Failed to extract function names from SQL statement
    ///
    /// This occurs when the underlying function name extraction fails,
    /// typically due to unsupported statement types.
    #[error("Failed to resolve function references: {0}")]
    FunctionReferenceResolution(#[from] AllFunctionNamesError),

    /// Function reference has invalid format (more than 2 parts)
    ///
    /// This occurs when a function name contains 3 or more dot-separated parts,
    /// such as `"catalog.schema.function"`. Currently, only bare functions
    /// (1 part) and qualified functions (2 parts) are supported.
    ///
    /// # Examples
    ///
    /// - Valid: `"count"`, `"eth_mainnet.decode_log"`
    /// - Invalid: `"catalog.schema.function"`, `"a.b.c.d"`
    #[error("Invalid function format (expected 1 or 2 parts, got more): {function}")]
    InvalidFunctionFormat { function: String },

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
}

/// Returns a list of all function names in the SQL statement.
///
/// Errors in case of some DML statements.
///
/// ## Note
///
/// This is an internal helper function. Use [`parse_function_names`] instead,
/// which provides structured [`FunctionReference`] types.
fn all_function_names(stmt: &Statement) -> Result<Vec<String>, AllFunctionNamesError> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr, Function, ObjectNamePart, Visit, Visitor};
    use itertools::Itertools;

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
                .join(".")
        })
        .collect())
}

/// Errors that occur when extracting function names from SQL statements
///
/// This error type is used by [`all_function_names`].
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
