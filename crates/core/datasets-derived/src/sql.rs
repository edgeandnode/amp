//! SQL parsing utilities for derived datasets.
//!
//! This module provides SQL parsing and table reference resolution needed
//! for processing derived dataset manifests.

use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    sql::{parser, parser::Statement},
};
use datasets_common::table_name::TableName;

use crate::sql_str::SqlStr;

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
    #[error("SQL syntax error: {0}")]
    InvalidSyntax(#[source] datafusion::error::DataFusionError),

    /// Multiple SQL statements found
    ///
    /// This occurs when the input contains more than one SQL statement
    /// (separated by semicolons). Only single statements are supported.
    #[error("Expected exactly one SQL statement, found {count}")]
    MultipleStatements { count: usize },

    /// No SQL statements found
    ///
    /// This occurs when the input is empty, contains only whitespace,
    /// or only semicolons. At least one statement is required.
    #[error("No SQL statements found in input")]
    NoStatements,
}

/// Resolves all table references from a SQL statement into structured [`TableReference`]s.
///
/// Extracts table names from a parsed SQL statement and resolves them into typed
/// [`TableReference`] variants. Schema names are parsed using the generic type `T`.
/// Fails fast if any table name has an invalid format or if schema name parsing fails.
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

/// Errors that occur when resolving table references from SQL statements
///
/// This error type is used by [`resolve_table_references`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveTableReferencesError<E = std::convert::Infallible> {
    /// Table reference contains an invalid identifier
    #[error("Invalid identifier in table reference: {0}")]
    InvalidIdentifier(#[source] DataFusionError),

    /// Table reference has unsupported format
    #[error("Unsupported table reference format (expected 1-3 parts): {0}")]
    UnsupportedTableReferenceFormat(#[source] DataFusionError),

    /// Table name has invalid format
    #[error("Invalid table name in reference '{table_ref}'")]
    InvalidTableName {
        table_ref: String,
        #[source]
        source: datasets_common::table_name::TableNameError,
    },

    /// Schema name has invalid format
    #[error("Invalid schema format in reference '{table_ref}': {source}")]
    InvalidSchemaFormat {
        table_ref: String,
        #[source]
        source: E,
    },

    /// Table reference is catalog-qualified (not supported)
    #[error("Catalog-qualified table references are not supported: {table_ref}")]
    CatalogQualifiedTable { table_ref: String },
}
