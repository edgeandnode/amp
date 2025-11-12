//! SQL parsing utilities.

use datafusion::{error::DataFusionError, sql::parser};
use datasets_derived::sql_str::SqlStr;

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
