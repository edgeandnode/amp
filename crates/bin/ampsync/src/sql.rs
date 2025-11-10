//! SQL utilities for safe identifier handling and query building.
//!
//! This module provides:
//! 1. **Validation**: Ensures identifiers are safe PostgreSQL identifiers
//! 2. **Quoting**: Properly escapes identifiers for use in SQL statements
//! 3. **Query Building**: Type-safe helpers for common SQL patterns
//!
//! # Security Model
//!
//! All SQL identifier handling goes through this module, providing a single
//! auditable boundary for SQL injection prevention:
//!
//! ```text
//! User Input → validate_identifier() → quote_identifier() → SQL Query
//!              (sqlparser check)        (pg_escape quoting)
//! ```
//!
//! ## Defense in Depth
//!
//! 1. **Parser-based validation** (sqlparser): Validates syntax, catches injection
//! 2. **Character restrictions**: Only alphanumeric, `_`, `$` allowed
//! 3. **Proper quoting** (pg_escape): Handles keywords, escaping, case-sensitivity
//!
//! As recommended by Leo: "if you want to validate that your string is a SQL identifier,
//! the best thing you can do is use sqlparser to parse it as an identifier"

use datasets_common::reference::Reference;
use pg_escape::quote_identifier;
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

/// Errors that occur during SQL identifier validation.
#[derive(Debug, thiserror::Error)]
pub enum ValidateIdentifierError {
    /// Identifier is empty
    #[error("Identifier cannot be empty")]
    Empty,

    /// Identifier exceeds PostgreSQL's 63-byte limit
    #[error("Identifier exceeds PostgreSQL limit of 63 bytes (got {length})")]
    TooLong { length: usize },

    /// Identifier contains invalid characters
    #[error("Identifier contains invalid character: '{character}'")]
    InvalidCharacter { character: char },

    /// Identifier must start with letter or underscore
    #[error("Identifier must start with letter or underscore, got '{first_char}'")]
    InvalidFirstCharacter { first_char: char },

    /// Identifier failed SQL parser validation
    #[error("Not a valid SQL identifier: {reason}")]
    ParserError { reason: String },

    /// Identifier parsed as multiple SQL statements (injection attempt)
    #[error("Identifier parsed as multiple SQL statements")]
    MultipleStatements,
}

/// Validate that a string is a safe PostgreSQL identifier.
///
/// This function validates that:
/// 1. The name parses successfully as a SQL identifier (via sqlparser)
/// 2. It's a simple, unqualified identifier (no dots for schema.table)
/// 3. It doesn't require quoting (no special characters)
/// 4. It doesn't exceed PostgreSQL's 63-byte limit
///
/// This prevents SQL injection while using a battle-tested SQL parser.
///
/// # Example
/// ```ignore
/// use ampsync::sql::validate_identifier;
///
/// assert!(validate_identifier("users").is_ok());
/// assert!(validate_identifier("user_accounts").is_ok());
/// assert!(validate_identifier("users; DROP TABLE").is_err());
/// assert!(validate_identifier("user-table").is_err());
/// ```
pub fn validate_identifier(name: &str) -> Result<(), ValidateIdentifierError> {
    // Check empty
    if name.is_empty() {
        return Err(ValidateIdentifierError::Empty);
    }

    // Check PostgreSQL length limit (63 bytes for identifiers)
    if name.len() > 63 {
        return Err(ValidateIdentifierError::TooLong { length: name.len() });
    }

    // Reject names that would require quoting or contain problematic characters
    // This ensures we only accept simple, unqualified identifiers
    for ch in name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '$' {
            return Err(ValidateIdentifierError::InvalidCharacter { character: ch });
        }
    }

    // First character must be letter or underscore (PostgreSQL rule)
    let first_char = name.chars().next().unwrap(); // Safe: we checked for empty above
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(ValidateIdentifierError::InvalidFirstCharacter { first_char });
    }

    // Use sqlparser to validate that this is a valid SQL identifier
    // This catches edge cases and SQL injection attempts
    let sql = format!("SELECT * FROM {}", name);
    let dialect = PostgreSqlDialect {};

    match Parser::parse_sql(&dialect, &sql) {
        Ok(statements) => {
            // Successfully parsed - verify it's a single SELECT statement
            if statements.len() != 1 {
                return Err(ValidateIdentifierError::MultipleStatements);
            }
            Ok(())
        }
        Err(e) => Err(ValidateIdentifierError::ParserError {
            reason: e.to_string(),
        }),
    }
}

/// Safely format a CREATE TABLE statement with a quoted table identifier.
///
/// This is the correct and safe way to build DDL statements with dynamic table names.
/// SQL does not support parameterization for identifiers (table/column names), so we:
/// 1. Quote the identifier using `pg_escape::quote_identifier()`
/// 2. Build the SQL string using `format!()`
///
/// **Safety**: Assumes the table_name has been validated using [`validate_identifier`].
/// The quoting handles reserved keywords, special characters, and escaping.
///
/// # Example
/// ```ignore
/// let sql = create_table("users", "id BIGINT, name TEXT", "id");
/// // Produces: CREATE TABLE IF NOT EXISTS "users" (id BIGINT, name TEXT, PRIMARY KEY (id))
/// ```
pub fn create_table(table_name: &str, columns: &str, primary_key: &str) -> String {
    let quoted = quote_identifier(table_name);
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))",
        quoted, columns, primary_key
    )
}

/// Safely format a COPY statement with a quoted table identifier.
///
/// # Example
/// ```ignore
/// let sql = copy_from_stdin("users");
/// // Produces: COPY "users" FROM STDIN WITH (FORMAT BINARY)
/// ```
pub fn copy_from_stdin(table_name: &str) -> String {
    let quoted = quote_identifier(table_name);
    format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", quoted)
}

/// Safely format a CREATE TEMP TABLE LIKE statement.
///
/// # Example
/// ```ignore
/// let sql = create_temp_table_like("_temp_123", "users");
/// // Produces: CREATE TEMP TABLE "_temp_123" (LIKE "users" INCLUDING ALL)
/// ```
pub fn create_temp_table_like(temp_name: &str, source_table: &str) -> String {
    let quoted_temp = quote_identifier(temp_name);
    let quoted_source = quote_identifier(source_table);
    format!(
        "CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL)",
        quoted_temp, quoted_source
    )
}

/// Safely format an INSERT INTO ... SELECT statement with conflict handling.
///
/// # Example
/// ```ignore
/// let sql = insert_from_select("users", "_temp_123", "_tx_id, _row_index");
/// // Produces: INSERT INTO "users" SELECT * FROM "_temp_123" ON CONFLICT (_tx_id, _row_index) DO NOTHING
/// ```
pub fn insert_from_select(
    target_table: &str,
    source_table: &str,
    conflict_columns: &str,
) -> String {
    let quoted_target = quote_identifier(target_table);
    let quoted_source = quote_identifier(source_table);
    format!(
        "INSERT INTO {} SELECT * FROM {} ON CONFLICT ({}) DO NOTHING",
        quoted_target, quoted_source, conflict_columns
    )
}

/// Safely format a DELETE with BETWEEN clause (for parameterized bounds).
///
/// The bounds should be provided as `$1` and `$2` parameters when executing.
///
/// # Example
/// ```ignore
/// let sql = delete_between("users", "_tx_id");
/// // Produces: DELETE FROM "users" WHERE _tx_id BETWEEN $1 AND $2
/// // Execute with: sqlx::query(&sql).bind(start).bind(end).execute(pool).await
/// ```
pub fn delete_between(table_name: &str, column: &str) -> String {
    let quoted_table = quote_identifier(table_name);
    format!(
        "DELETE FROM {} WHERE {} BETWEEN $1 AND $2",
        quoted_table, column
    )
}

/// Quote a column name for safe use in SQL.
///
/// This ensures column names (especially reserved keywords like "to", "from", "select")
/// are properly quoted for PostgreSQL. Uses `pg_escape::quote_identifier()` which:
/// - Wraps identifiers in double quotes when needed
/// - Escapes internal double quotes by doubling them
/// - Handles all PostgreSQL identifier edge cases
///
/// # Example
/// ```ignore
/// let quoted = quote_column("to");
/// // Produces: "to"
///
/// let safe = quote_column("my_column");
/// // May produce: my_column (no quotes needed) or "my_column" (implementation dependent)
/// ```
pub fn quote_column(column_name: &str) -> String {
    quote_identifier(column_name).to_string()
}

/// Build a column definition for CREATE TABLE with proper identifier quoting.
///
/// Formats a column as: `"column_name" TYPE [NOT NULL]`
///
/// # Example
/// ```ignore
/// let col_def = column_definition("to", "TEXT", false);
/// // Produces: "to" TEXT NOT NULL
///
/// let col_def = column_definition("user_id", "BIGINT", true);
/// // Produces: "user_id" BIGINT
/// ```
pub fn column_definition(column_name: &str, pg_type: &str, nullable: bool) -> String {
    let quoted_name = quote_identifier(column_name);
    let nullability = if nullable { "" } else { " NOT NULL" };
    format!("{} {}{}", quoted_name, pg_type, nullability)
}

/// Build a DataFusion streaming query for a fully qualified dataset table.
///
/// This constructs a query in the format:
/// `SELECT * FROM "namespace/name@revision".table`
///
/// The dataset reference (namespace/name@revision) is quoted as a single identifier
/// because it contains special characters (`/` and `@`) that would otherwise cause
/// SQL parsing errors. This format is then parsed by the catalog layer which extracts
/// the dataset reference from the schema portion.
///
/// **Safety**: Both the dataset reference and table name are properly quoted using
/// `pg_escape::quote_identifier()` for SQL safety.
pub fn streaming_query(dataset: &Reference, table: &str) -> String {
    let schema = format!("{}@{}", dataset.as_fqn(), dataset.revision());
    let quoted_schema = quote_identifier(&schema);
    let quoted_table = quote_identifier(table);
    format!("SELECT * FROM {}.{}", quoted_schema, quoted_table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("user_accounts").is_ok());
        assert!(validate_identifier("_internal").is_ok());
        assert!(validate_identifier("table123").is_ok());
        assert!(validate_identifier("t$ble").is_ok()); // $ is allowed
    }

    #[test]
    fn test_validate_identifier_empty() {
        let err = validate_identifier("").unwrap_err();
        assert!(matches!(err, ValidateIdentifierError::Empty));
    }

    #[test]
    fn test_validate_identifier_too_long() {
        let long_name = "a".repeat(64);
        let err = validate_identifier(&long_name).unwrap_err();
        assert!(matches!(err, ValidateIdentifierError::TooLong { .. }));
    }

    #[test]
    fn test_validate_identifier_invalid_chars() {
        assert!(validate_identifier("user-table").is_err());
        assert!(validate_identifier("user table").is_err());
        assert!(validate_identifier("user.table").is_err());
        assert!(validate_identifier("user'table").is_err());
        assert!(validate_identifier("user\"table").is_err());
    }

    #[test]
    fn test_validate_identifier_invalid_first_char() {
        let err = validate_identifier("123table").unwrap_err();
        assert!(matches!(
            err,
            ValidateIdentifierError::InvalidFirstCharacter { .. }
        ));
    }

    #[test]
    fn test_validate_identifier_sql_injection() {
        assert!(validate_identifier("users; DROP TABLE").is_err());
        assert!(validate_identifier("users--").is_err());
        assert!(validate_identifier("users\"; DROP TABLE sensitive_data; --").is_err());
    }

    #[test]
    fn test_create_table_formats_correctly() {
        let sql = create_table("users", "id BIGINT, name TEXT", "id");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        // pg_escape may or may not add quotes depending on the identifier
        assert!(sql.contains("users") || sql.contains("\"users\""));
        assert!(sql.contains("PRIMARY KEY (id)"));
    }

    #[test]
    fn test_copy_from_stdin_formats_correctly() {
        let sql = copy_from_stdin("users");
        assert!(sql.contains("COPY"));
        assert!(sql.contains("users") || sql.contains("\"users\""));
        assert!(sql.contains("FROM STDIN"));
        assert!(sql.contains("FORMAT BINARY"));
    }

    #[test]
    fn test_delete_between_formats_correctly() {
        let sql = delete_between("users", "_tx_id");
        assert!(sql.contains("DELETE FROM"));
        assert!(sql.contains("users") || sql.contains("\"users\""));
        assert!(sql.contains("_tx_id BETWEEN $1 AND $2"));
    }
}
