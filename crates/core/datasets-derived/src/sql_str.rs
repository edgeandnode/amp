//! SQL string validation and types.
//!
//! This module provides the [`SqlStr`] type for strings intended to contain SQL.
//! It performs minimal validation (non-empty, non-whitespace) and defers actual
//! SQL statement parsing to the query engine.

/// A string intended to contain SQL, with minimal validation.
///
/// This type performs basic string validation but does **not** parse or validate
/// SQL syntax. Actual SQL statement parsing is deferred to the query engine.
///
/// [`SqlStr`] is a marker type that provides minimal guarantees: the string is
/// non-empty and contains at least one non-whitespace character.
///
/// ## Validation Rules
///
/// [`SqlStr`] must:
/// - **Not be empty** (minimum length of 1 character)
/// - **Not be only whitespace** (must contain at least one non-whitespace character)
///
/// ## Non-Goals
///
/// This type does **not**:
/// - Parse SQL syntax
/// - Validate SQL statement correctness
/// - Check for SQL injection vulnerabilities
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct SqlStr(#[cfg_attr(feature = "schemars", schemars(length(min = 1)))] String);

impl SqlStr {
    /// Creates a new `SqlStr` without validating the input.
    ///
    /// # Safety
    ///
    /// This function bypasses all validation checks. The caller must ensure that the input
    /// string is non-empty and contains at least one non-whitespace character. Using this
    /// function with invalid input (empty string or whitespace-only) will violate the type's
    /// invariants and may lead to undefined behavior in code that relies on these guarantees.
    ///
    /// Only use this function when you have already validated the input through other means
    /// or when performance is critical, and you can guarantee the input is valid.
    pub fn new_unchecked(sql: String) -> Self {
        SqlStr(sql)
    }

    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the SqlStr and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for SqlStr {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<SqlStr> for String {
    fn eq(&self, other: &SqlStr) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for SqlStr {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<SqlStr> for str {
    fn eq(&self, other: &SqlStr) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for SqlStr {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<SqlStr> for &str {
    fn eq(&self, other: &SqlStr) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for SqlStr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<SqlStr> for SqlStr {
    fn as_ref(&self) -> &SqlStr {
        self
    }
}

impl std::ops::Deref for SqlStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for SqlStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for SqlStr {
    type Err = SqlStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        validate_sql_str(trimmed)?;
        Ok(SqlStr(trimmed.to_string()))
    }
}

impl serde::Serialize for SqlStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SqlStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Validates that a string is non-empty and contains at least one non-whitespace character.
///
/// This performs minimal validation only. It does **not** parse or validate SQL syntax.
///
/// ## Validation Rules
///
/// - Must not be empty
/// - Must contain at least one non-whitespace character
pub fn validate_sql_str(sql: &str) -> Result<(), SqlStrError> {
    if sql.is_empty() || sql.trim().is_empty() {
        return Err(SqlStrError {
            value: sql.to_string(),
        });
    }

    Ok(())
}

/// Error when a string fails minimal validation during [`SqlStr`] construction.
///
/// This error occurs when attempting to create a [`SqlStr`] from a string that is
/// either completely empty or contains only whitespace characters (spaces, tabs, newlines).
///
/// Note: This error only indicates the string failed minimal validation, not that
/// it failed SQL syntax parsing (which is deferred to the query engine).
#[derive(Debug, thiserror::Error)]
#[error("string cannot be empty or contain only whitespace, got: {value}")]
pub struct SqlStrError {
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::SqlStr;

    #[test]
    fn parse_with_simple_query_succeeds() {
        //* Given
        let input = "SELECT * FROM table";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with simple query");
        let sql = result.expect("should return valid SqlStr");
        assert_eq!(sql, "SELECT * FROM table");
    }

    #[test]
    fn parse_with_complex_query_succeeds() {
        //* Given
        let input = "SELECT id, name FROM users WHERE active = true";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with complex query");
        let sql = result.expect("should return valid SqlStr");
        assert_eq!(sql, "SELECT id, name FROM users WHERE active = true");
    }

    #[test]
    fn parse_with_leading_trailing_whitespace_succeeds() {
        //* Given
        let input = "  SELECT * FROM table  ";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with leading/trailing whitespace"
        );
        let sql = result.expect("should return valid SqlStr");
        assert_eq!(sql, "SELECT * FROM table");
    }

    #[test]
    fn parse_with_newlines_succeeds() {
        //* Given
        let input = "\nSELECT *\nFROM table\n";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with newlines");
        let sql = result.expect("should return valid SqlStr");
        assert_eq!(
            sql,
            "SELECT *
FROM table"
        );
    }

    #[test]
    fn parse_with_empty_string_fails() {
        //* Given
        let input = "";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with empty string");
    }

    #[test]
    fn parse_with_only_spaces_fails() {
        //* Given
        let input = "   ";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with only spaces");
    }

    #[test]
    fn parse_with_only_whitespace_fails() {
        //* Given
        let input = "\n\t  \n";

        //* When
        let result: Result<SqlStr, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with only whitespace characters"
        );
    }
}
