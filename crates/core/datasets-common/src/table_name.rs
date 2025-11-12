//! SQL table name validation and types.
//!
//! This module provides the `TableName` type for validated SQL table names that enforce
//! naming conventions and ensure compatibility across SQL engines (DataFusion, PostgreSQL).

use std::sync::Arc;

/// Maximum length for SQL identifiers (PostgreSQL standard).
const MAX_IDENTIFIER_LENGTH: usize = 63;

/// A validated SQL table name that enforces naming conventions.
///
/// Table names must follow SQL identifier rules to ensure compatibility
/// across SQL engines (DataFusion, PostgreSQL).
///
/// ## Format Requirements
///
/// A valid table name must:
/// - **Start** with a letter (`a-z`, `A-Z`) or underscore (`_`)
/// - **Contain** only letters, digits (`0-9`), underscores (`_`), and dollar signs (`$`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Not exceed** 63 bytes (PostgreSQL identifier limit)
///
/// ## Case Sensitivity
///
/// The type stores the table name as-is. However, note that:
/// - Unquoted identifiers in SQL are normalized to lowercase by DataFusion/PostgreSQL
/// - Quoted identifiers (in double quotes) preserve their case
#[derive(Debug, Clone, Eq, Ord, PartialOrd)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct TableName(
    #[cfg_attr(
        feature = "schemars",
        schemars(regex(pattern = r"^[a-zA-Z_][a-zA-Z0-9_$]*$"))
    )]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1, max = 63)))]
    String,
);

impl TableName {
    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the TableName and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for TableName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for TableName {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<TableName> for str {
    fn eq(&self, other: &TableName) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for TableName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<Arc<TableName>> for TableName {
    fn eq(&self, other: &Arc<TableName>) -> bool {
        self.0 == other.0
    }
}

impl PartialEq<TableName> for Arc<TableName> {
    fn eq(&self, other: &TableName) -> bool {
        self.0 == other.0
    }
}

impl std::hash::Hash for TableName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for TableName {
    type Err = TableNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_table_name(s)?;
        Ok(TableName(s.to_string()))
    }
}

impl serde::Serialize for TableName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for TableName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "metadata-db")]
impl From<metadata_db::physical_table::TableNameOwned> for TableName {
    fn from(value: metadata_db::physical_table::TableNameOwned) -> Self {
        // Convert to string and wrap - this should always be valid since TableNameOwned
        // comes from the database and should already be validated
        TableName(value.into_inner())
    }
}

#[cfg(feature = "metadata-db")]
impl From<TableName> for metadata_db::physical_table::TableNameOwned {
    fn from(value: TableName) -> Self {
        // SAFETY: TableName is validated at construction via FromStr, ensuring invariants are upheld.
        metadata_db::physical_table::TableName::from_owned_unchecked(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a TableName> for metadata_db::physical_table::TableName<'a> {
    fn from(value: &'a TableName) -> Self {
        // SAFETY: TableName is validated at construction via FromStr, ensuring invariants are upheld.
        metadata_db::physical_table::TableName::from_ref_unchecked(&value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::physical_table::TableName<'a>> for TableName {
    fn eq(&self, other: &metadata_db::physical_table::TableName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<TableName> for metadata_db::physical_table::TableName<'a> {
    fn eq(&self, other: &TableName) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&metadata_db::physical_table::TableName<'a>> for TableName {
    fn eq(&self, other: &&metadata_db::physical_table::TableName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<TableName> for &metadata_db::physical_table::TableName<'a> {
    fn eq(&self, other: &TableName) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&TableName> for metadata_db::physical_table::TableName<'a> {
    fn eq(&self, other: &&TableName) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::physical_table::TableName<'a>> for &TableName {
    fn eq(&self, other: &metadata_db::physical_table::TableName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Validates that a table name follows SQL identifier rules.
///
/// Checks:
/// - Not empty
/// - Not longer than 63 bytes (PostgreSQL limit)
/// - First character is a letter (`a-z`, `A-Z`) or underscore (`_`)
/// - All characters are letters, digits, underscores, or dollar signs (`[a-zA-Z0-9_$]`)
pub fn validate_table_name(name: &str) -> Result<(), TableNameError> {
    if name.is_empty() {
        return Err(TableNameError::Empty);
    }

    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(TableNameError::TooLong { length: name.len() });
    }

    // Check first character: must be letter or underscore
    let mut chars = name.chars();
    if let Some(first_char) = chars.next()
        && !(first_char.is_ascii_alphabetic() || first_char == '_')
    {
        return Err(TableNameError::InvalidFirstCharacter {
            character: first_char,
            value: name.to_string(),
        });
    }

    // Check remaining characters: must be alphanumeric, underscore, or dollar sign
    if let Some(invalid_char) = name
        .chars()
        .find(|&c| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
    {
        return Err(TableNameError::InvalidCharacter {
            character: invalid_char,
            value: name.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`TableName`] parsing failures.
#[derive(Debug, thiserror::Error)]
pub enum TableNameError {
    /// Table name is empty.
    #[error("table name cannot be empty")]
    Empty,
    /// Table name exceeds maximum length.
    #[error("table name is too long ({length} bytes, maximum is {MAX_IDENTIFIER_LENGTH})")]
    TooLong { length: usize },
    /// Table name starts with invalid character.
    #[error("table name '{value}' must start with a letter or underscore, not '{character}'")]
    InvalidFirstCharacter { character: char, value: String },
    /// Table name contains invalid character.
    #[error("invalid character '{character}' in table name '{value}'")]
    InvalidCharacter { character: char, value: String },
}

#[cfg(test)]
mod tests {
    use super::{TableNameError, validate_table_name};

    #[test]
    fn accept_valid_table_names() {
        // Lowercase
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("user_accounts").is_ok());
        assert!(validate_table_name("table123").is_ok());

        // Uppercase
        assert!(validate_table_name("Users").is_ok());
        assert!(validate_table_name("USER_ACCOUNTS").is_ok());
        assert!(validate_table_name("TABLE123").is_ok());

        // Mixed case
        assert!(validate_table_name("UserAccounts").is_ok());
        assert!(validate_table_name("Table_123").is_ok());

        // Starts with underscore
        assert!(validate_table_name("_users").is_ok());
        assert!(validate_table_name("_").is_ok());

        // Contains dollar sign
        assert!(validate_table_name("user$table").is_ok());
        assert!(validate_table_name("table$123").is_ok());

        // Single character
        assert!(validate_table_name("a").is_ok());
        assert!(validate_table_name("A").is_ok());

        // Max length (63 bytes)
        let max_length_name = "a".repeat(63);
        assert!(validate_table_name(&max_length_name).is_ok());
    }

    #[test]
    fn reject_empty_table_name() {
        let result = validate_table_name("");
        assert!(matches!(result, Err(TableNameError::Empty)));
    }

    #[test]
    fn reject_too_long_table_name() {
        let too_long_name = "a".repeat(64);
        let result = validate_table_name(&too_long_name);
        assert!(matches!(
            result,
            Err(TableNameError::TooLong { length: 64 })
        ));
    }

    #[test]
    fn reject_starts_with_digit() {
        let result = validate_table_name("123table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidFirstCharacter { character: '1', .. })
        ));
    }

    #[test]
    fn reject_starts_with_dollar() {
        let result = validate_table_name("$table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidFirstCharacter { character: '$', .. })
        ));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphen
        let result = validate_table_name("user-table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: '-', .. })
        ));

        // Space
        let result = validate_table_name("user table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: ' ', .. })
        ));

        // Dot
        let result = validate_table_name("user.table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: '.', .. })
        ));

        // Special characters
        let result = validate_table_name("user@table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_table_name("user#table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: '#', .. })
        ));

        let result = validate_table_name("user!table");
        assert!(matches!(
            result,
            Err(TableNameError::InvalidCharacter { character: '!', .. })
        ));
    }
}
