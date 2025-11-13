//! Dependency alias validation and types.
//!
//! This module provides the `DepAlias` type for validated dependency aliases in derived datasets.
//! Dependency aliases are used as keys in the `dependencies` map to reference external datasets
//! within SQL queries and function definitions.

/// Maximum length for dependency aliases (matching SQL identifier limit).
const MAX_ALIAS_LENGTH: usize = 63;

/// Reserved keyword for self-referencing same-dataset functions.
///
/// The literal string `"self"` is reserved for referencing functions within the same dataset.
/// This constant ensures consistent usage of the self-reference keyword across the codebase.
pub const SELF_REF_KEYWORD: &str = "self";

/// A validated dependency alias for referencing external datasets.
///
/// Dependency aliases are used as keys in the derived dataset manifest's `dependencies` map.
/// They serve as short, readable identifiers that can be referenced in SQL queries and
/// function definitions instead of using full dataset references.
///
/// ## Format Requirements
///
/// A valid dependency alias must:
/// - **Start** with a letter (`a-z`, `A-Z`) only (no underscore or digit)
/// - **Contain** only letters, digits (`0-9`), and underscores (`_`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Not exceed** 63 bytes (SQL identifier limit)
///
/// Note: Dollar signs (`$`) are not allowed anywhere in dependency aliases.
///
/// ## Case Sensitivity
///
/// The type stores the alias as-is. Usage in SQL contexts may normalize case depending
/// on the SQL engine's identifier handling rules.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct DepAlias(
    #[cfg_attr(
        feature = "schemars",
        schemars(regex(pattern = r"^[a-zA-Z][a-zA-Z0-9_]*$"))
    )]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1, max = 63)))]
    String,
);

impl DepAlias {
    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the DepAlias and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for DepAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for DepAlias {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for DepAlias {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<DepAlias> for str {
    fn eq(&self, other: &DepAlias) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for DepAlias {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl std::fmt::Display for DepAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for DepAlias {
    type Err = DepAliasError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_dep_alias(s)?;
        Ok(DepAlias(s.to_string()))
    }
}

impl serde::Serialize for DepAlias {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DepAlias {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Validates that a dependency alias follows identifier rules.
///
/// Checks:
/// - Not empty
/// - Not longer than 63 bytes
/// - First character is a letter (`a-z`, `A-Z`) only
/// - All characters are letters, digits, or underscores (`[a-zA-Z0-9_]`)
pub fn validate_dep_alias(alias: &str) -> Result<(), DepAliasError> {
    if alias.is_empty() {
        return Err(DepAliasError::Empty);
    }

    if alias.len() > MAX_ALIAS_LENGTH {
        return Err(DepAliasError::TooLong {
            length: alias.len(),
        });
    }

    // Check first character: must be letter only (no underscore or digit)
    let mut chars = alias.chars();
    if let Some(first_char) = chars.next()
        && !first_char.is_ascii_alphabetic()
    {
        return Err(DepAliasError::InvalidFirstCharacter {
            character: first_char,
            value: alias.to_string(),
        });
    }

    // Check remaining characters: must be alphanumeric or underscore (no dollar sign)
    if let Some(invalid_char) = alias
        .chars()
        .find(|&c| !(c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(DepAliasError::InvalidCharacter {
            character: invalid_char,
            value: alias.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`DepAlias`] parsing failures.
///
/// This error type is used when parsing strings into [`DepAlias`] instances via
/// [`std::str::FromStr`]. All variants include relevant context for debugging.
#[derive(Debug, thiserror::Error)]
pub enum DepAliasError {
    /// Dependency alias is empty
    ///
    /// This occurs when an empty string is provided as a dependency alias.
    /// Dependency aliases must contain at least one character.
    ///
    /// Common causes:
    /// - Empty string passed to `parse()` or `from_str()`
    /// - Whitespace-only input that was trimmed
    /// - Programmatic construction with empty string
    #[error("dependency alias cannot be empty")]
    Empty,

    /// Dependency alias exceeds maximum length
    ///
    /// This occurs when the input string exceeds 63 bytes, which is the PostgreSQL
    /// standard identifier length limit. This limit ensures compatibility with SQL
    /// databases and prevents issues when aliases are used as schema qualifiers in
    /// DataFusion TableReferences.
    ///
    /// The length check is performed on the byte length, not character count, which
    /// means multi-byte UTF-8 characters count as multiple bytes toward the limit.
    #[error("dependency alias is too long ({length} bytes, maximum is {MAX_ALIAS_LENGTH})")]
    TooLong {
        /// The actual length of the provided alias in bytes
        length: usize,
    },

    /// Dependency alias starts with invalid character
    ///
    /// This occurs when the first character of the alias is not a letter (a-z, A-Z).
    /// Dependency aliases must start with a letter to ensure compatibility with SQL
    /// identifiers and prevent conflicts with numeric literals or special syntax.
    ///
    /// Common invalid first characters:
    /// - Digits (0-9) - would be ambiguous with numeric literals
    /// - Underscore (_) - reserved for internal/system identifiers
    /// - Special characters ($, @, #, etc.) - have special meaning in SQL
    ///
    /// The entire input value is included for debugging purposes.
    #[error("dependency alias '{value}' must start with a letter, not '{character}'")]
    InvalidFirstCharacter {
        /// The invalid first character that was encountered
        character: char,
        /// The complete input string that was being parsed
        value: String,
    },

    /// Dependency alias contains invalid character
    ///
    /// This occurs when the alias contains a character that is not a letter, digit,
    /// or underscore. Only the characters [a-zA-Z0-9_] are allowed after the first
    /// character to ensure SQL identifier compatibility.
    ///
    /// Common invalid characters:
    /// - Hyphens (-) - not allowed in SQL identifiers without quoting
    /// - Dots (.) - used as delimiter in qualified names
    /// - Spaces - not allowed in identifiers without quoting
    /// - Dollar signs ($) - not allowed to maintain consistency
    /// - Special characters (@, #, !, etc.) - have special meaning in SQL
    ///
    /// The entire input value is included for debugging purposes.
    #[error("invalid character '{character}' in dependency alias '{value}'")]
    InvalidCharacter {
        /// The invalid character that was encountered
        character: char,
        /// The complete input string that was being parsed
        value: String,
    },
}

/// Schema qualifier for function references: dependency alias or `self`.
///
/// Represents the schema portion of qualified function references in SQL queries,
/// supporting either external dependency references or intra-dataset self-references.
///
/// ## Variants
///
/// - **`DepAlias`**: References external dataset dependency (e.g., `eth_mainnet.logs`)
/// - **`SelfRef`**: References current dataset function (e.g., `self.addSuffix`)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DepAliasOrSelfRef {
    /// External dataset dependency reference
    DepAlias(DepAlias),

    /// Current dataset self-reference
    SelfRef,
}

impl DepAliasOrSelfRef {
    /// Returns `true` if this is a `SelfRef` reference.
    pub fn is_self(&self) -> bool {
        matches!(self, DepAliasOrSelfRef::SelfRef)
    }

    /// Returns `true` if this is a `DepAlias` reference.
    pub fn is_dependency(&self) -> bool {
        matches!(self, DepAliasOrSelfRef::DepAlias(_))
    }

    /// Returns the dependency alias if this is a `DepAlias`, `None` otherwise.
    pub fn as_dependency(&self) -> Option<&DepAlias> {
        match self {
            DepAliasOrSelfRef::DepAlias(alias) => Some(alias),
            DepAliasOrSelfRef::SelfRef => None,
        }
    }
}

impl std::fmt::Display for DepAliasOrSelfRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DepAliasOrSelfRef::DepAlias(alias) => write!(f, "{}", alias),
            DepAliasOrSelfRef::SelfRef => write!(f, "{}", SELF_REF_KEYWORD),
        }
    }
}

impl std::str::FromStr for DepAliasOrSelfRef {
    type Err = DepAliasOrSelfRefError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == SELF_REF_KEYWORD {
            Ok(DepAliasOrSelfRef::SelfRef)
        } else {
            let alias = s.parse::<DepAlias>().map_err(|err| {
                DepAliasOrSelfRefError::InvalidDependencyAlias {
                    value: s.to_string(),
                    source: err,
                }
            })?;
            Ok(DepAliasOrSelfRef::DepAlias(alias))
        }
    }
}

impl serde::Serialize for DepAliasOrSelfRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DepAliasOrSelfRef::DepAlias(alias) => alias.serialize(serializer),
            DepAliasOrSelfRef::SelfRef => serializer.serialize_str(SELF_REF_KEYWORD),
        }
    }
}

impl<'de> serde::Deserialize<'de> for DepAliasOrSelfRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Error type for [`DepAliasOrSelfRef`] parsing failures.
///
/// This error type is used when parsing strings into [`DepAliasOrSelfRef`] instances via
/// [`std::str::FromStr`]. Since `DepAliasOrSelfRef` accepts both the literal `"self"` keyword
/// and validated dependency aliases, this error only occurs when the input is neither
/// `"self"` nor a valid dependency alias.
#[derive(Debug, thiserror::Error)]
pub enum DepAliasOrSelfRefError {
    /// Input is not "self" and failed dependency alias validation
    ///
    /// This occurs when attempting to parse a string that is not the reserved keyword `"self"`
    /// and does not conform to dependency alias validation rules. The input must be either:
    /// - The literal string `"self"` (case-sensitive) for same-dataset function references
    /// - A valid dependency alias matching identifier rules (start with letter, contain only
    ///   alphanumeric characters and underscores, max 63 bytes)
    ///
    /// Common causes:
    /// - Typo in the `"self"` keyword (e.g., `"Self"`, `"SELF"`, `"slf"`)
    /// - Invalid dependency alias format (starts with digit, contains special characters)
    /// - Empty string or whitespace-only input
    /// - Dependency alias exceeding maximum length
    ///
    /// The underlying [`DepAliasError`] provides detailed information about the specific
    /// validation failure (e.g., invalid first character, invalid character in alias,
    /// empty string, too long).
    ///
    /// # Examples of Invalid Input
    ///
    /// ```text
    /// "Self"         -> Not exactly "self" (case-sensitive), not valid alias (uppercase 'S')
    /// "123invalid"   -> Not "self", not valid alias (starts with digit)
    /// "my-alias"     -> Not "self", not valid alias (contains hyphen)
    /// "_private"     -> Not "self", not valid alias (starts with underscore)
    /// ""             -> Not "self", not valid alias (empty string)
    /// ```
    ///
    /// # Examples of Valid Input
    ///
    /// ```text
    /// "self"         -> SelfRef (reserved keyword)
    /// "eth_mainnet"  -> DependencyRef("eth_mainnet")
    /// "myDataset"    -> DependencyRef("myDataset")
    /// "d"            -> DependencyRef("d")
    /// ```
    #[error("invalid dependency alias '{value}' in function schema: {source}")]
    InvalidDependencyAlias {
        /// The complete input string that failed parsing
        value: String,
        /// The underlying dependency alias validation error providing specific details
        #[source]
        source: DepAliasError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // DepAlias validation tests

    #[test]
    fn validate_dep_alias_with_lowercase_succeeds() {
        //* When
        let result = validate_dep_alias("eth");

        //* Then
        assert!(result.is_ok(), "lowercase alias should be valid");
    }

    #[test]
    fn validate_dep_alias_with_lowercase_and_underscore_succeeds() {
        //* When
        let result = validate_dep_alias("eth_mainnet");

        //* Then
        assert!(result.is_ok(), "lowercase with underscore should be valid");
    }

    #[test]
    fn validate_dep_alias_with_lowercase_and_digits_succeeds() {
        //* When
        let result = validate_dep_alias("dataset123");

        //* Then
        assert!(result.is_ok(), "lowercase with digits should be valid");
    }

    #[test]
    fn validate_dep_alias_with_uppercase_succeeds() {
        //* When
        let result = validate_dep_alias("ETH");

        //* Then
        assert!(result.is_ok(), "uppercase alias should be valid");
    }

    #[test]
    fn validate_dep_alias_with_mixed_case_succeeds() {
        //* When
        let result = validate_dep_alias("EthMainnet");

        //* Then
        assert!(result.is_ok(), "mixed case alias should be valid");
    }

    #[test]
    fn validate_dep_alias_with_single_character_succeeds() {
        //* When
        let result = validate_dep_alias("a");

        //* Then
        assert!(result.is_ok(), "single character alias should be valid");
    }

    #[test]
    fn validate_dep_alias_with_max_length_succeeds() {
        //* Given
        let max_length_alias = "a".repeat(63);

        //* When
        let result = validate_dep_alias(&max_length_alias);

        //* Then
        assert!(result.is_ok(), "63-byte alias should be valid");
    }

    #[test]
    fn validate_dep_alias_with_empty_string_fails() {
        //* When
        let result = validate_dep_alias("");

        //* Then
        assert!(result.is_err(), "empty string should fail validation");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, DepAliasError::Empty),
            "Expected Empty error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_too_long_string_fails() {
        //* Given
        let too_long_alias = "a".repeat(64);

        //* When
        let result = validate_dep_alias(&too_long_alias);

        //* Then
        assert!(result.is_err(), "64-byte alias should fail validation");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, DepAliasError::TooLong { length: 64 }),
            "Expected TooLong error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_digit_first_fails() {
        //* When
        let result = validate_dep_alias("123eth");

        //* Then
        assert!(result.is_err(), "alias starting with digit should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidFirstCharacter { character: '1', .. }
            ),
            "Expected InvalidFirstCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_underscore_first_fails() {
        //* When
        let result = validate_dep_alias("_eth");

        //* Then
        assert!(
            result.is_err(),
            "alias starting with underscore should fail"
        );
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidFirstCharacter { character: '_', .. }
            ),
            "Expected InvalidFirstCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_dollar_first_fails() {
        //* When
        let result = validate_dep_alias("$eth");

        //* Then
        assert!(
            result.is_err(),
            "alias starting with dollar sign should fail"
        );
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidFirstCharacter { character: '$', .. }
            ),
            "Expected InvalidFirstCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_dollar_anywhere_fails() {
        //* When
        let result = validate_dep_alias("eth$mainnet");

        //* Then
        assert!(result.is_err(), "alias with dollar sign should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidCharacter { character: '$', .. }
            ),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_hyphen_fails() {
        //* When
        let result = validate_dep_alias("eth-mainnet");

        //* Then
        assert!(result.is_err(), "alias with hyphen should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidCharacter { character: '-', .. }
            ),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_space_fails() {
        //* When
        let result = validate_dep_alias("eth mainnet");

        //* Then
        assert!(result.is_err(), "alias with space should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidCharacter { character: ' ', .. }
            ),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    #[test]
    fn validate_dep_alias_with_dot_fails() {
        //* When
        let result = validate_dep_alias("eth.mainnet");

        //* Then
        assert!(result.is_err(), "alias with dot should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(
                error,
                DepAliasError::InvalidCharacter { character: '.', .. }
            ),
            "Expected InvalidCharacter error, got {:?}",
            error
        );
    }

    // DepAliasOrSelfRef parsing tests

    #[test]
    fn parse_dep_alias_or_self_ref_with_self_succeeds() {
        //* When
        let result = SELF_REF_KEYWORD.parse::<DepAliasOrSelfRef>();

        //* Then
        assert!(result.is_ok(), "parsing 'self' should succeed");
        let schema = result.expect("should return valid schema");
        assert!(
            matches!(schema, DepAliasOrSelfRef::SelfRef),
            "Expected SelfRef, got {:?}",
            schema
        );
        assert_eq!(schema.to_string(), SELF_REF_KEYWORD);
    }

    #[test]
    fn parse_dep_alias_or_self_ref_with_valid_alias_succeeds() {
        //* When
        let result = "eth_mainnet".parse::<DepAliasOrSelfRef>();

        //* Then
        assert!(result.is_ok(), "parsing valid alias should succeed");
        let schema = result.expect("should return valid schema");
        assert!(
            matches!(schema, DepAliasOrSelfRef::DepAlias(_)),
            "Expected DepAlias, got {:?}",
            schema
        );
        assert_eq!(schema.to_string(), "eth_mainnet");
    }

    #[test]
    fn parse_dep_alias_or_self_ref_with_invalid_alias_fails() {
        //* When
        let result = "123invalid".parse::<DepAliasOrSelfRef>();

        //* Then
        assert!(result.is_err(), "parsing invalid alias should fail");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, DepAliasOrSelfRefError::InvalidDependencyAlias { .. }),
            "Expected InvalidDependencyAlias error, got {:?}",
            error
        );
    }
}
