//! Dependency alias validation and types.
//!
//! This module provides the `DepAlias` type for validated dependency aliases in derived datasets.
//! Dependency aliases are used as keys in the `dependencies` map to reference external datasets
//! within SQL queries and function definitions.

/// Maximum length for dependency aliases (matching SQL identifier limit).
const MAX_ALIAS_LENGTH: usize = 63;

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

#[cfg(test)]
mod tests {
    use super::{DepAliasError, validate_dep_alias};

    #[test]
    fn accept_valid_dep_aliases() {
        // Lowercase
        assert!(validate_dep_alias("eth").is_ok());
        assert!(validate_dep_alias("eth_mainnet").is_ok());
        assert!(validate_dep_alias("dataset123").is_ok());

        // Uppercase
        assert!(validate_dep_alias("ETH").is_ok());
        assert!(validate_dep_alias("ETH_MAINNET").is_ok());
        assert!(validate_dep_alias("DATASET123").is_ok());

        // Mixed case
        assert!(validate_dep_alias("EthMainnet").is_ok());
        assert!(validate_dep_alias("Dataset_123").is_ok());

        // Contains underscore (but not at start)
        assert!(validate_dep_alias("eth_mainnet").is_ok());

        // Single character
        assert!(validate_dep_alias("a").is_ok());
        assert!(validate_dep_alias("A").is_ok());

        // Max length (63 bytes)
        let max_length_alias = "a".repeat(63);
        assert!(validate_dep_alias(&max_length_alias).is_ok());
    }

    #[test]
    fn reject_empty_dep_alias() {
        let result = validate_dep_alias("");
        assert!(matches!(result, Err(DepAliasError::Empty)));
    }

    #[test]
    fn reject_too_long_dep_alias() {
        let too_long_alias = "a".repeat(64);
        let result = validate_dep_alias(&too_long_alias);
        assert!(matches!(result, Err(DepAliasError::TooLong { length: 64 })));
    }

    #[test]
    fn reject_starts_with_digit() {
        let result = validate_dep_alias("123eth");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidFirstCharacter { character: '1', .. })
        ));
    }

    #[test]
    fn reject_starts_with_underscore() {
        let result = validate_dep_alias("_eth");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidFirstCharacter { character: '_', .. })
        ));

        let result = validate_dep_alias("_");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidFirstCharacter { character: '_', .. })
        ));
    }

    #[test]
    fn reject_dollar_sign() {
        // Dollar sign not allowed in first position
        let result = validate_dep_alias("$eth");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidFirstCharacter { character: '$', .. })
        ));

        // Dollar sign not allowed anywhere
        let result = validate_dep_alias("eth$mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '$', .. })
        ));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphen
        let result = validate_dep_alias("eth-mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '-', .. })
        ));

        // Space
        let result = validate_dep_alias("eth mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: ' ', .. })
        ));

        // Dot
        let result = validate_dep_alias("eth.mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '.', .. })
        ));

        // Special characters
        let result = validate_dep_alias("eth@mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_dep_alias("eth#mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '#', .. })
        ));

        let result = validate_dep_alias("eth!mainnet");
        assert!(matches!(
            result,
            Err(DepAliasError::InvalidCharacter { character: '!', .. })
        ));
    }
}
