//! Provider name validation and types.
//!
//! This module provides the `ProviderName` type for validated provider names that enforce
//! naming conventions and constraints required across the system.

use std::borrow::Borrow;

/// A validated provider name that enforces naming conventions and constraints.
///
/// Provider names must follow strict rules to ensure compatibility across systems,
/// file systems, and network protocols. This type provides compile-time guarantees
/// that all instances contain valid provider names.
///
/// ## Format Requirements
///
/// A valid provider name must:
/// - **Start** with a lowercase letter (`a-z`) or underscore (`_`)
/// - **Contain** only lowercase letters (`a-z`), digits (`0-9`), and underscores (`_`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Have no spaces** or special characters (except underscore)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProviderName(String);

impl ProviderName {
    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the ProviderName and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for ProviderName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ProviderName> for String {
    fn eq(&self, other: &ProviderName) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for ProviderName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<ProviderName> for str {
    fn eq(&self, other: &ProviderName) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for ProviderName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<ProviderName> for &str {
    fn eq(&self, other: &ProviderName) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for ProviderName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<ProviderName> for ProviderName {
    #[inline(always)]
    fn as_ref(&self) -> &ProviderName {
        self
    }
}

impl std::ops::Deref for ProviderName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for ProviderName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ProviderName {
    type Error = InvalidProviderName;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_provider_name(&value)?;
        Ok(ProviderName(value))
    }
}

impl std::fmt::Display for ProviderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for ProviderName {
    type Err = InvalidProviderName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_provider_name(s)?;
        Ok(ProviderName(s.to_string()))
    }
}

impl serde::Serialize for ProviderName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ProviderName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

/// Validates that a provider name follows the required format:
/// - Must start with a lowercase letter or underscore
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
fn validate_provider_name(name: &str) -> Result<(), InvalidProviderName> {
    if name.is_empty() {
        return Err(InvalidProviderName::Empty);
    }

    // Check first character: must be lowercase letter or underscore
    if let Some(first_char) = name.chars().next()
        && !(first_char.is_ascii_lowercase() || first_char == '_')
    {
        return Err(InvalidProviderName::InvalidCharacter {
            character: first_char,
            value: name.to_string(),
        });
    }

    // Check remaining characters: must be lowercase letter, underscore, or digit
    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()))
    {
        return Err(InvalidProviderName::InvalidCharacter {
            character: c,
            value: name.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`ProviderName`] parsing failures
#[derive(Debug, thiserror::Error)]
pub enum InvalidProviderName {
    /// Provider name is empty
    #[error("provider name cannot be empty")]
    Empty,
    /// Provider name contains invalid character
    #[error("invalid character '{character}' in provider name '{value}'")]
    InvalidCharacter { character: char, value: String },
}

#[cfg(test)]
mod tests {
    use super::{InvalidProviderName, validate_provider_name};

    #[test]
    fn accept_valid_provider_names() {
        assert!(validate_provider_name("my_provider").is_ok());
        assert!(validate_provider_name("my_provider_123").is_ok());
        assert!(validate_provider_name("__my_provider_123").is_ok());
        assert!(validate_provider_name("_test").is_ok());
        assert!(validate_provider_name("a").is_ok());
        assert!(validate_provider_name("test123").is_ok());
    }

    #[test]
    fn reject_empty_provider_name() {
        let result = validate_provider_name("");
        assert!(matches!(result, Err(InvalidProviderName::Empty)));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphens are not allowed
        let result = validate_provider_name("my-provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '-', .. })
        ));

        // Spaces are not allowed
        let result = validate_provider_name("my provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: ' ', .. })
        ));

        // Uppercase letters are not allowed
        let result = validate_provider_name("MyProvider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: 'M', .. })
        ));

        // Special characters are not allowed
        let result = validate_provider_name("my@provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_provider_name("my.provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '.', .. })
        ));

        let result = validate_provider_name("my#provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '#', .. })
        ));

        // Numbers at the beginning are not allowed
        let result = validate_provider_name("123provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '1', .. })
        ));

        let result = validate_provider_name("0_my_provider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: '0', .. })
        ));
    }

    #[test]
    fn reject_uppercase_in_middle() {
        let result = validate_provider_name("myProvider");
        assert!(matches!(
            result,
            Err(InvalidProviderName::InvalidCharacter { character: 'P', .. })
        ));
    }
}
