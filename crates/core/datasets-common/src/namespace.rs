//! Namespace validation and types.
//!
//! This module provides the `Namespace` type for validated namespaces that enforce
//! naming conventions and constraints required across the system.

/// The global namespace string constant.
const GLOBAL_NAMESPACE: &str = "_";

/// A validated namespace that enforces naming conventions and constraints.
///
/// Namespaces must follow strict rules to ensure compatibility across systems,
/// file systems, and network protocols. This type provides compile-time guarantees
/// that all instances contain valid namespaces.
///
/// ## Format Requirements
///
/// A valid namespace must:
/// - **Contain** only lowercase letters (`a-z`), digits (`0-9`), and underscores (`_`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Have no spaces** or special characters (except underscore)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct Namespace(
    #[cfg_attr(feature = "schemars", schemars(regex(pattern = r"^[a-z0-9_]*$")))]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1)))]
    String,
);

impl Namespace {
    /// Returns the global namespace (`"_"`).
    ///
    /// This represents the default global namespace used throughout the system
    /// when no specific namespace is provided.
    pub fn global() -> Namespace {
        Namespace(GLOBAL_NAMESPACE.to_string())
    }

    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the Namespace and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for Namespace {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Namespace> for String {
    fn eq(&self, other: &Namespace) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for Namespace {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<Namespace> for str {
    fn eq(&self, other: &Namespace) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for Namespace {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<Namespace> for &str {
    fn eq(&self, other: &Namespace) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for Namespace {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Namespace {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for Namespace {
    type Error = NamespaceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_namespace(&value)?;
        Ok(Namespace(value))
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Namespace {
    type Err = NamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_namespace(s)?;
        Ok(Namespace(s.to_string()))
    }
}

impl serde::Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "metadata-db")]
impl From<metadata_db::DatasetNamespaceOwned> for Namespace {
    fn from(value: metadata_db::DatasetNamespaceOwned) -> Self {
        // Convert to string and validate - this should always pass since DatasetNamespace
        // comes from the database and should already be valid
        Namespace(value.into_inner())
    }
}

#[cfg(feature = "metadata-db")]
impl From<Namespace> for metadata_db::DatasetNamespaceOwned {
    fn from(value: Namespace) -> Self {
        // SAFETY: Namespace is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetNamespace::from_owned_unchecked(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a Namespace> for metadata_db::DatasetNamespace<'a> {
    fn from(value: &'a Namespace) -> Self {
        // SAFETY: Namespace is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetNamespace::from_ref_unchecked(&value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::DatasetNamespace<'a>> for Namespace {
    fn eq(&self, other: &metadata_db::DatasetNamespace<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<Namespace> for metadata_db::DatasetNamespace<'a> {
    fn eq(&self, other: &Namespace) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&metadata_db::DatasetNamespace<'a>> for Namespace {
    fn eq(&self, other: &&metadata_db::DatasetNamespace<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<Namespace> for &metadata_db::DatasetNamespace<'a> {
    fn eq(&self, other: &Namespace) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::DatasetNamespace<'a>> for &Namespace {
    fn eq(&self, other: &metadata_db::DatasetNamespace<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&Namespace> for metadata_db::DatasetNamespace<'a> {
    fn eq(&self, other: &&Namespace) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Validates that a namespace follows the required format:
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
pub fn validate_namespace(namespace: &str) -> Result<(), NamespaceError> {
    if namespace.is_empty() {
        return Err(NamespaceError::Empty);
    }

    // Check characters: must be lowercase letter, underscore, or digit
    if let Some(c) = namespace
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(NamespaceError::InvalidCharacter {
            character: c,
            value: namespace.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`Namespace`] parsing failures
#[derive(Debug, thiserror::Error)]
pub enum NamespaceError {
    /// Namespace is empty
    #[error("namespace cannot be empty")]
    Empty,
    /// Namespace contains invalid character
    #[error("invalid character '{character}' in namespace '{value}'")]
    InvalidCharacter { character: char, value: String },
}

#[cfg(test)]
mod tests {
    use super::{NamespaceError, validate_namespace};

    #[test]
    fn accept_valid_namespaces() {
        assert!(validate_namespace("my_namespace").is_ok());
        assert!(validate_namespace("my_namespace_123").is_ok());
        assert!(validate_namespace("__my_namespace_123").is_ok());
        assert!(validate_namespace("0xabc123").is_ok());
    }

    #[test]
    fn reject_empty_namespace() {
        let result = validate_namespace("");
        assert!(matches!(result, Err(NamespaceError::Empty)));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphens are not allowed
        let result = validate_namespace("my-namespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: '-', .. })
        ));

        // Spaces are not allowed
        let result = validate_namespace("my namespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: ' ', .. })
        ));

        // Uppercase letters are not allowed
        let result = validate_namespace("MyNamespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: 'M', .. })
        ));

        // Special characters are not allowed
        let result = validate_namespace("my@namespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_namespace("my.namespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: '.', .. })
        ));

        let result = validate_namespace("my#namespace");
        assert!(matches!(
            result,
            Err(NamespaceError::InvalidCharacter { character: '#', .. })
        ));
    }
}
