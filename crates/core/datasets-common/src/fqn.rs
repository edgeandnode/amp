//! Fully qualified name (FQN) types combining namespace and name.
//!
//! This module provides the `FullyQualifiedName` type that combines a namespace
//! with a dataset name to create a globally unique identifier.

use crate::{
    name::{Name, NameError},
    namespace::{Namespace, NamespaceError},
};

/// A fully qualified name combining a namespace and dataset name.
///
/// This type provides a globally unique identifier for datasets by combining
/// a namespace with a dataset name using a `/` separator.
///
/// ## Format
///
/// The string representation follows the format: `<namespace>/<name>`
///
/// ## Example
///
/// ```text
/// my_namespace/my_dataset
/// ```
///
/// Both the namespace and name components must follow their respective
/// validation rules (lowercase letters, digits, and underscores only).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct FullyQualifiedName(pub Namespace, pub Name);

impl FullyQualifiedName {
    /// Access the namespace component
    pub fn namespace(&self) -> &Namespace {
        &self.0
    }

    /// Access the name component
    pub fn name(&self) -> &Name {
        &self.1
    }

    /// Consume the FQN and return the inner components
    pub fn into_parts(self) -> (Namespace, Name) {
        (self.0, self.1)
    }
}

impl std::fmt::Display for FullyQualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace(), self.name())
    }
}

impl std::str::FromStr for FullyQualifiedName {
    type Err = FullyQualifiedNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('/') {
            Some((namespace_str, name_str)) => {
                let namespace = namespace_str
                    .parse()
                    .map_err(FullyQualifiedNameError::InvalidNamespace)?;
                let name = name_str
                    .parse()
                    .map_err(FullyQualifiedNameError::InvalidName)?;
                Ok(Self(namespace, name))
            }
            None => Err(FullyQualifiedNameError::InvalidFormat(s.to_string())),
        }
    }
}

impl serde::Serialize for FullyQualifiedName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as a string in "namespace/name" format
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FullyQualifiedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when parsing a [`FullyQualifiedName`] from string
#[derive(Debug, thiserror::Error)]
pub enum FullyQualifiedNameError {
    /// The FQN format is invalid (missing `/` separator)
    #[error("invalid fully qualified name format '{0}', expected 'namespace/name'")]
    InvalidFormat(String),

    /// The namespace component is invalid
    #[error("invalid namespace in fully qualified name: {0}")]
    InvalidNamespace(NamespaceError),

    /// The name component is invalid
    #[error("invalid name in fully qualified name: {0}")]
    InvalidName(NameError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_valid_fqn_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with valid FQN format"
        );
        let fqn = result.expect("should return valid FullyQualifiedName");
        assert_eq!(fqn.namespace().as_str(), "my_namespace");
        assert_eq!(fqn.name().as_str(), "my_dataset");
        assert_eq!(fqn.to_string(), "my_namespace/my_dataset");
    }

    #[test]
    fn from_str_with_numbers_and_underscores_succeeds() {
        //* Given
        let input = "namespace_123/dataset_456";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with numbers and underscores"
        );
        let fqn = result.expect("should return valid FullyQualifiedName");
        assert_eq!(fqn.namespace().as_str(), "namespace_123");
        assert_eq!(fqn.name().as_str(), "dataset_456");
    }

    #[test]
    fn from_str_with_missing_separator_fails() {
        //* Given
        let input = "my_namespace_my_dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail without separator");
        let error = result.expect_err("should return InvalidFormat error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidFormat(_)),
            "Expected InvalidFormat error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_multiple_separators_fails() {
        //* Given
        let input = "my/namespace/dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with multiple separators in name component"
        );
        let error = result.expect_err("should return InvalidName error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidName(_)),
            "Expected InvalidName error due to '/' in name, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_uppercase_namespace_fails() {
        //* Given
        let input = "My_Namespace/my_dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with uppercase letters in namespace"
        );
        let error = result.expect_err("should return InvalidNamespace error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidNamespace(_)),
            "Expected InvalidNamespace error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_uppercase_name_fails() {
        //* Given
        let input = "my_namespace/My_Dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with uppercase letters in name"
        );
        let error = result.expect_err("should return InvalidName error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidName(_)),
            "Expected InvalidName error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_empty_namespace_fails() {
        //* Given
        let input = "/my_dataset";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with empty namespace");
        let error = result.expect_err("should return InvalidNamespace error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidNamespace(_)),
            "Expected InvalidNamespace error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_empty_name_fails() {
        //* Given
        let input = "my_namespace/";

        //* When
        let result: Result<FullyQualifiedName, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with empty name");
        let error = result.expect_err("should return InvalidName error");
        assert!(
            matches!(error, FullyQualifiedNameError::InvalidName(_)),
            "Expected InvalidName error, got {:?}",
            error
        );
    }

    #[test]
    fn display_formats_with_slash_separator() {
        //* Given
        let namespace: Namespace = "test_ns".parse().expect("should create valid namespace");
        let name: Name = "test_name".parse().expect("should create valid name");
        let fqn = FullyQualifiedName(namespace, name);

        //* When
        let result = format!("{}", fqn);

        //* Then
        assert_eq!(result, "test_ns/test_name");
    }

    #[test]
    fn into_parts_returns_namespace_and_name() {
        //* Given
        let fqn: FullyQualifiedName = "my_namespace/my_dataset"
            .parse()
            .expect("should create valid FullyQualifiedName");

        //* When
        let (namespace, name) = fqn.into_parts();

        //* Then
        assert_eq!(namespace.as_str(), "my_namespace");
        assert_eq!(name.as_str(), "my_dataset");
    }

    #[test]
    fn serialize_formats_as_string_with_separator() {
        //* Given
        let fqn: FullyQualifiedName = "my_namespace/my_dataset"
            .parse()
            .expect("should create valid FullyQualifiedName");

        //* When
        let result = serde_json::to_string(&fqn);

        //* Then
        assert!(result.is_ok(), "serialization should succeed");
        let serialized = result.expect("should return serialized string");
        assert_eq!(serialized, "\"my_namespace/my_dataset\"");
    }

    #[test]
    fn deserialize_with_valid_format_succeeds() {
        //* Given
        let json_input = "\"my_namespace/my_dataset\"";

        //* When
        let result: Result<FullyQualifiedName, _> = serde_json::from_str(json_input);

        //* Then
        assert!(
            result.is_ok(),
            "deserialization should succeed with valid format"
        );
        let fqn = result.expect("should return valid FullyQualifiedName");
        assert_eq!(fqn.namespace().as_str(), "my_namespace");
        assert_eq!(fqn.name().as_str(), "my_dataset");
    }

    #[test]
    fn serialize_deserialize_roundtrip_preserves_value() {
        //* Given
        let original: FullyQualifiedName = "my_namespace/my_dataset"
            .parse()
            .expect("should create valid FullyQualifiedName");

        //* When
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        let deserialized: FullyQualifiedName =
            serde_json::from_str(&serialized).expect("deserialization should succeed");

        //* Then
        assert_eq!(deserialized, original, "roundtrip should preserve value");
    }

    #[test]
    fn deserialize_with_invalid_namespace_fails() {
        //* Given
        let json_input = "\"invalid-namespace/dataset\"";

        //* When
        let result: Result<FullyQualifiedName, _> = serde_json::from_str(json_input);

        //* Then
        assert!(
            result.is_err(),
            "deserialization should fail with invalid namespace"
        );
    }
}
