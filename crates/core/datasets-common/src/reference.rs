//! Dataset reference type combining namespace, name, and version.
//!
//! This module provides the `DatasetReference` type that combines a namespace, dataset name,
//! and version tag to create a globally unique and versioned dataset identifier.

use crate::{name::Name, namespace::Namespace, version_tag::VersionTag};

/// A dataset reference combining namespace, name, and version.
///
/// This type provides a globally unique and versioned identifier for datasets by combining
/// a namespace, dataset name, and version tag.
///
/// ## Format
///
/// The string representation follows the format: `[<namespace>/]<name>[@<version>]`
///
/// Both namespace and version are optional.
///
/// ## Examples
///
/// ```text
/// my_namespace/my_dataset@1.0.0
/// my_dataset@1.0.0
/// my_namespace/my_dataset
/// my_dataset
/// ```
///
/// All components must follow their respective validation rules.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DatasetReference {
    pub namespace: Option<Namespace>,
    pub name: Name,
    pub version: Option<VersionTag>,
}

impl DatasetReference {
    /// Create a new [`DatasetReference`] from its components.
    pub fn new(namespace: Option<Namespace>, name: Name, version: Option<VersionTag>) -> Self {
        Self {
            namespace,
            name,
            version,
        }
    }
}

impl std::fmt::Display for DatasetReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(f, "{}/", namespace)?;
        }
        write!(f, "{}", self.name)?;
        if let Some(version) = &self.version {
            write!(f, "@{}", version)?;
        }
        Ok(())
    }
}

impl std::str::FromStr for DatasetReference {
    type Err = DatasetReferenceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Split on '@' to separate version from the rest
        let (fqn_part, version) = match s.rsplit_once('@') {
            Some((fqn, version_str)) => {
                let version = version_str
                    .parse()
                    .map_err(DatasetReferenceError::InvalidVersion)?;
                (fqn, Some(version))
            }
            None => (s, None),
        };

        // Split on '/' to separate namespace from name
        let (namespace, name_str) = match fqn_part.split_once('/') {
            Some((namespace_str, name_str)) => {
                let namespace = namespace_str
                    .parse()
                    .map_err(DatasetReferenceError::InvalidNamespace)?;
                (Some(namespace), name_str)
            }
            None => (None, fqn_part),
        };

        let name = name_str
            .parse()
            .map_err(DatasetReferenceError::InvalidName)?;

        Ok(Self {
            namespace,
            name,
            version,
        })
    }
}

impl serde::Serialize for DatasetReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as a string in "namespace/name@version" format
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DatasetReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when parsing a [`DatasetReference`] from string.
#[derive(Debug, thiserror::Error)]
pub enum DatasetReferenceError {
    /// The namespace component is invalid
    #[error("invalid namespace in dataset reference: {0}")]
    InvalidNamespace(crate::namespace::NamespaceError),

    /// The name component is invalid
    #[error("invalid name in dataset reference: {0}")]
    InvalidName(crate::name::NameError),

    /// The version component is invalid
    #[error("invalid version in dataset reference: {0}")]
    InvalidVersion(crate::version_tag::VersionTagError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_valid_dataset_reference_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with valid DatasetReference format"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(
            dataset_id.namespace.as_deref().unwrap_or(""),
            "my_namespace"
        );
        assert_eq!(dataset_id.name.as_str(), "my_dataset");
        assert_eq!(
            dataset_id.version.as_ref().map(|v| v.to_string()).unwrap(),
            "1.0.0"
        );
        assert_eq!(dataset_id.to_string(), "my_namespace/my_dataset@1.0.0");
    }

    #[test]
    fn from_str_with_prerelease_version_succeeds() {
        //* Given
        let input = "namespace_123/dataset_456@2.1.3-alpha.1";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with prerelease version"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(
            dataset_id.namespace.as_deref().unwrap_or(""),
            "namespace_123"
        );
        assert_eq!(dataset_id.name.as_str(), "dataset_456");
        assert_eq!(
            dataset_id.version.as_ref().map(|v| v.to_string()).unwrap(),
            "2.1.3-alpha.1"
        );
    }

    #[test]
    fn from_str_with_missing_version_separator_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed without version - version is optional"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(
            dataset_id.namespace.as_deref().unwrap_or(""),
            "my_namespace"
        );
        assert_eq!(dataset_id.name.as_str(), "my_dataset");
        assert_eq!(dataset_id.version, None);
        assert_eq!(dataset_id.to_string(), "my_namespace/my_dataset");
    }

    #[test]
    fn from_str_with_missing_namespace_separator_succeeds() {
        //* Given
        let input = "my_namespace_my_dataset@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed - namespace is optional, so this is valid as name@version"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(dataset_id.namespace, None);
        assert_eq!(dataset_id.name.as_str(), "my_namespace_my_dataset");
        assert_eq!(
            dataset_id.version.as_ref().map(|v| v.to_string()).unwrap(),
            "1.0.0"
        );
        assert_eq!(dataset_id.to_string(), "my_namespace_my_dataset@1.0.0");
    }

    #[test]
    fn from_str_with_uppercase_namespace_fails() {
        //* Given
        let input = "My_Namespace/my_dataset@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with uppercase letters in namespace"
        );
        let error = result.expect_err("should return InvalidNamespace error");
        assert!(
            matches!(error, DatasetReferenceError::InvalidNamespace(_)),
            "Expected InvalidNamespace error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_uppercase_name_fails() {
        //* Given
        let input = "my_namespace/My_Dataset@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with uppercase letters in name"
        );
        let error = result.expect_err("should return InvalidName error");
        assert!(
            matches!(error, DatasetReferenceError::InvalidName(_)),
            "Expected InvalidName error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_invalid_version_fails() {
        //* Given
        let input = "my_namespace/my_dataset@invalid";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with invalid version");
        let error = result.expect_err("should return InvalidVersion error");
        assert!(
            matches!(error, DatasetReferenceError::InvalidVersion(_)),
            "Expected InvalidVersion error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_empty_namespace_fails() {
        //* Given
        let input = "/my_dataset@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with empty namespace");
        let error = result.expect_err("should return InvalidNamespace error");
        assert!(
            matches!(error, DatasetReferenceError::InvalidNamespace(_)),
            "Expected InvalidNamespace error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_empty_name_fails() {
        //* Given
        let input = "my_namespace/@1.0.0";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with empty name");
        let error = result.expect_err("should return InvalidName error");
        assert!(
            matches!(error, DatasetReferenceError::InvalidName(_)),
            "Expected InvalidName error, got {:?}",
            error
        );
    }

    #[test]
    fn display_formats_with_separators() {
        //* Given
        let namespace: Namespace = "test_ns".parse().expect("should create valid namespace");
        let name: Name = "test_name".parse().expect("should create valid name");
        let version: VersionTag = "3.2.1".parse().expect("should create valid version");
        let dataset_id = DatasetReference::new(Some(namespace), name, Some(version));

        //* When
        let result = format!("{}", dataset_id);

        //* Then
        assert_eq!(result, "test_ns/test_name@3.2.1");
    }

    #[test]
    fn serialize_formats_as_string_with_separators() {
        //* Given
        let dataset_id: DatasetReference = "my_namespace/my_dataset@1.0.0"
            .parse()
            .expect("should create valid DatasetReference");

        //* When
        let result = serde_json::to_string(&dataset_id);

        //* Then
        assert!(result.is_ok(), "serialization should succeed");
        let serialized = result.expect("should return serialized string");
        assert_eq!(serialized, "\"my_namespace/my_dataset@1.0.0\"");
    }

    #[test]
    fn deserialize_with_valid_format_succeeds() {
        //* Given
        let json_input = "\"my_namespace/my_dataset@1.0.0\"";

        //* When
        let result: Result<DatasetReference, _> = serde_json::from_str(json_input);

        //* Then
        assert!(
            result.is_ok(),
            "deserialization should succeed with valid format"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(
            dataset_id.namespace.as_deref().unwrap_or(""),
            "my_namespace"
        );
        assert_eq!(dataset_id.name.as_str(), "my_dataset");
        assert_eq!(
            dataset_id.version.as_ref().map(|v| v.to_string()).unwrap(),
            "1.0.0"
        );
    }

    #[test]
    fn serialize_deserialize_roundtrip_preserves_value() {
        //* Given
        let original: DatasetReference = "my_namespace/my_dataset@2.1.3"
            .parse()
            .expect("should create valid DatasetReference");

        //* When
        let serialized = serde_json::to_string(&original).expect("serialization should succeed");
        let deserialized: DatasetReference =
            serde_json::from_str(&serialized).expect("deserialization should succeed");

        //* Then
        assert_eq!(deserialized, original, "roundtrip should preserve value");
    }

    #[test]
    fn deserialize_with_invalid_namespace_fails() {
        //* Given
        let json_input = "\"invalid-namespace/dataset@1.0.0\"";

        //* When
        let result: Result<DatasetReference, _> = serde_json::from_str(json_input);

        //* Then
        assert!(
            result.is_err(),
            "deserialization should fail with invalid namespace"
        );
    }

    #[test]
    fn from_str_handles_at_sign_in_version_correctly() {
        //* Given - version with build metadata containing @
        let input = "my_namespace/my_dataset@1.0.0+build123";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "should handle version with build metadata");
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(
            dataset_id.version.as_ref().map(|v| v.to_string()).unwrap(),
            "1.0.0+build123"
        );
    }

    #[test]
    fn from_str_with_only_name_succeeds() {
        //* Given
        let input = "my_dataset";

        //* When
        let result: Result<DatasetReference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with only name - both namespace and version are optional"
        );
        let dataset_id = result.expect("should return valid DatasetReference");
        assert_eq!(dataset_id.namespace, None);
        assert_eq!(dataset_id.name.as_str(), "my_dataset");
        assert_eq!(dataset_id.version, None);
        assert_eq!(dataset_id.to_string(), "my_dataset");
    }

    #[test]
    fn display_formats_without_namespace() {
        //* Given
        let name: Name = "test_name".parse().expect("should create valid name");
        let version: VersionTag = "3.2.1".parse().expect("should create valid version");
        let dataset_id = DatasetReference::new(None, name, Some(version));

        //* When
        let result = format!("{}", dataset_id);

        //* Then
        assert_eq!(result, "test_name@3.2.1");
    }

    #[test]
    fn display_formats_without_version() {
        //* Given
        let namespace: Namespace = "test_ns".parse().expect("should create valid namespace");
        let name: Name = "test_name".parse().expect("should create valid name");
        let dataset_id = DatasetReference::new(Some(namespace), name, None);

        //* When
        let result = format!("{}", dataset_id);

        //* Then
        assert_eq!(result, "test_ns/test_name");
    }

    #[test]
    fn display_formats_with_only_name() {
        //* Given
        let name: Name = "test_name".parse().expect("should create valid name");
        let dataset_id = DatasetReference::new(None, name, None);

        //* When
        let result = format!("{}", dataset_id);

        //* Then
        assert_eq!(result, "test_name");
    }
}
