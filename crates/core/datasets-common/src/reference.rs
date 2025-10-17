//! Dataset reference types combining namespace, name, and version.
//!
//! This module provides the `Reference` type that combines a namespace, dataset name,
//! and version to create a complete, versioned dataset identifier.

use crate::{
    fqn::FullyQualifiedName,
    name::{Name, NameError},
    namespace::{Namespace, NamespaceError},
    version::{Version, VersionParseError},
};

/// A complete dataset reference combining namespace, name, and version.
///
/// This type provides a fully versioned identifier for datasets by combining
/// a namespace, dataset name, and version using `/` and `@` separators.
///
/// ## Format
///
/// The string representation follows the format: `<namespace>/<name>@<version>`
///
/// ## Examples
///
/// ```text
/// my_namespace/my_dataset@1.0.0
/// my_namespace/my_dataset@0.4.10-rc.1+20230502
/// my_namespace/my_dataset@latest
/// my_namespace/my_dataset@dev
/// my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
/// ```
///
/// ## PURL Format
///
/// References can also be represented as Package URLs (PURLs) with the format:
/// `pkg:amp/<namespace>/<name>@<version>`
///
/// All three components must follow their respective validation rules.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(with = "String"))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        extend("pattern" = r"^(pkg:amp/)?[a-z0-9_]+/[a-z_][a-z0-9_]*@.+$"),
        extend("examples" = [
            "my_namespace/my_dataset@1.0.0",
            "my_namespace/my_dataset@0.4.10-rc.1+20230502",
            "my_namespace/my_dataset@latest",
            "my_namespace/my_dataset@dev",
            "my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "pkg:amp/my_namespace/my_dataset@1.0.0"
        ])
    )
)]
pub struct Reference(Namespace, Name, Version);

impl Reference {
    /// Create a new reference from components.
    pub fn new(namespace: Namespace, name: Name, version: Version) -> Self {
        Self(namespace, name, version)
    }

    /// Access the namespace component.
    pub fn namespace(&self) -> &Namespace {
        &self.0
    }

    /// Access the name component.
    pub fn name(&self) -> &Name {
        &self.1
    }

    /// Access the version component.
    pub fn version(&self) -> &Version {
        &self.2
    }

    /// Consume the reference and return the fully qualified name (namespace and name without version).
    pub fn into_fqn(self) -> FullyQualifiedName {
        FullyQualifiedName::new(self.0, self.1)
    }

    /// Consume the reference and return the inner components.
    pub fn into_parts(self) -> (Namespace, Name, Version) {
        (self.0, self.1, self.2)
    }

    /// Convert this reference to a Package URL (PURL) format.
    ///
    /// Returns a string in the format: `pkg:amp/<namespace>/<name>@<version>`
    ///
    /// ## Example
    ///
    /// ```text
    /// pkg:amp/my_namespace/my_dataset@1.0.0
    /// ```
    pub fn to_purl(&self) -> String {
        format!("pkg:amp/{}/{}@{}", self.0, self.1, self.2)
    }

    /// Parse a Package URL (PURL) into a Reference.
    ///
    /// The input must be in the format: `pkg:amp/<namespace>/<name>@<version>`
    ///
    /// ## Example
    ///
    /// ```text
    /// pkg:amp/my_namespace/my_dataset@1.0.0
    /// ```
    pub fn parse_purl(s: &str) -> Result<Self, PurlParseError> {
        // Check for PURL prefix
        let remainder = s
            .strip_prefix("pkg:amp/")
            .ok_or_else(|| PurlParseError::InvalidPrefix(s.to_string()))?;

        // Parse the remainder as a standard reference
        parse_reference_parts(remainder).map_err(PurlParseError::Reference)
    }
}

impl std::fmt::Display for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.0, self.1, self.2)
    }
}

impl std::str::FromStr for Reference {
    type Err = ReferenceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try parsing as PURL first
        if let Ok(reference) = Self::parse_purl(s) {
            return Ok(reference);
        }
        parse_reference_parts(s)
    }
}

/// Parse reference parts from a string in the format `<namespace>/<name>@<version>`.
fn parse_reference_parts(s: &str) -> Result<Reference, ReferenceError> {
    // Split by '@' to separate version
    let (fqn_part, version_str) = s
        .split_once('@')
        .ok_or_else(|| ReferenceError::MissingVersionSeparator(s.to_string()))?;

    // Split by '/' to separate namespace and name
    let (namespace_str, name_str) = fqn_part
        .split_once('/')
        .ok_or_else(|| ReferenceError::MissingNamespaceSeparator(s.to_string()))?;

    // Parse each component
    let namespace = namespace_str
        .parse()
        .map_err(ReferenceError::InvalidNamespace)?;
    let name = name_str.parse().map_err(ReferenceError::InvalidName)?;
    let version = version_str
        .parse()
        .map_err(ReferenceError::InvalidVersion)?;

    Ok(Reference(namespace, name, version))
}

impl serde::Serialize for Reference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Reference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <&'de str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when parsing a [`Reference`] from string.
#[derive(Debug, thiserror::Error)]
pub enum ReferenceError {
    /// The reference format is missing the '@' separator for version
    #[error("invalid reference format '{0}', missing '@' separator for version")]
    MissingVersionSeparator(String),

    /// The reference format is missing the '/' separator for namespace and name
    #[error("invalid reference format '{0}', missing '/' separator between namespace and name")]
    MissingNamespaceSeparator(String),

    /// The namespace component is invalid
    #[error("invalid namespace in reference: {0}")]
    InvalidNamespace(NamespaceError),

    /// The name component is invalid
    #[error("invalid name in reference: {0}")]
    InvalidName(NameError),

    /// The version component is invalid
    #[error("invalid version in reference: {0}")]
    InvalidVersion(VersionParseError),
}

/// Errors that can occur when parsing a PURL (Package URL).
#[derive(Debug, thiserror::Error)]
pub enum PurlParseError {
    /// The PURL prefix is invalid or missing
    #[error("invalid PURL format '{0}', expected format 'pkg:amp/<namespace>/<name>@<version>'")]
    InvalidPrefix(String),

    /// Error parsing the reference portion of the PURL
    #[error(transparent)]
    Reference(#[from] ReferenceError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_valid_tag_version_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset@1.0.0";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "from_str should succeed with valid semver tag format"
        );
        let reference = result.expect("from_str should return valid Reference with semver tag");
        assert_eq!(
            reference.namespace().as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(
            reference.name().as_str(),
            "my_dataset",
            "name should match input"
        );
        assert!(
            reference.version().is_tag(),
            "version should be parsed as Tag variant"
        );
        assert_eq!(
            reference.to_string(),
            input,
            "Display should produce original input"
        );
    }

    #[test]
    fn from_str_with_complex_semver_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset@0.4.10-rc.1+20230502";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "from_str should succeed with complex semver including pre-release and build metadata"
        );
        let reference = result.expect("from_str should return valid Reference with complex semver");
        assert_eq!(
            reference.namespace().as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(
            reference.name().as_str(),
            "my_dataset",
            "name should match input"
        );
        assert!(
            reference.version().is_tag(),
            "version should be parsed as Tag variant"
        );
        assert_eq!(
            reference.to_string(),
            input,
            "Display should preserve complete semver format including pre-release and build metadata"
        );
    }

    #[test]
    fn from_str_with_latest_version_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset@latest";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_ok(),
            "parsing should succeed with 'latest' version"
        );
        let reference = result.expect("should return valid Reference");
        assert!(
            reference.version().is_latest(),
            "version should be parsed as Latest variant"
        );
        assert_eq!(
            reference.to_string(),
            input,
            "Display should produce original input"
        );
    }

    #[test]
    fn from_str_with_dev_version_succeeds() {
        //* Given
        let input = "my_namespace/my_dataset@dev";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with 'dev' version");
        let reference = result.expect("should return valid Reference");
        assert!(
            reference.version().is_dev(),
            "version should be parsed as Dev variant"
        );
        assert_eq!(
            reference.to_string(),
            input,
            "Display should produce original input"
        );
    }

    #[test]
    fn from_str_with_hash_version_succeeds() {
        //* Given
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let input = format!("my_namespace/my_dataset@{}", hash);

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with hash version");
        let reference = result.expect("should return valid Reference");
        assert!(
            reference.version().is_hash(),
            "version should be parsed as Hash variant"
        );
        assert_eq!(
            reference.to_string(),
            input,
            "Display should produce original input"
        );
    }

    #[test]
    fn from_str_with_purl_format_succeeds() {
        //* Given
        let input = "pkg:amp/my_namespace/my_dataset@1.0.0";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(result.is_ok(), "parsing should succeed with PURL format");
        let reference = result.expect("should return valid Reference");
        assert_eq!(
            reference.namespace().as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(
            reference.name().as_str(),
            "my_dataset",
            "name should match input"
        );
        assert!(
            reference.version().is_tag(),
            "version should be parsed as Tag variant"
        );
    }

    #[test]
    fn parse_purl_with_valid_format_succeeds() {
        //* Given
        let input = "pkg:amp/my_namespace/my_dataset@1.0.0";

        //* When
        let result = Reference::parse_purl(input);

        //* Then
        assert!(
            result.is_ok(),
            "parse_purl should succeed with valid PURL format"
        );
        let reference = result.expect("should return valid Reference");
        assert_eq!(
            reference.namespace().as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(
            reference.name().as_str(),
            "my_dataset",
            "name should match input"
        );
    }

    #[test]
    fn parse_purl_with_invalid_prefix_fails() {
        //* Given
        let input = "pkg:npm/%40angular/animation@12.3.1";

        //* When
        let result = Reference::parse_purl(input);

        //* Then
        assert!(result.is_err(), "parse_purl should fail with wrong prefix");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, PurlParseError::InvalidPrefix(_)),
            "Expected PurlParseError::InvalidPrefix, got {:?}",
            error
        );
    }

    #[test]
    fn to_purl_formats_correctly() {
        //* Given
        let reference: Reference = "my_namespace/my_dataset@1.0.0"
            .parse()
            .expect("should create valid Reference");

        //* When
        let purl = reference.to_purl();

        //* Then
        assert_eq!(
            purl, "pkg:amp/my_namespace/my_dataset@1.0.0",
            "to_purl should format reference with pkg:amp/ prefix"
        );
    }

    #[test]
    fn into_parts_returns_all_components() {
        //* Given
        let reference: Reference = "my_namespace/my_dataset@1.0.0"
            .parse()
            .expect("should create valid Reference");

        //* When
        let (namespace, name, version) = reference.into_parts();

        //* Then
        assert_eq!(
            namespace.as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(name.as_str(), "my_dataset", "name should match input");
        assert!(version.is_tag(), "version should be Tag variant");
    }

    #[test]
    fn from_str_with_missing_version_separator_fails() {
        //* Given
        let input = "my_namespace/my_dataset";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail without version separator"
        );
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, ReferenceError::MissingVersionSeparator(_)),
            "Expected MissingVersionSeparator error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_missing_namespace_separator_fails() {
        //* Given
        let input = "my_namespace_my_dataset@1.0.0";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail without namespace separator"
        );
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, ReferenceError::MissingNamespaceSeparator(_)),
            "Expected MissingNamespaceSeparator error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_invalid_namespace_fails() {
        //* Given
        let input = "Invalid-Namespace/my_dataset@1.0.0";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing should fail with invalid namespace"
        );
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, ReferenceError::InvalidNamespace(_)),
            "Expected InvalidNamespace error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_invalid_name_fails() {
        //* Given
        let input = "my_namespace/Invalid-Name@1.0.0";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with invalid name");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, ReferenceError::InvalidName(_)),
            "Expected InvalidName error, got {:?}",
            error
        );
    }

    #[test]
    fn from_str_with_invalid_version_fails() {
        //* Given
        let input = "my_namespace/my_dataset@not-a-version";

        //* When
        let result: Result<Reference, _> = input.parse();

        //* Then
        assert!(result.is_err(), "parsing should fail with invalid version");
        let error = result.expect_err("should return error");
        assert!(
            matches!(error, ReferenceError::InvalidVersion(_)),
            "Expected InvalidVersion error, got {:?}",
            error
        );
    }

    #[test]
    fn serialize_formats_as_string() {
        //* Given
        let reference: Reference = "my_namespace/my_dataset@1.0.0"
            .parse()
            .expect("should create valid Reference");

        //* When
        let result = serde_json::to_string(&reference);

        //* Then
        assert!(result.is_ok(), "serialization should succeed");
        let serialized = result.expect("should return serialized string");
        assert_eq!(
            serialized, r#""my_namespace/my_dataset@1.0.0""#,
            "serialized format should be JSON string with namespace/name@version format"
        );
    }

    #[test]
    fn deserialize_with_valid_format_succeeds() {
        //* Given
        let json_input = r#""my_namespace/my_dataset@1.0.0""#;

        //* When
        let result: Result<Reference, _> = serde_json::from_str(json_input);

        //* Then
        assert!(
            result.is_ok(),
            "deserialization should succeed with valid format"
        );
        let reference = result.expect("should return valid Reference");
        assert_eq!(
            reference.namespace().as_str(),
            "my_namespace",
            "namespace should match input"
        );
        assert_eq!(
            reference.name().as_str(),
            "my_dataset",
            "name should match input"
        );
        assert!(
            reference.version().is_tag(),
            "version should be parsed as Tag variant"
        );
    }
}
