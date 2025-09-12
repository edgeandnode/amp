//! Common types and utilities for dataset definitions.
//!
//! This module provides shared structures used across different dataset definition formats,
//! including serializable schema representations and common dataset metadata.

use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType as ArrowDataType;

use crate::{BoxError, SPECIAL_BLOCK_NUM, Table as LogicalTable};
/// Common metadata fields required by all dataset definitions.
///
/// All dataset definitions must have a kind, network and name. The name must match the filename.
/// Schema is optional for TOML dataset format.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name. Must be unique within the network and match the filename
    pub name: Name,
    /// Dataset kind. See specific dataset definitions for supported values.
    /// Common values include: "evm-rpc", "firehose", "substreams", "sql", "manifest"
    pub kind: String,
    /// Network name, e.g. "mainnet", "sepolia", "polygon"
    pub network: String,
    /// Dataset schema. Lists the tables defined by this dataset.
    /// Optional for TOML format, required for JSON format
    pub schema: Option<Schema>,
}

/// A serializable representation of a collection of Arrow schemas without metadata.
///
/// This structure maps table names to their field definitions, providing a way to serialize
/// and deserialize Arrow schemas while filtering out the special `SPECIAL_BLOCK_NUM` field.
///
/// Structure: table name -> field name -> data type
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Schema(HashMap<String, HashMap<String, DataType>>);

impl From<Vec<LogicalTable>> for Schema {
    fn from(tables: Vec<LogicalTable>) -> Self {
        let inner = tables
            .into_iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(HashMap::new(), |mut acc, field| {
                        acc.insert(field.name().clone(), field.data_type().clone().into());
                        acc
                    });
                (table.name().to_string(), inner_map)
            })
            .collect();

        Self(inner)
    }
}

impl From<&[LogicalTable]> for Schema {
    fn from(tables: &[LogicalTable]) -> Self {
        let inner = tables
            .iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(HashMap::new(), |mut acc, field| {
                        acc.insert(field.name().clone(), field.data_type().clone().into());
                        acc
                    });
                (table.name().to_string(), inner_map)
            })
            .collect();

        Self(inner)
    }
}

/// A validated dataset name that enforces naming conventions and constraints.
///
/// Dataset names must follow strict rules to ensure compatibility across systems,
/// file systems, and network protocols. This type provides compile-time guarantees
/// that all instances contain valid dataset names.
///
/// ## Format Requirements
///
/// A valid dataset name must:
/// - **Start** with a lowercase letter (`a-z`) or underscore (`_`)
/// - **Contain** only lowercase letters (`a-z`), digits (`0-9`), and underscores (`_`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Have no spaces** or special characters (except underscore)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct Name(
    #[cfg_attr(feature = "schemars", schemars(regex(pattern = r"^[a-z_][a-z0-9_]*$")))]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1)))]
    String,
);

impl Name {
    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the DatasetName and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for Name {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Name> for String {
    fn eq(&self, other: &Name) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for Name {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<Name> for str {
    fn eq(&self, other: &Name) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for Name {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<Name> for &str {
    fn eq(&self, other: &Name) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for Name {
    type Error = NameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_dataset_name(&value)?;
        Ok(Name(value))
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Name {
    type Err = NameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_dataset_name(s)?;
        Ok(Name(s.to_string()))
    }
}

impl serde::Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

/// Validates that a dataset name follows the required format:
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
pub fn validate_dataset_name(name: &str) -> Result<(), NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }

    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(NameError::InvalidCharacter {
            character: c,
            name: name.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`Name`] parsing failures
#[derive(Debug, thiserror::Error)]
pub enum NameError {
    /// Dataset name is empty
    #[error("name cannot be empty")]
    Empty,
    /// Dataset name contains invalid character
    #[error("invalid character '{character}' in name '{name}'")]
    InvalidCharacter { character: char, name: String },
}

/// Semver version wrapper with JSON schema support and version manipulation utilities.
///
/// Provides serialization and version manipulation utilities for dataset versioning.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct Version(#[cfg_attr(feature = "schemars", schemars(with = "String"))] semver::Version);

impl Version {
    /// Create a new [`Version`] from major, minor, and patch components.
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(semver::Version {
            major,
            minor,
            patch,
            pre: semver::Prerelease::EMPTY,
            build: semver::BuildMetadata::EMPTY,
        })
    }

    /// Convert the Semver version to a string with underscores.
    ///
    /// Example: SemverVersion(1, 0, 0) -> String("1_0_0").
    pub fn to_underscore_version(&self) -> String {
        self.0.to_string().replace('.', "_")
    }

    /// Convert a Semver version string to a string with underscores.
    ///
    /// Example: String("1.0.0") -> String("1_0_0").
    pub fn version_identifier(version: &str) -> Result<String, BoxError> {
        let version = version.parse::<Self>()?;
        Ok(version.to_underscore_version())
    }

    pub fn from_version_identifier(v_identifier: &str) -> Result<Self, BoxError> {
        v_identifier.replace("_", ".").parse().map_err(Into::into)
    }
}

impl std::ops::Deref for Version {
    type Target = semver::Version;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<semver::Version> for Version {
    fn eq(&self, other: &semver::Version) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Version> for semver::Version {
    fn eq(&self, other: &Version) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for Version {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Version {
    type Err = semver::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// Semver version requirement wrapper with JSON schema support.
///
/// Used for specifying dependency version constraints in derived datasets.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct VersionReq(
    #[cfg_attr(feature = "schemars", schemars(with = "String"))] semver::VersionReq,
);

impl std::ops::Deref for VersionReq {
    type Target = semver::VersionReq;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<semver::VersionReq> for VersionReq {
    fn eq(&self, other: &semver::VersionReq) -> bool {
        self.0 == *other
    }
}

impl PartialEq<VersionReq> for semver::VersionReq {
    fn eq(&self, other: &VersionReq) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for VersionReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for VersionReq {
    type Err = semver::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// Apache Arrow data _new-type_ wrapper with JSON schema support.
///
/// This wrapper provides serialization and JSON schema generation capabilities
/// for Apache Arrow data types used in dataset table definitions. Arrow data types
/// define the structure and format of columnar data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
#[cfg_attr(
    feature = "schemars",
    schemars(description = "Arrow data type, e.g. `Int32`, `Utf8`, etc.")
)]
pub struct DataType(#[cfg_attr(feature = "schemars", schemars(with = "String"))] pub ArrowDataType);

impl DataType {
    /// Returns a reference to the inner Arrow [`DataType`](ArrowDataType).
    pub fn as_arrow(&self) -> &ArrowDataType {
        &self.0
    }

    /// Consumes the [`DataType`] and returns the inner Arrow [`DataType`](ArrowDataType).
    pub fn into_arrow(self) -> ArrowDataType {
        self.0
    }
}

impl std::ops::Deref for DataType {
    type Target = ArrowDataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ArrowDataType> for DataType {
    fn from(value: ArrowDataType) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{NameError, validate_dataset_name};

    #[test]
    fn accept_valid_dataset_names() {
        assert!(validate_dataset_name("my_dataset").is_ok());
        assert!(validate_dataset_name("my_dataset_123").is_ok());
        assert!(validate_dataset_name("__my_dataset_123").is_ok());
    }

    #[test]
    fn reject_empty_dataset_name() {
        let result = validate_dataset_name("");
        assert!(matches!(result, Err(NameError::Empty)));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphens are not allowed
        let result = validate_dataset_name("my-dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '-', .. })
        ));

        // Spaces are not allowed
        let result = validate_dataset_name("my dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: ' ', .. })
        ));

        // Uppercase letters are not allowed
        let result = validate_dataset_name("MyDataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: 'M', .. })
        ));

        // Special characters are not allowed
        let result = validate_dataset_name("my@dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_dataset_name("my.dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '.', .. })
        ));

        let result = validate_dataset_name("my#dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '#', .. })
        ));
    }
}
