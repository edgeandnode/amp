//! Validated data file path for static dataset manifests.
//!
//! This module provides the [`FilePath`] newtype for validated relative file paths
//! used in static dataset manifests. Validation is performed at parse time (deserialization
//! boundary) so domain code can trust the path is well-formed.

use std::path::Path;

/// A validated relative path to a data file in a static dataset manifest.
///
/// The path is relative to the provider's object store root and must satisfy:
/// - Not empty
/// - Relative (must not start with `/`)
/// - No path traversal (`..` components)
/// - Forward slashes only (no `\`)
/// - Has a filename with an extension
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct FilePath(
    #[cfg_attr(feature = "schemars", schemars(length(min = 1)))]
    #[cfg_attr(
        feature = "schemars",
        schemars(
            extend("pattern" = r"^(?!/)(?!.*\\)(?!.*(?:^|/)\.\./)[^\s]+\.[a-zA-Z0-9]+$"),
            extend("examples" = ["data/prices.csv", "tables/users.csv"]),
            extend("description" = "Relative file path with forward slashes, no traversal (..), and a file extension")
        )
    )]
    String,
);

impl FilePath {
    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the `DataFilePath` and returns the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for FilePath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for FilePath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for FilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for FilePath {
    type Err = DataFilePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_data_file_path(s)?;
        Ok(FilePath(s.to_string()))
    }
}

impl TryFrom<String> for FilePath {
    type Error = DataFilePathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_data_file_path(&value)?;
        Ok(FilePath(value))
    }
}

impl serde::Serialize for FilePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FilePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

/// Validates that a string is a well-formed relative data file path.
fn validate_data_file_path(s: &str) -> Result<(), DataFilePathError> {
    if s.is_empty() {
        return Err(DataFilePathError::Empty);
    }

    if s.starts_with('/') {
        return Err(DataFilePathError::Absolute);
    }

    if s.contains('\\') {
        return Err(DataFilePathError::BackslashSeparator);
    }

    if s.split('/').any(|component| component == "..") {
        return Err(DataFilePathError::PathTraversal);
    }

    let path = Path::new(s);

    if path.file_stem().and_then(|s| s.to_str()).is_none() {
        return Err(DataFilePathError::NoFileName);
    }

    if path.extension().and_then(|s| s.to_str()).is_none() {
        return Err(DataFilePathError::NoExtension);
    }

    Ok(())
}

/// Error type for [`FilePath`] validation failures.
#[derive(Debug, thiserror::Error)]
pub enum DataFilePathError {
    /// Path is empty.
    #[error("data file path cannot be empty")]
    Empty,
    /// Path is absolute (starts with `/`).
    #[error("data file path must be relative, not absolute")]
    Absolute,
    /// Path contains `..` traversal components.
    #[error("data file path must not contain path traversal (..)")]
    PathTraversal,
    /// Path contains backslash separators.
    #[error("data file path must use forward slashes, not backslashes")]
    BackslashSeparator,
    /// Path has no filename.
    #[error("data file path must have a filename")]
    NoFileName,
    /// Path has no file extension.
    #[error("data file path must have a file extension")]
    NoExtension,
}
