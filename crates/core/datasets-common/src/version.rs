//! Version types for dataset definitions.
//!
//! This module provides the `Version` enum for representing different ways
//! to reference dataset versions: by semver tag, by content hash, or by symbolic references.

use crate::{version_hash::VersionHash, version_tag::VersionTag};

/// A version reference that can be a semantic version tag, content hash, or symbolic reference.
///
/// This enum unifies the different ways to reference dataset versions:
/// - `Tag`: A semantic version tag (e.g., `1.0.0`, `2.1.3-alpha.1`)
/// - `Hash`: A 32-byte SHA-256 content hash
/// - `Latest`: The latest tagged semver version
/// - `Dev`: The latest hash based on timestamp ordering
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum Version {
    /// A semantic version tag
    Tag(VersionTag),
    /// A 32-byte SHA-256 content hash
    Hash(VersionHash),
    /// The latest tagged semver version
    Latest,
    /// The latest hash given a timestamp ordering
    Dev,
}

impl Version {
    /// Returns `true` if the version is a `Tag` variant.
    pub fn is_tag(&self) -> bool {
        matches!(self, Version::Tag(_))
    }

    /// Returns `true` if the version is a `Hash` variant.
    pub fn is_hash(&self) -> bool {
        matches!(self, Version::Hash(_))
    }

    /// Returns `true` if the version is the `Latest` variant.
    pub fn is_latest(&self) -> bool {
        matches!(self, Version::Latest)
    }

    /// Returns `true` if the version is the `Dev` variant.
    pub fn is_dev(&self) -> bool {
        matches!(self, Version::Dev)
    }

    /// Returns a reference to the inner `VersionTag` if this is a `Tag` variant.
    pub fn as_tag(&self) -> Option<&VersionTag> {
        match self {
            Version::Tag(tag) => Some(tag),
            _ => None,
        }
    }

    /// Returns a reference to the inner `VersionHash` if this is a `Hash` variant.
    pub fn as_hash(&self) -> Option<&VersionHash> {
        match self {
            Version::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    /// Consumes the `Version` and returns the inner `VersionTag` if this is a `Tag` variant.
    pub fn into_tag(self) -> Option<VersionTag> {
        match self {
            Version::Tag(tag) => Some(tag),
            _ => None,
        }
    }

    /// Consumes the `Version` and returns the inner `VersionHash` if this is a `Hash` variant.
    pub fn into_hash(self) -> Option<VersionHash> {
        match self {
            Version::Hash(hash) => Some(hash),
            _ => None,
        }
    }
}

impl From<VersionTag> for Version {
    fn from(tag: VersionTag) -> Self {
        Version::Tag(tag)
    }
}

impl From<VersionHash> for Version {
    fn from(hash: VersionHash) -> Self {
        Version::Hash(hash)
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Version::Tag(tag) => write!(f, "{}", tag),
            Version::Hash(hash) => write!(f, "{}", hash),
            Version::Latest => write!(f, "latest"),
            Version::Dev => write!(f, "dev"),
        }
    }
}

impl std::str::FromStr for Version {
    type Err = VersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(Version::Latest),
            "dev" => Ok(Version::Dev),
            _ => {
                // Try parsing as a version hash first
                if let Ok(hash) = s.parse::<VersionHash>() {
                    return Ok(Version::Hash(hash));
                }
                // Try parsing as a version tag
                if let Ok(tag) = s.parse::<VersionTag>() {
                    return Ok(Version::Tag(tag));
                }
                Err(VersionParseError {
                    input: s.to_string(),
                })
            }
        }
    }
}

impl serde::Serialize for Version {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as string using Display implementation
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Error type for [`Version`] parsing failures.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid version format: '{input}' (expected semver tag, 64-char hex hash, 'latest', or 'dev')"
)]
pub struct VersionParseError {
    /// The input string that could not be parsed
    pub input: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_parses_all_version_formats() {
        //* Given
        let tag_str = "1.0.0";
        let hash_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let latest_str = "latest";
        let dev_str = "dev";

        //* When
        let tag_version: Version = tag_str
            .parse()
            .expect("parsing valid semver tag should succeed");
        let hash_version: Version = hash_str
            .parse()
            .expect("parsing valid 64-char hex hash should succeed");
        let latest_version: Version = latest_str
            .parse()
            .expect("parsing 'latest' keyword should succeed");
        let dev_version: Version = dev_str
            .parse()
            .expect("parsing 'dev' keyword should succeed");

        //* Then
        assert!(tag_version.is_tag(), "parsed version should be Tag variant");
        assert_eq!(
            tag_version.to_string(),
            "1.0.0",
            "Tag variant should display as semver string"
        );

        assert!(
            hash_version.is_hash(),
            "parsed version should be Hash variant"
        );
        assert_eq!(
            hash_version.to_string(),
            hash_str,
            "Hash variant should display as hex string"
        );

        assert!(
            latest_version.is_latest(),
            "parsed version should be Latest variant"
        );
        assert_eq!(
            latest_version.to_string(),
            "latest",
            "Latest variant should display as 'latest'"
        );

        assert!(dev_version.is_dev(), "parsed version should be Dev variant");
        assert_eq!(
            dev_version.to_string(),
            "dev",
            "Dev variant should display as 'dev'"
        );
    }

    #[test]
    fn from_str_with_invalid_format_fails() {
        //* Given
        let invalid_str = "not-a-valid-version";

        //* When
        let result: Result<Version, _> = invalid_str.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing invalid version format should fail"
        );
    }
}
