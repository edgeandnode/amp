//! Revision types for dataset definitions.
//!
//! This module provides the `Revision` enum for representing different ways
//! to reference dataset revisions: by semver tag, by content hash, or by symbolic references.

use crate::{hash::Hash, version::Version};

/// A revision reference that can be a semantic version tag, content hash, or symbolic reference.
///
/// This enum unifies the different ways to reference dataset revisions:
/// - `Version`: A semantic version tag (e.g., `1.0.0`, `2.1.3-alpha.1`)
/// - `Hash`: A 32-byte SHA-256 content hash
/// - `Latest`: The latest tagged semver version
/// - `Dev`: The latest hash based on timestamp ordering
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum Revision {
    /// A semantic version tag
    Version(Version),
    /// A 32-byte SHA-256 content hash
    Hash(Hash),
    /// The latest tagged semver version
    Latest,
    /// The latest hash given a timestamp ordering
    Dev,
}

impl Revision {
    /// Returns `true` if the revision is a `Version` variant.
    pub fn is_version(&self) -> bool {
        matches!(self, Revision::Version(_))
    }

    /// Returns `true` if the revision is a `Hash` variant.
    pub fn is_hash(&self) -> bool {
        matches!(self, Revision::Hash(_))
    }

    /// Returns `true` if the revision is the `Latest` variant.
    pub fn is_latest(&self) -> bool {
        matches!(self, Revision::Latest)
    }

    /// Returns `true` if the revision is the `Dev` variant.
    pub fn is_dev(&self) -> bool {
        matches!(self, Revision::Dev)
    }

    /// Returns a reference to the inner `Version` if this is a `Version` variant.
    pub fn as_version(&self) -> Option<&Version> {
        match self {
            Revision::Version(version) => Some(version),
            _ => None,
        }
    }

    /// Returns a reference to the inner `Hash` if this is a `Hash` variant.
    pub fn as_hash(&self) -> Option<&Hash> {
        match self {
            Revision::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    /// Consumes the `Revision` and returns the inner `Version` if this is a `Version` variant.
    pub fn into_version(self) -> Option<Version> {
        match self {
            Revision::Version(version) => Some(version),
            _ => None,
        }
    }

    /// Consumes the `Revision` and returns the inner `Hash` if this is a `Hash` variant.
    pub fn into_hash(self) -> Option<Hash> {
        match self {
            Revision::Hash(hash) => Some(hash),
            _ => None,
        }
    }
}

impl From<Version> for Revision {
    fn from(version: Version) -> Self {
        Revision::Version(version)
    }
}

impl From<Hash> for Revision {
    fn from(hash: Hash) -> Self {
        Revision::Hash(hash)
    }
}

/// Displays the revision in its canonical string format.
///
/// # Alternate Format
///
/// Use `{:#}` for compact display. When the revision is a hash, it will be shortened to 7 characters.
/// Other revision types (Version, Latest, Dev) are displayed unchanged.
///
/// ```no_run
/// format!("{}", version_revision); // "1.0.0"
/// format!("{}", latest_revision);  // "latest"
/// format!("{}", dev_revision);     // "dev"
/// format!("{}", hash_revision);    // "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
/// format!("{:#}", hash_revision);  // "b94d27b"
/// ```
impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Revision::Version(v) => write!(f, "{v}"),
            Revision::Hash(h) => {
                if f.alternate() {
                    f.write_str(h.as_short_hash_str())
                } else {
                    write!(f, "{h}")
                }
            }
            Revision::Latest => f.write_str("latest"),
            Revision::Dev => f.write_str("dev"),
        }
    }
}

impl std::str::FromStr for Revision {
    type Err = RevisionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(Revision::Latest),
            "dev" => Ok(Revision::Dev),
            _ => {
                // Try parsing as a hash first
                if let Ok(hash) = s.parse::<Hash>() {
                    return Ok(Revision::Hash(hash));
                }
                // Try parsing as a version
                if let Ok(version) = s.parse::<Version>() {
                    return Ok(Revision::Version(version));
                }
                Err(RevisionParseError {
                    input: s.to_string(),
                })
            }
        }
    }
}

impl serde::Serialize for Revision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as string using Display implementation
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Revision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Error type for [`Revision`] parsing failures.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid revision format: '{input}' (expected semver tag, 64-char hex hash, 'latest', or 'dev')"
)]
pub struct RevisionParseError {
    /// The input string that could not be parsed
    pub input: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_parses_all_revision_formats() {
        //* Given
        let version_str = "1.0.0";
        let hash_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let latest_str = "latest";
        let dev_str = "dev";

        //* When
        let version_revision: Revision = version_str
            .parse()
            .expect("parsing valid semver version should succeed");
        let hash_revision: Revision = hash_str
            .parse()
            .expect("parsing valid 64-char hex hash should succeed");
        let latest_revision: Revision = latest_str
            .parse()
            .expect("parsing 'latest' keyword should succeed");
        let dev_revision: Revision = dev_str
            .parse()
            .expect("parsing 'dev' keyword should succeed");

        //* Then
        assert!(
            version_revision.is_version(),
            "parsed revision should be Version variant"
        );
        assert_eq!(
            version_revision.to_string(),
            "1.0.0",
            "Version variant should display as semver string"
        );

        assert!(
            hash_revision.is_hash(),
            "parsed revision should be Hash variant"
        );
        assert_eq!(
            hash_revision.to_string(),
            hash_str,
            "Hash variant should display as hex string"
        );

        assert!(
            latest_revision.is_latest(),
            "parsed revision should be Latest variant"
        );
        assert_eq!(
            latest_revision.to_string(),
            "latest",
            "Latest variant should display as 'latest'"
        );

        assert!(
            dev_revision.is_dev(),
            "parsed revision should be Dev variant"
        );
        assert_eq!(
            dev_revision.to_string(),
            "dev",
            "Dev variant should display as 'dev'"
        );
    }

    #[test]
    fn from_str_with_invalid_format_fails() {
        //* Given
        let invalid_str = "not-a-valid-revision";

        //* When
        let result: Result<Revision, _> = invalid_str.parse();

        //* Then
        assert!(
            result.is_err(),
            "parsing invalid revision format should fail"
        );
    }

    #[test]
    fn display_hash_variant_with_default_format_shows_full_hash() {
        //* Given
        let hash_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let revision: Revision = hash_str.parse().expect("should parse valid hash revision");

        //* When
        let result = format!("{}", revision);

        //* Then
        assert_eq!(
            result, hash_str,
            "default format should show full 64-character hash"
        );
    }

    #[test]
    fn display_hash_variant_with_alternate_format_shows_short_hash() {
        //* Given
        let revision: Revision = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("should parse valid hash revision");

        //* When
        let result = format!("{:#}", revision);

        //* Then
        assert_eq!(
            result, "b94d27b",
            "alternate format should show 7-character short hash"
        );
    }
}
