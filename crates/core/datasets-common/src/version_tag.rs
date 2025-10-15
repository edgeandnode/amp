//! Version tag type for dataset definitions.
//!
//! This module provides wrapper types around semver for dataset version tagging
//! with additional utilities and JSON schema support.

/// Semver version tag wrapper with JSON schema support and version manipulation utilities.
///
/// Provides serialization and version manipulation utilities for dataset version tagging.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct VersionTag(#[cfg_attr(feature = "schemars", schemars(with = "String"))] semver::Version);

impl Default for VersionTag {
    fn default() -> Self {
        Self(semver::Version::new(0, 0, 0))
    }
}

impl VersionTag {
    /// Create a new [`VersionTag`] from major, minor, and patch components.
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(semver::Version {
            major,
            minor,
            patch,
            pre: semver::Prerelease::EMPTY,
            build: semver::BuildMetadata::EMPTY,
        })
    }

    /// Convert the version tag to an underscore-separated string.
    ///
    /// Examples:
    /// - `1.0.0` → `1_0_0`
    /// - `2.1.3-alpha.1+build.123` → `2_1_3-alpha_1+build_123`
    pub fn to_underscore_version(&self) -> String {
        self.0.to_string().replace('.', "_")
    }

    /// Parse a version tag from an underscore-separated string.
    ///
    /// This is the counterpart of [`to_underscore_version`](Self::to_underscore_version).
    ///
    /// Examples:
    /// - `1_0_0` → `1.0.0`
    /// - `2_1_3-alpha_1+build_123` → `2.1.3-alpha.1+build.123`
    pub fn try_from_underscore_version(v_identifier: &str) -> Result<Self, VersionTagError> {
        v_identifier.replace("_", ".").parse()
    }
}

impl std::ops::Deref for VersionTag {
    type Target = semver::Version;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<semver::Version> for VersionTag {
    fn as_ref(&self) -> &semver::Version {
        &self.0
    }
}

#[cfg(feature = "metadata-db")]
impl From<metadata_db::DatasetVersionTagOwned> for VersionTag {
    fn from(value: metadata_db::DatasetVersionTagOwned) -> Self {
        // SAFETY: DatasetVersionOwned is a new-type wrapper around semver::Version,
        // and into_inner() returns the owned semver::Version without any validation
        // or transformation, making this conversion safe and lossless.
        Self(value.into_inner())
    }
}

#[cfg(feature = "metadata-db")]
impl From<VersionTag> for metadata_db::DatasetVersionTagOwned {
    fn from(value: VersionTag) -> Self {
        // SAFETY: This conversion is safe and lossless as both types have identical representation
        metadata_db::DatasetVersionTag::from_owned(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a VersionTag> for metadata_db::DatasetVersionTag<'a> {
    fn from(value: &'a VersionTag) -> Self {
        // SAFETY: This conversion is safe and lossless as both types have identical representation
        metadata_db::DatasetVersionTag::from_ref(&value.0)
    }
}

impl PartialEq<semver::Version> for VersionTag {
    fn eq(&self, other: &semver::Version) -> bool {
        self.0 == *other
    }
}

impl PartialEq<VersionTag> for semver::Version {
    fn eq(&self, other: &VersionTag) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for VersionTag {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for VersionTag {
    type Err = VersionTagError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self).map_err(VersionTagError)
    }
}

/// Wrapper error for version tag parsing errors.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct VersionTagError(pub semver::Error);
