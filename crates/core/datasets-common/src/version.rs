//! Version and version requirement types for dataset definitions.
//!
//! This module provides wrapper types around semver for dataset versioning
//! with additional utilities and JSON schema support.

/// Semver version wrapper with JSON schema support and version manipulation utilities.
///
/// Provides serialization and version manipulation utilities for dataset versioning.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct Version(#[cfg_attr(feature = "schemars", schemars(with = "String"))] semver::Version);

impl Default for Version {
    fn default() -> Self {
        Self(semver::Version::new(0, 0, 0))
    }
}

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

    /// Convert the version to an underscore-separated string.
    ///
    /// Examples:
    /// - `1.0.0` → `1_0_0`
    /// - `2.1.3-alpha.1+build.123` → `2_1_3-alpha_1+build_123`
    pub fn to_underscore_version(&self) -> String {
        self.0.to_string().replace('.', "_")
    }

    /// Parse a version from an underscore-separated string.
    ///
    /// This is the counterpart of [`to_underscore_version`](Self::to_underscore_version).
    ///
    /// Examples:
    /// - `1_0_0` → `1.0.0`
    /// - `2_1_3-alpha_1+build_123` → `2.1.3-alpha.1+build.123`
    pub fn try_from_underscore_version(v_identifier: &str) -> Result<Self, VersionError> {
        v_identifier.replace("_", ".").parse()
    }
}

impl std::ops::Deref for Version {
    type Target = semver::Version;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<semver::Version> for Version {
    fn as_ref(&self) -> &semver::Version {
        &self.0
    }
}

impl From<metadata_db::DatasetVersionOwned> for Version {
    fn from(value: metadata_db::DatasetVersionOwned) -> Self {
        Self(value.into_inner())
    }
}

impl From<Version> for metadata_db::DatasetVersionOwned {
    fn from(value: Version) -> Self {
        metadata_db::DatasetVersion::from_owned(value.0)
    }
}

impl<'a> From<&'a Version> for metadata_db::DatasetVersion<'a> {
    fn from(value: &'a Version) -> Self {
        metadata_db::DatasetVersion::from_ref(&value.0)
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
    type Err = VersionError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self).map_err(VersionError)
    }
}

/// Wrapper error for version parsing errors.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct VersionError(pub semver::Error);
