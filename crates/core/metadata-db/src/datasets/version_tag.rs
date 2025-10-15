//! Semantic versioning utilities for proper lexicographical sorting
//!
//! This module provides a [`VersionTag`] _new-type_ wrapper around [`semver::Version`] that ensures
//! proper lexicographical sorting of semantic version strings by padding version components to a
//! consistent width when encoding to the database.

use std::borrow::Cow;

/// Maximum width for each version component when padding
const COMPONENT_WIDTH: usize = 4;

/// An owned semantic version type for database return values and owned storage scenarios.
///
/// This is a type alias for `VersionTag<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a semantic version with owned storage is required.
/// Prefer this alias when working with versions that need to be stored or returned from the database,
/// rather than just representing a semantic version with owned storage in general.
pub type VersionTagOwned = VersionTag<'static>;

/// A semantic version wrapper that provides proper lexicographical sorting.
///
/// This _new-type_ wrapper around `Cow<semver::Version>` ensures that semantic versions sort correctly
/// in lexicographical order by padding each component (major.minor.patch) to a consistent width
/// with leading zeros when stored in the database. It can handle both owned and borrowed versions efficiently.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionTag<'a>(Cow<'a, semver::Version>);

impl<'a> VersionTag<'a> {
    /// Create a new VersionTag wrapper from a reference to semver::Version (borrowed)
    pub fn from_ref(version: &'a semver::Version) -> Self {
        Self(Cow::Borrowed(version))
    }

    /// Create a new VersionTag wrapper from an owned semver::Version
    pub fn from_owned(version: semver::Version) -> VersionTag<'static> {
        VersionTag(Cow::Owned(version))
    }

    /// Consume and return the inner semver::Version (owned)
    pub fn into_inner(self) -> semver::Version {
        match self {
            VersionTag(Cow::Owned(version)) => version,
            VersionTag(Cow::Borrowed(version)) => version.to_owned(),
        }
    }

    /// Returns the zero-padded version string for database storage.
    ///
    /// This method formats the version components with leading zeros to ensure
    /// proper lexicographical sorting in the database.
    fn to_zero_padded_string(&self) -> String {
        semver_to_zero_padded_string::<COMPONENT_WIDTH>(&self.0)
    }
}

impl<'a> std::ops::Deref for VersionTag<'a> {
    type Target = semver::Version;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> std::fmt::Display for VersionTag<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for VersionTag<'static> {
    type Err = semver::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cleaned_version = string_to_clean_semver(s);
        semver::Version::parse(&cleaned_version).map(Self::from_owned)
    }
}

impl sqlx::Type<sqlx::Postgres> for VersionTag<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for VersionTag<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let padded = self.to_zero_padded_string();
        <String as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&padded, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for VersionTagOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&'r str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        s.parse().map_err(Into::into)
    }
}

/// Converts a semver::Version to a zero-padded string for database storage.
fn semver_to_zero_padded_string<const COMPONENT_WIDTH: usize>(version: &semver::Version) -> String {
    let major = format!("{:0width$}", version.major, width = COMPONENT_WIDTH);
    let minor = format!("{:0width$}", version.minor, width = COMPONENT_WIDTH);
    let patch = format!("{:0width$}", version.patch, width = COMPONENT_WIDTH);

    let base = format!("{}.{}.{}", major, minor, patch);

    if !version.pre.is_empty() && !version.build.is_empty() {
        format!("{}-{}+{}", base, version.pre, version.build)
    } else if !version.pre.is_empty() {
        format!("{}-{}", base, version.pre)
    } else if !version.build.is_empty() {
        format!("{}+{}", base, version.build)
    } else {
        base
    }
}

/// Converts a string to a clean semver string by removing leading zeros.
fn string_to_clean_semver(s: &str) -> String {
    // Split on '+' to separate build metadata (e.g., "1.2.3-alpha+build")
    let mut build_parts = s.splitn(2, '+');
    let version_and_pre = build_parts.next().unwrap_or(s);
    let build_part = build_parts.next();

    // Split on '-' to separate version from prerelease (e.g., "1.2.3-alpha")
    let mut parts = version_and_pre.splitn(2, '-');
    let version_part = parts.next().unwrap_or(version_and_pre);
    let pre_part = parts.next();

    // Remove leading zeros from each version component
    let base = version_part
        .split('.')
        .map(|component| component.trim_start_matches('0'))
        .map(|component| if component.is_empty() { "0" } else { component })
        .collect::<Vec<_>>()
        .join(".");

    // Reconstruct the full version string
    match (pre_part, build_part) {
        (Some(pre), Some(build)) => format!("{}-{}+{}", base, pre, build),
        (Some(pre), None) => format!("{}-{}", base, pre),
        (None, Some(build)) => format!("{}+{}", base, build),
        (None, None) => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_with_borrowed_semver_version_succeeds() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let version = VersionTag::from_ref(&semver);

        //* Then
        assert_eq!(version.major, 1, "major version should be preserved");
        assert_eq!(version.minor, 2, "minor version should be preserved");
        assert_eq!(version.patch, 3, "patch version should be preserved");
    }

    #[test]
    fn from_with_borrowed_semver_version_succeeds() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let version = VersionTag::from_ref(&semver);

        //* Then
        assert_eq!(version.major, 1, "major version should be preserved");
        assert_eq!(version.minor, 2, "minor version should be preserved");
        assert_eq!(version.patch, 3, "patch version should be preserved");
    }

    #[test]
    fn from_with_owned_semver_version_succeeds() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let version = VersionTag::from_owned(semver);

        //* Then
        assert_eq!(version.major, 1, "major version should be preserved");
        assert_eq!(version.minor, 2, "minor version should be preserved");
        assert_eq!(version.patch, 3, "patch version should be preserved");
    }

    #[test]
    fn from_owned_with_semver_version_succeeds() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let version = VersionTag::from_owned(semver);

        //* Then
        assert_eq!(version.major, 1, "major version should be preserved");
        assert_eq!(version.minor, 2, "minor version should be preserved");
        assert_eq!(version.patch, 3, "patch version should be preserved");
    }

    #[test]
    fn deref_with_borrowed_version_provides_semver_access() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);
        let version = VersionTag::from_ref(&semver);

        //* When
        let major = version.major;
        let minor = version.minor;
        let patch = version.patch;

        //* Then
        assert_eq!(major, 1, "should access major through deref");
        assert_eq!(minor, 2, "should access minor through deref");
        assert_eq!(patch, 3, "should access patch through deref");
    }

    #[test]
    fn to_zero_padded_string_with_basic_version_returns_padded_string() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0001.0002.0003",
            "should pad version components with leading zeros"
        );
    }

    #[test]
    fn to_zero_padded_string_with_prerelease_includes_prerelease() {
        //* Given
        let mut semver = semver::Version::new(1, 2, 3);
        semver.pre = semver::Prerelease::new("alpha").expect("should create valid prerelease");
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0001.0002.0003-alpha",
            "should include prerelease after padded version"
        );
    }

    #[test]
    fn from_str_with_zero_padded_string_succeeds() {
        //* Given
        let padded = "0001.0002.0003";

        //* When
        let result = padded.parse::<VersionTag>();

        //* Then
        assert!(result.is_ok(), "parsing zero-padded string should succeed");
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
    }

    #[test]
    fn from_str_with_zero_padded_string_with_prerelease_succeeds() {
        //* Given
        let padded = "0001.0002.0003-alpha";

        //* When
        let result = padded.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing zero-padded string with prerelease should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
        assert_eq!(
            version.pre.as_str(),
            "alpha",
            "should parse prerelease correctly"
        );
    }

    #[test]
    fn from_str_with_regular_semver_string_succeeds() {
        //* Given
        let regular = "1.2.3";

        //* When
        let result = regular.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing regular semver string should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
    }

    #[test]
    fn to_zero_padded_string_with_different_versions_sorts_lexicographically() {
        //* Given
        let semver_1_2_3 = semver::Version::new(1, 2, 3);
        let semver_1_10_1 = semver::Version::new(1, 10, 1);
        let semver_2_0_0 = semver::Version::new(2, 0, 0);

        let version_1_2_3 = VersionTag::from_ref(&semver_1_2_3);
        let version_1_10_1 = VersionTag::from_ref(&semver_1_10_1);
        let version_2_0_0 = VersionTag::from_ref(&semver_2_0_0);

        //* When
        let padded_1_2_3 = version_1_2_3.to_zero_padded_string();
        let padded_1_10_1 = version_1_10_1.to_zero_padded_string();
        let padded_2_0_0 = version_2_0_0.to_zero_padded_string();

        //* Then
        assert!(
            padded_1_2_3 < padded_1_10_1,
            "0001.0002.0003 should be lexicographically less than 0001.0010.0001"
        );
        assert!(
            padded_1_10_1 < padded_2_0_0,
            "0001.0010.0001 should be lexicographically less than 0002.0000.0000"
        );
    }

    #[test]
    fn to_zero_padded_string_with_large_versions_handles_correctly() {
        //* Given
        let semver = semver::Version::new(999, 999, 999);
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0999.0999.0999",
            "should handle large version numbers correctly"
        );
    }

    #[test]
    fn to_zero_padded_string_and_from_str_roundtrip_preserves_version() {
        //* Given
        let original = VersionTag::from_owned(semver::Version::new(1, 2, 3));

        //* When
        let padded = original.to_zero_padded_string();
        let parsed_result = padded.parse::<VersionTag>();

        //* Then
        assert!(parsed_result.is_ok(), "roundtrip parsing should succeed");
        let parsed = parsed_result.expect("should return valid version");
        assert_eq!(
            original, parsed,
            "roundtrip should preserve version equality"
        );
    }

    #[test]
    fn into_owned_with_borrowed_version_creates_owned_version() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);
        let borrowed_version = VersionTag::from_ref(&semver);

        //* When
        let owned_version = borrowed_version.to_owned();

        //* Then
        assert_eq!(
            owned_version.major, 1,
            "owned version should preserve major"
        );
        assert_eq!(
            owned_version.minor, 2,
            "owned version should preserve minor"
        );
        assert_eq!(
            owned_version.patch, 3,
            "owned version should preserve patch"
        );
    }

    #[test]
    fn from_with_borrowed_version_uses_cow_borrowed() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let borrowed = VersionTag::from_ref(&semver);

        //* Then
        assert!(
            matches!(borrowed.0, Cow::Borrowed(_)),
            "should use Cow::Borrowed for borrowed version"
        );
    }

    #[test]
    fn from_with_owned_version_uses_cow_owned() {
        //* Given
        let semver = semver::Version::new(1, 2, 3);

        //* When
        let owned = VersionTag::from_owned(semver);

        //* Then
        assert!(
            matches!(owned.0, Cow::Owned(_)),
            "should use Cow::Owned for owned version"
        );
    }

    #[test]
    fn to_zero_padded_string_with_zero_version_returns_padded_zeros() {
        //* Given
        let semver = semver::Version::new(0, 0, 0);
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0000.0000.0000",
            "should pad 0.0.0 version with leading zeros"
        );
    }

    #[test]
    fn from_str_with_zero_version_succeeds() {
        //* Given
        let zero_padded = "0000.0000.0000";

        //* When
        let result = zero_padded.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing 0.0.0 zero-padded string should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 0, "should parse major version as 0");
        assert_eq!(version.minor, 0, "should parse minor version as 0");
        assert_eq!(version.patch, 0, "should parse patch version as 0");
    }

    #[test]
    fn to_zero_padded_string_with_build_metadata_includes_build() {
        //* Given
        let semver = "1.2.3+20230615"
            .parse::<semver::Version>()
            .expect("valid semver");
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0001.0002.0003+20230615",
            "should include build metadata after padded version"
        );
    }

    #[test]
    fn to_zero_padded_string_with_prerelease_and_build_includes_both() {
        //* Given
        let semver = "1.2.3-alpha+20230615"
            .parse::<semver::Version>()
            .expect("valid semver");
        let version = VersionTag::from_ref(&semver);

        //* When
        let result = version.to_zero_padded_string();

        //* Then
        assert_eq!(
            result, "0001.0002.0003-alpha+20230615",
            "should include both prerelease and build metadata"
        );
    }

    #[test]
    fn from_str_with_build_metadata_succeeds() {
        //* Given
        let with_build = "1.2.3+build123";

        //* When
        let result = with_build.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing version with build metadata should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
        assert_eq!(
            version.build.as_str(),
            "build123",
            "should parse build metadata correctly"
        );
    }

    #[test]
    fn from_str_with_zero_padded_build_metadata_succeeds() {
        //* Given
        let padded_with_build = "0001.0002.0003+build123";

        //* When
        let result = padded_with_build.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing zero-padded version with build metadata should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
        assert_eq!(
            version.build.as_str(),
            "build123",
            "should parse build metadata correctly"
        );
    }

    #[test]
    fn from_str_with_prerelease_and_build_metadata_succeeds() {
        //* Given
        let complex = "1.2.3-alpha+build123";

        //* When
        let result = complex.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing version with prerelease and build metadata should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
        assert_eq!(
            version.pre.as_str(),
            "alpha",
            "should parse prerelease correctly"
        );
        assert_eq!(
            version.build.as_str(),
            "build123",
            "should parse build metadata correctly"
        );
    }

    #[test]
    fn from_str_with_zero_padded_prerelease_and_build_succeeds() {
        //* Given
        let padded_complex = "0001.0002.0003-alpha+build123";

        //* When
        let result = padded_complex.parse::<VersionTag>();

        //* Then
        assert!(
            result.is_ok(),
            "parsing zero-padded version with prerelease and build should succeed"
        );
        let version = result.expect("should return valid version");
        assert_eq!(version.major, 1, "should parse major version correctly");
        assert_eq!(version.minor, 2, "should parse minor version correctly");
        assert_eq!(version.patch, 3, "should parse patch version correctly");
        assert_eq!(
            version.pre.as_str(),
            "alpha",
            "should parse prerelease correctly"
        );
        assert_eq!(
            version.build.as_str(),
            "build123",
            "should parse build metadata correctly"
        );
    }

    #[test]
    fn to_zero_padded_string_with_build_sorts_by_version_not_build() {
        //* Given
        let semver_1_2_3_z = "1.2.3+zzz"
            .parse::<semver::Version>()
            .expect("valid semver");
        let semver_1_2_3_a = "1.2.3+aaa"
            .parse::<semver::Version>()
            .expect("valid semver");
        let semver_1_2_4_a = "1.2.4+aaa"
            .parse::<semver::Version>()
            .expect("valid semver");

        let version_1_2_3_z = VersionTag::from_ref(&semver_1_2_3_z);
        let version_1_2_3_a = VersionTag::from_ref(&semver_1_2_3_a);
        let version_1_2_4_a = VersionTag::from_ref(&semver_1_2_4_a);

        //* When
        let padded_1_2_3_z = version_1_2_3_z.to_zero_padded_string();
        let padded_1_2_3_a = version_1_2_3_a.to_zero_padded_string();
        let padded_1_2_4_a = version_1_2_4_a.to_zero_padded_string();

        //* Then
        assert!(
            padded_1_2_3_z < padded_1_2_4_a,
            "version 1.2.3+zzz should be lexicographically less than 1.2.4+aaa"
        );
        assert!(
            padded_1_2_3_a < padded_1_2_4_a,
            "version 1.2.3+aaa should be lexicographically less than 1.2.4+aaa"
        );
        // Note: Build metadata affects lexicographical ordering but not semver precedence
        assert!(
            padded_1_2_3_z > padded_1_2_3_a,
            "1.2.3+zzz should sort after 1.2.3+aaa lexicographically"
        );
    }

    #[test]
    fn version_tag_with_different_build_metadata_are_not_equal() {
        //* Given
        let semver_a = "1.2.3+build123"
            .parse::<semver::Version>()
            .expect("valid semver");
        let semver_b = "1.2.3+build456"
            .parse::<semver::Version>()
            .expect("valid semver");

        let version_a = VersionTag::from_owned(semver_a);
        let version_b = VersionTag::from_owned(semver_b);

        //* When
        let result = version_a != version_b;

        //* Then
        assert!(
            result,
            "versions with different build metadata are considered distinct in VersionTag"
        );
    }
}
