//! Semantic version tag wrapper for dataset versions
//!
//! This module provides a [`VersionTag`] new-type wrapper around [`Cow<str>`] that stores
//! semantic version strings for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`datasets_common::VersionTag`], which enforce
//! valid semver format before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`VersionTag`] are responsible for ensuring invariants are met:
//! - Version strings must be valid semantic versions (e.g., "1.2.3", "1.0.0-alpha")
//! - Version strings must not be empty

use std::borrow::Cow;

/// An owned semantic version type for database return values and owned storage scenarios.
///
/// This is a type alias for `VersionTag<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a semantic version with owned storage is required.
/// Prefer this alias when working with versions that need to be stored or returned from the database,
/// rather than just representing a semantic version with owned storage in general.
pub type VersionTagOwned = VersionTag<'static>;

/// A semantic version wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` stores semantic version strings for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionTag<'a>(Cow<'a, str>);

impl<'a> VersionTag<'a> {
    /// Create a new VersionTag wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided version string upholds valid semver format.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(version: &'a str) -> Self {
        Self(Cow::Borrowed(version))
    }

    /// Create a new VersionTag wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided version string upholds valid semver format.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(version: String) -> VersionTag<'static> {
        VersionTag(Cow::Owned(version))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            VersionTag(Cow::Owned(version)) => version,
            VersionTag(Cow::Borrowed(version)) => version.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> std::ops::Deref for VersionTag<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for VersionTag<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for VersionTag<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<VersionTag<'a>> for &str {
    fn eq(&self, other: &VersionTag<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for VersionTag<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<VersionTag<'a>> for str {
    fn eq(&self, other: &VersionTag<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for VersionTag<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<VersionTag<'a>> for String {
    fn eq(&self, other: &VersionTag<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for VersionTag<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for VersionTag<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for VersionTagOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(VersionTag::from_owned_unchecked(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ref_unchecked_with_valid_semver_succeeds() {
        //* Given
        let version_str = "1.2.3";

        //* When
        let version = VersionTag::from_ref_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3",
            "version string should be preserved"
        );
    }

    #[test]
    fn from_owned_unchecked_with_valid_semver_succeeds() {
        //* Given
        let version_str = "1.2.3".to_string();

        //* When
        let version = VersionTag::from_owned_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3",
            "version string should be preserved"
        );
    }

    #[test]
    fn from_ref_unchecked_with_prerelease_succeeds() {
        //* Given
        let version_str = "1.2.3-alpha.1";

        //* When
        let version = VersionTag::from_ref_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3-alpha.1",
            "prerelease should be preserved"
        );
    }

    #[test]
    fn from_owned_unchecked_with_prerelease_succeeds() {
        //* Given
        let version_str = "1.2.3-alpha.1".to_string();

        //* When
        let version = VersionTag::from_owned_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3-alpha.1",
            "prerelease should be preserved"
        );
    }

    #[test]
    fn from_ref_unchecked_with_build_metadata_succeeds() {
        //* Given
        let version_str = "1.2.3+build.123";

        //* When
        let version = VersionTag::from_ref_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3+build.123",
            "build metadata should be preserved"
        );
    }

    #[test]
    fn from_ref_unchecked_with_prerelease_and_build_succeeds() {
        //* Given
        let version_str = "1.2.3-alpha.1+build.123";

        //* When
        let version = VersionTag::from_ref_unchecked(version_str);

        //* Then
        assert_eq!(
            version.as_str(),
            "1.2.3-alpha.1+build.123",
            "prerelease and build metadata should be preserved"
        );
    }

    #[test]
    fn deref_provides_str_access() {
        //* Given
        let version_str = "1.2.3";
        let version = VersionTag::from_ref_unchecked(version_str);

        //* When
        let deref_str: &str = &*version;

        //* Then
        assert_eq!(deref_str, "1.2.3", "should deref to str");
    }

    #[test]
    fn as_str_returns_inner_string() {
        //* Given
        let version_str = "1.2.3";
        let version = VersionTag::from_ref_unchecked(version_str);

        //* When
        let result = version.as_str();

        //* Then
        assert_eq!(result, "1.2.3", "should return inner string");
    }

    #[test]
    fn into_inner_with_borrowed_version_returns_owned() {
        //* Given
        let version_str = "1.2.3";
        let borrowed_version = VersionTag::from_ref_unchecked(version_str);

        //* When
        let owned = borrowed_version.into_inner();

        //* Then
        assert_eq!(owned, "1.2.3", "should convert borrowed to owned");
    }

    #[test]
    fn into_inner_with_owned_version_returns_owned() {
        //* Given
        let version_str = "1.2.3".to_string();
        let owned_version = VersionTag::from_owned_unchecked(version_str);

        //* When
        let owned = owned_version.into_inner();

        //* Then
        assert_eq!(owned, "1.2.3", "should return owned string");
    }

    #[test]
    fn equality_with_str_slice_works() {
        //* Given
        let version = VersionTag::from_ref_unchecked("1.2.3");

        //* Then
        assert_eq!(version, "1.2.3", "should equal str slice");
        assert_eq!("1.2.3", version, "str slice should equal version");
    }

    #[test]
    fn equality_with_string_works() {
        //* Given
        let version = VersionTag::from_ref_unchecked("1.2.3");
        let string = "1.2.3".to_string();

        //* Then
        assert_eq!(version, string, "should equal String");
        assert_eq!(string, version, "String should equal version");
    }

    #[test]
    fn display_formatting_works() {
        //* Given
        let version = VersionTag::from_ref_unchecked("1.2.3-alpha+build");

        //* When
        let formatted = format!("{}", version);

        //* Then
        assert_eq!(formatted, "1.2.3-alpha+build", "should format correctly");
    }

    #[test]
    fn debug_formatting_works() {
        //* Given
        let version = VersionTag::from_ref_unchecked("1.2.3");

        //* When
        let formatted = format!("{:?}", version);

        //* Then
        assert_eq!(formatted, "\"1.2.3\"", "should debug format correctly");
    }

    #[test]
    fn from_ref_unchecked_uses_cow_borrowed() {
        //* Given
        let version_str = "1.2.3";

        //* When
        let borrowed = VersionTag::from_ref_unchecked(version_str);

        //* Then
        assert!(
            matches!(borrowed.0, Cow::Borrowed(_)),
            "should use Cow::Borrowed for borrowed version"
        );
    }

    #[test]
    fn from_owned_unchecked_uses_cow_owned() {
        //* Given
        let version_str = "1.2.3".to_string();

        //* When
        let owned = VersionTag::from_owned_unchecked(version_str);

        //* Then
        assert!(
            matches!(owned.0, Cow::Owned(_)),
            "should use Cow::Owned for owned version"
        );
    }
}
