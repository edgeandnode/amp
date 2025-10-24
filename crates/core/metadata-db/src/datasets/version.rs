//! Semantic version wrapper for dataset versions
//!
//! This module provides a [`Version`] new-type wrapper around [`Cow<str>`] that stores
//! semantic version strings for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`datasets_common::Version`], which enforce
//! valid semver format before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`Version`] are responsible for ensuring invariants are met:
//! - Version strings must be valid semantic versions (e.g., "1.2.3", "1.0.0-alpha")
//! - Version strings must not be empty

use std::borrow::Cow;

/// An owned semantic version type for database return values and owned storage scenarios.
///
/// This is a type alias for `Version<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a semantic version with owned storage is required.
/// Prefer this alias when working with versions that need to be stored or returned from the database,
/// rather than just representing a semantic version with owned storage in general.
pub type VersionOwned = Version<'static>;

/// A semantic version wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` stores semantic version strings for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version<'a>(Cow<'a, str>);

impl<'a> Version<'a> {
    /// Create a new Version wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided version string upholds valid semver format.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(version: &'a str) -> Self {
        Self(Cow::Borrowed(version))
    }

    /// Create a new Version wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided version string upholds valid semver format.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(version: String) -> Version<'static> {
        Version(Cow::Owned(version))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Version(Cow::Owned(version)) => version,
            Version(Cow::Borrowed(version)) => version.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Version<'a>> for Version<'a> {
    fn from(version: &'a Version<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input version.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input version already upholds invariants, so the referenced data is valid.
        Version::from_ref_unchecked(version.as_ref())
    }
}

impl<'a> std::ops::Deref for Version<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Version<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Version<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Version<'a>> for &str {
    fn eq(&self, other: &Version<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Version<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Version<'a>> for str {
    fn eq(&self, other: &Version<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Version<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Version<'a>> for String {
    fn eq(&self, other: &Version<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Version<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Version<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Version<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Version<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for VersionOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Version::from_owned_unchecked(s))
    }
}
