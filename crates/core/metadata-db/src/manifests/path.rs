//! Manifest path new-type wrapper for database values
//!
//! This module provides a [`Path`] new-type wrapper around [`Cow<str>`] that maintains
//! manifest path invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through manifest storage operations, which ensure paths follow the
//! content-addressable format before converting into this database-layer type. Database values
//! are trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`Path`] are responsible for ensuring invariants are met:
//! - Manifest paths must be valid object store paths
//! - Manifest paths typically follow the pattern: `{hash}` (content-addressable)
//! - Manifest paths must not be empty

use std::borrow::Cow;

/// An owned manifest path type for database return values and owned storage scenarios.
///
/// This is a type alias for `Path<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a manifest path with owned storage is required.
/// Prefer this alias when working with paths that need to be stored or returned from the database,
/// rather than just representing a manifest path with owned storage in general.
pub type PathOwned = Path<'static>;

/// A manifest path wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains manifest path invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Path<'a>(Cow<'a, str>);

impl<'a> Path<'a> {
    /// Create a new Path wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided path upholds the manifest path invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(path: &'a str) -> Self {
        Self(Cow::Borrowed(path))
    }

    /// Create a new Path wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided path upholds the manifest path invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(path: String) -> Path<'static> {
        Path(Cow::Owned(path))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Path(Cow::Owned(path)) => path,
            Path(Cow::Borrowed(path)) => path.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Path<'a>> for Path<'a> {
    fn from(path: &'a Path<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input path.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input path already upholds invariants, so the referenced data is valid.
        Path::from_ref_unchecked(path.as_ref())
    }
}

impl<'a> std::ops::Deref for Path<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Path<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Path<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Path<'a>> for &str {
    fn eq(&self, other: &Path<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Path<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Path<'a>> for str {
    fn eq(&self, other: &Path<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Path<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Path<'a>> for String {
    fn eq(&self, other: &Path<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Path<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Path<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PathOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Path::from_owned_unchecked(s))
    }
}
