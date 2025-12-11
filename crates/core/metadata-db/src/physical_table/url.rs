//! Physical table URL new-type wrapper for database values
//!
//! This module provides a [`Url`] new-type wrapper around [`Cow<str>`] that maintains
//! physical table URL invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`common::catalog::physical::PhyTableUrl`], which enforce
//! the required format before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`Url`] are responsible for ensuring invariants are met:
//! - URLs must be valid object store URLs (parseable by `url::Url`)
//! - URLs must have a supported scheme (s3, gs, azure, file)
//! - URLs should represent directory locations (typically ending with `/`)

use std::borrow::Cow;

/// An owned physical table URL type for database return values and owned storage scenarios.
///
/// This is a type alias for `Url<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a physical table URL with owned storage is required.
/// Prefer this alias when working with URLs that need to be stored or returned from the database,
/// rather than just representing a URL with owned storage in general.
pub type UrlOwned = Url<'static>;

/// A physical table URL wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains physical table URL invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Url<'a>(Cow<'a, str>);

impl<'a> Url<'a> {
    /// Create a new Url wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided URL upholds the physical table URL invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(url: &'a str) -> Self {
        Self(Cow::Borrowed(url))
    }

    /// Create a new Url wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided URL upholds the physical table URL invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(url: String) -> Url<'static> {
        Url(Cow::Owned(url))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Url(Cow::Owned(url)) => url,
            Url(Cow::Borrowed(url)) => url.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Url<'a>> for Url<'a> {
    fn from(value: &'a Url<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input Url.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input Url already upholds invariants, so the referenced data is valid.
        Url::from_ref_unchecked(value.as_ref())
    }
}

impl<'a> std::ops::Deref for Url<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Url<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Url<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Url<'a>> for &str {
    fn eq(&self, other: &Url<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Url<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Url<'a>> for str {
    fn eq(&self, other: &Url<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Url<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Url<'a>> for String {
    fn eq(&self, other: &Url<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Url<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Url<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Url<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Url<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for UrlOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Url::from_owned_unchecked(s))
    }
}
