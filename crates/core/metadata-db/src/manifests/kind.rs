//! Manifest kind new-type wrapper for database values
//!
//! This module provides a [`Kind`] new-type wrapper around [`Cow<str>`] that maintains
//! manifest kind invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through manifest registration operations, which ensure kinds are
//! valid before converting into this database-layer type. Database values are trusted as
//! already valid, following the principle of "validate at boundaries, trust database data."
//!
//! Types that convert into [`Kind`] are responsible for ensuring invariants are met:
//! - Manifest kind must be a non-empty string identifying the dataset type
//! - Examples: `"evm-rpc"`, `"solana"`, `"firehose"`, `"manifest"`, `"unknown"`

use std::borrow::Cow;

/// An owned manifest kind type for database return values and owned storage scenarios.
///
/// This is a type alias for `Kind<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a manifest kind with owned storage is required.
/// Prefer this alias when working with kinds that need to be stored or returned from the database,
/// rather than just representing a manifest kind with owned storage in general.
pub type KindOwned = Kind<'static>;

/// A manifest kind wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains manifest kind invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct Kind<'a>(Cow<'a, str>);

impl<'a> Kind<'a> {
    /// Create a new Kind wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided kind upholds the manifest kind invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(kind: &'a str) -> Self {
        Self(Cow::Borrowed(kind))
    }

    /// Create a new Kind wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided kind upholds the manifest kind invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(kind: String) -> Kind<'static> {
        Kind(Cow::Owned(kind))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Kind(Cow::Owned(kind)) => kind,
            Kind(Cow::Borrowed(kind)) => kind.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Kind<'a>> for Kind<'a> {
    fn from(value: &'a Kind<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input Kind.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input Kind already upholds invariants, so the referenced data is valid.
        Kind::from_ref_unchecked(value.as_ref())
    }
}

impl<'a> std::ops::Deref for Kind<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Kind<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Kind<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Kind<'a>> for &str {
    fn eq(&self, other: &Kind<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Kind<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Kind<'a>> for str {
    fn eq(&self, other: &Kind<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Kind<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Kind<'a>> for String {
    fn eq(&self, other: &Kind<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Kind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Kind<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Kind<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Kind<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for KindOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Kind::from_owned_unchecked(s))
    }
}
