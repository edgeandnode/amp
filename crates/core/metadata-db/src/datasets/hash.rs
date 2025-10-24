//! Dataset version hash new-type wrapper for database values
//!
//! This module provides a [`Hash`] new-type wrapper around [`Cow<str>`] that maintains
//! version hash invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`datasets_common::Hash`], which enforce the
//! required format before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`Hash`] are responsible for ensuring invariants are met:
//! - Version hashes must be exactly 64 characters long (64 hex digits)
//! - Version hashes must contain only valid hex digits (`0-9`, `a-f`, `A-F`)

use std::borrow::Cow;

/// An owned version hash type for database return values and owned storage scenarios.
///
/// This is a type alias for `Hash<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a version hash with owned storage is required.
/// Prefer this alias when working with version hashes that need to be stored or returned from the database,
/// rather than just representing a version hash with owned storage in general.
pub type HashOwned = Hash<'static>;

/// A version hash wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains version hash invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash<'a>(Cow<'a, str>);

impl<'a> Hash<'a> {
    /// Create a new Hash wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided version hash upholds the version hash invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(hash: &'a str) -> Self {
        Self(Cow::Borrowed(hash))
    }

    /// Create a new Hash wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided version hash upholds the version hash invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(hash: String) -> Hash<'static> {
        Hash(Cow::Owned(hash))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Hash(Cow::Owned(hash)) => hash,
            Hash(Cow::Borrowed(hash)) => hash.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Hash<'a>> for Hash<'a> {
    fn from(hash: &'a Hash<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input hash.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input hash already upholds invariants, so the referenced data is valid.
        Hash::from_ref_unchecked(hash.as_ref())
    }
}

impl<'a> std::ops::Deref for Hash<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Hash<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Hash<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Hash<'a>> for &str {
    fn eq(&self, other: &Hash<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Hash<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Hash<'a>> for str {
    fn eq(&self, other: &Hash<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Hash<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Hash<'a>> for String {
    fn eq(&self, other: &Hash<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Hash<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Hash<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Hash<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Hash<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for HashOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Hash::from_owned_unchecked(s))
    }
}
