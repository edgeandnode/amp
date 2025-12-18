//! File name new-type wrapper for database values
//!
//! This module provides a [`Name`] new-type wrapper around [`Cow<str>`] that maintains
//! file name invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."

use std::borrow::Cow;

/// An owned file name type for database return values and owned storage scenarios.
///
/// This is a type alias for `Name<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a file name with owned storage is required.
/// Prefer this alias when working with names that need to be stored or returned from the database,
/// rather than just representing a file name with owned storage in general.
pub type NameOwned = Name<'static>;

/// A file name wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains file name invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Name<'a>(Cow<'a, str>);

impl<'a> Name<'a> {
    /// Create a new Name wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided name upholds the file name invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(name: &'a str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Create a new Name wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided name upholds the file name invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(name: String) -> Name<'static> {
        Name(Cow::Owned(name))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Name(Cow::Owned(name)) => name,
            Name(Cow::Borrowed(name)) => name.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Name<'a>> for Name<'a> {
    fn from(value: &'a Name<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input Name.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input Name already upholds invariants, so the referenced data is valid.
        Name::from_ref_unchecked(value.as_ref())
    }
}

impl<'a> std::ops::Deref for Name<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Name<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Name<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Name<'a>> for &str {
    fn eq(&self, other: &Name<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Name<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Name<'a>> for str {
    fn eq(&self, other: &Name<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Name<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Name<'a>> for String {
    fn eq(&self, other: &Name<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Name<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Name<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Name<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Name<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NameOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Name::from_owned_unchecked(s))
    }
}
