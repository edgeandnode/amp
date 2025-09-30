//! Dataset name type wrapper for efficient storage and manipulation
//!
//! This module provides a [`Name`] _new-type_ wrapper around [`Cow<str>`] that ensures
//! efficient handling of dataset names with support for both borrowed and owned strings.

use std::borrow::Cow;

/// An owned dataset name type for database return values and owned storage scenarios.
///
/// This is a type alias for `Name<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a dataset name with owned storage is required.
/// Prefer this alias when working with names that need to be stored or returned from the database,
/// rather than just representing a dataset name with owned storage in general.
pub type NameOwned = Name<'static>;

/// A dataset name wrapper that provides efficient string handling.
///
/// This _new-type_ wrapper around `Cow<str>` provides efficient handling of dataset names
/// with support for both borrowed and owned strings. It can handle both owned and borrowed
/// names efficiently through the use of copy-on-write semantics.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Name<'a>(Cow<'a, str>);

impl<'a> Name<'a> {
    /// Create a new Name wrapper from a reference to str (borrowed)
    pub fn from_ref(name: &'a str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Create a new Name wrapper from an owned String
    pub fn from_owned(name: String) -> Name<'static> {
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
        Ok(Name::from_owned(s))
    }
}
