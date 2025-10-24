//! Dataset namespace new-type wrapper for database values
//!
//! This module provides a [`Namespace`] new-type wrapper around [`Cow<str>`] that maintains
//! dataset namespace invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`datasets_common::Namespace`], which enforce the
//! required format before converting into this database-layer type. Database values are
//! trusted as already valid, following the principle of "validate at boundaries, trust
//! database data."
//!
//! Types that convert into [`Namespace`] are responsible for ensuring invariants are met:
//! - Namespaces must start with a lowercase letter or underscore
//! - Namespaces must contain only lowercase letters, digits, and underscores
//! - Namespaces must not be empty

use std::borrow::Cow;

/// An owned dataset namespace type for database return values and owned storage scenarios.
///
/// This is a type alias for `Namespace<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a dataset namespace with owned storage is required.
/// Prefer this alias when working with namespaces that need to be stored or returned from the database,
/// rather than just representing a dataset namespace with owned storage in general.
pub type NamespaceOwned = Namespace<'static>;

/// A dataset namespace wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains dataset namespace invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace<'a>(Cow<'a, str>);

impl<'a> Namespace<'a> {
    /// Create a new Namespace wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided namespace upholds the dataset namespace invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(namespace: &'a str) -> Self {
        Self(Cow::Borrowed(namespace))
    }

    /// Create a new Namespace wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided namespace upholds the dataset namespace invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(namespace: String) -> Namespace<'static> {
        Namespace(Cow::Owned(namespace))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Namespace(Cow::Owned(namespace)) => namespace,
            Namespace(Cow::Borrowed(namespace)) => namespace.to_owned(),
        }
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> From<&'a Namespace<'a>> for Namespace<'a> {
    fn from(namespace: &'a Namespace<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input namespace.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input namespace already upholds invariants, so the referenced data is valid.
        Namespace::from_ref_unchecked(namespace.as_ref())
    }
}

impl<'a> std::ops::Deref for Namespace<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for Namespace<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for Namespace<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<Namespace<'a>> for &str {
    fn eq(&self, other: &Namespace<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> PartialEq<str> for Namespace<'a> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Namespace<'a>> for str {
    fn eq(&self, other: &Namespace<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> PartialEq<String> for Namespace<'a> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl<'a> PartialEq<Namespace<'a>> for String {
    fn eq(&self, other: &Namespace<'a>) -> bool {
        self == other.as_str()
    }
}

impl<'a> std::fmt::Display for Namespace<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for Namespace<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for Namespace<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for Namespace<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NamespaceOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Namespace::from_owned_unchecked(s))
    }
}
