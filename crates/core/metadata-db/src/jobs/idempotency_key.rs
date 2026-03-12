//! Idempotency key new-type wrapper for database values
//!
//! This module provides an [`IdempotencyKey`] new-type wrapper around [`Cow<str>`] that
//! maintains idempotency key invariants for database operations. The type provides efficient
//! handling with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through the controller's key computation logic, which produces a
//! hash-based key. Database values are trusted as already valid, following the principle
//! of "validate at boundaries, trust database data."
//!
//! Types that convert into [`IdempotencyKey`] are responsible for ensuring invariants are met:
//! - Idempotency keys must not be empty

use std::borrow::Cow;

/// An owned idempotency key type for database return values and owned storage scenarios.
///
/// This is a type alias for `IdempotencyKey<'static>`, specifically intended for use as a
/// return type from database queries or in any context where an idempotency key with owned
/// storage is required.
pub type IdempotencyKeyOwned = IdempotencyKey<'static>;

/// An idempotency key wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains idempotency key invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey<'a>(Cow<'a, str>);

impl<'a> IdempotencyKey<'a> {
    /// Create a new IdempotencyKey wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided key upholds the idempotency key invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(key: &'a str) -> Self {
        Self(Cow::Borrowed(key))
    }

    /// Create a new IdempotencyKey wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided key upholds the idempotency key invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(key: String) -> IdempotencyKeyOwned {
        IdempotencyKey(Cow::Owned(key))
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            IdempotencyKey(Cow::Owned(key)) => key,
            IdempotencyKey(Cow::Borrowed(key)) => key.to_owned(),
        }
    }
}

impl<'a> From<&'a IdempotencyKey<'a>> for IdempotencyKey<'a> {
    fn from(value: &'a IdempotencyKey<'a>) -> Self {
        // SAFETY: The input IdempotencyKey already upholds invariants, so the referenced data is valid.
        IdempotencyKey::from_ref_unchecked(value.as_ref())
    }
}

impl<'a> std::ops::Deref for IdempotencyKey<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for IdempotencyKey<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> std::fmt::Display for IdempotencyKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for IdempotencyKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for IdempotencyKey<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for IdempotencyKey<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for IdempotencyKeyOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(IdempotencyKey::from_owned_unchecked(s))
    }
}
