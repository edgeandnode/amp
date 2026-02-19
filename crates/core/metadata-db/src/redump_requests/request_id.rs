//! Redump request ID new-type wrapper for database values
//!
//! This module provides a [`RequestId`] new-type wrapper around `i64` that maintains
//! request ID invariants for database operations.

use sqlx::{Database, Postgres, encode::IsNull, error::BoxDynError};

/// A type-safe identifier for redump request records.
///
/// [`RequestId`] is a new-type wrapper around `i64` that maintains the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct RequestId(i64);

impl RequestId {
    /// Creates a RequestId from an i64 without validation
    ///
    /// # Safety
    /// The caller must ensure the provided value upholds the request ID invariants:
    /// - Value must be positive (> 0)
    /// - Value must fit within the range of i64
    pub fn from_i64_unchecked(value: impl Into<i64>) -> Self {
        Self(value.into())
    }

    /// Converts the RequestId to its inner i64 value
    pub fn into_i64(self) -> i64 {
        self.0
    }
}

impl std::ops::Deref for RequestId {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<Postgres> for RequestId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for RequestId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for RequestId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <i64 as sqlx::Decode<Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants.
        Ok(Self::from_i64_unchecked(id))
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for RequestId {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        <i64 as sqlx::Encode<'q, Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl serde::Serialize for RequestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = i64::deserialize(deserializer)?;
        // SAFETY: Deserialized values are trusted to uphold invariants.
        Ok(Self::from_i64_unchecked(id))
    }
}
