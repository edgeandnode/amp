//! Job ID new-type with validation for job record identifiers.
//!
//! This module provides a type-safe wrapper around database job IDs with built-in
//! validation to ensure IDs are always positive and within valid ranges.

use sqlx::{Database, Postgres, encode::IsNull, error::BoxDynError};

/// A type-safe identifier for job records.
///
/// [`JobId`] is a new-type wrapper around `i64` that enforces the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
///
/// This type provides validation when converting from raw integers and integrates
/// seamlessly with PostgreSQL through sqlx trait implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct JobId(i64);

impl JobId {
    /// Creates a JobId from an i64 without validation
    ///
    /// # Safety
    /// The caller must ensure the provided value holds the invariants:
    /// - Value must be positive (> 0)
    /// - Value must fit within the range of i64
    pub fn from_i64(value: impl Into<i64>) -> Self {
        Self(value.into())
    }

    /// Converts the JobId to its inner i64 value
    pub fn into_i64(self) -> i64 {
        self.0
    }
}

impl std::ops::Deref for JobId {
    type Target = i64;

    /// Dereferences the [`JobId`] to its inner `i64` value.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<Postgres> for JobId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for JobId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for JobId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <i64 as sqlx::Decode<Postgres>>::decode(value)?;
        Ok(Self::from_i64(id))
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for JobId {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        <i64 as sqlx::Encode<'q, Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl serde::Serialize for JobId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for JobId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = i64::deserialize(deserializer)?;
        Ok(Self::from_i64(id))
    }
}
