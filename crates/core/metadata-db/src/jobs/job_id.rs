//! Job ID new-type wrapper for database values
//!
//! This module provides a [`JobId`] new-type wrapper around `i64` that maintains job ID
//! invariants for database operations.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`worker::JobId`], which enforce the required
//! constraints before converting into this database-layer type. Database values are trusted
//! as already valid, following the principle of "validate at boundaries, trust database data."
//!
//! Types that convert into [`JobId`] are responsible for ensuring invariants are met:
//! - Job IDs must be positive (> 0)
//! - Job IDs must fit within the range of `i64`

use sqlx::{Database, Postgres, encode::IsNull, error::BoxDynError};

/// A type-safe identifier for job records.
///
/// [`JobId`] is a new-type wrapper around `i64` that maintains the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type. The type integrates seamlessly with
/// PostgreSQL through sqlx trait implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct JobId(i64);

impl JobId {
    /// Creates a JobId from an i64 without validation
    ///
    /// # Safety
    /// The caller must ensure the provided value upholds the job ID invariants:
    /// - Value must be positive (> 0)
    /// - Value must fit within the range of i64
    ///
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_i64_unchecked(value: impl Into<i64>) -> Self {
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
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Self::from_i64_unchecked(id))
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
        // SAFETY: Deserialized values are trusted to uphold invariants; typically from database or internal communication.
        Ok(Self::from_i64_unchecked(id))
    }
}
