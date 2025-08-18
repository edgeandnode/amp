//! Location ID new-type with validation for dataset table storage location identifiers.
//!
//! This module provides a type-safe wrapper around database location IDs with built-in
//! validation to ensure IDs are always positive and within valid ranges.

use sqlx::{Database, Postgres, encode::IsNull, error::BoxDynError};

/// A type-safe identifier for location records tracking dataset table storage locations.
///
/// [`LocationId`] is a new-type wrapper around `i64` that enforces the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
///
/// This type provides validation when converting from raw integers and integrates
/// seamlessly with PostgreSQL through sqlx trait implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LocationId(i64);

impl TryFrom<u64> for LocationId {
    type Error = LocationIdU64Error;

    /// Attempts to convert an `u64` to a [`LocationId`] with validation.
    ///
    /// # Errors
    ///
    /// - `LocationIdU64Error::Zero` if the value is zero
    /// - `LocationIdU64Error::Overflow` if the value exceeds `i64::MAX`
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(LocationIdU64Error::Zero)
        } else if value > i64::MAX as u64 {
            Err(LocationIdU64Error::Overflow(value))
        } else {
            Ok(Self(value as i64))
        }
    }
}

impl TryFrom<i64> for LocationId {
    type Error = LocationIdI64ConvError;

    /// Attempts to convert an `i64` to a [`LocationId`] with validation.
    ///
    /// # Errors
    ///
    /// - `LocationIdI64ConvError::NonPositive` if the value is zero or negative
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            Err(LocationIdI64ConvError::NonPositive(value))
        } else {
            Ok(Self(value))
        }
    }
}

impl std::ops::Deref for LocationId {
    type Target = i64;

    /// Dereferences the [`LocationId`] to its inner `i64` value.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for LocationId {
    type Err = LocationIdFromStrError;

    /// Parses a string as a [`LocationId`].
    ///
    /// # Errors
    ///
    /// - `LocationIdFromStrError::ParseError` if the string is not a valid `i64`
    /// - `LocationIdFromStrError::NonPositive` if the parsed value is zero or negative
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s
            .parse::<i64>()
            .map_err(LocationIdFromStrError::ParseError)?;
        id.try_into()
            .map_err(|err| LocationIdFromStrError::NonPositive(err))
    }
}

impl std::fmt::Display for LocationId {
    /// Formats the [`LocationId`] for display, showing the inner `i64` value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl sqlx::Type<Postgres> for LocationId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for LocationId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

// Implement sqlx traits for database operations
impl<'r> sqlx::Decode<'r, Postgres> for LocationId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <i64 as sqlx::Decode<Postgres>>::decode(value)?;
        LocationId::try_from(id).map_err(|err| Box::new(err) as BoxDynError)
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for LocationId {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        <i64 as sqlx::Encode<'q, Postgres>>::encode_by_ref(&self.0, buf)
    }
}

/// Errors that can occur when converting from `u64` to [`LocationId`].
#[derive(Debug, thiserror::Error)]
pub enum LocationIdU64Error {
    /// The provided value is zero, but [`LocationId`] requires positive values.
    #[error("LocationId must be positive, got zero")]
    Zero,
    /// The provided `u64` value exceeds [`i64::MAX`] and would cause an overflow.
    #[error("Value {0} exceeds i64::MAX and would overflow")]
    Overflow(u64),
}

/// Errors that can occur when converting from `i64` to [`LocationId`].
#[derive(Debug, thiserror::Error)]
pub enum LocationIdI64ConvError {
    /// The provided value is zero or negative, but [`LocationId`] requires positive values.
    #[error("LocationId must be positive, got: {0}")]
    NonPositive(i64),
}

/// Errors that can occur when parsing a string as a [`LocationId`].
#[derive(Debug, thiserror::Error)]
pub enum LocationIdFromStrError {
    /// The string is not a valid `i64`.
    #[error("Invalid number format: {0}")]
    ParseError(#[from] std::num::ParseIntError),
    /// The parsed value is zero or negative, but [`LocationId`] requires positive values.
    #[error(transparent)]
    NonPositive(#[from] LocationIdI64ConvError),
}
