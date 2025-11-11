//! File ID new-type with validation for file metadata record identifiers.
//!
//! This module provides a type-safe wrapper around database file IDs with built-in
//! validation to ensure IDs are always positive and within valid ranges.

use sqlx::{Database, Postgres, encode::IsNull, error::BoxDynError};

/// A type-safe identifier for file metadata records tracking Parquet files and their metadata.
///
/// [`FileId`] is a new-type wrapper around `i64` that enforces the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
///
/// This type provides validation when converting from raw integers and integrates
/// seamlessly with PostgreSQL through sqlx trait implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId(i64);

impl TryFrom<u64> for FileId {
    type Error = FileIdU64Error;

    /// Attempts to convert an `u64` to a [`FileId`] with validation.
    ///
    /// # Errors
    ///
    /// - `FileIdU64Error::Zero` if the value is zero
    /// - `FileIdU64Error::Overflow` if the value exceeds `i64::MAX`
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(FileIdU64Error::Zero)
        } else if value > i64::MAX as u64 {
            Err(FileIdU64Error::Overflow(value))
        } else {
            Ok(Self(value as i64))
        }
    }
}

impl TryFrom<i64> for FileId {
    type Error = FileIdI64ConvError;

    /// Attempts to convert an `i64` to a [`FileId`] with validation.
    ///
    /// # Errors
    ///
    /// - `FileIdI64ConvError::NonPositive` if the value is zero or negative
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            Err(FileIdI64ConvError::NonPositive(value))
        } else {
            Ok(Self(value))
        }
    }
}

impl std::ops::Deref for FileId {
    type Target = i64;

    /// Dereferences the [`FileId`] to its inner `i64` value.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for FileId {
    type Err = FileIdFromStrError;

    /// Parses a string as a [`FileId`].
    ///
    /// # Errors
    ///
    /// - `FileIdFromStrError::ParseError` if the string is not a valid `i64`
    /// - `FileIdFromStrError::NonPositive` if the parsed value is zero or negative
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.parse::<i64>().map_err(FileIdFromStrError::ParseError)?;
        id.try_into().map_err(FileIdFromStrError::NonPositive)
    }
}

impl std::fmt::Display for FileId {
    /// Formats the [`FileId`] for display, showing the inner `i64` value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl sqlx::Type<Postgres> for FileId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::postgres::PgHasArrayType for FileId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <i64 as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

// Implement sqlx traits for database operations
impl<'r> sqlx::Decode<'r, Postgres> for FileId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let id = <i64 as sqlx::Decode<Postgres>>::decode(value)?;
        id.try_into().map_err(|err| Box::new(err) as BoxDynError)
    }
}

impl<'q> sqlx::Encode<'q, Postgres> for FileId {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        <i64 as sqlx::Encode<'q, Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl serde::Serialize for FileId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FileId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = i64::deserialize(deserializer)?;
        id.try_into().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when converting from `u64` to [`FileId`].
#[derive(Debug, thiserror::Error)]
pub enum FileIdU64Error {
    /// The provided value is zero, but [`FileId`] requires positive values.
    #[error("FileId must be positive, got zero")]
    Zero,
    /// The provided `u64` value exceeds [`i64::MAX`] and would cause an overflow.
    #[error("Value {0} exceeds i64::MAX and would overflow")]
    Overflow(u64),
}

/// Errors that can occur when converting from `i64` to [`FileId`].
#[derive(Debug, thiserror::Error)]
pub enum FileIdI64ConvError {
    /// The provided value is zero or negative, but [`FileId`] requires positive values.
    #[error("FileId must be positive, got: {0}")]
    NonPositive(i64),
}

/// Errors that can occur when parsing a string as a [`FileId`].
#[derive(Debug, thiserror::Error)]
pub enum FileIdFromStrError {
    /// The string is not a valid `i64`.
    #[error("Invalid number format: {0}")]
    ParseError(#[source] std::num::ParseIntError),
    /// The parsed value is zero or negative, but [`FileId`] requires positive values.
    #[error(transparent)]
    NonPositive(FileIdI64ConvError),
}
