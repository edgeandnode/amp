//! Job ID new-type with validation for job record identifiers.
//!
//! This module provides a type-safe wrapper around job IDs with built-in
//! validation to ensure IDs are always positive and within valid ranges.

/// A type-safe identifier for job records.
///
/// [`JobId`] is a new-type wrapper around `i64` that enforces the following invariants:
/// - Values must be positive (> 0)
/// - Values must fit within the range of `i64`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(i64);

impl TryFrom<u64> for JobId {
    type Error = JobIdU64Error;

    /// Attempts to convert a `u64` to a [`JobId`] with validation.
    ///
    /// # Errors
    ///
    /// - `JobIdU64Error::Zero` if the value is zero
    /// - `JobIdU64Error::Overflow` if the value exceeds `i64::MAX`
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(JobIdU64Error::Zero)
        } else if let Ok(i) = i64::try_from(value) {
            Ok(Self(i))
        } else {
            Err(JobIdU64Error::Overflow(value))
        }
    }
}

impl TryFrom<i64> for JobId {
    type Error = JobIdI64ConvError;

    /// Attempts to convert an `i64` to a [`JobId`] with validation.
    ///
    /// # Errors
    ///
    /// - `JobIdI64ConvError::NonPositive` if the value is zero or negative
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            Err(JobIdI64ConvError::NonPositive(value))
        } else {
            Ok(Self(value))
        }
    }
}

impl From<JobId> for metadata_db::JobId {
    fn from(value: JobId) -> Self {
        // SAFETY: worker JobId is always valid (positive i64)
        // It is safe to convert without additional checks
        Self::from_i64(*value)
    }
}

impl From<&JobId> for metadata_db::JobId {
    fn from(value: &JobId) -> Self {
        // SAFETY: worker JobId is always valid (positive i64)
        // It is safe to convert without additional checks
        Self::from_i64(**value)
    }
}

impl From<metadata_db::JobId> for JobId {
    fn from(value: metadata_db::JobId) -> Self {
        // SAFETY: metadata_db JobId is always valid (positive i64)
        // It is safe to convert without additional checks
        Self(value.into_i64())
    }
}

impl std::ops::Deref for JobId {
    type Target = i64;

    /// Dereferences the [`JobId`] to its inner `i64` value.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for JobId {
    type Err = JobIdFromStrError;

    /// Parses a string as a [`JobId`].
    ///
    /// # Errors
    ///
    /// - `JobIdFromStrError::ParseError` if the string is not a valid `i64`
    /// - `JobIdFromStrError::NonPositive` if the parsed value is zero or negative
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let id = s.parse::<i64>().map_err(JobIdFromStrError::ParseError)?;
        id.try_into().map_err(JobIdFromStrError::NonPositive)
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
        id.try_into().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur when converting from `u64` to [`JobId`].
#[derive(Debug, thiserror::Error)]
pub enum JobIdU64Error {
    /// The provided value is zero, but [`JobId`] requires positive values.
    #[error("JobId must be positive, got zero")]
    Zero,
    /// The provided `u64` value exceeds [`i64::MAX`] and would cause an overflow.
    #[error("Value {0} exceeds i64::MAX and would overflow")]
    Overflow(u64),
}

/// Errors that can occur when converting from `i64` to [`JobId`].
#[derive(Debug, thiserror::Error)]
pub enum JobIdI64ConvError {
    /// The provided value is zero or negative, but [`JobId`] requires positive values.
    #[error("JobId must be positive, got: {0}")]
    NonPositive(i64),
}

/// Errors that can occur when parsing a string as a [`JobId`].
#[derive(Debug, thiserror::Error)]
pub enum JobIdFromStrError {
    /// The string is not a valid `i64`.
    #[error("Invalid number format: {0}")]
    ParseError(#[from] std::num::ParseIntError),
    /// The parsed value is zero or negative, but [`JobId`] requires positive values.
    #[error(transparent)]
    NonPositive(#[from] JobIdI64ConvError),
}
