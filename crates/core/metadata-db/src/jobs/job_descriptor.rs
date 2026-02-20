//! Job descriptor new-type wrapper for database values
//!
//! This module provides a [`JobDescriptorRaw`] new-type wrapper around [`Cow<RawValue>`] that
//! represents job descriptors for database operations as validated JSON.

use std::borrow::Cow;

use serde_json::value::RawValue;

/// An owned job descriptor type for database return values and owned storage scenarios.
///
/// This is a type alias for `JobDescriptorRaw<'static>`, specifically intended for use as a return
/// type from database queries or in any context where a job descriptor with owned storage is
/// required.
pub type JobDescriptorRawOwned = JobDescriptorRaw<'static>;

/// Job descriptor wrapper for database values.
///
/// This new-type wrapper around `Cow<RawValue>` represents job descriptors as validated JSON
/// for database storage. It supports both borrowed and owned RawValue through copy-on-write
/// semantics, enabling efficient handling without unnecessary allocations.
///
/// The wrapped `RawValue` guarantees valid JSON structure at the type level.
/// Memory footprint: borrowed form is a reference (~24 bytes), owned form is `Box<RawValue>`
/// which is equivalent to `Box<str>` in size.
#[derive(Clone, Debug)]
pub struct JobDescriptorRaw<'a>(Cow<'a, RawValue>);

impl<'a> JobDescriptorRaw<'a> {
    /// Create a new JobDescriptorRaw from a RawValue reference (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the RawValue is valid. No validation is performed.
    pub const fn from_ref_unchecked(raw: &'a RawValue) -> Self {
        Self(Cow::Borrowed(raw))
    }

    /// Create a new JobDescriptorRaw from an owned RawValue
    ///
    /// # Safety
    /// The caller must ensure the RawValue is valid. No validation is performed.
    pub const fn from_owned_unchecked(raw: Box<RawValue>) -> JobDescriptorRaw<'static> {
        JobDescriptorRaw(Cow::Owned(raw))
    }

    /// Get a reference to the JSON string
    pub fn as_str(&self) -> &str {
        self.0.get()
    }

    /// Get a reference to the underlying RawValue
    pub fn as_raw(&self) -> &RawValue {
        &self.0
    }
}

impl<'a> AsRef<RawValue> for JobDescriptorRaw<'a> {
    fn as_ref(&self) -> &RawValue {
        &self.0
    }
}

impl sqlx::Type<sqlx::Postgres> for JobDescriptorRaw<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <RawValue as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for JobDescriptorRaw<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        // Encode the RawValue as JSONB using sqlx::types::Json wrapper
        use sqlx::types::Json;
        <Json<&RawValue> as sqlx::Encode<sqlx::Postgres>>::encode(Json(self.as_raw()), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobDescriptorRaw<'static> {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        // Decode using sqlx::types::Json wrapper to get RawValue
        use sqlx::types::Json;
        let Json(raw): Json<&RawValue> =
            <Json<&RawValue> as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(JobDescriptorRaw::from_owned_unchecked(raw.to_owned()))
    }
}

impl<'a> From<&'a JobDescriptorRaw<'a>> for JobDescriptorRaw<'a> {
    fn from(value: &'a JobDescriptorRaw<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input JobDescriptorRaw.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input JobDescriptorRaw already upholds invariants, so the referenced data is valid.
        JobDescriptorRaw::from_ref_unchecked(value.as_ref())
    }
}

#[cfg(test)]
impl Default for JobDescriptorRaw<'static> {
    fn default() -> Self {
        // Create a minimal valid JSON object for testing
        let raw: Box<RawValue> =
            serde_json::from_str("{}").expect("Empty JSON object should be valid");
        JobDescriptorRaw::from_owned_unchecked(raw)
    }
}
