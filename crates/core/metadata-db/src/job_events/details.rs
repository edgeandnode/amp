//! Event detail new-type wrapper for database values
//!
//! This module provides an [`EventDetail`] new-type wrapper around [`Cow<RawValue>`] that
//! represents structured error details for job events and status as validated JSON.

use std::borrow::Cow;

use serde_json::value::RawValue;

/// An owned event detail type for database return values and owned storage scenarios.
///
/// This is a type alias for `EventDetail<'static>`, specifically intended for use as a return
/// type from database queries or in any context where an event detail with owned storage is
/// required.
pub type EventDetailOwned = EventDetail<'static>;

/// Event detail wrapper for database values.
///
/// This new-type wrapper around `Cow<RawValue>` represents structured error details
/// as validated JSON for database storage. It supports both borrowed and owned RawValue
/// through copy-on-write semantics, enabling efficient handling without unnecessary
/// allocations.
///
/// The wrapped `RawValue` guarantees valid JSON structure at the type level.
#[derive(Clone, Debug)]
pub struct EventDetail<'a>(Cow<'a, RawValue>);

impl<'a> EventDetail<'a> {
    /// Create a new EventDetail from a RawValue reference (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the RawValue is valid. No validation is performed.
    pub const fn from_ref_unchecked(raw: &'a RawValue) -> Self {
        Self(Cow::Borrowed(raw))
    }

    /// Create a new EventDetail from an owned RawValue
    ///
    /// # Safety
    /// The caller must ensure the RawValue is valid. No validation is performed.
    pub const fn from_owned_unchecked(raw: Box<RawValue>) -> EventDetail<'static> {
        EventDetail(Cow::Owned(raw))
    }

    /// Create an EventDetail from a `serde_json::Value`.
    ///
    /// # Panics
    ///
    /// Panics if `serde_json::Value` fails to serialize to JSON. This cannot happen
    /// in practice because `Value` is always valid JSON by construction.
    pub fn from_value(value: &serde_json::Value) -> EventDetail<'static> {
        // SAFETY: `serde_json::Value` is always valid JSON — `to_raw_value` cannot fail.
        let raw = serde_json::value::to_raw_value(value)
            .expect("serde_json::Value should always serialize to valid JSON");
        EventDetail::from_owned_unchecked(raw)
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

impl EventDetail<'static> {
    /// Consume and return the inner `Box<RawValue>`.
    ///
    /// Only available on owned (`'static`) descriptors where the `Cow` is guaranteed `Owned`.
    ///
    /// # Panics
    ///
    /// Panics if called on a borrowed variant, which cannot happen because the
    /// `'static` lifetime bound makes `Cow::Borrowed` unrepresentable.
    pub fn into_inner(self) -> Box<RawValue> {
        match self.0 {
            Cow::Owned(boxed) => boxed,
            // SAFETY: `'static` lifetime guarantees `Cow::Owned` — no borrowed reference
            // can satisfy `'static` for `RawValue`.
            Cow::Borrowed(_) => unreachable!("'static lifetime guarantees Cow::Owned"),
        }
    }
}

impl<'a> AsRef<RawValue> for EventDetail<'a> {
    fn as_ref(&self) -> &RawValue {
        &self.0
    }
}

impl sqlx::Type<sqlx::Postgres> for EventDetail<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <RawValue as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for EventDetail<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        use sqlx::types::Json;
        <Json<&RawValue> as sqlx::Encode<sqlx::Postgres>>::encode(Json(self.as_raw()), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for EventDetail<'static> {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        use sqlx::types::Json;
        let Json(raw): Json<&RawValue> =
            <Json<&RawValue> as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(EventDetail::from_owned_unchecked(raw.to_owned()))
    }
}

impl<'a> From<&'a EventDetail<'a>> for EventDetail<'a> {
    fn from(value: &'a EventDetail<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input EventDetail.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input EventDetail already upholds invariants, so the referenced data is valid.
        EventDetail::from_ref_unchecked(value.as_ref())
    }
}
