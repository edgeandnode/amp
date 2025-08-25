//! LocationUrl new-type for the metadata database
//!
//! This module provides a `LocationUrl` type that wraps `url::Url`.
//! It holds the URL validity invariant, with construction only through
//! safe conversions or unsafe methods.

use url::Url;

/// A validated URL wrapper that holds the URL validity invariant.
///
/// This type wraps `url::Url` and ensures the URL is syntactically valid.
/// Object store compatibility validation is left to the caller.
#[derive(Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct LocationUrl(Url);

impl LocationUrl {
    /// Creates a new [`LocationUrl`] without validation.
    ///
    /// # Safety
    /// The caller must ensure that the URL is syntactically valid.
    pub unsafe fn new_unchecked(url: Url) -> Self {
        Self(url)
    }

    /// Returns the inner URL.
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    /// Consumes self and returns the inner URL.
    pub fn into_url(self) -> Url {
        self.0
    }
}

impl std::ops::Deref for LocationUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for LocationUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for LocationUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<&LocationUrl> for String {
    fn from(value: &LocationUrl) -> Self {
        value.0.to_string()
    }
}

impl AsRef<str> for LocationUrl {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl serde::Serialize for LocationUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_str().serialize(serializer)
    }
}

impl<DB> sqlx::Type<DB> for LocationUrl
where
    DB: sqlx::Database,
    str: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <str as sqlx::Type<DB>>::type_info()
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for LocationUrl
where
    DB: sqlx::Database,
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as sqlx::Decode<'r, DB>>::decode(value)?;
        let url = s.parse()?;
        Ok(Self(url))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for LocationUrl
where
    DB: sqlx::Database,
    for<'a> &'a str: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.0.as_str().encode_by_ref(buf)
    }
}
