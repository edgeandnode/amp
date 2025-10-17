//! Common utilities for HTTP handlers

/// A string wrapper that ensures the value is not empty or whitespace-only
///
/// This invariant-holding _new-type_ validates that strings contain at least one non-whitespace character.
/// Validation occurs during:
/// - JSON/serde deserialization
/// - Parsing from `&str` via `FromStr`
///
/// ## Behavior
/// - Input strings are validated by checking if they contain non-whitespace characters after trimming
/// - Empty strings or whitespace-only strings are rejected with [`EmptyStringError`]
/// - The **original string is preserved** including any leading/trailing whitespace
/// - Once created, the string is guaranteed to contain at least one non-whitespace character
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(value_type = String))]
pub struct NonEmptyString(String);

impl NonEmptyString {
    /// Creates a new NonEmptyString without validation
    ///
    /// ## Safety
    /// The caller must ensure that the string contains at least one non-whitespace character.
    /// Passing an empty string or whitespace-only string violates the type's invariant and
    /// may lead to undefined behavior in code that relies on this guarantee.
    pub unsafe fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the NonEmptyString and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for NonEmptyString {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::ops::Deref for NonEmptyString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NonEmptyString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::str::FromStr for NonEmptyString {
    type Err = EmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(EmptyStringError);
        }
        Ok(NonEmptyString(s.to_string()))
    }
}

impl<'de> serde::Deserialize<'de> for NonEmptyString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for NonEmptyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Error type for NonEmptyString parsing failures
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("string cannot be empty or whitespace-only")]
pub struct EmptyStringError;
