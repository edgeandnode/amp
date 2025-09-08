//! Common utilities for HTTP handlers

use common::BoxResult;

/// Validates that a dataset name follows the required format:
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
pub fn validate_dataset_name(name: &str) -> BoxResult<()> {
    if name.is_empty() {
        return Err("name must not be empty".into());
    }

    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(format!("invalid character '{c}' in name '{name}'").into());
    }

    Ok(())
}

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
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        &self.0
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

#[cfg(test)]
mod tests {
    use super::validate_dataset_name;

    #[test]
    fn accept_valid_dataset_names() {
        assert!(validate_dataset_name("my_dataset").is_ok());
        assert!(validate_dataset_name("my_dataset_123").is_ok());
        assert!(validate_dataset_name("__my_dataset_123").is_ok());
    }

    #[test]
    fn reject_invalid_dataset_names() {
        // Empty name is not allowed
        assert!(
            validate_dataset_name("").is_err(),
            "empty name is not allowed"
        );

        // Hyphens are not allowed
        assert!(
            validate_dataset_name("my-dataset").is_err(),
            "hyphens are not allowed"
        );
        assert!(
            validate_dataset_name("my_dataset-123").is_err(),
            "hyphens are not allowed"
        );

        // Spaces are not allowed
        assert!(
            validate_dataset_name("my dataset").is_err(),
            "spaces are not allowed"
        );
        assert!(
            validate_dataset_name("my dataset_123").is_err(),
            "spaces are not allowed"
        );

        // Uppercase letters are not allowed
        assert!(
            validate_dataset_name("MyDataset").is_err(),
            "uppercase letters are not allowed"
        );
        assert!(
            validate_dataset_name("MY_DATASET").is_err(),
            "uppercase letters are not allowed"
        );

        // Special characters are not allowed
        assert!(
            validate_dataset_name("my@dataset").is_err(),
            "special character '@' is not allowed"
        );
        assert!(
            validate_dataset_name("my.dataset").is_err(),
            "special character '.' is not allowed"
        );
        assert!(
            validate_dataset_name("my#dataset").is_err(),
            "special character '#' is not allowed"
        );
        assert!(
            validate_dataset_name("my$dataset").is_err(),
            "special character '$' is not allowed"
        );
        assert!(
            validate_dataset_name("my&dataset").is_err(),
            "special character '&' is not allowed"
        );
        assert!(
            validate_dataset_name("my*dataset").is_err(),
            "special character '*' is not allowed"
        );
        assert!(
            validate_dataset_name("my/dataset").is_err(),
            "special character '/' is not allowed"
        );
        assert!(
            validate_dataset_name("my\\dataset").is_err(),
            "special character '\\' is not allowed"
        );
    }
}
