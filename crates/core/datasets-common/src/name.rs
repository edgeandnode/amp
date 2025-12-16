//! Dataset name validation and types.
//!
//! This module provides the `Name` type for validated dataset names that enforce
//! naming conventions and constraints required across the system.

/// A validated dataset name that enforces naming conventions and constraints.
///
/// Dataset names must follow strict rules to ensure compatibility across systems,
/// file systems, and network protocols. This type provides compile-time guarantees
/// that all instances contain valid dataset names.
///
/// ## Format Requirements
///
/// A valid dataset name must:
/// - **Start** with a lowercase letter (`a-z`) or underscore (`_`)
/// - **Contain** only lowercase letters (`a-z`), digits (`0-9`), and underscores (`_`)
/// - **Not be empty** (minimum length of 1 character)
/// - **Have no spaces** or special characters (except underscore)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct Name(
    #[cfg_attr(feature = "schemars", schemars(regex(pattern = r"^[a-z_][a-z0-9_]*$")))]
    #[cfg_attr(feature = "schemars", schemars(length(min = 1)))]
    String,
);

impl Name {
    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the DatasetName and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for Name {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Name> for String {
    fn eq(&self, other: &Name) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for Name {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<Name> for str {
    fn eq(&self, other: &Name) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for Name {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<Name> for &str {
    fn eq(&self, other: &Name) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<Name> for Name {
    #[inline(always)]
    fn as_ref(&self) -> &Name {
        self
    }
}

impl std::ops::Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for Name {
    type Error = NameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_dataset_name(&value)?;
        Ok(Name(value))
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for Name {
    type Err = NameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_dataset_name(s)?;
        Ok(Name(s.to_string()))
    }
}

impl serde::Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "metadata-db")]
impl From<metadata_db::DatasetNameOwned> for Name {
    fn from(value: metadata_db::DatasetNameOwned) -> Self {
        // Convert to string and validate - this should always pass since DatasetName
        // comes from the database and should already be valid
        Name(value.into_inner())
    }
}

#[cfg(feature = "metadata-db")]
impl From<Name> for metadata_db::DatasetNameOwned {
    fn from(value: Name) -> Self {
        // SAFETY: Name is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetName::from_owned_unchecked(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a Name> for metadata_db::DatasetName<'a> {
    fn from(value: &'a Name) -> Self {
        // SAFETY: Name is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetName::from_ref_unchecked(&value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::DatasetName<'a>> for Name {
    fn eq(&self, other: &metadata_db::DatasetName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<Name> for metadata_db::DatasetName<'a> {
    fn eq(&self, other: &Name) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&metadata_db::DatasetName<'a>> for Name {
    fn eq(&self, other: &&metadata_db::DatasetName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<Name> for &metadata_db::DatasetName<'a> {
    fn eq(&self, other: &Name) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::DatasetName<'a>> for &Name {
    fn eq(&self, other: &metadata_db::DatasetName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&Name> for metadata_db::DatasetName<'a> {
    fn eq(&self, other: &&Name) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Validates that a dataset name follows the required format:
/// - Must start with a lowercase letter or underscore
/// - Must be lowercase
/// - Can only contain letters, underscores, and numbers
pub fn validate_dataset_name(name: &str) -> Result<(), NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }

    // Check first character: must be lowercase letter or underscore
    if let Some(first_char) = name.chars().next()
        && !(first_char.is_ascii_lowercase() || first_char == '_')
    {
        return Err(NameError::InvalidCharacter {
            character: first_char,
            value: name.to_string(),
        });
    }

    // Check remaining characters: must be lowercase letter, underscore, or digit
    if let Some(c) = name
        .chars()
        .find(|&c| !(c.is_ascii_lowercase() || c == '_' || c.is_numeric()))
    {
        return Err(NameError::InvalidCharacter {
            character: c,
            value: name.to_string(),
        });
    }

    Ok(())
}

/// Error type for [`Name`] parsing failures
#[derive(Debug, thiserror::Error)]
pub enum NameError {
    /// Dataset name is empty
    #[error("name cannot be empty")]
    Empty,
    /// Dataset name contains invalid character
    #[error("invalid character '{character}' in name '{value}'")]
    InvalidCharacter { character: char, value: String },
}

#[cfg(test)]
mod tests {
    use super::{NameError, validate_dataset_name};

    #[test]
    fn accept_valid_dataset_names() {
        assert!(validate_dataset_name("my_dataset").is_ok());
        assert!(validate_dataset_name("my_dataset_123").is_ok());
        assert!(validate_dataset_name("__my_dataset_123").is_ok());
    }

    #[test]
    fn reject_empty_dataset_name() {
        let result = validate_dataset_name("");
        assert!(matches!(result, Err(NameError::Empty)));
    }

    #[test]
    fn reject_invalid_characters() {
        // Hyphens are not allowed
        let result = validate_dataset_name("my-dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '-', .. })
        ));

        // Spaces are not allowed
        let result = validate_dataset_name("my dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: ' ', .. })
        ));

        // Uppercase letters are not allowed
        let result = validate_dataset_name("MyDataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: 'M', .. })
        ));

        // Special characters are not allowed
        let result = validate_dataset_name("my@dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '@', .. })
        ));

        let result = validate_dataset_name("my.dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '.', .. })
        ));

        let result = validate_dataset_name("my#dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '#', .. })
        ));

        // Numbers at the beginning are not allowed
        let result = validate_dataset_name("123dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '1', .. })
        ));

        let result = validate_dataset_name("0_my_dataset");
        assert!(matches!(
            result,
            Err(NameError::InvalidCharacter { character: '0', .. })
        ));
    }
}
