//! Version hash types for dataset definitions.
//!
//! This module provides the `VersionHash` type for representing 32-byte version hashes
//! used to identify specific versions of datasets.

use sha2::{Digest as _, Sha256};

/// A 32-byte hash representing a dataset version.
///
/// `VersionHash` stores a hex-encoded SHA-256 hash string providing a semantic
/// type for version identification hashes. It is used to uniquely identify
/// dataset versions through content-addressable hashing.
///
/// ## Format Requirements
///
/// A valid version hash must:
/// - **Have exactly 64 characters** (64 hex digits)
/// - **Contain only valid hex digits** (`0-9`, `a-f`, `A-F`)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct VersionHash(
    #[cfg_attr(feature = "schemars", schemars(regex(pattern = r"^[0-9a-fA-F]{64}$")))]
    #[cfg_attr(feature = "schemars", schemars(length(min = 64, max = 64)))]
    String,
);

impl VersionHash {
    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the VersionHash and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for VersionHash {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<VersionHash> for String {
    fn eq(&self, other: &VersionHash) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for VersionHash {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<VersionHash> for str {
    fn eq(&self, other: &VersionHash) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for VersionHash {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<VersionHash> for &str {
    fn eq(&self, other: &VersionHash) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for VersionHash {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for VersionHash {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for VersionHash {
    type Error = VersionHashError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_version_hash(&value)?;
        Ok(VersionHash(value))
    }
}

impl std::fmt::Display for VersionHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for VersionHash {
    type Err = VersionHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_version_hash(s)?;
        Ok(VersionHash(s.to_string()))
    }
}

impl serde::Serialize for VersionHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for VersionHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

impl From<metadata_db::DatasetVersionHashOwned> for VersionHash {
    fn from(value: metadata_db::DatasetVersionHashOwned) -> Self {
        // Convert to string - Database values are trusted to uphold invariants
        VersionHash(value.into_inner())
    }
}

impl From<VersionHash> for metadata_db::DatasetVersionHashOwned {
    fn from(value: VersionHash) -> Self {
        // SAFETY: VersionHash is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetVersionHash::from_owned_unchecked(value.0)
    }
}

impl<'a> From<&'a VersionHash> for metadata_db::DatasetVersionHash<'a> {
    fn from(value: &'a VersionHash) -> Self {
        // SAFETY: VersionHash is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::DatasetVersionHash::from_ref_unchecked(&value.0)
    }
}

/// Validates that a version hash follows the required format:
/// - Must be exactly 64 characters long
/// - Must contain only valid hex digits
pub fn validate_version_hash(hash: &str) -> Result<(), VersionHashError> {
    // Check exact length
    if hash.len() != 64 {
        return Err(VersionHashError::InvalidLength {
            expected: 64,
            actual: hash.len(),
        });
    }

    // Check all characters are valid hex
    for (idx, ch) in hash.chars().enumerate() {
        if !ch.is_ascii_hexdigit() {
            return Err(VersionHashError::InvalidHexCharacter {
                character: ch,
                index: idx,
            });
        }
    }

    Ok(())
}

/// Error type for [`VersionHash`] validation failures.
#[derive(Debug, thiserror::Error)]
pub enum VersionHashError {
    /// Version hash has incorrect length
    #[error("invalid version hash length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    /// Version hash contains invalid hex character
    #[error("invalid hex character '{character}' at index {index}")]
    InvalidHexCharacter { character: char, index: usize },
}

/// Computes an SHA-256 hash of the provided data and returns it as a [`VersionHash`].
///
/// Uses the SHA-256 cryptographic hash function from the RustCrypto `sha2` crate.
/// SHA-256 produces a deterministic 32-byte output that is collision-resistant and
/// suitable for content addressing.
pub fn hash<T: AsRef<[u8]>>(data: T) -> VersionHash {
    let result = Sha256::digest(data.as_ref());
    let bytes: [u8; 32] = result.into();

    // SAFETY: We know this is a valid version hash as we just created it from SHA-256 output
    VersionHash(hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_valid_hash_succeeds() {
        //* Given
        let hash_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        //* When
        let result: Result<VersionHash, _> = hash_str.parse();

        //* Then
        assert!(result.is_ok(), "parsing valid hash string should succeed");
        let hash = result.expect("should return valid VersionHash");
        assert_eq!(hash.as_str(), hash_str);
    }

    #[test]
    fn from_str_with_invalid_hash_fails() {
        //* Given
        let invalid_hash = "not a valid hash";

        //* When
        let result: Result<VersionHash, _> = invalid_hash.parse();

        //* Then
        assert!(result.is_err(), "parsing invalid hash should fail");
    }

    #[test]
    fn hash_with_known_input_produces_expected_output() {
        //* Given
        let data = b"hello world";
        let expected_hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        //* When
        let result = hash(data);

        //* Then
        assert_eq!(
            result.as_str(),
            expected_hash,
            "SHA-256 hash should match expected value"
        );
    }

    #[test]
    fn serialize_and_deserialize_roundtrip_succeeds() {
        //* Given
        let hash_str = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let original_hash: VersionHash = hash_str.parse().expect("should parse valid hash");

        //* When
        let json = serde_json::to_string(&original_hash)
            .expect("serialization should succeed with valid VersionHash");
        let deserialized: VersionHash =
            serde_json::from_str(&json).expect("deserialization should succeed with valid JSON");

        //* Then
        assert_eq!(deserialized, original_hash);
    }
}
