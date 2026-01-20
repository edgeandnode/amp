//! Common utilities for HTTP handlers

use common::catalog::dataset_access::DatasetAccess;
use datasets_derived::Manifest as DerivedDatasetManifest;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// Parse, validate, and re-serialize a derived dataset manifest to canonical JSON format
///
/// This function handles derived datasets which require comprehensive validation:
/// 1. Deserialize from JSON string
/// 2. Validate manifest using dataset store (SQL, dependencies, tables, functions)
/// 3. Re-serialize to canonical JSON
pub async fn parse_and_canonicalize_derived_dataset_manifest(
    manifest_str: impl AsRef<str>,
    store: &impl DatasetAccess,
) -> Result<String, ParseDerivedManifestError> {
    let manifest: DerivedDatasetManifest = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseDerivedManifestError::Deserialization)?;

    datasets_derived::validate(&manifest, store)
        .await
        .map_err(ParseDerivedManifestError::ManifestValidation)?;

    serde_json::to_string(&manifest).map_err(ParseDerivedManifestError::Serialization)
}

/// Error type for derived dataset manifest parsing and validation
#[derive(Debug, thiserror::Error)]
pub enum ParseDerivedManifestError {
    /// Failed to deserialize the JSON string into a `DerivedDatasetManifest` struct
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed manifest validation after successful deserialization
    #[error("manifest validation failed: {0}")]
    ManifestValidation(#[from] datasets_derived::ManifestValidationError),

    /// Failed to serialize the validated manifest back to canonical JSON
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}

/// Parse and re-serialize a raw dataset manifest to canonical JSON format
///
/// This function handles the common pattern for raw datasets (EvmRpc, Firehose, Solana):
/// 1. Deserialize from JSON string
/// 2. Re-serialize to canonical JSON
///
/// Returns canonical JSON string on success
pub fn parse_and_canonicalize_raw_dataset_manifest<T>(
    manifest_str: impl AsRef<str>,
) -> Result<String, ParseRawManifestError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let manifest: T = serde_json::from_str(manifest_str.as_ref())
        .map_err(ParseRawManifestError::Deserialization)?;
    serde_json::to_string(&manifest).map_err(ParseRawManifestError::Serialization)
}

/// Error type for raw dataset manifest parsing and canonicalization
///
/// Represents the different failure points when processing raw dataset manifests
/// (EvmRpc, Firehose, Solana) through the parse → canonicalize pipeline.
#[derive(Debug, thiserror::Error)]
pub enum ParseRawManifestError {
    /// Failed to deserialize the JSON string into the manifest struct
    ///
    /// This occurs when:
    /// - JSON syntax is invalid
    /// - JSON structure doesn't match the manifest schema
    /// - Required fields are missing or have wrong types
    #[error("failed to deserialize manifest: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Failed to serialize the manifest back to canonical JSON
    ///
    /// This occurs when:
    /// - The manifest structure cannot be serialized (rare, indicates a bug)
    /// - Memory allocation fails during serialization
    ///
    /// Note: This should rarely happen since we already deserialized successfully
    #[error("failed to serialize manifest: {0}")]
    Serialization(#[source] serde_json::Error),
}
