//! Worker build metadata information
//!
//! This module defines the `BuildInfo` struct which contains metadata about a worker node,
//! including build information (version, commit SHA, timestamps) that is stored in the
//! metadata database.
//!
//! ## Backwards Compatibility
//!
//! The struct is designed for backwards compatibility with older workers and database entries.
//! All fields are optional and default to `None` when missing or null during deserialization,
//! allowing older workers or database entries to be deserialized successfully.
//!
//! During serialization, `None` values are omitted from the output to reduce payload size
//! and avoid storing redundant default values.

/// Worker build metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BuildInfo {
    /// Git describe output (e.g. `v0.0.22-15-g8b065bde`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// Full commit SHA hash (e.g. `8b065bde1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,

    /// Commit timestamp in ISO 8601 format (e.g. `2025-10-30T11:14:07Z`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_timestamp: Option<String>,

    /// Build date (e.g. `2025-10-30`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_date: Option<String>,
}

impl From<BuildInfo> for metadata_db::WorkerInfoOwned {
    fn from(value: BuildInfo) -> Self {
        let raw = serde_json::value::to_raw_value(&value)
            .expect("BuildInfo should serialize to RawValue");
        // SAFETY: to_raw_value produces valid JSON, ensuring RawValue invariants are upheld.
        metadata_db::WorkerInfo::from_owned_unchecked(raw)
    }
}

impl<'a> From<&'a BuildInfo> for metadata_db::WorkerInfo<'a> {
    fn from(value: &'a BuildInfo) -> Self {
        let raw =
            serde_json::value::to_raw_value(value).expect("BuildInfo should serialize to RawValue");
        // SAFETY: to_raw_value produces valid JSON, ensuring RawValue invariants are upheld.
        metadata_db::WorkerInfo::from_owned_unchecked(raw)
    }
}

impl TryFrom<metadata_db::WorkerInfoOwned> for BuildInfo {
    type Error = BuildInfoDeserializeError;

    fn try_from(value: metadata_db::WorkerInfoOwned) -> Result<Self, Self::Error> {
        serde_json::from_str(value.as_str()).map_err(BuildInfoDeserializeError)
    }
}

/// Error type for BuildInfo deserialization failures
#[derive(Debug, thiserror::Error)]
#[error("failed to deserialize BuildInfo from database")]
pub struct BuildInfoDeserializeError(#[source] serde_json::Error);
