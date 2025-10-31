//! Worker metadata information
//!
//! This module defines the `WorkerInfo` struct which contains metadata about a worker node,
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

/// Worker metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerInfo {
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

impl From<WorkerInfo> for metadata_db::WorkerInfoOwned {
    fn from(value: WorkerInfo) -> Self {
        let raw = serde_json::value::to_raw_value(&value)
            .expect("WorkerInfo should serialize to RawValue");
        // SAFETY: to_raw_value produces valid JSON, ensuring RawValue invariants are upheld.
        metadata_db::WorkerInfo::from_owned_unchecked(raw)
    }
}

impl<'a> From<&'a WorkerInfo> for metadata_db::WorkerInfo<'a> {
    fn from(value: &'a WorkerInfo) -> Self {
        let raw = serde_json::value::to_raw_value(value)
            .expect("WorkerInfo should serialize to RawValue");
        // SAFETY: to_raw_value produces valid JSON, ensuring RawValue invariants are upheld.
        metadata_db::WorkerInfo::from_owned_unchecked(raw)
    }
}

impl TryFrom<metadata_db::WorkerInfoOwned> for WorkerInfo {
    type Error = WorkerInfoDeserializeError;

    fn try_from(value: metadata_db::WorkerInfoOwned) -> Result<Self, Self::Error> {
        serde_json::from_str(value.as_str()).map_err(WorkerInfoDeserializeError)
    }
}

/// Error type for WorkerInfo deserialization failures
#[derive(Debug, thiserror::Error)]
#[error("failed to deserialize WorkerInfo from database")]
pub struct WorkerInfoDeserializeError(#[source] serde_json::Error);
