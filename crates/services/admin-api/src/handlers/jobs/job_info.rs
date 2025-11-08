//! Job information types for API responses

use worker::job::{Job, JobId};

/// Represents job information for the API response
///
/// This struct represents job metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about scheduled jobs and their current state.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "utoipa",
    schema(description = "Represents job information for the API response")
)]
pub struct JobInfo {
    /// Unique identifier for the job (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: JobId,
    /// Job creation timestamp in ISO 8601 / RFC 3339 format
    pub created_at: String,
    /// Job last update timestamp in ISO 8601 / RFC 3339 format
    pub updated_at: String,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job (Scheduled, Running, Completed, Stopped, Failed, etc.)
    pub status: String,
    /// Job descriptor containing job-specific parameters as JSON
    #[cfg_attr(feature = "utoipa", schema(value_type = serde_json::Value))]
    pub descriptor: serde_json::Value,
}

impl From<Job> for JobInfo {
    fn from(value: Job) -> Self {
        Self {
            id: value.id,
            created_at: value.created_at.to_rfc3339(),
            updated_at: value.updated_at.to_rfc3339(),
            node_id: value.node_id.to_string(),
            status: value.status.to_string(),
            descriptor: value.desc,
        }
    }
}
