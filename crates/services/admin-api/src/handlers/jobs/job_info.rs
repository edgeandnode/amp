//! Job information types for API responses

use metadata_db::JobId;

/// Represents job information for the API response
///
/// This struct represents job metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about scheduled jobs and their current state.
#[derive(Debug, serde::Serialize)]
pub struct JobInfo {
    /// Unique identifier for the job
    pub id: JobId,
    /// Job creation timestamp (ISO 8601 format)
    pub created_at: String,
    /// Job last update timestamp (ISO 8601 format)
    pub updated_at: String,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job
    pub status: String,
    /// Job descriptor (contains job-specific parameters)
    pub descriptor: serde_json::Value,
}

impl From<metadata_db::JobWithDetails> for JobInfo {
    fn from(value: metadata_db::JobWithDetails) -> Self {
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
