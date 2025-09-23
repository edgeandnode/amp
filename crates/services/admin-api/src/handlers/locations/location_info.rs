//! Location information types for API responses

use metadata_db::{JobId, LocationId};

/// Location information returned by the API
///
/// This struct represents location metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about where dataset table data is stored.
#[derive(Debug, serde::Serialize)]
pub struct LocationInfo {
    /// Unique identifier for this location
    pub id: i64,
    /// Name of the dataset this location belongs to
    pub dataset: String,
    /// Version of the dataset (e.g., "v1.0", or empty string for unversioned)
    pub dataset_version: String,
    /// Name of the table within the dataset (e.g., "blocks", "transactions")
    pub table: String,
    /// Full URL to the storage location (e.g., "s3://bucket/path/file.parquet")
    pub url: String,
    /// Whether this location is currently active for queries
    pub active: bool,
    /// Writer job ID (if one exists)
    pub writer: Option<JobId>,
}

impl From<metadata_db::Location> for LocationInfo {
    /// Converts a database `Location` record into an API-friendly `LocationInfo`
    ///
    /// This conversion handles:
    /// - Converting the URL from `url::Url` to `String` for JSON serialization
    /// - Preserving all other fields as-is
    fn from(value: metadata_db::Location) -> Self {
        Self {
            id: *value.id,
            dataset: value.dataset,
            dataset_version: value.dataset_version,
            table: value.table,
            url: value.url.to_string(),
            active: value.active,
            writer: value.writer,
        }
    }
}

/// Location information with writer job details returned by the API
///
/// This struct represents detailed location metadata from the database in a format
/// suitable for API responses. It includes all the essential information about where
/// dataset table data is stored, plus details about any writer job assigned to it.
#[derive(Debug, serde::Serialize)]
pub struct LocationInfoWithDetails {
    /// Unique identifier for this location
    pub id: LocationId,
    /// Name of the dataset this location belongs to
    pub dataset: String,
    /// Version of the dataset (e.g., "v1.0", or empty string for unversioned)
    pub dataset_version: String,
    /// Name of the table within the dataset (e.g., "blocks", "transactions")
    pub table: String,
    /// Full URL to the storage location (e.g., "s3://bucket/path/file.parquet")
    pub url: String,
    /// Whether this location is currently active for queries
    pub active: bool,
    /// Writer job information (if one exists)
    pub writer: Option<JobInfo>,
}

impl From<metadata_db::LocationWithDetails> for LocationInfoWithDetails {
    fn from(value: metadata_db::LocationWithDetails) -> Self {
        Self {
            id: value.id,
            dataset: value.dataset,
            dataset_version: value.dataset_version,
            table: value.table,
            url: value.url.to_string(),
            active: value.active,
            writer: value.writer.map(Into::into),
        }
    }
}

/// Job information returned by the API
///
/// This struct represents job metadata in a format suitable for API responses.
/// It contains essential information about a job without exposing internal
/// database implementation details.
#[derive(Debug, serde::Serialize)]
pub struct JobInfo {
    /// Unique identifier for this job
    pub id: JobId,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job
    pub status: String,
    /// Job descriptor as JSON value
    pub descriptor: serde_json::Value,
}

impl From<metadata_db::Job> for JobInfo {
    fn from(value: metadata_db::Job) -> Self {
        Self {
            id: value.id.into(),
            node_id: value.node_id.to_string(),
            status: value.status.to_string(),
            descriptor: value.desc,
        }
    }
}
