//! Location information types for API responses

use metadata_db::{JobId, LocationId};

/// Location information returned by the API
///
/// This struct represents location metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about where dataset table data is stored.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct LocationInfo {
    /// Unique identifier for this location (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: LocationId,
    /// Name of the dataset this location belongs to
    pub dataset: String,
    /// Version of the dataset using semantic versioning (e.g., "1.0.0", or empty string for unversioned)
    pub dataset_version: String,
    /// Name of the table within the dataset (e.g., "blocks", "transactions")
    pub table: String,
    /// Full URL to the storage location (e.g., "s3://bucket/path/table.parquet", "file:///local/path/table.parquet")
    pub url: String,
    /// Whether this location is currently active for queries
    pub active: bool,
    /// Writer job ID (64-bit integer, if one exists)
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
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
            id: value.id,
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
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(
    feature = "utoipa",
    schema(description = "Location information with writer job details")
)]
pub struct LocationInfoWithDetails {
    /// Unique identifier for this location (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: LocationId,
    /// Name of the dataset this location belongs to
    pub dataset: String,
    /// Version of the dataset using semantic versioning (e.g., "1.0.0", or empty string for unversioned)
    pub dataset_version: String,
    /// Name of the table within the dataset (e.g., "blocks", "transactions")
    pub table: String,
    /// Full URL to the storage location (e.g., "s3://bucket/path/table.parquet", "file:///local/path/table.parquet")
    pub url: String,
    /// Whether this location is currently active for queries
    pub active: bool,
    /// Writer job information with full details (if one exists)
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
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobInfo {
    /// Unique identifier for this job (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: JobId,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job (Scheduled, Running, Completed, Stopped, Failed, etc.)
    pub status: String,
    /// Job descriptor containing job-specific parameters as JSON
    pub descriptor: serde_json::Value,
}

impl From<metadata_db::Job> for JobInfo {
    fn from(value: metadata_db::Job) -> Self {
        Self {
            id: value.id,
            node_id: value.node_id.to_string(),
            status: value.status.to_string(),
            descriptor: value.desc,
        }
    }
}
