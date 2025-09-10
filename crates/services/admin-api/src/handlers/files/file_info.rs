//! File information types for API responses

use metadata_db::{FileId, LocationId};

/// File information returned by the API
///
/// This struct represents file metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about Parquet files and their associated metadata within locations.
#[derive(Debug, serde::Serialize)]
pub struct FileInfo {
    /// Unique identifier for this file
    pub id: FileId,
    /// Location ID this file belongs to
    pub location_id: LocationId,
    /// Name of the file (e.g., "blocks_0000000000_0000099999.parquet")
    pub file_name: String,
    /// Base location URL (e.g., "s3://bucket/path/") - combine with file_name for full file URL
    pub url: String,
    /// Size of the file object in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size: Option<i64>,
    /// ETag of the file object (for caching and versioning)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_e_tag: Option<String>,
    /// Version identifier of the file object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_version: Option<String>,
    /// Parquet file metadata as JSON
    pub metadata: serde_json::Value,
}

impl From<metadata_db::FileMetadataWithDetails> for FileInfo {
    /// Converts a database `FileMetadataWithDetails` record into an API-friendly `FileInfo`
    ///
    /// This conversion handles:
    /// - Converting the base location URL to `String` for JSON serialization
    /// - Preserving all other fields as-is from the database record
    fn from(value: metadata_db::FileMetadataWithDetails) -> Self {
        Self {
            id: value.id,
            location_id: value.location_id,
            file_name: value.file_name,
            url: value.url.to_string(),
            object_size: value.object_size,
            object_e_tag: value.object_e_tag,
            object_version: value.object_version,
            metadata: value.metadata,
        }
    }
}
