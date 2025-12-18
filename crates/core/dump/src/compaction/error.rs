use std::sync::Arc;

use common::{
    BoxError,
    parquet::{
        errors::ParquetError, file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use datafusion::error::DataFusionError;
use metadata_db::files::FileId;
use object_store::{Error as ObjectStoreError, path::Error as PathError};
use tokio::task::JoinError;

use crate::WriterProperties;

pub type CompactionResult<T> = Result<T, CompactorError>;
pub type CollectionResult<T> = Result<T, CollectorError>;

/// Errors that occur during Parquet file compaction operations
///
/// Compaction merges multiple small Parquet files into larger files to improve
/// query performance and reduce storage overhead. This error type covers all
/// phases of compaction: chain building, file reading, writing, and metadata updates.
#[derive(thiserror::Error, Debug)]
pub enum CompactorError {
    /// Failed to build canonical chain for compaction
    ///
    /// This occurs during the chain-building phase when determining which files
    /// should be compacted together. The canonical chain represents the sequence
    /// of Parquet files that will be merged based on block ranges and timestamps.
    ///
    /// Common causes:
    /// - Inconsistent block range metadata in files
    /// - Overlapping block ranges that cannot be merged
    /// - Files missing required metadata for chain construction
    /// - Logic errors in chain building algorithm
    ///
    /// This error prevents compaction from proceeding as there's no valid merge plan.
    #[error("failed to build canonical chain: {0}")]
    CanonicalChain(#[source] BoxError),

    /// Failed to create compaction writer
    ///
    /// This occurs when initializing a Parquet writer with the given properties
    /// fails. The writer is responsible for encoding and writing the merged data
    /// to the new compacted Parquet file.
    ///
    /// Common causes:
    /// - Invalid Parquet writer properties (compression, page size, etc.)
    /// - Memory allocation failures when creating writer buffers
    /// - Object store connection issues preventing file creation
    /// - Insufficient permissions to write to storage location
    /// - Storage quota exceeded
    ///
    /// The error includes the writer properties that caused the failure for debugging.
    #[error("failed to create writer (writer properties: {opts:?}): {err}")]
    CreateWriter {
        #[source]
        err: BoxError,
        opts: Box<ParquetWriterProperties>,
    },

    /// Failed to write data to Parquet file
    ///
    /// This occurs during the write phase when encoding and writing data to the
    /// compacted Parquet file fails. This happens after successfully reading source
    /// files but before finalizing the output file.
    ///
    /// Common causes:
    /// - Disk full or storage quota exceeded
    /// - Object store connection lost during write
    /// - Memory allocation failures during encoding
    /// - Data encoding errors (e.g., values exceeding type limits)
    /// - Writer buffer flush failures
    ///
    /// A write failure leaves the compaction incomplete. The partial output file
    /// should be cleaned up, and source files remain unchanged.
    #[error("failed to write to parquet file: {0}")]
    FileWrite(#[source] BoxError),

    /// Failed to read/stream data from Parquet files
    ///
    /// This occurs when reading and streaming data from input Parquet files during
    /// compaction. The compactor must read all source files to merge their contents.
    ///
    /// Common causes:
    /// - Corrupted Parquet files (invalid headers, metadata, or data pages)
    /// - Object store connection failures during read
    /// - Files deleted or moved during compaction
    /// - Schema incompatibilities between files
    /// - Decompression errors in compressed Parquet pages
    /// - Memory allocation failures when reading large row groups
    ///
    /// Read failures prevent compaction from completing. Source files should be
    /// investigated for corruption or consistency issues.
    #[error("failed to stream parquet file: {0}")]
    FileStream(#[source] DataFusionError),

    /// Task join error during compaction
    ///
    /// This occurs when a compaction task running in a separate thread or async task
    /// panics or is cancelled. Compaction operations are typically run in background
    /// tasks to avoid blocking the main thread.
    ///
    /// Common causes:
    /// - Panic in compaction code (indicates a bug)
    /// - Task cancelled due to shutdown signal
    /// - Task aborted due to timeout
    /// - Out of memory causing task termination
    ///
    /// This is typically a fatal error indicating either a bug or system resource issue.
    #[error("compaction task join error: {0}")]
    Join(#[source] JoinError),

    /// Failed to update GC manifest in metadata database
    ///
    /// This occurs when updating the garbage collection manifest with information
    /// about the newly created compacted file. The GC manifest tracks which files
    /// are eligible for cleanup.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Transaction conflicts with concurrent operations
    /// - Database constraint violations
    /// - Insufficient database permissions
    ///
    /// If this fails, the compacted file exists in storage but is not tracked in the
    /// GC manifest. Manual cleanup or retry may be needed.
    #[error("failed to update gc manifest for files {file_ids:?}: {err}")]
    ManifestUpdate {
        #[source]
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to delete records from GC manifest
    ///
    /// This occurs when removing old file records from the garbage collection manifest
    /// after successful compaction. Old files should be marked for deletion once the
    /// compacted file is ready.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Records already deleted by another process
    /// - Insufficient database permissions
    ///
    /// If this fails, old file records remain in the GC manifest even though they've
    /// been replaced by the compacted file. This may delay garbage collection.
    #[error("failed to delete from gc manifest for files {file_ids:?}: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to commit file metadata to database
    ///
    /// This occurs when recording the new compacted file's metadata (path, size,
    /// block ranges, etc.) in the metadata database fails.
    ///
    /// Common causes:
    /// - Database connection lost during commit
    /// - Transaction conflicts with concurrent operations
    /// - Database constraint violations (e.g., duplicate file IDs)
    /// - Insufficient database permissions
    /// - Database out of disk space
    ///
    /// If this fails, the compacted file exists in storage but is not registered
    /// in the metadata database. The file is orphaned and may need manual cleanup.
    #[error("failed to commit file metadata: {0}")]
    MetadataCommit(#[source] metadata_db::Error),
}

impl CompactorError {
    pub fn chain_error(err: BoxError) -> Self {
        Self::CanonicalChain(err)
    }

    pub fn metadata_commit_error(err: metadata_db::Error) -> Self {
        Self::MetadataCommit(err)
    }

    pub fn manifest_update_error(file_ids: &[FileId]) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| Self::ManifestUpdate {
            err,
            file_ids: Arc::from(file_ids),
        }
    }

    pub fn create_writer_error(opts: &Arc<WriterProperties>) -> impl FnOnce(BoxError) -> Self {
        move |err| Self::CreateWriter {
            err,
            opts: opts.parquet.clone().into(),
        }
    }
}

impl From<ParquetError> for CompactorError {
    fn from(err: ParquetError) -> Self {
        Self::FileStream(DataFusionError::ParquetError(Box::new(err)))
    }
}

impl From<DataFusionError> for CompactorError {
    fn from(err: DataFusionError) -> Self {
        Self::FileStream(err)
    }
}

impl From<JoinError> for CompactorError {
    fn from(err: JoinError) -> Self {
        Self::Join(err)
    }
}

/// Errors that occur during garbage collection operations
///
/// Garbage collection identifies and removes orphaned or obsolete Parquet files
/// from storage after they've been compacted or are no longer referenced. This
/// error type covers all phases of GC: consistency checks, file deletion, and
/// metadata cleanup.
#[derive(thiserror::Error, Debug)]
pub enum CollectorError {
    /// File not found in object store
    ///
    /// This occurs when attempting to delete a file that doesn't exist in storage.
    /// The file may have already been deleted or the metadata is out of sync.
    ///
    /// Common causes:
    /// - File already deleted by another garbage collection process
    /// - Metadata references non-existent file (orphaned metadata)
    /// - Object store path changed or bucket reconfigured
    /// - Manual file deletion outside of GC process
    ///
    /// This is often not a fatal error - if the file is already gone, the GC
    /// goal is achieved. The metadata should still be cleaned up.
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// Object store operation failed
    ///
    /// This occurs when interacting with the underlying object store (S3, GCS,
    /// local filesystem, etc.) fails during file deletion operations.
    ///
    /// Common causes:
    /// - Object store connection lost during deletion
    /// - Network timeouts or connectivity issues
    /// - Insufficient permissions to delete files
    /// - Object store service unavailability
    /// - Rate limiting by object store service
    ///
    /// Object store errors are often transient and retryable. Permanent failures
    /// may indicate configuration or permission issues.
    #[error("object store error: {0}")]
    ObjectStore(#[source] ObjectStoreError),

    /// Failed to stream file metadata from database
    ///
    /// This occurs when querying the metadata database for files eligible for
    /// garbage collection fails. The GC process needs to read file metadata to
    /// determine which files can be safely deleted.
    ///
    /// Common causes:
    /// - Database connection lost during query
    /// - Query timeout due to large result set
    /// - Database unavailability
    /// - Insufficient database permissions for SELECT queries
    ///
    /// This prevents garbage collection from proceeding as the list of deletable
    /// files cannot be determined.
    #[error("failed to stream file metadata: {0}")]
    FileStream(#[source] metadata_db::Error),

    /// Task join error during garbage collection
    ///
    /// This occurs when a garbage collection task running in a separate thread
    /// or async task panics or is cancelled. GC operations are typically run in
    /// background tasks.
    ///
    /// Common causes:
    /// - Panic in GC code (indicates a bug)
    /// - Task cancelled due to shutdown signal
    /// - Task aborted due to timeout
    /// - Out of memory causing task termination
    ///
    /// This is typically a fatal error indicating either a bug or system resource issue.
    #[error("garbage collection task join error: {0}")]
    Join(#[source] JoinError),

    /// Failed to delete file metadata records
    ///
    /// This occurs when removing file metadata from the metadata database after
    /// successfully deleting the files from storage. The files are gone but the
    /// database still has stale metadata.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Insufficient database permissions for DELETE queries
    /// - Database constraint violations preventing deletion
    ///
    /// This leaves orphaned metadata in the database. The metadata should be
    /// cleaned up manually or via retry.
    #[error("failed to delete file metadata for files {file_ids:?}: {err}")]
    FileMetadataDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Vec<FileId>,
    },

    /// Failed to delete manifest record
    ///
    /// This occurs when removing a file entry from the garbage collection manifest
    /// after the file has been successfully deleted. The GC manifest tracks files
    /// eligible for cleanup.
    ///
    /// Common causes:
    /// - Database connection lost during delete
    /// - Transaction conflicts with concurrent operations
    /// - Record already deleted by another process
    /// - Insufficient database permissions
    ///
    /// This leaves a stale entry in the GC manifest. The entry should be cleaned
    /// up to prevent repeated deletion attempts.
    #[error("failed to delete file {file_id} from gc manifest: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_id: FileId,
    },

    /// Failed to parse URL for file
    ///
    /// This occurs when converting a file path stored in metadata to a URL for
    /// object store operations fails. URLs are used to identify files in the
    /// object store.
    ///
    /// Common causes:
    /// - Corrupted file path in database (invalid URL characters)
    /// - Malformed file:// or s3:// URL in metadata
    /// - Invalid percent-encoding in path
    ///
    /// This indicates corrupted metadata and should be investigated. The file
    /// may be inaccessible for deletion without manual intervention.
    #[error("url parse error for file {file_id}: {err}")]
    Parse {
        file_id: FileId,
        #[source]
        err: url::ParseError,
    },

    /// Invalid object store path
    ///
    /// This occurs when a path cannot be converted to a valid object store path.
    /// Object stores have specific path format requirements.
    ///
    /// Common causes:
    /// - Path contains invalid characters for object store
    /// - Path doesn't match expected format (e.g., S3 bucket/key structure)
    /// - Empty path components
    /// - Path exceeds maximum length limits
    ///
    /// This indicates a mismatch between stored paths and object store requirements.
    #[error("path error: {0}")]
    Path(#[source] PathError),

    /// Multiple errors occurred during collection
    ///
    /// This occurs when garbage collection encounters multiple independent failures
    /// across different files or operations. The GC process attempts to continue
    /// despite individual failures, collecting all errors for reporting.
    ///
    /// Common causes:
    /// - Batch deletion with some files failing due to different reasons
    /// - Mixed success/failure when deleting across multiple storage locations
    /// - Partial database connection issues affecting some operations
    ///
    /// Each individual error should be examined to determine if retry is appropriate.
    /// Some files may have been successfully deleted while others failed.
    #[error("multiple errors occurred:\n{}", .errors.iter().enumerate().map(|(i, e)| format!("  {}: {}", i + 1, e)).collect::<Vec<_>>().join("\n"))]
    MultipleErrors { errors: Vec<CollectorError> },
}

impl From<JoinError> for CollectorError {
    fn from(err: JoinError) -> Self {
        Self::Join(err)
    }
}

impl From<ObjectStoreError> for CollectorError {
    fn from(err: ObjectStoreError) -> Self {
        Self::ObjectStore(err)
    }
}

impl CollectorError {
    pub fn file_stream_error(err: metadata_db::Error) -> Self {
        Self::FileStream(err)
    }

    pub fn file_metadata_delete<'a>(
        file_ids: impl Iterator<Item = &'a FileId>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| Self::FileMetadataDelete {
            err,
            file_ids: file_ids.cloned().collect(),
        }
    }

    pub fn gc_manifest_delete(file_id: FileId) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| Self::ManifestDelete { err, file_id }
    }

    pub fn parse_error(file_id: FileId) -> impl FnOnce(url::ParseError) -> Self {
        move |err| Self::Parse { file_id, err }
    }

    pub fn path_error(err: PathError) -> Self {
        Self::Path(err)
    }
}
