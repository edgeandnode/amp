use std::sync::Arc;

use common::{
    BoxError,
    parquet::{
        errors::ParquetError, file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use datafusion::error::DataFusionError;
use metadata_db::FileId;
use object_store::{Error as ObjectStoreError, path::Error as PathError};
use tokio::task::JoinError;

use crate::{ConsistencyCheckError, WriterProperties};

pub type CompactionResult<T> = Result<T, CompactorError>;
pub type CollectionResult<T> = Result<T, CollectorError>;

#[derive(thiserror::Error, Debug)]
pub enum CompactorError {
    /// Failed to build canonical chain for compaction
    ///
    /// This occurs during the chain-building phase when determining which files
    /// to compact together.
    #[error("failed to build canonical chain: {0}")]
    CanonicalChain(#[source] BoxError),

    /// Failed to create compaction writer
    ///
    /// This occurs when initializing a Parquet writer with the given properties
    /// fails, typically due to invalid configuration or resource constraints.
    #[error("failed to create writer (writer properties: {opts:?}): {err}")]
    CreateWriter {
        #[source]
        err: BoxError,
        opts: Box<ParquetWriterProperties>,
    },

    /// Failed to write data to Parquet file
    ///
    /// This occurs during the write phase when encoding and writing data fails.
    #[error("failed to write to parquet file: {0}")]
    FileWrite(#[source] BoxError),

    /// Failed to read/stream data from Parquet files
    ///
    /// This occurs when reading input files during compaction fails.
    #[error("failed to stream parquet file: {0}")]
    FileStream(#[source] DataFusionError),

    /// Task join error during compaction
    ///
    /// This occurs when a compaction task panics or is cancelled.
    #[error("compaction task join error: {0}")]
    Join(#[source] JoinError),

    /// Failed to update GC manifest in metadata database
    ///
    /// This occurs when updating the garbage collection manifest with new
    /// compacted file information fails.
    #[error("failed to update gc manifest for files {file_ids:?}: {err}")]
    ManifestUpdate {
        #[source]
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to delete records from GC manifest
    ///
    /// This occurs when cleaning up old file records from the manifest fails.
    #[error("failed to delete from gc manifest for files {file_ids:?}: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },

    /// Failed to commit file metadata to database
    ///
    /// This occurs when recording the new compacted file's metadata fails.
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

#[derive(thiserror::Error, Debug)]
pub enum CollectorError {
    /// Consistency check failed during garbage collection
    ///
    /// This occurs when validating file consistency before collection fails.
    #[error("consistency check failed: {0}")]
    Consistency(#[source] ConsistencyCheckError),

    /// File not found in object store
    ///
    /// This occurs when attempting to access a file that doesn't exist.
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// Object store operation failed
    ///
    /// This occurs when interacting with the underlying object store fails.
    #[error("object store error: {0}")]
    ObjectStore(#[source] ObjectStoreError),

    /// Failed to stream file metadata from database
    ///
    /// This occurs when reading file metadata records fails.
    #[error("failed to stream file metadata: {0}")]
    FileStream(#[source] metadata_db::Error),

    /// Task join error during garbage collection
    ///
    /// This occurs when a collection task panics or is cancelled.
    #[error("garbage collection task join error: {0}")]
    Join(#[source] JoinError),

    /// Failed to delete file metadata records
    ///
    /// This occurs when removing file metadata from the database fails.
    #[error("failed to delete file metadata for files {file_ids:?}: {err}")]
    FileMetadataDelete {
        #[source]
        err: metadata_db::Error,
        file_ids: Vec<FileId>,
    },

    /// Failed to delete manifest record
    ///
    /// This occurs when removing a file from the GC manifest fails.
    #[error("failed to delete file {file_id} from gc manifest: {err}")]
    ManifestDelete {
        #[source]
        err: metadata_db::Error,
        file_id: FileId,
    },

    /// Failed to parse URL for file
    ///
    /// This occurs when converting a file path to a URL fails.
    #[error("url parse error for file {file_id}: {err}")]
    Parse {
        file_id: FileId,
        #[source]
        err: url::ParseError,
    },

    /// Invalid object store path
    ///
    /// This occurs when a path cannot be used with the object store.
    #[error("path error: {0}")]
    Path(#[source] PathError),

    /// Multiple errors occurred during collection
    ///
    /// This occurs when multiple independent failures happen during garbage collection.
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
    pub fn consistency_check_error(error: ConsistencyCheckError) -> Self {
        Self::Consistency(error)
    }

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
