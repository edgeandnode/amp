use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

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

use crate::{
    ConsistencyCheckError, WriterProperties,
    compaction::{Collector, NozzleCompactorTaskType, compactor::Compactor},
};

pub type CompactionResult<T> = Result<T, CompactorError>;
pub type CollectionResult<T> = Result<T, CollectorError>;

pub trait CompactionErrorExt: std::error::Error + From<JoinError> + Send + Sync + 'static {
    type Task: NozzleCompactorTaskType<Error = Self>;
    /// Whether the error is merely for debugging and does not indicate a failure
    /// Default implementation returns false for all error variants
    fn is_debug(&self) -> bool {
        false
    }
    /// Whether the error is merely informational and does not indicate a failure
    /// Default implementation returns false for all error variants
    fn is_informational(&self) -> bool {
        false
    }

    /// Whether the error is recoverable and the task can continue running
    /// Default implementation returns true for all error variants
    fn is_recoverable(&self) -> bool {
        true
    }

    /// Whether the error indicates that the task was cancelled
    fn is_cancellation(&self) -> bool;
}

#[derive(Debug)]
pub enum CompactorError
where
    Self: CompactionErrorExt,
{
    /// Catching errors while building the canonical chain for a table
    CanonicalChainError { err: BoxError },
    /// Catching cases where the canonical chain is None or empty
    EmptyChain,
    /// Catching errors while creating a compaction writer
    CreateWriterError {
        err: BoxError,
        opts: Box<ParquetWriterProperties>,
    },
    /// Catching errors while writing data to a parquet file
    FileWriteError { err: BoxError },
    /// Catching errors while reading data and metadata from parquet files
    FileStreamError { err: DataFusionError },
    /// Catching send errors while building compaction futures
    SendError,
    /// Catching join errors while awaiting compaction futures
    JoinError { err: JoinError },
    /// Catching errors while updating the gc manifest in the metadata db
    ManifestUpdateError {
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },
    /// Catching errors while deleting records from the gc manifest in the metadata db
    ManifestDeleteError {
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },
    /// Catching errors while committing a new record to the file_metadata table in the metadata db
    MetadataCommitError { err: metadata_db::Error },
}

impl CompactorError {
    pub fn empty_chain() -> Self {
        Self::EmptyChain
    }

    pub fn chain_error(err: BoxError) -> Self {
        Self::CanonicalChainError { err }
    }

    pub fn metadata_commit_error(err: metadata_db::Error) -> Self {
        CompactorError::MetadataCommitError { err }
    }

    pub fn manifest_update_error(file_ids: &[FileId]) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CompactorError::ManifestUpdateError {
            err,
            file_ids: Arc::from(file_ids),
        }
    }

    pub fn create_writer_error(opts: &Arc<WriterProperties>) -> impl FnOnce(BoxError) -> Self {
        move |err| CompactorError::CreateWriterError {
            opts: opts.parquet.clone().into(),
            err,
        }
    }
}

impl From<ParquetError> for CompactorError {
    fn from(err: ParquetError) -> Self {
        CompactorError::FileStreamError {
            err: DataFusionError::ParquetError(Box::new(err)),
        }
    }
}

impl From<DataFusionError> for CompactorError {
    fn from(err: DataFusionError) -> Self {
        CompactorError::FileStreamError { err }
    }
}

impl Display for CompactorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CompactorError::CreateWriterError { opts, err } => {
                write!(
                    f,
                    "Failed to create writer. WriterProperties: {opts:?}, Error: {err}",
                )
            }
            CompactorError::FileWriteError { err, .. } => {
                write!(f, "Error occurred while writing compacted file: {err}")
            }
            CompactorError::MetadataCommitError { err, .. } => {
                write!(f, "Error committing new metadata record: {err}",)
            }
            CompactorError::ManifestUpdateError { err, file_ids, .. } => {
                write!(
                    f,
                    "Error inserting file IDs {file_ids:?} into GC manifest: {err}",
                )
            }
            CompactorError::ManifestDeleteError { err, file_ids, .. } => {
                write!(
                    f,
                    "Error deleting file IDs {file_ids:?} from GC manifest: {err}",
                )
            }
            CompactorError::JoinError { err, .. } => err.fmt(f),
            CompactorError::CanonicalChainError { err, .. } => {
                write!(f, "Error building canonical chain: {err}")
            }
            CompactorError::EmptyChain => {
                write!(f, "Canonical chain is empty or None")
            }
            CompactorError::FileStreamError { err, .. } => {
                write!(f, "Error reading data or metadata from parquet file: {err}")
            }
            CompactorError::SendError => {
                write!(f, "Error sending data to compaction task")
            }
        }
    }
}

impl Error for CompactorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CompactorError::FileWriteError { err, .. } => err.source(),
            CompactorError::MetadataCommitError { err, .. } => err.source(),
            CompactorError::ManifestUpdateError { err, .. } => err.source(),
            CompactorError::ManifestDeleteError { err, .. } => err.source(),
            CompactorError::JoinError { err, .. } => err.source(),
            CompactorError::CanonicalChainError { err, .. } => err.source(),
            CompactorError::FileStreamError { err, .. } => err.source(),
            CompactorError::CreateWriterError { err, .. } => err.source(),
            CompactorError::SendError => None,
            CompactorError::EmptyChain => None,
        }
    }
}

impl From<JoinError> for CompactorError {
    fn from(err: JoinError) -> Self {
        CompactorError::JoinError { err }
    }
}

impl CompactionErrorExt for CompactorError {
    type Task = Compactor;

    fn is_debug(&self) -> bool {
        matches!(self, CompactorError::EmptyChain)
    }

    fn is_cancellation(&self) -> bool {
        match self {
            CompactorError::JoinError { err, .. } => err.is_cancelled(),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub enum CollectorError
where
    Self: CompactionErrorExt,
{
    Consistency {
        error: ConsistencyCheckError,
    },
    FileNotFound {
        path: String,
    },
    ObjectStoreError {
        err: ObjectStoreError,
    },
    FileStreamError {
        err: metadata_db::Error,
    },
    JoinError {
        err: JoinError,
    },
    FileMetadataDeleteError {
        err: metadata_db::Error,
        file_ids: Vec<FileId>,
    },
    ManifestDeleteError {
        err: metadata_db::Error,
        file_id: FileId,
    },
    ParseError {
        file_id: FileId,
        err: url::ParseError,
    },
    PathError {
        err: PathError,
    },
    MultipleErrors {
        errors: Vec<Box<CollectorError>>,
    },
}

impl From<JoinError> for CollectorError {
    fn from(err: JoinError) -> Self {
        CollectorError::JoinError { err }
    }
}

impl From<ObjectStoreError> for CollectorError {
    fn from(err: ObjectStoreError) -> Self {
        CollectorError::ObjectStoreError { err }
    }
}

impl CompactionErrorExt for CollectorError {
    type Task = Collector;

    fn is_cancellation(&self) -> bool {
        match self {
            CollectorError::JoinError { err, .. } => err.is_cancelled(),
            _ => false,
        }
    }
}

impl CollectorError {
    pub fn consistency_check_error(error: ConsistencyCheckError) -> Self {
        Self::Consistency { error }
    }

    pub fn file_stream_error(err: metadata_db::Error) -> Self {
        Self::FileStreamError { err }
    }

    pub fn file_metadata_delete(file_ids: Vec<FileId>, err: metadata_db::Error) -> Self {
        CollectorError::FileMetadataDeleteError { err, file_ids }
    }

    pub fn gc_manifest_delete(file_id: FileId) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CollectorError::ManifestDeleteError { err, file_id }
    }

    pub fn parse_error(file_id: FileId) -> impl FnOnce(url::ParseError) -> Self {
        move |err| CollectorError::ParseError { file_id, err }
    }

    pub fn path_error(err: PathError) -> Self {
        Self::PathError { err }
    }
}

impl Display for CollectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Consistency { error, .. } => error.fmt(f),
            Self::FileNotFound { path } => f.write_str(&format!("File not found: {path}")),
            Self::ObjectStoreError { err } => err.fmt(f),
            Self::FileStreamError { err } => err.fmt(f),
            Self::JoinError { err } => err.fmt(f),
            Self::FileMetadataDeleteError {
                err,
                file_ids: file_id,
            } => {
                write!(
                    f,
                    "Error deleting File IDs: [{}] from file_metadata table in Metadata Db: {err}",
                    file_id
                        .iter()
                        .map(|id| (**id).to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Self::ManifestDeleteError { err, file_id } => {
                write!(
                    f,
                    "Error deleting file ID {} from gc_manifest table in Metadata Db: {err}",
                    *file_id
                )
            }
            Self::ParseError { file_id, err } => {
                write!(f, "URL parse error for file {file_id}: {err}")
            }
            Self::PathError { err } => {
                write!(f, "Path error: {err}")
            }
            Self::MultipleErrors { errors } => {
                write!(f, "Multiple errors occurred:")?;
                for (i, error) in errors.iter().enumerate() {
                    write!(f, "\n  {}: {}", i + 1, error)?;
                }
                Ok(())
            }
        }
    }
}

impl Error for CollectorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CollectorError::Consistency { error, .. } => error.source(),
            CollectorError::FileNotFound { .. } => None,
            CollectorError::FileStreamError { err, .. } => err.source(),
            CollectorError::JoinError { err, .. } => err.source(),
            CollectorError::FileMetadataDeleteError { err, .. } => err.source(),
            CollectorError::ManifestDeleteError { err, .. } => err.source(),
            CollectorError::ParseError { err, .. } => err.source(),
            CollectorError::PathError { err, .. } => err.source(),
            CollectorError::ObjectStoreError { err, .. } => err.source(),
            CollectorError::MultipleErrors { .. } => None,
        }
    }
}
