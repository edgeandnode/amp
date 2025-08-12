use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
};

use common::{
    BoxError,
    catalog::physical::PhysicalTable,
    parquet::{
        errors::ParquetError, file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use datafusion::error::DataFusionError;
use metadata_db::FileId;
use object_store::Error as ObjectStoreError;
use tokio::task::JoinError;

use crate::compaction::{Collector, compactor::Compactor};

pub type CompactionResult<T> = Result<T, CompactionError>;
pub type DeletionResult<T> = Result<T, DeletionError>;

#[derive(Debug)]
pub enum CompactionError {
    /// Catching errors while building the canonical chain for a table
    CanonicalChainError { err: BoxError, table_ref: String },
    /// Catching errors while creating a compaction writer
    CreateWriterError {
        table_ref: String,
        opts: ParquetWriterProperties,
        err: BoxError,
    },
    /// Catching errors while writing data to a parquet file
    FileWriteError { err: BoxError },
    /// Catching errors while reading data and metadata from parquet files
    FileStreamError { err: DataFusionError },
    /// Catching send errors while building compaction futures
    SendError,
    /// Catching join errors while awaiting compaction futures
    JoinError {
        compactor: Compactor,
        err: JoinError,
    },
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
    MetadataCommitError {
        err: metadata_db::Error,
        location: Arc<str>,
    },
}

impl CompactionError {
    pub fn chain_error(table: &PhysicalTable) -> impl FnOnce(BoxError) -> Self {
        move |err| CompactionError::CanonicalChainError {
            err,
            table_ref: table.table_ref().to_string(),
        }
    }

    pub fn metadata_commit_error(
        location: impl Into<Arc<str>>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CompactionError::MetadataCommitError {
            err,
            location: location.into(),
        }
    }

    pub fn manifest_update_error(
        file_ids: Arc<[FileId]>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CompactionError::ManifestUpdateError { err, file_ids }
    }

    pub fn create_writer_error(
        table: &PhysicalTable,
        opts: &ParquetWriterProperties,
    ) -> impl FnOnce(BoxError) -> Self {
        move |err| CompactionError::CreateWriterError {
            table_ref: table.table_ref().to_string(),
            opts: opts.clone(),
            err,
        }
    }

    pub fn join_error(compactor: Compactor) -> impl FnOnce(JoinError) -> Self {
        move |err: JoinError| CompactionError::JoinError { compactor, err }
    }
}

impl<'a, T: AsRef<[FileId]> + Send + Sync + 'a> From<(metadata_db::Error, T)> for CompactionError {
    fn from((err, file_ids): (metadata_db::Error, T)) -> Self {
        CompactionError::ManifestDeleteError {
            err,
            file_ids: Arc::from(file_ids.as_ref()),
        }
    }
}

impl From<ParquetError> for CompactionError {
    fn from(err: ParquetError) -> Self {
        CompactionError::FileStreamError {
            err: DataFusionError::ParquetError(Box::new(err)),
        }
    }
}

impl From<DataFusionError> for CompactionError {
    fn from(err: DataFusionError) -> Self {
        CompactionError::FileStreamError { err }
    }
}

impl Display for CompactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            CompactionError::CreateWriterError {
                table_ref,
                opts,
                err,
            } => {
                write!(
                    f,
                    "Failed to create writer. 
                    Table: {table_ref},
                    WriterProperties: {opts:?},
                    Error: {err}",
                )
            }
            CompactionError::FileWriteError { err, .. } => {
                write!(f, "Error occured while writing compacted file: {}", err)
            }
            CompactionError::MetadataCommitError { err, location, .. } => {
                write!(
                    f,
                    "Error committing metadata for compacted file at {}: {}",
                    location, err
                )
            }
            CompactionError::ManifestUpdateError { err, file_ids, .. } => {
                write!(
                    f,
                    "Error inserting file IDs {:?} into GC manifest: {}",
                    file_ids, err
                )
            }
            CompactionError::ManifestDeleteError { err, file_ids, .. } => {
                write!(
                    f,
                    "Error deleting file IDs {:?} from GC manifest: {}",
                    file_ids, err
                )
            }
            CompactionError::JoinError { err, compactor, .. } => {
                let table_ref = compactor.table.table_ref();
                write!(
                    f,
                    "Task failed to execute to completion: {}; error: {}",
                    table_ref, err
                )
            }
            _ => write!(f, "An unknown compaction error occurred"),
        }
    }
}

impl std::error::Error for CompactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CompactionError::FileWriteError { err, .. } => err.source(),
            CompactionError::MetadataCommitError { err, .. } => err.source(),
            CompactionError::ManifestUpdateError { err, .. } => err.source(),
            CompactionError::ManifestDeleteError { err, .. } => err.source(),
            CompactionError::JoinError { err, .. } => err.source(),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum DeletionError {
    FileDeleteError {
        collector: Collector,
        err: ObjectStoreError,
        file_id: FileId,
        location: Arc<str>,
        not_found: bool,
    },
    FileStreamError {
        collector: Collector,
        err: metadata_db::Error,
    },
    JoinError {
        collector: Collector,
        err: JoinError,
    },
    ManifestDeleteError {
        collector: Collector,
        err: metadata_db::Error,
        file_ids: Vec<FileId>,
    },
}

impl DeletionError {
    pub fn manifest_update_error(
        collector: Collector,
        file_ids: Vec<FileId>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| DeletionError::ManifestDeleteError {
            collector,
            err,
            file_ids,
        }
    }
    pub fn deletion_stream_error(collector: Collector) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| DeletionError::FileStreamError { collector, err }
    }

    pub fn file_delete_error(
        file_id: FileId,
        location: &str,
        collector: Collector,
    ) -> impl FnOnce(ObjectStoreError) -> Self {
        move |err| match err {
            ObjectStoreError::NotFound { path, source } => DeletionError::FileDeleteError {
                err: ObjectStoreError::NotFound { path, source },
                file_id,
                location: Arc::from(location),
                not_found: true,
                collector,
            },
            _ => DeletionError::FileDeleteError {
                err,
                file_id,
                location: Arc::from(location),
                not_found: false,
                collector,
            },
        }
    }

    pub fn join_error(collector: Collector) -> impl FnOnce(JoinError) -> Self {
        move |err| DeletionError::JoinError { collector, err }
    }
}

impl Display for DeletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeletionError::FileDeleteError {
                file_id,
                location,
                not_found: true,
                ..
            } => write!(
                f,
                "Failed to delete file.
                FileId: {file_id},
                Location: {location},
                Error: File not found; removing from GC Manifest"
            ),
            DeletionError::FileDeleteError {
                err,
                file_id,
                location,
                not_found: false,
                ..
            } => write!(
                f,
                "Failed to delete file.
                FileId: {file_id},
                Path: {location}, 
                Error: {err}"
            ),
            DeletionError::FileStreamError { err, .. } => write!(
                f,
                "Failed to create expired file stream.
                Error: {err}"
            ),
            DeletionError::JoinError { err, .. } => write!(
                f,
                "Failed to execute deletion task to completion.
                Error: {err}"
            ),
            DeletionError::ManifestDeleteError { err, file_ids, .. } => write!(
                f,
                "Failed to delete deleted FileIds from GC Manifest.
                FileIds: {file_ids:?},
                Error: {err}"
            ),
        }
    }
}

impl std::error::Error for DeletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DeletionError::FileDeleteError {
                err,
                not_found: false,
                ..
            } => err.source(),
            // Not found errors are not considered a failure
            DeletionError::FileDeleteError {
                not_found: true, ..
            } => None,
            DeletionError::FileStreamError { err, .. } => err.source(),
            DeletionError::JoinError { err, .. } => err.source(),
            DeletionError::ManifestDeleteError { err, .. } => err.source(),
        }
    }
}
