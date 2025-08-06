use std::{fmt::Display, sync::Arc, time::Duration};

use common::{Timestamp, catalog::physical::PhysicalTable};
use futures::{
    SinkExt, StreamExt, TryStreamExt,
    future::{self, BoxFuture},
    stream,
};
use metadata_db::{FileId, GcManifestRow, LocationId, MetadataDb};
use object_store::{Error as ObjectStoreError, path::Path};
use tokio::task::{JoinError, JoinHandle};

use crate::compactor::delete_from_gc_manifest;

pub static COLLECTOR_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes

pub type DeletionResult<T> = Result<T, DeletionError>;

#[derive(Debug)]
struct GcManifest {
    #[allow(dead_code)]
    location_id: LocationId,
    file_id: FileId,
    location: Path,
    expiration: Timestamp,
}

impl From<GcManifestRow> for GcManifest {
    fn from(
        GcManifestRow {
            location_id,
            file_id,
            file_path,
            expiration,
        }: GcManifestRow,
    ) -> Self {
        let location = Path::from(file_path);

        let expiration = Timestamp(Duration::from_micros(
            expiration.and_utc().timestamp_micros() as u64,
        ));

        GcManifest {
            location_id,
            file_id,
            location,
            expiration,
        }
    }
}

pub(super) struct DeletionTask {
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
    task: JoinHandle<DeletionResult<Collector>>,
    previous: Option<Timestamp>,
}

impl DeletionTask {
    fn elapsed_since_previous(&self) -> Option<Duration> {
        self.previous.map(|previous| {
            let now = Timestamp::now();
            now.0.saturating_sub(previous.0)
        })
    }

    pub(super) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    fn ready(&self) -> bool {
        self.is_finished()
            && self
                .elapsed_since_previous()
                .map_or(false, |elapsed| elapsed >= COLLECTOR_INTERVAL)
    }

    pub(super) async fn try_run(&mut self) {
        if self.ready() {
            let task = &mut self.task;

            let collector = match task.await.map_err(DeletionError::join_error(Collector::new(
                &self.table,
                &self.metadata_db,
            ))) {
                Ok(Ok(collector)) => collector,
                Ok(Err(DeletionError::JoinError { collector, err }))
                | Err(DeletionError::JoinError { collector, err }) => {
                    tracing::error!("{err}");
                    collector
                }
                _ => unreachable!("Unexpected error while waiting for compaction task"),
            };

            self.task = tokio::spawn(collector.collect());
        }
    }
}

#[derive(Debug)]
struct DeletionOutput {
    #[allow(dead_code)]
    location_id: LocationId,
    successes: Vec<FileId>,
    not_found: Vec<FileId>,
    errors: Vec<(FileId, Box<ObjectStoreError>)>,
    size: usize,
}

impl DeletionOutput {
    fn new(location_id: LocationId, manifest_list: &[GcManifest]) -> Self {
        let size = manifest_list.len().max(1);
        let successes = Vec::with_capacity(size);
        let not_found = Vec::with_capacity(size);
        let errors = Vec::with_capacity(size);
        DeletionOutput {
            location_id,
            successes,
            not_found,
            errors,
            size,
        }
    }

    fn successes(&self) -> &[FileId] {
        &self.successes
    }

    fn insert_success(&mut self, file_id: FileId) {
        self.successes.push(file_id);
    }

    fn not_found(&self) -> &[FileId] {
        &self.not_found
    }

    fn insert_not_found(&mut self, file_id: FileId) {
        self.not_found.push(file_id);
    }

    #[allow(dead_code)]
    fn errors(&self) -> &[(FileId, Box<ObjectStoreError>)] {
        &self.errors
    }

    fn insert_error(&mut self, file_id: FileId, err: ObjectStoreError) {
        self.errors.push((file_id, Box::new(err)));
    }
}

#[derive(Clone, Debug)]
pub struct Collector {
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
}

impl Collector {
    pub fn start(table: Arc<PhysicalTable>, metadata_db: Arc<MetadataDb>) -> DeletionTask {
        DeletionTask {
            task: tokio::spawn(future::ok(Collector::new(&table, &metadata_db))),
            table,
            metadata_db,
            previous: None,
        }
    }

    fn new<'a>(table: &'a Arc<PhysicalTable>, metadata_db: &'a Arc<MetadataDb>) -> Self {
        Collector {
            table: Arc::clone(table),
            metadata_db: Arc::clone(metadata_db),
        }
    }

    async fn collect(self) -> DeletionResult<Self> {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let table = Arc::clone(&self.table);

        let location_id = table.location_id();
        let object_store = table.object_store();

        let output =
            try_delete_expired_files(location_id, Arc::clone(&self.metadata_db), object_store)
                .await?;

        if output.size == 0 {
            tracing::debug!(
                "No expired files to delete for table {} at location {}",
                table.table_ref(),
                location_id
            );
            return Ok(self);
        }

        let file_ids = [output.successes(), output.not_found()].concat();

        delete_from_gc_manifest::<Vec<FileId>, DeletionError>(&self.metadata_db, &file_ids).await?;
        Ok(self)
    }
}

fn try_delete_expired_files<'a>(
    location_id: LocationId,
    metadata_db: Arc<MetadataDb>,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> BoxFuture<'a, DeletionResult<DeletionOutput>> {
    let metadata_db = Arc::clone(&metadata_db);
    let object_store = Arc::clone(&object_store);
    Box::pin(async move {
        let now = Timestamp::now();
        let secs = now.0.as_secs() as i64;
        let nsecs = now.0.subsec_nanos();
        let manifest_list: Vec<GcManifest> = metadata_db
            .stream_expired_files(location_id, secs, nsecs)
            .map_err(|err| DeletionError::FileStreamError {
                err: metadata_db::Error::DbError(err),
            })
            .map_ok(|row| GcManifest::from(row))
            .try_collect()
            .await?;

        if manifest_list.is_empty() {
            return Ok(DeletionOutput::new(location_id, &[]));
        }

        let (tx, file_ids) = futures::channel::mpsc::unbounded::<(&FileId, &str)>();

        let locations = stream::iter(&manifest_list)
            .filter(|GcManifest { expiration, .. }| future::ready(*expiration <= now))
            .map(
                |GcManifest {
                     file_id, location, ..
                 }| {
                    let mut sender = tx.clone();
                    async move {
                        sender.send((file_id, location.as_ref())).await.unwrap();

                        Ok(Path::from(location.as_ref()))
                    }
                },
            )
            .buffered(manifest_list.len())
            .boxed();

        let output = object_store
            .delete_stream(locations)
            .zip(file_ids)
            .map(|(result, (file_id, location))| {
                result
                    .map_err(DeletionError::file_delete_error(*file_id, location))
                    .map(|_| file_id)
            })
            .fold(
                DeletionOutput::new(location_id, &manifest_list),
                |mut output, result| async move {
                    match result {
                        Ok(file_id) => output.insert_success(*file_id),
                        Err(DeletionError::FileDeleteError {
                            file_id,
                            not_found: true,
                            ..
                        }) => {
                            output.insert_not_found(file_id);
                        }
                        Err(DeletionError::FileDeleteError {
                            err,
                            file_id,
                            not_found: false,
                            ..
                        }) => {
                            output.insert_error(file_id, err);
                        }
                        _ => unreachable!("Unexpected error while deleting expired files"),
                    }

                    output
                },
            )
            .await;
        Ok(output)
    })
}

#[derive(Debug)]
pub enum DeletionError {
    FileDeleteError {
        err: ObjectStoreError,
        file_id: FileId,
        location: Arc<str>,
        not_found: bool,
    },
    FileStreamError {
        err: metadata_db::Error,
    },
    JoinError {
        collector: Collector,
        err: JoinError,
    },
    ManifestDeleteError {
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },
}

impl DeletionError {
    #[allow(dead_code)]
    fn deletion_stream_error(err: metadata_db::Error) -> Self {
        DeletionError::FileStreamError { err }
    }

    fn file_delete_error(file_id: FileId, location: &str) -> impl FnOnce(ObjectStoreError) -> Self {
        move |err| match err {
            ObjectStoreError::NotFound { path, source } => DeletionError::FileDeleteError {
                err: ObjectStoreError::NotFound { path, source },
                file_id,
                location: Arc::from(location),
                not_found: true,
            },
            _ => DeletionError::FileDeleteError {
                err,
                file_id,
                location: Arc::from(location),
                not_found: false,
            },
        }
    }

    fn join_error(collector: Collector) -> impl FnOnce(JoinError) -> Self {
        move |err| DeletionError::JoinError { collector, err }
    }
}

impl Display for DeletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeletionError::FileDeleteError {
                err,
                file_id,
                location,
                not_found,
            } => {
                if *not_found {
                    write!(f, "File not found: {} at {}", file_id, location)
                } else {
                    write!(
                        f,
                        "Failed to delete file {} at {}: {}",
                        file_id, location, err
                    )
                }
            }
            DeletionError::FileStreamError { err } => {
                write!(f, "Failed to stream expired files: {}", err)
            }
            DeletionError::JoinError { collector, err } => {
                write!(f, "Join error for collector {:?}: {}", collector, err)
            }
            DeletionError::ManifestDeleteError { err, file_ids } => write!(
                f,
                "Failed to delete manifest for files {:?}: {}",
                file_ids, err
            ),
        }
    }
}

impl std::error::Error for DeletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DeletionError::FileDeleteError { err, .. } => err.source(),
            DeletionError::FileStreamError { err } => err.source(),
            DeletionError::JoinError { err, .. } => err.source(),
            DeletionError::ManifestDeleteError { err, .. } => err.source(),
        }
    }
}

impl<'a, T: AsRef<[FileId]> + 'a> From<(metadata_db::Error, T)> for DeletionError {
    fn from((err, file_ids): (metadata_db::Error, T)) -> Self {
        DeletionError::ManifestDeleteError {
            err,
            file_ids: Arc::from(file_ids.as_ref()),
        }
    }
}
