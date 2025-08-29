use std::{sync::Arc, time::Duration};

use common::{Timestamp, catalog::physical::PhysicalTable};
use futures::{
    FutureExt, StreamExt, TryStreamExt, future,
    stream::{self, BoxStream},
};
use metadata_db::{FileId, GcManifestRow, MetadataDb};
use object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

use crate::{
    compaction::{
        CompactionProperties, DeletionTask,
        error::{DeletionError, DeletionResult},
    },
    consistency_check,
};

#[derive(Clone, Debug)]
pub struct Collector {
    pub table: Arc<PhysicalTable>,
    pub opts: Arc<CompactionProperties>,
}

impl Collector {
    /// Starts the deletion task by spawning a new asynchronous task
    /// that returns a new Collector.
    pub fn start(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> DeletionTask {
        DeletionTask {
            task: tokio::spawn(future::ok(Collector::new(table, opts)).boxed()),
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            previous: None,
        }
    }

    pub(super) fn new<'a>(
        table: &'a Arc<PhysicalTable>,
        opts: &'a Arc<CompactionProperties>,
    ) -> Self {
        Collector {
            table: Arc::clone(table),
            opts: Arc::clone(opts),
        }
    }

    #[tracing::instrument(skip_all, err, fields(table = %self.table.table_ref()))]
    pub(super) async fn collect(self) -> DeletionResult<Self> {
        let table = Arc::clone(&self.table);

        let location_id = table.location_id();
        let object_store = table.object_store();

        let now = Timestamp::now();
        let secs = now.0.as_secs() as i64;
        let nsecs = now.0.subsec_nanos();
        let metadata_db = table.metadata_db();

        let expired_stream = metadata_db.stream_expired_files(location_id, secs, nsecs);

        match DeletionOutput::try_from_manifest_stream(object_store, expired_stream, now).await {
            Ok(output) if output.len() > 0 => {
                tracing::info!(
                    "Deleted {} files ({} successes, {} not found, {} errors) for table {}",
                    output.len(),
                    output.successes.len(),
                    output.not_found.len(),
                    output.errors.len(),
                    table.table_ref()
                );
                output.update_manifest(&metadata_db).await.map_err(
                    DeletionError::manifest_update_error(
                        self.clone(),
                        [output.successes, output.not_found].concat(),
                    ),
                )?;

                if let Err(error) = consistency_check(&self.table)
                    .await
                    .map_err(DeletionError::consistency_check_error(&self))
                {
                    tracing::error!("{error}");
                    return Ok(self);
                }

                Ok(self)
            }
            Ok(_) => Ok(self),
            Err(err) => {
                tracing::error!("{err}");
                Ok(self)
            }
        }
    }
}

#[derive(Debug)]
struct DeletionOutput {
    successes: Vec<FileId>,
    not_found: Vec<FileId>,
    errors: Vec<(FileId, Box<ObjectStoreError>)>,
}

impl DeletionOutput {
    fn with_capacity(size: usize) -> Self {
        let successes = Vec::with_capacity(size);
        let not_found = Vec::with_capacity(size);
        let errors = Vec::with_capacity(size);

        DeletionOutput {
            successes,
            not_found,
            errors,
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn try_from_manifest_stream<'a>(
        object_store: Arc<dyn ObjectStore>,
        expired_stream: BoxStream<'a, Result<GcManifestRow, metadata_db::Error>>,
        now: Timestamp,
    ) -> Result<Self, metadata_db::Error> {
        let (file_ids, file_paths) = expired_stream
            .try_fold(
                (Vec::new(), Vec::new()),
                |(mut file_ids, mut file_paths),
                 GcManifestRow {
                     file_id,
                     file_path,
                     expiration,
                     ..
                 }| {
                    if Duration::from_micros(expiration.and_utc().timestamp_micros() as u64)
                        .saturating_sub(now.0)
                        .is_zero()
                    {
                        file_ids.push(file_id);
                        file_paths.push(Ok(Path::from(file_path)));
                    }

                    future::ok((file_ids, file_paths))
                },
            )
            .await?;

        let size = file_ids.len();

        tracing::debug!("Deleting {} expired files", size);

        Ok(object_store
            .delete_stream(stream::iter(file_paths).boxed())
            .enumerate()
            .fold(Self::with_capacity(size), move |mut acc, (idx, result)| {
                let file_id = file_ids.get(idx).expect("Index out of bounds");

                match result {
                    Ok(..) => acc.insert_success(*file_id),
                    Err(ObjectStoreError::NotFound { path, .. }) => acc.insert_not_found(*file_id, path),
                    Err(err) => acc.insert_error(*file_id, err),
                }
                future::ready(acc)
            })
            .await)
    }

    pub async fn update_manifest(
        &self,
        metadata_db: &MetadataDb,
    ) -> Result<(), metadata_db::Error> {
        if self.successes.is_empty() && self.not_found.is_empty() {
            return Ok(());
        }

        let file_ids = [self.successes(), self.not_found()].concat();

        metadata_db.delete_file_ids(&file_ids).await
    }

    pub fn len(&self) -> usize {
        self.successes.len() + self.not_found.len() + self.errors.len()
    }

    fn successes(&self) -> &[FileId] {
        &self.successes
    }

    #[tracing::instrument(skip_all)]
    fn insert_success(&mut self, file_id: FileId) {
        tracing::debug!("File deleted successfully: {file_id}");
        self.successes.push(file_id);
    }

    fn not_found(&self) -> &[FileId] {
        &self.not_found
    }

    #[tracing::instrument(skip_all)]
    fn insert_not_found(&mut self, file_id: FileId, path: String) {
        tracing::debug!("File not found: {path}");
        self.not_found.push(file_id);
    }

    #[tracing::instrument(skip_all)]
    fn insert_error(&mut self, file_id: FileId, err: ObjectStoreError) {
        tracing::error!("Error deleting file {file_id}: {err}");
        self.errors.push((file_id, Box::new(err)));
    }
}
