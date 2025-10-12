use std::{
    fmt::{Debug, Display, Formatter},
    ops::Deref,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use common::{catalog::physical::PhysicalTable, config::ParquetConfig};
use futures::{
    FutureExt, StreamExt, TryStreamExt, future,
    stream::{self, BoxStream},
};
use metadata_db::{FileId, GcManifestRow, MetadataDb};
use object_store::{Error as ObjectStoreError, ObjectStore, path::Path};

use crate::{
    WriterProperties,
    compaction::{
        AmpCompactorTaskType,
        error::{CollectionResult, CollectorError},
    },
    consistency_check,
    metrics::MetricsRegistry,
};

#[derive(Debug, Clone)]
pub struct CollectorProperties {
    pub active: Arc<AtomicBool>,
    pub interval: Duration,
    pub file_lock_duration: Duration,
}

impl<'a> From<&'a ParquetConfig> for CollectorProperties {
    fn from(config: &'a ParquetConfig) -> Self {
        CollectorProperties {
            active: Arc::new(AtomicBool::new(config.collector.active)),
            interval: config.collector.min_interval,
            file_lock_duration: config.collector.deletion_lock_duration,
        }
    }
}

#[derive(Clone)]
pub struct Collector {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) opts: Arc<WriterProperties>,
    pub(super) metrics: Option<Arc<MetricsRegistry>>,
}

impl Debug for Collector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Garbage Collector {{ table: {} }}",
            self.table.table_ref()
        )
    }
}

impl Collector {
    #[tracing::instrument(skip_all, err)]
    pub(super) async fn collect(self) -> CollectionResult<Self> {
        let metadata_db = self.table.metadata_db();

        let location_id = self.table.location_id();

        let expired_stream = metadata_db.stream_expired_files(location_id);

        match DeletionOutput::try_from_manifest_stream(Arc::clone(&self.table), expired_stream)
            .await
        {
            Ok(output) if output.len() > 0 => {
                if let Some(metrics) = &self.metrics {
                    let dataset = self.table.dataset().name.as_str();
                    let table_name = self.table.table_name();
                    output.update_metrics(metrics, dataset, table_name);
                }

                output.update_manifest(metadata_db).await.map_err(|err| {
                    CollectorError::file_metadata_delete(
                        [output.successes, output.not_found].concat(),
                        err,
                    )
                })?;

                if let Err(error) = consistency_check(&self.table)
                    .await
                    .map_err(CollectorError::consistency_check_error)
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

impl Display for Collector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Garbage Collector {{ table: {}, opts: {:?} }}",
            self.table.table_ref(),
            self.opts
        )
    }
}

impl AmpCompactorTaskType for Collector {
    type Error = CollectorError;

    fn new(
        table: &Arc<PhysicalTable>,
        _cache: &common::ParquetFooterCache,
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Collector {
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            metrics: metrics.clone(),
        }
    }

    fn run<'a>(self) -> future::BoxFuture<'a, Result<Self, Self::Error>> {
        self.collect().boxed()
    }

    fn interval(opts: &Arc<WriterProperties>) -> Duration {
        opts.collector.interval
    }

    fn active(opts: &Arc<WriterProperties>) -> bool {
        opts.collector
            .active
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn deactivate(opts: &mut Arc<WriterProperties>) {
        opts.collector
            .active
            .store(false, std::sync::atomic::Ordering::SeqCst);
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

    #[tracing::instrument(skip_all, fields(table=%table.table_ref()))]
    pub async fn try_from_manifest_stream<'a>(
        table: Arc<PhysicalTable>,
        expired_stream: BoxStream<'a, Result<GcManifestRow, metadata_db::Error>>,
    ) -> CollectionResult<Self> {
        let (file_ids, file_paths) = expired_stream
            .map_err(CollectorError::file_stream_error)
            .try_fold(
                (Vec::new(), Vec::new()),
                async |(mut file_ids, mut file_paths),
                       GcManifestRow {
                           file_id,
                           file_path: file_name,
                           ..
                       }| {
                    let url = table
                        .url()
                        .join(&file_name)
                        .map_err(CollectorError::parse_error(file_id))?;

                    file_ids.push(file_id);
                    file_paths.push(Ok(Path::from(url.path())));

                    Ok((file_ids, file_paths))
                },
            )
            .await?;

        let size = file_ids.len();

        Ok(table
            .object_store()
            .delete_stream(stream::iter(file_paths).boxed())
            .enumerate()
            .fold(Self::with_capacity(size), move |mut acc, (idx, result)| {
                let file_id = file_ids.get(idx).expect("Index out of bounds");

                match result {
                    Ok(..) => acc.insert_success(*file_id),
                    Err(ObjectStoreError::NotFound { .. }) => acc.insert_not_found(*file_id),
                    Err(err) => acc.insert_error(*file_id, err),
                }
                future::ready(acc)
            })
            .await)
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_manifest(
        &self,
        metadata_db: &MetadataDb,
    ) -> Result<(), metadata_db::Error> {
        if self.successes.is_empty() && self.not_found.is_empty() {
            return Ok(());
        }

        let file_ids = [self.successes(), self.not_found()].concat();
        tracing::debug!(
            "Deleting {:?} from metadata-db",
            file_ids.iter().map(Deref::deref)
        );

        metadata_db.delete_file_ids(&file_ids).await
    }

    fn update_metrics(&self, metrics: &Arc<MetricsRegistry>, dataset: &str, table: &str) {
        metrics.inc_files_deleted(
            self.successes().len(),
            dataset.to_string(),
            table.to_string(),
        );
        metrics.inc_files_not_found(
            self.not_found().len(),
            dataset.to_string(),
            table.to_string(),
        );
        metrics.inc_files_failed_to_delete(
            self.errors.len(),
            dataset.to_string(),
            table.to_string(),
        );
    }

    pub fn len(&self) -> usize {
        self.successes.len() + self.not_found.len() + self.errors.len()
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

    fn insert_error(&mut self, file_id: FileId, err: ObjectStoreError) {
        tracing::warn!("Error deleting file {file_id}: {err}");
        self.errors.push((file_id, Box::new(err)));
    }
}
