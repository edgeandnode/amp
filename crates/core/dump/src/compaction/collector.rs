use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display, Formatter},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use amp_data_store::{DataStore, DeleteFilesStreamError};
use common::catalog::physical::PhysicalTable;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use metadata_db::{MetadataDb, files::FileId, gc::GcManifestRow};
use object_store::{Error as ObjectStoreError, path::Path};

use crate::{
    WriterProperties,
    compaction::error::{CollectionResult, CollectorError},
    config::ParquetConfig,
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
            interval: config.collector.min_interval.clone().into(),
            file_lock_duration: config.collector.deletion_lock_duration.clone().into(),
        }
    }
}
#[derive(Clone)]
pub struct Collector {
    metadata_db: MetadataDb,
    store: DataStore,
    table: Arc<PhysicalTable>,
    props: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
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
    pub fn new(
        metadata_db: MetadataDb,
        store: DataStore,
        props: Arc<WriterProperties>,
        table: Arc<PhysicalTable>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Collector {
            metadata_db,
            store,
            table,
            props,
            metrics,
        }
    }

    #[tracing::instrument(skip_all, err, fields(location_id=%self.table.location_id(), table=self.table.table_ref_compact()))]
    pub(super) async fn collect(self) -> CollectionResult<Self> {
        let table_name: Arc<str> = Arc::from(self.table.table_name().as_str());

        let location_id = self.table.location_id();

        let found_file_ids_to_paths: BTreeMap<FileId, Path> =
            metadata_db::gc::stream_expired(&self.metadata_db, location_id)
                .map_err(CollectorError::file_stream_error)
                .map(|manifest_row| {
                    let GcManifestRow {
                        file_id,
                        file_path: file_name,
                        ..
                    } = manifest_row?;

                    // Use relative path (table path + filename), not the absolute URL path
                    let path = self.table.path().child(file_name.as_str());
                    Ok::<_, CollectorError>((file_id, path))
                })
                .try_collect()
                .await?;

        tracing::debug!("Expired files found: {}", found_file_ids_to_paths.len());

        if found_file_ids_to_paths.is_empty() {
            return Ok(self);
        }

        if let Some(metrics) = &self.metrics {
            metrics.inc_expired_files_found(
                found_file_ids_to_paths.len(),
                self.table.table_name().to_string(),
            );
        }

        let file_ids_to_delete = found_file_ids_to_paths.keys().copied().collect::<Vec<_>>();
        let paths_to_remove =
            metadata_db::files::delete_by_ids(&self.metadata_db, &file_ids_to_delete)
                .await
                .map_err(CollectorError::file_metadata_delete(
                    found_file_ids_to_paths.keys(),
                ))?
                .into_iter()
                .filter_map(|file_id| found_file_ids_to_paths.get(&file_id).cloned())
                .collect::<BTreeSet<_>>();

        tracing::debug!("Metadata entries deleted: {}", paths_to_remove.len());

        if let Some(metrics) = &self.metrics {
            metrics.inc_expired_entries_deleted(
                paths_to_remove.len(),
                self.table.table_name().to_string(),
            );
        }

        let store = self.store.clone();
        let mut delete_stream =
            store.delete_files_stream(stream::iter(paths_to_remove).map(Ok).boxed());

        let mut files_deleted = 0;
        let mut files_not_found = 0;

        while let Some(path) = delete_stream.next().await {
            match path {
                Ok(path) => {
                    tracing::debug!("Deleted expired file: {}", path);
                    files_deleted += 1;
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_files_deleted(1, table_name.clone().to_string());
                    }
                }
                Err(DeleteFilesStreamError(ObjectStoreError::NotFound { path, .. })) => {
                    tracing::debug!("Expired file not found: {}", path);
                    files_not_found += 1;
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_files_not_found(1, table_name.to_string());
                    }
                }
                Err(DeleteFilesStreamError(err)) => {
                    tracing::debug!("Expired files deleted: {}", files_deleted);
                    tracing::debug!("Expired files not found: {}", files_not_found);
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_failed_collections(table_name.to_string());
                    }

                    return Err(err.into());
                }
            }
        }

        tracing::debug!("Expired files deleted: {}", files_deleted);
        tracing::debug!("Expired files not found: {}", files_not_found);

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.inc_successful_collections(table_name.to_string());
        }

        return Ok(self);
    }
}

impl Display for Collector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Garbage Collector {{ table: {}, opts: {:?} }}",
            self.table.table_ref(),
            self.props
        )
    }
}
