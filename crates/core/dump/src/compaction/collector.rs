use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display, Formatter},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use common::{catalog::physical::PhysicalTable, config::ParquetConfig};
use futures::{StreamExt, TryStreamExt};
use metadata_db::{FileId, GcManifestRow};
use object_store::{Error as ObjectStoreError, path::Path};

use crate::{
    WriterProperties,
    compaction::error::{CollectionResult, CollectorError},
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
    pub fn new(
        table: &Arc<PhysicalTable>,
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Collector {
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            metrics: metrics.clone(),
        }
    }

    #[tracing::instrument(skip_all, err, fields(location_id=%self.table.location_id(), table=self.table.table_ref_compact()))]
    pub(super) async fn collect(self) -> CollectionResult<Self> {
        let table_name: Arc<str> = Arc::from(self.table.table_name().as_str());

        let metadata_db = self.table.metadata_db();

        let location_id = self.table.location_id();

        let found_file_ids_to_paths: BTreeMap<FileId, Path> = metadata_db
            .stream_expired_files(location_id)
            .map_err(CollectorError::file_stream_error)
            .map(|manifest_row| {
                let GcManifestRow {
                    file_id,
                    file_path: file_name,
                    ..
                } = manifest_row?;

                let url = self
                    .table
                    .url()
                    .join(&file_name)
                    .map_err(CollectorError::parse_error(file_id))?;
                let path = Path::from_url_path(url.path()).map_err(CollectorError::path_error)?;
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

        let paths_to_remove = metadata_db
            .delete_file_ids(found_file_ids_to_paths.keys())
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

        let object_store = self.table.object_store();
        let table_path = self.table.path();

        let (files_deleted, files_not_found) = object_store
            .list(Some(table_path))
            .try_filter(|object_meta| {
                futures::future::ready(paths_to_remove.contains(&object_meta.location))
            })
            .try_fold(
                (0, 0),
                |(mut files_deleted, mut files_not_found), object_meta| {
                    let object_store = Arc::clone(&object_store);
                    let table_name = Arc::clone(&table_name);
                    let location = object_meta.location.clone();
                    let metrics = self.metrics.as_ref();

                    async move {
                        tracing::debug!("Deleting expired file: {}", location);
                        match object_store.delete(&location).await {
                            Ok(_) => {
                                tracing::debug!("Deleted expired file: {}", location);
                                if let Some(metrics) = metrics {
                                    metrics.inc_files_deleted(1, table_name.clone().to_string());
                                }
                                files_deleted += 1;
                            }
                            Err(ObjectStoreError::NotFound { .. }) => {
                                tracing::debug!("Expired file not found: {}", location);
                                if let Some(metrics) = metrics {
                                    metrics.inc_files_not_found(1, table_name.to_string());
                                }
                                files_not_found += 1;
                            }
                            Err(e) => {
                                tracing::debug!("Expired files deleted: {}", files_deleted);
                                tracing::debug!("Expired files not found: {}", files_not_found);
                                if let Some(metrics) = &metrics {
                                    metrics.inc_failed_collections(table_name.to_string());
                                }

                                return Err(e);
                            }
                        };
                        Ok::<_, ObjectStoreError>((files_deleted, files_not_found))
                    }
                },
            )
            .await?;

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
            self.opts
        )
    }
}
