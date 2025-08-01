use std::{collections::HashMap, sync::Arc, time::Duration};

use common::{
    BoxError, Timestamp,
    catalog::physical::PhysicalTable,
    metadata::segments::{Segment, canonical_chain},
    parquet::{
        arrow::arrow_reader::ArrowReaderMetadata,
        file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use dashmap::{DashMap, DashSet};
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::{StreamExt, TryStreamExt, stream};
use metadata_db::{
    FileId, FileLeaseId, FileLeaseRow, FooterBytes, GcStatus, LocationId, MetadataDb,
};
use object_store::{Error as ObjectStoreError, ObjectMeta, path::Path};
use tokio::{sync::mpsc, task::JoinSet};

use crate::parquet_writer::ParquetFileWriter;

struct FileLease {
    _id: FileLeaseId,
    _gc_status: GcStatus,
    _location_id: LocationId,
    file_id: FileId,
    location: Path,
    _created_at: Option<Timestamp>,
    _expires_at: Option<Timestamp>,
}

impl From<FileLeaseRow> for FileLease {
    fn from(
        FileLeaseRow {
            id,
            location_id,
            gc_status,
            file_id,
            file_path,
            created_at,
            expires_at,
        }: FileLeaseRow,
    ) -> Self {
        let location = Path::from(file_path);

        let created_at = created_at.map(|ts| {
            Timestamp(Duration::from_micros(
                ts.and_utc().timestamp_micros().abs() as u64
            ))
        });

        let expires_at = expires_at.map(|ts| {
            Timestamp(Duration::from_micros(
                ts.and_utc().timestamp_micros().abs() as u64
            ))
        });

        FileLease {
            _id: id,
            _location_id: location_id,
            _gc_status: gc_status,
            file_id,
            location,
            _created_at: created_at,
            _expires_at: expires_at,
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CompactionOutput {
    location_id: LocationId,
    file_ids: Vec<FileId>,
    location: Path,
    file_name: String,
    parquet_meta: serde_json::Value,
    object_size: u64,
    object_e_tag: Option<String>,
    object_version: Option<String>,
    footer: FooterBytes,
}

impl CompactionOutput {
    async fn update_metadata(&self, metadata_db: Arc<MetadataDb>) -> Result<(), BoxError> {
        let location_id = self.location_id;
        let file_name = self.file_name.clone();
        let object_size = self.object_size;
        let object_e_tag = self.object_e_tag.clone();
        let object_version = self.object_version.clone();
        let parquet_meta = self.parquet_meta.clone();
        let footer = self.footer.clone();
        let component_ids = self.file_ids.as_slice();

        metadata_db
            .insert_metadata(
                location_id,
                file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta,
                footer,
            )
            .await?;

        let gc_status = GcStatus::Pending;

        metadata_db
            .create_leases(gc_status, location_id, component_ids)
            .await?;

        Ok(())
    }

    async fn revert_changes(&self, object_store: Arc<dyn object_store::ObjectStore>) {
        if let Err(err) = object_store.delete(&self.location).await {
            tracing::error!(
                "Failed to delete compacted file {}: {}",
                self.file_name,
                err
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TableType {
    Raw,
}

impl TableType {
    pub fn get_size(&self, segment: &Segment) -> usize {
        match self {
            _ => segment.object.size as usize,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Compactor {
    tables: HashMap<LocationId, Arc<PhysicalTable>>,
    table_type: TableType,
    opts: ParquetWriterProperties,

    outstanding_deletion_tasks: DashMap<LocationId, DashSet<Arc<CompactionOutput>>>,
}

impl Compactor {
    pub fn new(tables: Vec<Arc<PhysicalTable>>, opts: &ParquetWriterProperties) -> Self {
        let tables = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
            acc.insert(table.location_id(), table);
            acc
        });
        Compactor {
            tables,
            table_type: TableType::Raw,
            opts: opts.clone(),
            outstanding_deletion_tasks: DashMap::new(),
        }
    }

    pub fn spawn(
        tables: &[Arc<PhysicalTable>],
        opts: ParquetWriterProperties,
    ) -> Arc<mpsc::UnboundedSender<Arc<LocationId>>> {
        let (trigger, mut receiver) = mpsc::unbounded_channel::<Arc<LocationId>>();

        let tables = tables.to_vec();
        tokio::spawn(async move {
            let opts = Arc::new(opts);
            let compactor = Compactor::new(tables, &opts);

            while let Some(location_id) = receiver.recv().await {
                compactor.compact(*location_id).await?;
                compactor.set_lease().await?;
                compactor.delete_expired_files().await?;
            }

            Ok::<(), BoxError>(())
        });

        Arc::new(trigger)
    }

    async fn compact(&self, location_id: LocationId) -> Result<(), BoxError> {
        let table = self
            .tables
            .get(&location_id)
            .ok_or_else(|| format!("Table with location_id {} not found", location_id))?;
        let reader_factory = Arc::clone(&table.reader_factory);

        let cannonical_segments = table
            .segments()
            .await
            .map(|segments| canonical_chain(segments).map(|c| c.0).unwrap_or_default())?;

        if cannonical_segments.is_empty() {
            return Ok(());
        }

        let mut current_chunk = Vec::new();
        let mut current_size = 0;
        let mut join_set = JoinSet::<Result<Arc<CompactionOutput>, BoxError>>::new();

        for segment in cannonical_segments {
            current_size += self.table_type.get_size(&segment);
            current_chunk.push(segment);
            if current_size >= 2 * 1024 * 1024 * 1024 {
                if current_chunk.len() > 1 {
                    let chain = canonical_chain(current_chunk).ok_or(BoxError::from(format!(
                        "Failed to create canonical chain from segments for table {}",
                        table.table_name()
                    )))?;
                    let opts = self.opts.clone();
                    let table = Arc::clone(&table);
                    let reader_factory = Arc::clone(&reader_factory);
                    let metrics = ExecutionPlanMetricsSet::new();

                    join_set.spawn(async move {
                        let range = chain.range();

                        let mut writer =
                            ParquetFileWriter::new(Arc::clone(&table), opts, range.start())?;
                        let mut file_ids = Vec::new();

                        for (partition_index, segment) in chain.0.into_iter().enumerate() {
                            let file_id = segment.file_id.unwrap();

                            let file_meta = FileMeta {
                                object_meta: segment.object,
                                extensions: Some(Arc::new(file_id)),
                                range: None,
                                metadata_size_hint: None,
                            };

                            let mut async_file_reader: Box<dyn AsyncFileReader> = reader_factory
                                .create_reader(partition_index, file_meta, None, &metrics)?;

                            let reader_metadata = ArrowReaderMetadata::load_async(
                                &mut async_file_reader,
                                Default::default(),
                            )
                            .await?;

                            let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(
                                async_file_reader,
                                reader_metadata,
                            )
                            .build()?;

                            while let Some(ref batch) = stream.try_next().await? {
                                writer.write(batch).await?;
                            }
                            file_ids.push(file_id);
                        }

                        let (
                            parquet_meta,
                            ObjectMeta {
                                size: object_size,
                                e_tag: object_e_tag,
                                version: object_version,
                                location,
                                ..
                            },
                            footer,
                        ) = writer.close(range).await?;

                        let file_name = parquet_meta.filename.to_string();
                        let parquet_meta = serde_json::to_value(parquet_meta).map_err(|e| {
                            BoxError::from(format!("Failed to serialize metadata: {}", e))
                        })?;

                        Ok(Arc::new(CompactionOutput {
                            location_id,
                            file_name,
                            location,
                            parquet_meta,
                            object_size,
                            object_e_tag,
                            object_version,
                            file_ids,
                            footer,
                        }))
                    });
                }

                current_chunk = Vec::new();
                current_size = 0;
            }
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(compaction)) => {
                    self.outstanding_deletion_tasks
                        .entry(location_id.clone())
                        .and_modify(|entry| {
                            entry.insert(Arc::clone(&compaction));
                        })
                        .or_insert_with(|| DashSet::from_iter(vec![Arc::clone(&compaction)]));
                }
                Ok(Err(err)) => tracing::error!("Compaction failed: {}", err),
                Err(err) => tracing::error!(
                    "Unable to join compaction task for location {}: {}",
                    location_id,
                    err
                ),
            }
        }

        Ok(())
    }

    async fn set_lease(&self) -> Result<(), BoxError> {
        stream::iter(self.tables.iter())
            .for_each_concurrent(2, |(location_id, table)| async move {
                let metadata_db = table.metadata_db();
                let object_store = table.object_store();

                let tasks = self
                    .outstanding_deletion_tasks
                    .remove(location_id)
                    .map(|(_, tasks)| tasks)
                    .unwrap_or_default();

                for task in tasks.iter() {
                    if let Err(err) = task.update_metadata(Arc::clone(&metadata_db)).await {

                        tracing::error!(
                            "Failed to update metadata for compaction output for location {}: {}, deleting compacted file",
                            location_id,
                            err
                        );

                        task.revert_changes(Arc::clone(&object_store)).await;
                    }
                }
            })
            .await;

        Ok(())
    }

    async fn delete_expired_files(&self) -> Result<(), BoxError> {
        stream::iter(self.tables.iter())
            .map(Ok)
            .try_for_each_concurrent(10, |(location_id, table)| async move {
                let metadata_db = table.metadata_db();
                let object_store = &table.object_store();

                metadata_db
                    .stream_expired_file_leases(*location_id)
                    .map_err(BoxError::from)
                    .try_for_each(|file_lease| async move {
                        let FileLease {
                            ref location,
                            ref file_id,
                            ..
                        } = FileLease::from(file_lease);

                        match object_store.delete(location).await {
                            Ok(..) => {
                                tracing::info!(
                                    "Deleted expired file {} from location {}",
                                    location,
                                    location_id
                                );

                                let component_ids = vec![file_id.clone()];

                                table
                                    .metadata_db()
                                    .create_leases(
                                        GcStatus::Pending,
                                        *location_id,
                                        component_ids.as_slice(),
                                    )
                                    .await?;

                                Ok(())
                            }
                            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
                            Err(err) => Err(BoxError::from(err)),
                        }
                    })
                    .await
            })
            .await?;

        Ok(())
    }
}
