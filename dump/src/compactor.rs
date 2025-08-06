use std::{fmt::Display, sync::Arc, time::Duration};

use common::{
    BLOCK_NUM, BoxError, SPECIAL_BLOCK_NUM,
    catalog::{physical::PhysicalTable, reader::NozzleReaderFactory},
    metadata::{
        parquet::ParquetMeta,
        segments::{BlockRange, Chain, canonical_chain},
    },
    parquet::{
        arrow::arrow_reader::ArrowReaderMetadata,
        file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    physical_plan::{metrics::ExecutionPlanMetricsSet, stream::RecordBatchStreamAdapter},
};
use futures::{
    FutureExt, StreamExt, TryFutureExt, TryStreamExt,
    future::{self, BoxFuture},
    stream::{self, BoxStream},
};
use metadata_db::{FileId, FooterBytes, LocationId, MetadataDb};
use object_store::{Error as ObjectStoreError, ObjectMeta, path::Path};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use crate::{
    collector::{Collector, DeletionTask},
    parquet_writer::ParquetFileWriter,
};

pub static FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

pub type CompactionResult<T> = Result<T, CompactionError>;

pub struct NozzleCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl NozzleCompactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        metadata_db: Arc<MetadataDb>,
        opts: &ParquetWriterProperties,
        threshold: FileSizeLimit,
    ) -> Self {
        let compaction_task = Compactor::start(table.clone(), opts.clone(), threshold);
        let deletion_task = Collector::start(table, metadata_db);
        NozzleCompactor {
            compaction_task,
            deletion_task,
        }
    }

    pub async fn try_run(&mut self) {
        self.compaction_task.try_run().await;

        self.deletion_task.try_run().await;
    }
}

pub struct CompactionTask {
    task: JoinHandle<CompactionResult<Compactor>>,
    table: Arc<PhysicalTable>,
    opts: ParquetWriterProperties,
    threshold: FileSizeLimit,
}

impl CompactionTask {
    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    async fn try_run(&mut self) {
        if self.is_finished() {
            let task = &mut self.task;

            let compactor = match task
                .map_err(CompactionError::join_error(Compactor::new(
                    &self.table,
                    &self.opts,
                    self.threshold,
                )))
                .await
            {
                Ok(Ok(compactor)) => compactor,
                Ok(Err(CompactionError::JoinError { compactor, err }))
                | Err(CompactionError::JoinError { compactor, err }) => {
                    tracing::error!("{err}");
                    compactor
                }
                _ => unreachable!("Unexpected error while waiting for compaction task"),
            };

            self.task = tokio::spawn(compactor.compact());
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
    async fn commit_metadata(
        &self,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<(), metadata_db::Error> {
        let location_id = self.location_id;
        let file_name = self.file_name.clone();
        let object_size = self.object_size;
        let object_e_tag = self.object_e_tag.clone();
        let object_version = self.object_version.clone();
        let parquet_meta = self.parquet_meta.clone();
        let footer = self.footer.clone();

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
            .await
    }

    async fn upsert_gc_manifest(
        &self,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<(), metadata_db::Error> {
        metadata_db
            .upsert_gc_manifest(self.location_id, &self.file_ids, FILE_LOCK_DURATION)
            .await
    }
}

impl
    From<(
        (ParquetMeta, ObjectMeta, FooterBytes),
        LocationId,
        Vec<FileId>,
    )> for CompactionOutput
{
    fn from(
        (
            (
                parquet_meta,
                ObjectMeta {
                    location,
                    size: object_size,
                    e_tag: object_e_tag,
                    version: object_version,
                    ..
                },
                footer,
            ),
            location_id,
            file_ids,
        ): (
            (ParquetMeta, ObjectMeta, FooterBytes),
            LocationId,
            Vec<FileId>,
        ),
    ) -> Self {
        let file_name = parquet_meta.filename.to_string();
        let parquet_meta = serde_json::to_value(parquet_meta)
            .expect("Failed to serialize parquet metadata to JSON");

        CompactionOutput {
            location_id,
            file_ids,
            location,
            file_name,
            parquet_meta,
            object_size,
            object_e_tag,
            object_version,
            footer,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Compactor {
    table: Arc<PhysicalTable>,
    opts: ParquetWriterProperties,

    threshold: FileSizeLimit,
}

impl Compactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        opts: ParquetWriterProperties,
        threshold: FileSizeLimit,
    ) -> CompactionTask {
        CompactionTask {
            task: tokio::spawn(future::ok(Compactor::new(&table, &opts, threshold))),
            table,
            opts,
            threshold,
        }
    }

    fn new<'a>(
        table: &'a Arc<PhysicalTable>,
        opts: &'a ParquetWriterProperties,
        threshold: FileSizeLimit,
    ) -> Self {
        Compactor {
            table: Arc::clone(table),
            opts: opts.clone(),
            threshold,
        }
    }

    async fn compact(self) -> CompactionResult<Self> {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let table = Arc::clone(&self.table);

        let canonical_chain = match table
            .segments()
            .await
            .map(|segments| canonical_chain(segments).unwrap())
        {
            Ok(segments) => segments,
            Err(err) => {
                tracing::error!(
                    "Failed to retrieve segments for table {} at location {}: {}",
                    self.table.table_ref(),
                    self.table.location_id(),
                    err
                );
                return Ok(self);
            }
        };

        if canonical_chain.0.is_empty() {
            return Ok(self);
        }

        let reader_factory = Arc::clone(&table.reader_factory);
        let max_groups = canonical_chain.0.len() / 2 + 1;

        let mut reader_stream = reader_stream(canonical_chain, reader_factory, self.threshold);

        let mut compaction_groups = Vec::with_capacity(max_groups);
        let mut current_group = Vec::with_capacity(compaction_groups.capacity());
        let mut current_size = 0;

        while let Some((file_id, range, size, stream)) = reader_stream
            .try_next()
            .await
            .map_err(CompactionError::file_write_error)?
        {
            current_size += size;
            current_group.push((file_id, range, stream));

            if self.threshold.is_exceeded(current_size) {
                if current_group.len() > 1 {
                    compaction_groups.push(current_group);
                }

                let capacity = compaction_groups.capacity()
                    - compaction_groups.iter().map(|g| g.len()).sum::<usize>();

                current_group = Vec::with_capacity(capacity);
                current_size = 0;
            }
        }

        if current_group.len() > 1 {
            compaction_groups.push(current_group);
        }

        compaction_groups.shrink_to_fit();

        let mut join_set = compaction_groups.into_iter().fold(
            JoinSet::<CompactionResult<()>>::new(),
            |mut join_set, group| {
                let table = Arc::clone(&self.table);
                let metadata_db = table.metadata_db();

                let opts = self.opts.clone();

                let mut range = group.first().unwrap().1.clone();
                range.numbers = range.start()..=group.last().unwrap().1.end();

                join_set.spawn(try_do_compaction(table, metadata_db, opts, range, group));

                join_set
            },
        );

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(..)) => return Ok(self),
                Ok(Err(err)) => {
                    tracing::error!("{err}");
                    return Ok(self);
                }
                Err(err) => {
                    return Err(CompactionError::join_error(self)(err));
                }
            }
        }
        Ok(self)
    }
}

async fn try_do_compaction(
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
    opts: ParquetWriterProperties,
    range: BlockRange,
    group: Vec<(FileId, BlockRange, SendableRecordBatchStream)>,
) -> CompactionResult<()> {
    let compaction_output = try_write_compacted_file(table, opts, range, group).await?;

    try_update_metadata_db(metadata_db, compaction_output).await
}

async fn try_write_compacted_file(
    table: Arc<PhysicalTable>,
    opts: ParquetWriterProperties,
    range: BlockRange,
    group: Vec<(FileId, BlockRange, SendableRecordBatchStream)>,
) -> CompactionResult<Arc<CompactionOutput>> {
    let location_id = table.location_id();

    let mut writer = ParquetFileWriter::new(table, opts, range.start())
        .map_err(CompactionError::file_write_error)?;

    let mut file_ids = Vec::with_capacity(group.len());

    let mut stream = stream::iter(group).flat_map(|(file_id, _, stream)| {
        file_ids.push(file_id);
        stream
    });

    while let Some(batch) = stream
        .try_next()
        .await
        .map_err(CompactionError::file_write_error)?
    {
        writer
            .write(&batch)
            .await
            .map_err(CompactionError::file_write_error)?;
    }

    let writer_output: (ParquetMeta, ObjectMeta, Vec<u8>) = writer
        .close(range.clone())
        .await
        .map_err(CompactionError::file_write_error)?;

    let compaction_output = Arc::new(CompactionOutput::from((
        writer_output,
        location_id,
        file_ids,
    )));

    Ok(compaction_output)
}

async fn try_update_metadata_db(
    metadata_db: Arc<MetadataDb>,
    compaction_output: Arc<CompactionOutput>,
) -> CompactionResult<()> {
    compaction_output
        .commit_metadata(Arc::clone(&metadata_db))
        .await
        .map_err(CompactionError::metadata_commit_error(
            compaction_output.location.as_ref(),
        ))?;

    compaction_output
        .upsert_gc_manifest(Arc::clone(&metadata_db))
        .await
        .map_err(CompactionError::manifest_update_error(Arc::from(
            compaction_output.file_ids.clone(),
        )))?;

    Ok(())
}

pub fn delete_from_gc_manifest<
    'a,
    T: AsRef<[FileId]> + Send + Sync + 'a,
    E: From<(metadata_db::Error, &'a T)> + 'a,
>(
    metadata_db: &'a MetadataDb,
    file_ids: &'a T,
) -> BoxFuture<'a, Result<(), E>> {
    metadata_db
        .delete_file_ids(file_ids.as_ref())
        .map_err(move |err| E::from((err, file_ids)))
        .boxed()
}

pub fn reader_stream<'a>(
    canonical_chain: Chain,
    reader_factory: Arc<NozzleReaderFactory>,
    threshold: FileSizeLimit,
) -> BoxStream<'a, DataFusionResult<(FileId, BlockRange, i64, SendableRecordBatchStream)>> {
    let buffer_size = canonical_chain.0.len();

    stream::iter(canonical_chain.0)
        .enumerate()
        .map(move |(partition_index, segment)| {
            let reader_factory = Arc::clone(&reader_factory);
            let metrics = ExecutionPlanMetricsSet::new();

            async move {
                let file_id = segment.file_id.unwrap();
                let range = segment.range;
                let mut file_meta = FileMeta::from(segment.object);
                file_meta.extensions = Some(Arc::new(file_id));

                let mut reader: Box<dyn AsyncFileReader> =
                    reader_factory.create_reader(partition_index, file_meta, None, &metrics)?;
                let reader_metadata =
                    ArrowReaderMetadata::load_async(&mut reader, Default::default()).await?;

                let schema = Arc::clone(reader_metadata.schema());
                let size = threshold.get_size(&reader_metadata);

                let stream =
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, reader_metadata)
                        .build()?
                        .map_err(DataFusionError::from);

                let sendable_stream: SendableRecordBatchStream =
                    Box::pin(RecordBatchStreamAdapter::new(schema, stream));

                Ok::<_, DataFusionError>((file_id, range, size, sendable_stream))
            }
        })
        .buffered(buffer_size)
        .boxed()
}

#[derive(Debug)]
pub enum CompactionError {
    FileWriteError {
        err: BoxError,
    },
    #[allow(dead_code)]
    FileDeleteError {
        err: ObjectStoreError,
        location: Arc<str>,
        not_found: bool,
    },
    JoinError {
        compactor: Compactor,
        err: JoinError,
    },
    ManifestUpdateError {
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },
    ManifestDeleteError {
        err: metadata_db::Error,
        file_ids: Arc<[FileId]>,
    },
    MetadataCommitError {
        err: metadata_db::Error,
        location: Arc<str>,
    },
}

impl CompactionError {
    #[allow(dead_code)]
    pub fn cleanup_failed_compaction<'a, const MAX_TRIES: usize>(
        self,
        compactor: Compactor,
        metadata_db: Arc<MetadataDb>,
        location_id: LocationId,
        mut retry_count: usize,
        file_ids: Arc<[FileId]>,
    ) -> BoxFuture<'a, Compactor> {
        async move {

            match &self {
                // If the file write failed, no cleanup is needed, just log the error
                Self::FileWriteError { .. } => {
                    tracing::error!("{self}");
                    compactor
                }
                // If the commit to file_metadata failed, we need to delete the file
                Self::MetadataCommitError { location, .. } => {
                    if retry_count == 0 {
                        tracing::error!("{self}");
                    } else if retry_count > MAX_TRIES {
                        tracing::error!("Cleanup failed after {MAX_TRIES} attempts: {self}");
                        return compactor;
                    } else {
                        tracing::warn!(
                            "Retrying cleanup for compaction error: {self}, attempt {retry_count}/{MAX_TRIES}"
                        );
                    }

                    let location = Path::from(location.as_ref());

                    match compactor
                        .table
                        .object_store()
                        .delete(&location)
                        .map_err(CompactionError::file_delete_error(location.as_ref()))
                        .await
                    {
                        Ok(..)
                        | Err(Self::FileDeleteError {
                            not_found: true, ..
                        }) => compactor,
                        Err(err) => {
                            if retry_count > MAX_TRIES {
                                tracing::error!("Cleanup failed after {MAX_TRIES} attempts: {self}");
                                compactor
                            } else {
                                retry_count += 1;
                                tracing::info!(
                                    "Retrying cleanup for compaction location {location_id}, attempt {retry_count}/{MAX_TRIES}"
                                );
                                    err.cleanup_failed_compaction::<MAX_TRIES>(
                                        compactor,
                                        metadata_db,
                                        location_id,
                                        retry_count,
                                        file_ids,
                                    )
                                    .await
                            }
                        }
                    }
                }
                // If the manifest update failed, we need to try to mark the file IDs for deletion
                Self::ManifestUpdateError { file_ids, .. } => {
                    tracing::error!("{self}");
                    let location_id = compactor.table.location_id();
                    match metadata_db
                        .upsert_gc_manifest(location_id, &file_ids, FILE_LOCK_DURATION)
                        .map_err(Self::manifest_update_error(file_ids.clone()))
                        .await
                    {
                        Ok(..) => compactor,
                        Err(err) => {
                            if retry_count > MAX_TRIES {
                                tracing::error!("Cleanup failed: {err}");
                                compactor
                            } else {
                                retry_count += 1;
                                tracing::info!(
                                    "Retrying cleanup for compaction location {location_id}, attempt {retry_count}/{MAX_TRIES}"
                                );
                                err.cleanup_failed_compaction::<MAX_TRIES>(
                                    compactor,
                                    metadata_db,
                                    location_id,
                                    retry_count,
                                    Arc::clone(file_ids),
                                )
                                .await
                            }
                        }
                    }
                }
                Self::FileDeleteError {
                    not_found: true, ..
                } => compactor,
                Self::FileDeleteError {
                    location,
                    ..
                } => {
                    if retry_count > MAX_TRIES {
                        tracing::error!("Cleanup failed after {MAX_TRIES} attempts: {self}");
                        compactor
                    } else {
                        retry_count += 1;
                        tracing::info!(
                            "Retrying cleanup for compaction location {location_id}, attempt {retry_count}/{MAX_TRIES}"
                        );
                        match compactor
                            .table
                            .object_store()
                            .delete(&Path::from(location.as_ref()))
                            .map_err(CompactionError::file_delete_error(location.as_ref()))
                            .await
                        {
                            Ok(..)
                            | Err(Self::FileDeleteError {
                                not_found: true, ..
                            }) => compactor,
                            Err(err) => {
                                err.cleanup_failed_compaction::<MAX_TRIES>(
                                    compactor,
                                    metadata_db,
                                    location_id,
                                    retry_count,
                                    file_ids,
                                )
                                .await
                            }
                        }
                    }
                }
                Self::ManifestDeleteError { err, file_ids } => {
                    if retry_count > MAX_TRIES {
                        tracing::error!("Cleanup failed after {MAX_TRIES} attempts: {err}");
                        compactor
                    } else {
                        retry_count += 1;
                        tracing::info!(
                            "Retrying cleanup for compaction location {location_id}, attempt {retry_count}/{MAX_TRIES}"
                        );
                        match metadata_db
                            .upsert_gc_manifest(location_id, file_ids, FILE_LOCK_DURATION)
                            .map_err(Self::manifest_update_error(file_ids.clone()))
                            .await
                        {
                            Ok(..) => compactor,
                            Err(err) => {
                                err.cleanup_failed_compaction::<MAX_TRIES>(
                                    compactor,
                                    metadata_db,
                                    location_id,
                                    retry_count,
                                    file_ids.clone(),
                                )
                                .await
                            }
                        }
                    }
                }
                Self::JoinError { .. } => {
                    tracing::error!("{self}");
                    compactor
                }
            }
        }
        .boxed()
    }
}

impl CompactionError {
    fn metadata_commit_error(
        location: impl Into<Arc<str>>,
    ) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CompactionError::MetadataCommitError {
            err,
            location: location.into(),
        }
    }

    fn manifest_update_error(file_ids: Arc<[FileId]>) -> impl FnOnce(metadata_db::Error) -> Self {
        move |err| CompactionError::ManifestUpdateError { err, file_ids }
    }

    fn file_write_error<E: Into<BoxError>>(err: E) -> Self {
        CompactionError::FileWriteError { err: err.into() }
    }

    #[allow(dead_code)]
    fn file_delete_error(location: impl Into<Arc<str>>) -> impl FnOnce(ObjectStoreError) -> Self {
        move |err: ObjectStoreError| match err {
            ObjectStoreError::NotFound { .. } => CompactionError::FileDeleteError {
                err,
                location: location.into(),
                not_found: true,
            },
            _ => CompactionError::FileDeleteError {
                err,
                location: location.into(),
                not_found: false,
            },
        }
    }

    fn join_error(compactor: Compactor) -> impl FnOnce(JoinError) -> Self {
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

impl Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionError::FileWriteError { err, .. } => {
                write!(f, "Error occured while writing compacted file: {}", err)
            }
            CompactionError::FileDeleteError {
                err,
                location,
                not_found,
                ..
            } => {
                if *not_found {
                    write!(f, "File {} not found, no action taken", location)
                } else {
                    write!(f, "Error deleting file at {}: {}", location, err)
                }
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
                write!(f, "Compaction task for table {} failed: {}", table_ref, err)
            }
        }
    }
}

impl std::error::Error for CompactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CompactionError::FileWriteError { err, .. } => err.source(),
            CompactionError::FileDeleteError {
                not_found: true, ..
            } => None,
            CompactionError::FileDeleteError { err, .. } => err.source(),
            CompactionError::MetadataCommitError { err, .. } => err.source(),
            CompactionError::ManifestUpdateError { err, .. } => err.source(),
            CompactionError::ManifestDeleteError { err, .. } => err.source(),
            CompactionError::JoinError { err, .. } => err.source(),
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum FileSizeLimit {
    Block,
    Byte,
    #[allow(dead_code)]
    Row,
}

// impl Default f
impl FileSizeLimit {
    const BLOCK_THRESHOLD: i64 = 100_000;
    const ROW_THRESHOLD: i64 = 1_000_000; // 1 million rows
    const SIZE_THRESHOLD: i64 = 2 * 1024 * 1024 * 1024; // 2 GB

    pub fn is_exceeded(&self, size: i64) -> bool {
        match self {
            FileSizeLimit::Block => size >= Self::BLOCK_THRESHOLD,
            FileSizeLimit::Row => size >= Self::ROW_THRESHOLD,
            FileSizeLimit::Byte => size >= Self::SIZE_THRESHOLD,
        }
    }

    pub fn get_size(&self, reader_metadata: &ArrowReaderMetadata) -> i64 {
        match self {
            FileSizeLimit::Block => {
                let metadata = reader_metadata.metadata();

                let (distinct_block_count, ..) =
                    metadata
                        .row_groups()
                        .iter()
                        .fold((0, 0), |(mut distinct, mut pmax), rg| {
                            let column_descriptor = rg
                                .schema_descr()
                                .columns()
                                .iter()
                                .find(|c| c.name() == BLOCK_NUM || c.name() == SPECIAL_BLOCK_NUM)
                                .expect("Block number column not found in schema");

                            let column_chunk = rg
                                .columns()
                                .iter()
                                .find(|c| c.column_path() == column_descriptor.path())
                                .expect("Column stats not found for block number column");

                            let column_statistics = column_chunk.statistics().unwrap();

                            let min_block_num = column_statistics
                                .min_bytes_opt()
                                .map(|bytes| i64::from_le_bytes(bytes[0..8].try_into().unwrap()))
                                .unwrap_or_default();

                            let max_block_num = column_statistics
                                .max_bytes_opt()
                                .map(|bytes| i64::from_le_bytes(bytes[0..8].try_into().unwrap()))
                                .unwrap_or_default();

                            let distinct_block_count =
                                column_statistics.distinct_count_opt().unwrap_or_default() as i64;

                            distinct += min_block_num - pmax + distinct_block_count;

                            pmax = max_block_num;

                            (distinct, pmax)
                        });

                distinct_block_count
            }
            FileSizeLimit::Byte => reader_metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|rg| rg.total_byte_size())
                .sum(),
            FileSizeLimit::Row => reader_metadata.metadata().file_metadata().num_rows() as i64,
        }
    }
}
