use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use common::{
    BlockNum,
    catalog::{physical::PhysicalTable, reader::NozzleReaderFactory},
    metadata::{
        parquet::ParquetMeta,
        segments::{BlockRange, Chain, Segment, canonical_chain},
    },
    parquet::{
        arrow::{
            ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderMetadata,
            async_reader::AsyncFileReader,
        },
        file::properties::WriterProperties as ParquetWriterProperties,
    },
};
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::{metrics::ExecutionPlanMetricsSet, stream::RecordBatchStreamAdapter},
};
use futures::{
    FutureExt, StreamExt, TryFutureExt, TryStreamExt, future,
    stream::{self, BoxStream},
};
use metadata_db::{FileId, FooterBytes, LocationId, MetadataDb};
use object_store::{ObjectMeta, path::Path};
use tokio::task::JoinSet;

use super::size::SegmentSize;
use crate::{
    compaction::{
        COMPACTOR_INTERVAL, CompactionError, CompactionResult, CompactionTask, FILE_LOCK_DURATION,
        size::SegmentSizeLimit,
    },
    parquet_writer::ParquetFileWriter,
};

#[derive(Clone, Debug)]
pub struct Compactor {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) opts: CompactionProperties,
}

impl Compactor {
    pub fn start(table: &Arc<PhysicalTable>, opts: CompactionProperties) -> CompactionTask {
        CompactionTask {
            task: tokio::spawn(future::ok(Compactor::new(table, &opts)).boxed()),
            table: Arc::clone(table),
            opts,
            previous: None,
        }
    }

    pub(super) fn new<'a>(table: &'a Arc<PhysicalTable>, opts: &'a CompactionProperties) -> Self {
        Compactor {
            table: Arc::clone(table),
            opts: opts.clone(),
        }
    }

    #[tracing::instrument(skip_all, fields(location_id = self.table.location_id()))]
    pub(super) async fn compact(self) -> CompactionResult<Self> {
        let table = Arc::clone(&self.table);
        let metadata_db = table.metadata_db();
        let opts = self.opts.clone();

        // await: We need to await the PhysicalTable::segments method
        let compaction_stream = CompactionStream::from_table(table, opts).await?;

        // await: We need to collect the stream
        let mut compaction_groups = compaction_stream.into_compaction_groups().await.into_iter();

        let mut join_set = JoinSet::new();

        // await: We need to await the creation of a compaction group (which awaits reading from the metadata db)
        while let Some(group) = compaction_groups.next() {
            join_set.spawn(group.compact(Arc::clone(&metadata_db)));
        }

        // await: We need to await the completion of compaction tasks
        while let Some(result) = join_set.join_next().await {
            match result {
                // Happy path
                Ok(Ok(..)) => continue,
                // Error occurred during compaction, trace it and move on
                Ok(Err(err)) => tracing::error!("{err}"),
                // Something went wrong with the join, trace it and move on
                Err(err) => {
                    tracing::error!("{err}");
                }
            }
        }

        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub struct CompactionProperties {
    pub compactor_interval: std::time::Duration,
    pub metadata_concurrency: usize,
    pub parquet_writer_props: ParquetWriterProperties,
    pub size_limit: SegmentSizeLimit,
}

impl CompactionProperties {
    pub fn new(
        metadata_concurrency: usize,
        opts: &ParquetWriterProperties,
        size_limit: SegmentSizeLimit,
    ) -> Self {
        let parquet_writer_props = opts.clone();

        Self {
            compactor_interval: COMPACTOR_INTERVAL,
            metadata_concurrency,
            parquet_writer_props,
            size_limit,
        }
    }
}

impl Display for CompactionProperties {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_map()
            .entry(&"metadata_concurrency", &self.metadata_concurrency)
            .entry(&"parquet_writer_props", &self.parquet_writer_props)
            .entry(&"size_limit", &self.size_limit)
            .finish()
    }
}

pub struct CompactionStream<'stream> {
    pub table: Arc<PhysicalTable>,
    pub opts: CompactionProperties,
    pub range: BlockRange,
    pub remain: usize,
    pub stream: BoxStream<'stream, CompactionResult<CompactionFile>>,
}

impl CompactionStream<'_> {
    #[tracing::instrument(skip_all, fields(opts = format!("{opts}")))]
    pub async fn from_table(
        table: Arc<PhysicalTable>,
        opts: CompactionProperties,
    ) -> CompactionResult<Self> {
        let chain = table
            .segments()
            .map_ok(|segments| canonical_chain(segments).unwrap_or_else(|| Chain(Vec::new())))
            .map_err(CompactionError::chain_error(table.as_ref()))
            .await?;

        let remain = chain.0.len();
        tracing::info!("Scanning {} segments for compaction", remain);

        let range = chain.range();
        let reader_factory = Arc::clone(&table.reader_factory);

        let stream = stream::iter(chain.0)
            .enumerate()
            .map(move |(partition_index, segment)| {
                CompactionFile::try_new(Arc::clone(&reader_factory), partition_index, segment)
            })
            .buffered(opts.metadata_concurrency)
            .boxed();

        Ok(Self {
            table,
            opts,
            range,
            remain,
            stream,
        })
    }

    pub async fn next_file(&mut self) -> Option<CompactionResult<CompactionFile>> {
        (self.remain > 0)
            .then_some(self.stream.next().await.inspect(|_| self.remain -= 1))
            .flatten()
    }

    pub async fn next_group(&mut self) -> CompactionResult<Option<CompactionGroup>> {
        let mut streams = Vec::new();
        let mut total_size = SegmentSize::default();
        let mut start = self.range.start();

        while let Some(file) = self.next_file().await {
            match file {
                // No error we add the file to the group and optionally return the group
                Ok(file) => {
                    start = start.min(file.range.start());
                    total_size += file.size;
                    streams.push(file);

                    let (size_exceeded, length_exceeded) =
                        self.opts.size_limit.is_exceeded(&total_size);

                    // Size threshold met and length threshold met, return the group
                    if size_exceeded && length_exceeded {
                        return CompactionGroup::try_new(
                            streams,
                            &self.table,
                            &self.opts.parquet_writer_props,
                            start,
                        )
                        .map(Some);
                    // Size threshold met but length threshold not met
                    } else if size_exceeded {
                        streams = Vec::new();
                        total_size = SegmentSize::default();
                    } else if self.remain == 0 && length_exceeded {
                        return CompactionGroup::try_new(
                            streams,
                            &self.table,
                            &self.opts.parquet_writer_props,
                            start,
                        )
                        .map(Some);
                    }
                }
                // If there is an error, we print it and stop creating any more groups
                Err(err) => {
                    tracing::error!("{err}");
                    self.remain = 0;

                    // We treat what ever the current group is as the final group
                    if total_size.length >= self.opts.size_limit.0.length {
                        return CompactionGroup::try_new(
                            streams,
                            &self.table,
                            &self.opts.parquet_writer_props,
                            start,
                        )
                        .map(Some);
                    }
                }
            }
        }

        // If we reach here, it means we have exhausted all files, and the current group is not valid
        Ok(None)
    }

    /// This function will return an iterator over the compaction groups created from the
    /// files in the stream.
    #[tracing::instrument(skip_all, fields(opts = format!("{}", self.opts)))]
    pub async fn into_compaction_groups(mut self) -> Vec<CompactionGroup> {
        let mut groups = Vec::new();

        while let Some(group) = self.next_group().await.transpose() {
            match group {
                Ok(group) => {
                    groups.push(group);
                }
                Err(err) => {
                    tracing::error!("{err}");
                }
            }
        }

        groups
    }
}

pub struct CompactionGroup {
    pub location_id: LocationId,
    pub streams: Vec<CompactionFile>,
    pub writer: ParquetFileWriter,
}

impl CompactionGroup {
    pub fn try_new(
        streams: Vec<CompactionFile>,
        table: &Arc<PhysicalTable>,
        opts: &ParquetWriterProperties,
        start: BlockNum,
    ) -> CompactionResult<Self> {
        let writer = ParquetFileWriter::new(Arc::clone(table), opts, start)
            .map_err(CompactionError::create_writer_error(table, opts))?;

        let location_id = table.location_id();

        Ok(Self {
            streams,
            writer,
            location_id,
        })
    }

    #[tracing::instrument(skip_all, fields(location_id = self.location_id))]
    async fn write_and_finish(mut self) -> CompactionResult<CompactionWriterOutput> {
        let range = {
            let start_range = &self
                .streams
                .first()
                .expect("At least one stream in group")
                .range;

            let end_range = &self
                .streams
                .last()
                .expect("At least one stream in group")
                .range;

            let network = start_range.network.to_owned();
            let numbers = start_range.start()..=end_range.end();

            BlockRange {
                network,
                numbers,
                hash: end_range.hash,
                prev_hash: start_range.prev_hash,
            }
        };

        let mut file_ids = Vec::with_capacity(self.streams.len());

        for file in self.streams.iter_mut() {
            file_ids.push(file.file_id);
            while let Some(ref batch) = file.stream.try_next().await? {
                self.writer.write(batch).await?;
            }
        }

        let (parquet_meta, object_meta, footer) = self
            .writer
            .close(range)
            .await
            .map_err(|err| CompactionError::FileWriteError { err })?;

        Ok(CompactionWriterOutput::new(
            self.location_id,
            parquet_meta,
            object_meta,
            file_ids,
            footer,
        ))
    }

    pub async fn compact(self, metadata_db: Arc<MetadataDb>) -> CompactionResult<()> {
        let output = self.write_and_finish().await?;

        output
            .commit_metadata(Arc::clone(&metadata_db))
            .await
            .map_err(CompactionError::metadata_commit_error(
                output.location.as_ref(),
            ))?;

        output
            .upsert_gc_manifest(Arc::clone(&metadata_db))
            .await
            .map_err(CompactionError::manifest_update_error(&output.file_ids))?;

        Ok(())
    }
}

pub struct CompactionFile {
    pub file_id: FileId,
    pub range: BlockRange,
    pub stream: SendableRecordBatchStream,
    pub size: SegmentSize,
}

impl CompactionFile {
    pub async fn try_new(
        reader_factory: Arc<NozzleReaderFactory>,
        partition_index: usize,
        segment: Segment,
    ) -> CompactionResult<Self> {
        let file_id = segment.file_id.expect("Segment must have a FileId");
        let range = segment.range;

        let mut file_meta = FileMeta::from(segment.object);

        file_meta.extensions = Some(Arc::new(file_id));

        let mut input: Box<dyn AsyncFileReader> = reader_factory.create_reader(
            partition_index,
            file_meta,
            None,
            &ExecutionPlanMetricsSet::new(),
        )?;

        let reader_metadata =
            ArrowReaderMetadata::load_async(&mut input, Default::default()).await?;

        let schema = Arc::clone(reader_metadata.schema());
        let size = SegmentSize::from(&reader_metadata);

        let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(input, reader_metadata)
            .build()?
            .map_err(DataFusionError::from);

        let sendable_stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let compaction_item = CompactionFile {
            file_id,
            range,
            size,
            stream: sendable_stream,
        };

        Ok(compaction_item)
    }
}

#[derive(Debug)]
pub struct CompactionWriterOutput {
    pub location_id: LocationId,
    pub file_ids: Vec<FileId>,
    pub location: Path,
    pub file_name: String,
    pub parquet_meta: serde_json::Value,
    pub object_size: u64,
    pub object_e_tag: Option<String>,
    pub object_version: Option<String>,
    pub footer: FooterBytes,
}

impl CompactionWriterOutput {
    fn new(
        location_id: LocationId,
        parquet_meta: ParquetMeta,
        ObjectMeta {
            location,
            size: object_size,
            e_tag: object_e_tag,
            version: object_version,
            ..
        }: ObjectMeta,
        file_ids: Vec<FileId>,
        footer: FooterBytes,
    ) -> Self {
        let file_name = parquet_meta.filename.to_string();
        let parquet_meta = serde_json::to_value(parquet_meta)
            .expect("Failed to serialize parquet metadata to JSON");

        CompactionWriterOutput {
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
