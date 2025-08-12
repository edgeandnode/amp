use std::{
    pin,
    sync::Arc,
    task::{Context, Poll},
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
    arrow::array::RecordBatch,
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::{metrics::ExecutionPlanMetricsSet, stream::RecordBatchStreamAdapter},
};
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
    future::{self, BoxFuture},
    stream::{self, BoxStream},
};
use metadata_db::{FileId, FooterBytes, LocationId, MetadataDb};
use object_store::{ObjectMeta, path::Path};
use tokio::{
    sync::mpsc::{self, error::SendError},
    task::JoinSet,
};
pub use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    compaction::{
        CompactionResult, CompactionTask, FILE_LOCK_DURATION, FileSize, FileSizeLimit,
        error::CompactionError,
    },
    parquet_writer::ParquetFileWriter,
};

type FileStreamSender = mpsc::UnboundedSender<CompactionFile>;
type FileStreamReceiver = mpsc::UnboundedReceiver<CompactionFile>;

#[derive(Clone, Debug)]
pub struct Compactor {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) opts: ParquetWriterProperties,
    pub(super) size_limit: FileSizeLimit,
}

impl Compactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        opts: &ParquetWriterProperties,
        size_limit: FileSizeLimit,
    ) -> CompactionTask {
        let size_limit = size_limit.into();

        CompactionTask {
            task: tokio::spawn(future::ok(Compactor::new(&table, &opts, size_limit))),
            table,
            opts: opts.clone(),
            size_limit,
        }
    }

    pub(super) fn new<'a>(
        table: &'a Arc<PhysicalTable>,
        opts: &'a ParquetWriterProperties,
        size_limit: FileSizeLimit,
    ) -> Self {
        Compactor {
            table: Arc::clone(table),
            opts: opts.clone(),
            size_limit,
        }
    }

    #[tracing::instrument(skip_all, fields(location_id = self.table.location_id()))]
    pub(super) async fn compact(self) -> CompactionResult<Self> {
        let table = Arc::clone(&self.table);
        let size_limit = self.size_limit.clone();
        let opts = self.opts.clone();

        // await: We need to await the PhysicalTable::segments method
        let compaction_stream = CompactionStream::from_table::<8>(table, opts, size_limit).await?;

        let mut compaction_futures = compaction_stream.into_futures();

        let mut join_set = JoinSet::new();

        // await: We need to await the creation of a compaction group (which awaits reading from the metadata db)
        while let Some(future) = compaction_futures.next().await {
            join_set.spawn(future);
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

/// A stream of compaction jobs.
pub(super) struct CompactionStream<'a> {
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
    inner: BoxStream<'a, CompactionResult<CompactionFile>>,
    statistics_concurrency: usize,
    size_limit: FileSizeLimit,
    opts: ParquetWriterProperties,
    range: BlockRange,
}

impl<'a> CompactionStream<'a> {
    #[tracing::instrument(skip_all, fields(location_id = table.location_id(), statistics_concurrency = BUFFER_SIZE))]
    pub async fn from_table<const BUFFER_SIZE: usize>(
        table: Arc<PhysicalTable>,
        opts: ParquetWriterProperties,
        size_limit: FileSizeLimit,
    ) -> CompactionResult<Self> {
        let chain = table
            .segments()
            .map_ok(|segments| canonical_chain(segments).unwrap_or_else(|| Chain(Vec::new())))
            .map_err(CompactionError::chain_error(table.as_ref()))
            .await?;

        tracing::info!("Scanning {} segments for compaction", chain.0.len());

        let reader_factory = Arc::clone(&table.reader_factory);
        let metadata_db = Arc::clone(&table.metadata_db());

        let range = chain.range();

        let inner = stream::iter(chain.0)
            .enumerate()
            .map(move |(partition_index, segment)| {
                CompactionFile::new(reader_factory.clone(), partition_index, segment, size_limit)
            })
            .buffered(BUFFER_SIZE)
            .boxed();

        let statistics_concurrency = BUFFER_SIZE;

        Ok(Self {
            opts,
            range,
            table,
            inner,
            size_limit,
            metadata_db,
            statistics_concurrency,
        })
    }

    #[tracing::instrument(skip_all, fields(location_id = %self.table.location_id(), statistics_concurrency = self.statistics_concurrency))]
    pub fn into_futures(self) -> BoxStream<'a, BoxFuture<'a, CompactionResult<()>>> {
        self.take_while(|result| match result {
            // If no error occurred while constructing the file group, we can send the future to the join set
            Ok(_) => future::ready(true),
            // If an error occurred, we log it and stop producing any more futures
            Err(err) => {
                tracing::error!("{err}");
                future::ready(false)
            }
        })
        .filter_map(move |result| async move {
            match result {
                Ok(file_group) => Some(file_group.try_compaction().boxed()),
                _ => None,
            }
        })
        .boxed()
    }
}

impl<'a> Stream for CompactionStream<'a> {
    type Item = CompactionResult<CompactionFileGroup<'a>>;

    /// See [`CompactionFileGroupBuilder`]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        let range = stream.range.clone();
        let size_limit = stream.size_limit;

        match CompactionFileGroup::builder(size_limit, range).try_build(stream, cx) {
            Some(Ok(Some(file_group))) => Poll::Ready(Some(Ok(file_group))),
            Some(Ok(None)) => Poll::Pending,
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

pub(super) struct CompactionFileGroup<'a> {
    /// A stream of `CompactionFiles`
    inner: BoxStream<'a, CompactionFile>,
    /// The writer for the new compacted file output
    writer: ParquetFileWriter,
    /// The range of blocks being compacted
    range: BlockRange,
    /// The size of the compacted file output
    size: FileSize,
    /// The location ID of the compacted file
    location_id: LocationId,
    /// The file IDs being compacted
    file_ids: Vec<FileId>,
    /// The metadata database
    metadata_db: Arc<MetadataDb>,
}

impl CompactionFileGroup<'_> {
    fn builder(size_limit: FileSizeLimit, range: BlockRange) -> CompactionFileGroupBuilder {
        let (sender, receiver) = mpsc::unbounded_channel();
        CompactionFileGroupBuilder {
            start: BlockNum::MAX,
            stop: BlockNum::MIN,
            size: FileSize::default(),
            length: 0,
            range,
            size_limit,
            sender,
            receiver,
        }
    }

    async fn write(&mut self) -> CompactionResult<()> {
        use CompactionError::{FileStreamError, FileWriteError};
        let mut stream = pin::pin!(&mut self.inner);

        // We flatten the inner stream (a stream of streams of `RecordBatches`) and produce batches in order
        while let Some(ref batch) = stream
            .as_mut()
            .flatten()
            .try_next()
            .await
            .map_err(|err| FileStreamError { err })?
        {
            self.writer
                .write(batch)
                .await
                .map_err(|err| FileWriteError { err: err.into() })?;
        }

        Ok(())
    }

    async fn close(mut self) -> CompactionResult<CompactionWriterOutput> {
        use CompactionError::FileWriteError;
        let range = self.range.clone();
        let file_ids = std::mem::take(&mut self.file_ids);
        let location_id = std::mem::take(&mut self.location_id);

        self.writer
            .close(range)
            .and_then(move |(parquet_meta, location, footer)| async move {
                Ok(CompactionWriterOutput::new(
                    location_id,
                    parquet_meta,
                    location,
                    file_ids,
                    footer,
                ))
            })
            .await
            .map_err(|err| FileWriteError { err })
    }

    #[tracing::instrument(skip_all, fields(location_id = %self.location_id, file_count = self.file_ids.len(), size = self.size.value(), start = self.range.start(), stop = self.range.end()))]
    async fn try_compaction(mut self) -> CompactionResult<()> {
        tracing::info!("Beginning compaction");

        let metadata_db = Arc::clone(&self.metadata_db);

        // await: We need to await reading the stream of record batches and writing them to
        self.write().await?;

        let output = self.close().await?;

        tracing::info!(
            "Completed writing {} bytes to {}",
            output.object_size,
            output.location()
        );

        tracing::info!("Committing metadata for new file at {}", output.location());
        output
            .commit_metadata(metadata_db.clone())
            .await
            .map_err(|err| CompactionError::MetadataCommitError {
                err,
                location: output.location(),
            })?;

        tracing::info!("Updating GC manifest");
        output
            .upsert_gc_manifest(metadata_db)
            .await
            .map_err(|err| CompactionError::ManifestUpdateError {
                err,
                file_ids: output.file_ids(),
            })?;

        tracing::info!("Compaction completed");
        Ok(())
    }
}

impl Stream for CompactionFileGroup<'_> {
    type Item = CompactionFile;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(file)) => {
                this.file_ids.push(file.file_id);
                Poll::Ready(Some(file))
            }
            poll => poll,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact_size = self.file_ids.capacity() - self.file_ids.len();
        (exact_size, Some(exact_size))
    }
}

struct CompactionFileGroupBuilder {
    start: BlockNum,
    stop: BlockNum,
    range: BlockRange,
    size: FileSize,
    size_limit: FileSizeLimit,
    length: usize,
    sender: FileStreamSender,
    receiver: FileStreamReceiver,
}

impl CompactionFileGroupBuilder {
    #[tracing::instrument(skip_all, fields(start = self.start, stop = self.stop, size = self.size.value(), size_limit = self.size_limit.value()))]
    fn finish<'a>(
        self,
        stream: &CompactionStream<'a>,
    ) -> CompactionResult<CompactionFileGroup<'a>> {
        let opts = stream.opts.clone();
        let table = &stream.table;
        let location_id = table.location_id();
        let metadata_db = Arc::clone(&stream.metadata_db);

        let BlockRange {
            network,
            hash,
            prev_hash,
            ..
        } = stream.range.clone();

        let numbers = self.start..=self.stop;

        let range = BlockRange {
            numbers,
            network,
            hash,
            prev_hash,
        };

        ParquetFileWriter::new(Arc::clone(table), &opts, self.start)
            .map(|writer| {
                tracing::info!("Building compaction task");

                let inner = UnboundedReceiverStream::from(self.receiver).boxed();

                CompactionFileGroup {
                    inner,
                    writer,
                    range,
                    size: self.size,
                    location_id,
                    metadata_db,
                    file_ids: Vec::with_capacity(self.length),
                }
            })
            .map_err(CompactionError::create_writer_error(table, &opts))
    }

    // Loops through items of a `CompactionStream` and generates a `CompactionFileGroup`
    pub fn try_build<'a>(
        mut self,
        stream: &mut CompactionStream<'a>,
        cx: &mut Context<'_>,
    ) -> Option<CompactionResult<Option<CompactionFileGroup<'a>>>> {
        loop {
            match stream.inner.poll_next_unpin(cx) {
                // if we receive a pending poll, we continue to wait for the next item.
                Poll::Pending => continue,
                // If we receive a file, we update the group and attempt to finish it.
                Poll::Ready(Some(Ok(file))) => {
                    self.update(file);

                    // Check if the group is ready to be finished and sent to a writer.
                    match (
                        self.size_limit_criteria_met(),
                        self.file_count_criteria_met(),
                    ) {
                        // If the size limit is reached and there are multiple files, we finish the group.
                        (true, true) => {
                            return Some(self.finish(&stream).map(Some));
                        }
                        // If the size limit is reached but there is only one file we skip the group.
                        (true, false) => return Some(Ok(None)),
                        // If the size limit is not reached, we continue to collect files.
                        (false, _) => continue,
                    }
                }
                // If an error occurs while processing the file, we return the error.
                Poll::Ready(Some(Err(error))) => return Some(Err(error)),
                // If the stream is finished and we have collected files, we finish the group
                // if there are multiple files; else we return None to indicate the underlying
                // stream has finished.
                Poll::Ready(None) => {
                    return self
                        .file_count_criteria_met()
                        .then_some(self.finish(&stream).map(Some));
                }
            }
        }
    }

    /// Check if the size limit criteria is met. Currently a greater than test
    ///
    /// TODO: more nuanced criteria and implementation
    fn size_limit_criteria_met(&self) -> bool {
        self.size_limit.is_exceeded(self.size)
    }

    /// Check if the file count criteria is met. Currently a greater than 1 test
    ///
    /// TODO: more nuanced criteria and implementation
    fn file_count_criteria_met(&self) -> bool {
        self.length > 1
    }

    fn update(&mut self, file: CompactionFile) {
        if self.start > file.range.start() {
            self.start = file.range.start();
            self.range.prev_hash = file.range.prev_hash;
        }

        if self.stop < file.range.end() {
            self.stop = file.range.end();
            self.range.hash = file.range.hash;
        }

        self.size += file.size;

        self.length += 1;

        self.sender.send(file).unwrap();
    }
}

pub(super) struct CompactionFile {
    file_id: FileId,
    range: BlockRange,
    size: FileSize,
    stream: SendableRecordBatchStream,
}

impl CompactionFile {
    pub async fn new(
        reader_factory: Arc<NozzleReaderFactory>,
        partition_index: usize,
        segment: Segment,
        size_limit: FileSizeLimit,
    ) -> CompactionResult<Self> {
        let file_id = segment.file_id.unwrap();
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
        let size = size_limit.get_size(&reader_metadata);

        let stream = ParquetRecordBatchStreamBuilder::new_with_metadata(input, reader_metadata)
            .build()?
            .map_err(DataFusionError::from);

        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let compaction_item = CompactionFile {
            file_id,
            range,
            size,
            stream: sendable_stream,
        };

        Ok(compaction_item)
    }
}

impl From<SendError<CompactionFile>> for CompactionError {
    fn from(_: SendError<CompactionFile>) -> Self {
        CompactionError::SendError {}
    }
}

impl Stream for CompactionFile {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(super) struct CompactionWriterOutput {
    location_id: LocationId,
    file_ids: Vec<FileId>,
    pub(super) location: Path,
    file_name: String,
    parquet_meta: serde_json::Value,
    object_size: u64,
    object_e_tag: Option<String>,
    object_version: Option<String>,
    footer: FooterBytes,
}

impl CompactionWriterOutput {
    pub(super) fn new(
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
    pub(super) fn location(&self) -> Arc<str> {
        self.location.as_ref().into()
    }

    pub(super) fn file_ids(&self) -> Arc<[FileId]> {
        Arc::from(self.file_ids.as_slice())
    }

    pub(super) async fn commit_metadata(
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

    pub(super) async fn upsert_gc_manifest(
        &self,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<(), metadata_db::Error> {
        metadata_db
            .upsert_gc_manifest(self.location_id, &self.file_ids, FILE_LOCK_DURATION)
            .await
    }
}
