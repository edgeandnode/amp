//! # Compaction Planning and Grouping

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{
    ParquetFooterCache,
    catalog::{physical::PhysicalTable, reader::NozzleReaderFactory},
    metadata::{
        SegmentSize,
        segments::{BlockRange, Segment},
    },
    parquet::arrow::{
        ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderMetadata,
        async_reader::AsyncFileReader,
    },
};
use datafusion::{
    datasource::physical_plan::{FileMeta, ParquetFileReaderFactory},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::{metrics::ExecutionPlanMetricsSet, stream::RecordBatchStreamAdapter},
};
use futures::{
    Stream, StreamExt, TryFutureExt, TryStreamExt,
    stream::{self, BoxStream},
};
use metadata_db::FileId;

use crate::{
    WriterProperties,
    compaction::{CompactionResult, CompactorError, compactor::CompactionGroup},
};

pub struct CompactionFile {
    pub file_id: FileId,
    pub range: BlockRange,
    pub sendable_stream: SendableRecordBatchStream,
    pub size: SegmentSize,
    pub is_tail: bool,
}

impl CompactionFile {
    pub async fn try_new(
        reader_factory: Arc<NozzleReaderFactory>,
        partition_index: usize,
        segment: Segment,
        is_tail: bool,
    ) -> CompactionResult<Self> {
        let file_id = segment.id;
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

        let sendable_stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let compaction_item = CompactionFile {
            file_id,
            range,
            size,
            sendable_stream,
            is_tail,
        };

        Ok(compaction_item)
    }

    pub fn is_contiguous(&self, other: &CompactionFile) -> bool {
        self.range.network == other.range.network
            && self.range.start() <= self.range.end()
            && other.range.start() <= other.range.end()
            && (self.range.end() + 1 == other.range.start()
                || self.range.start() == other.range.end() + 1)
    }
}

/// A stream that yields groups of files to be compacted together based on the
/// the algorithm defined in `CompactionProperties`.
///
/// If an error occurs while processing the input stream of files, the error is logged
/// and the stream terminates. Any groups that have already been yielded will still be
/// available to the consumer of the stream to process.
pub struct CompactionPlan<'a> {
    /// Stream of files to be considered for compaction.
    files: BoxStream<'a, CompactionResult<CompactionFile>>,
    /// Compaction properties configuring the compaction algorithm
    /// and other properties of the compaction process.
    opts: Arc<WriterProperties>,
    /// The physical table being compacted.
    table: Arc<PhysicalTable>,
    /// The current group of files being built for compaction.
    current_group: CompactionGroup,
    /// The file currently being added to the current group.
    current_file: Option<CompactionFile>,
    /// The next candidate file to consider adding to the current group.
    current_candidate: Option<CompactionFile>,
    /// Indicates whether the stream has been fully processed.
    done: bool,
}

impl<'a> CompactionPlan<'a> {
    #[tracing::instrument(skip_all, err)]
    pub async fn from_table(
        table: Arc<PhysicalTable>,
        opts: Arc<WriterProperties>,
    ) -> CompactionResult<Self> {
        let chain = table
            .canonical_chain()
            .map_err(CompactorError::chain_error)
            .await?
            .ok_or(CompactorError::empty_chain())?;

        let size = chain.0.len();

        tracing::info!("Scanning {size} segments for compaction");

        let reader_factory: Arc<NozzleReaderFactory> = Arc::new(NozzleReaderFactory {
            location_id: table.location_id(),
            metadata_db: table.metadata_db().clone(),
            object_store: Arc::clone(&table.object_store()),
            parquet_footer_cache: ParquetFooterCache::builder(1).build(),
        });

        let files = stream::iter(chain.0)
            .enumerate()
            .map(move |(partition_index, segment)| {
                let reader_factory = Arc::clone(&reader_factory);
                let is_tail = partition_index == size - 1;
                CompactionFile::try_new(reader_factory, partition_index, segment, is_tail)
                    .map_err(CompactorError::from)
            })
            .buffered(10)
            .boxed();
        let current_group = CompactionGroup::new_empty(&opts, &table);

        Ok(Self {
            files,
            opts,
            table,
            current_group,
            current_file: None,
            current_candidate: None,
            done: false,
        })
    }

    pub fn try_compact_all(self) -> BoxStream<'a, CompactionResult<()>> {
        let write_concurrency = self.opts.compactor.write_concurrency;
        self.map(CompactionGroup::compact)
            .buffer_unordered(write_concurrency)
            .boxed()
    }
}

impl<'a> Stream for CompactionPlan<'a> {
    type Item = CompactionGroup;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let algorithm = this.opts.compactor.algorithm;

        Poll::Ready({
            // Loop through files, grouping them according to the compaction algorithm.
            loop {
                // If we're done processing files, return None.
                if this.done {
                    break None;
                // If we have a current file, add it to the current group and continue.
                } else if let Some(current_file) = this.current_file.take() {
                    this.current_group.push(current_file);
                // If we have a current candidate, check if it can be added to the current group.
                } else if let Some(candidate) = this.current_candidate.take() {
                    // If it can, update the current file and continue.
                    if algorithm.predicate(&this.current_group, &candidate) {
                        this.current_file = Some(candidate);
                    // If it can't, and the current group is empty or has a single file,
                    // start a new group with the candidate as the current file.
                    } else if this.current_group.is_empty_or_singleton() {
                        this.current_file = Some(candidate);
                        this.current_group = CompactionGroup::new_empty(&this.opts, &this.table);
                    // If it can't, and the current group has multiple files,
                    // yield the current group and start a new group with the
                    // candidate as the current file.
                    } else {
                        this.current_candidate = Some(candidate);
                        let group = std::mem::replace(
                            &mut this.current_group,
                            CompactionGroup::new_empty(&this.opts, &this.table),
                        );
                        break Some(group);
                    }
                // If we have no current file or candidate, poll the next file from the stream.
                } else {
                    match futures::ready!(this.files.as_mut().poll_next(cx)) {
                        // If we get a new file, set it as the current candidate and continue.
                        Some(Ok(candidate)) => {
                            this.current_candidate = Some(candidate);
                            continue;
                        }
                        // If we get an error, log it and stop processing.
                        Some(Err(err)) => {
                            tracing::error!("{err}");
                            this.done = true;
                            break None;
                        }
                        // If the stream is exhausted, and the current group is empty or has
                        // a single file, we're done.
                        None if this.current_group.is_empty_or_singleton() => {
                            this.done = true;
                            break None;
                        }
                        // Otherwise, yield the current group and finish processing by
                        // setting `done` to true.
                        None => {
                            let group = std::mem::replace(
                                &mut this.current_group,
                                CompactionGroup::new_empty(&this.opts, &this.table),
                            );
                            this.done = true;
                            break Some(group);
                        }
                    }
                }
            }
        })
    }
}
