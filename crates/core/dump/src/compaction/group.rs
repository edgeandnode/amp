//! # Compaction Planning and Grouping

use std::sync::Arc;

use common::{
    ParquetFooterCache,
    catalog::{physical::PhysicalTable, reader::NozzleReaderFactory},
    metadata::segments::{BlockRange, Segment},
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
    StreamExt, TryFutureExt, TryStreamExt,
    stream::{self, BoxStream},
};
use metadata_db::FileId;

use crate::compaction::{
    CompactionProperties, CompactionResult, CompactorError, SegmentSize, compactor::CompactionGroup,
};
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

pub struct CompactionGroupGenerator<'stream> {
    pub stream: BoxStream<'stream, CompactionResult<CompactionFile>>,
    pub opts: Arc<CompactionProperties>,
    pub remain: usize,
    pub table: Arc<PhysicalTable>,
}

impl CompactionGroupGenerator<'_> {
    pub async fn from_table(
        table: Arc<PhysicalTable>,
        opts: Arc<CompactionProperties>,
    ) -> CompactionResult<Self> {
        let chain = table
            .canonical_chain()
            .map_err(CompactorError::chain_error)
            .await?
            .ok_or(format!("Compaction chain not found for table {}", table.table_ref()).into())
            .map_err(CompactorError::chain_error)?;

        let remain = chain.0.len();
        tracing::info!("Scanning {remain} segments for compaction");

        let reader_factory: Arc<NozzleReaderFactory> = Arc::new(NozzleReaderFactory {
            location_id: table.location_id(),
            metadata_db: table.metadata_db().clone(),
            object_store: Arc::clone(&table.object_store()),
            parquet_footer_cache: ParquetFooterCache::builder(1).build(),
        });

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
            remain,
            stream,
        })
    }

    pub async fn next_file(&mut self) -> Option<CompactionResult<CompactionFile>> {
        (self.remain > 0)
            .then_some(self.stream.next().await.inspect(|_| self.remain -= 1))
            .flatten()
    }

    #[tracing::instrument(skip_all, err, fields(opts = self.opts.to_string()))]
    pub async fn next_group(&mut self) -> CompactionResult<Option<CompactionGroup>> {
        let mut streams = Vec::new();
        let mut total_size = SegmentSize::default();

        while let Some(file) = self.next_file().await {
            match file {
                // No error we add the file to the group and optionally return the group
                Ok(file) => {
                    total_size += file.size;
                    streams.push(file);

                    let (size_exceeded, length_exceeded) =
                        self.opts.size_limit.is_exceeded(&total_size);

                    // Size threshold met and length threshold met, return the group
                    if size_exceeded && length_exceeded {
                        tracing::info!(
                            "Compaction group with {} created for table {}",
                            streams.len(),
                            self.table.table_ref()
                        );
                        return CompactionGroup::new(streams, &self.table, &self.opts).map(Some);
                    // Size threshold met but length threshold not met
                    } else if size_exceeded {
                        streams = Vec::new();
                        total_size = SegmentSize::default();
                    } else if self.remain == 0 && length_exceeded {
                        return CompactionGroup::new(streams, &self.table, &self.opts).map(Some);
                    }
                }
                // If there is an error, we print it and stop creating any more groups
                Err(err) => {
                    tracing::error!("{err}");
                    self.remain = 0;

                    // We treat what ever the current group is as the final group
                    if total_size.length >= self.opts.size_limit.0.length {
                        return CompactionGroup::new(streams, &self.table, &self.opts).map(Some);
                    }
                }
            }
        }

        // If we reach here, it means we have exhausted all files, and the current group is not valid
        Ok(None)
    }

    /// This function will return an iterator over the compaction groups created from the
    /// files in the stream.
    #[tracing::instrument(skip_all, fields(opts = self.opts.to_string()))]
    pub async fn into_compaction_groups(mut self) -> Vec<CompactionGroup> {
        let mut groups = Vec::new();

        while let Some(group) = self.next_group().await.transpose() {
            match group {
                Ok(group) => {
                    tracing::debug!(
                        "Compaction group with {} segments created for table {}",
                        group.streams.len(),
                        self.table.table_ref()
                    );

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
