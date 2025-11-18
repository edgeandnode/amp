use std::{
    fmt::{Debug, Display, Formatter},
    ops::RangeInclusive,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::{
    BlockNum, ParquetFooterCache,
    catalog::physical::PhysicalTable,
    config::ParquetConfig,
    metadata::{SegmentSize, segments::BlockRange},
};
use futures::{StreamExt, TryStreamExt, stream};
use metadata_db::MetadataDb;
use monitoring::logging;

use crate::{
    WriterProperties,
    compaction::{
        CompactionAlgorithm, CompactionResult, CompactorError,
        plan::{CompactionFile, CompactionPlan},
    },
    metrics::MetricsRegistry,
    parquet_writer::{ParquetFileWriter, ParquetFileWriterOutput},
};

#[derive(Debug, Clone)]
pub struct CompactorProperties {
    pub active: Arc<AtomicBool>,
    pub algorithm: CompactionAlgorithm,
    pub interval: Duration,
    pub metadata_concurrency: usize,
    pub write_concurrency: usize,
}

impl<'a> From<&'a ParquetConfig> for CompactorProperties {
    fn from(config: &'a ParquetConfig) -> Self {
        CompactorProperties {
            active: Arc::new(AtomicBool::new(config.compactor.active)),
            algorithm: CompactionAlgorithm::from(config),
            interval: config.compactor.min_interval.clone().into(),
            metadata_concurrency: config.compactor.metadata_concurrency,
            write_concurrency: config.compactor.write_concurrency,
        }
    }
}

#[derive(Clone)]
pub struct Compactor {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) cache: ParquetFooterCache,
    pub(super) opts: Arc<WriterProperties>,
    pub(super) metrics: Option<Arc<MetricsRegistry>>,
}

impl Debug for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compactor {{ table: {}, algorithm: {} }}",
            self.table.table_ref(),
            self.opts.compactor.algorithm.kind()
        )
    }
}

impl Display for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compactor {{ opts: {:?}, table: {} }}",
            self.opts,
            self.table.table_ref()
        )
    }
}

impl Compactor {
    pub fn new(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
    ) -> Self {
        Compactor {
            table: Arc::clone(table),
            cache: cache.clone(),
            opts: Arc::clone(opts),
            metrics: metrics.clone(),
        }
    }

    #[tracing::instrument(skip_all, fields(table = self.table.table_ref_compact()))]
    pub(super) async fn compact(self) -> CompactionResult<Self> {
        if !self.opts.compactor.active.load(Ordering::SeqCst) {
            return Ok(self);
        }

        let snapshot = self
            .table
            .snapshot(false, self.cache.clone())
            .await
            .map_err(CompactorError::chain_error)?;
        let opts = Arc::clone(&self.opts);
        let table_name = self.table.table_name();

        // await: We need to await the PhysicalTable::segments method
        let mut comapction_futures_stream = if let Some(plan) =
            CompactionPlan::from_snapshot(&snapshot, opts, &self.metrics).await?
        {
            let groups = plan.collect::<Vec<_>>().await;
            tracing::debug!(
                table = %table_name,
                group_count = groups.len(),
                "Compaction Groups: {:?}",
                groups
            );

            stream::iter(groups)
                .map(|group| group.compact())
                .buffered(self.opts.compactor.write_concurrency)
        } else {
            return Ok(self);
        };

        while let Some(res) = comapction_futures_stream.next().await {
            match res {
                Ok(block_num) => {
                    tracing::info!(
                        table = %table_name,
                        block_num,
                        "compaction group completed successfully"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_successful_compactions(table_name.to_string(), block_num);
                    }
                }
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        error_source = logging::error_source(&err),
                        table = %table_name,
                        "compaction failed"
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_failed_compactions(table_name.to_string());
                    }
                }
            }
        }

        Ok(self)
    }
}

pub struct CompactionGroup {
    pub opts: Arc<WriterProperties>,
    pub metrics: Option<Arc<MetricsRegistry>>,
    pub size: SegmentSize,
    pub streams: Vec<CompactionFile>,
    pub table: Arc<PhysicalTable>,
}

impl Debug for CompactionGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ file_count: {}, range: {:?} }}",
            self.streams.len(),
            self.range()
        )
    }
}

impl CompactionGroup {
    pub fn new_empty(
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
        table: &Arc<PhysicalTable>,
    ) -> Self {
        CompactionGroup {
            opts: Arc::clone(opts),
            metrics: metrics.clone(),
            size: SegmentSize::default(),
            streams: Vec::new(),
            table: Arc::clone(table),
        }
    }

    pub fn push(&mut self, file: CompactionFile) {
        self.size += file.size;
        self.streams.push(file);
    }

    pub fn is_empty_or_singleton(&self) -> bool {
        self.streams.len() <= 1
    }

    async fn write_and_finish(self) -> CompactionResult<ParquetFileWriterOutput> {
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

        let mut writer = ParquetFileWriter::new(
            Arc::clone(&self.table),
            &self.opts,
            range.start(),
            self.opts.max_row_group_bytes,
        )
        .map_err(CompactorError::create_writer_error(&self.opts))?;

        let mut parent_ids = Vec::with_capacity(self.streams.len());
        for mut file in self.streams {
            while let Some(ref batch) = file.sendable_stream.try_next().await? {
                writer.write(batch).await?;
            }
            parent_ids.push(file.file_id);
        }
        // Increment generation for new file
        let generation = self.size.generation + 1;

        writer
            .close(range, parent_ids, generation)
            .await
            .map_err(CompactorError::FileWrite)
    }

    #[tracing::instrument(skip_all, fields(files = self.len(), start = self.range().start(), end = self.range().end()))]
    pub async fn compact(self) -> CompactionResult<BlockNum> {
        let start = *self.range().start();
        let start_time = std::time::Instant::now();
        let metadata_db = self.table.metadata_db().clone();
        let duration = self.opts.collector.file_lock_duration;

        // Calculate input metrics before compaction
        let input_file_count = self.streams.len() as u64;
        let input_bytes: u64 = self.streams.iter().map(|s| s.size.bytes).sum();

        // Extract values before move
        let metrics = self.metrics.clone();
        let table_name = self.table.table_name().to_string();
        let location_id = *self.table.location_id();

        let output = self.write_and_finish().await?;

        // Calculate output metrics
        let output_bytes = output.object_meta.size;
        let duration_millis = start_time.elapsed().as_millis() as f64;

        // Record metrics if available
        if let Some(ref metrics) = metrics {
            metrics.record_compaction(
                table_name,
                location_id,
                input_file_count,
                input_bytes,
                output_bytes,
                duration_millis,
            );
        }

        output
            .commit_metadata(&metadata_db)
            .await
            .map_err(CompactorError::metadata_commit_error)?;

        output
            .upsert_gc_manifest(&metadata_db, duration)
            .await
            .map_err(CompactorError::manifest_update_error(&output.parent_ids))?;

        tracing::info!("Compaction Success: {}", output.object_meta.location,);

        Ok(start)
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    pub fn range(&self) -> RangeInclusive<BlockNum> {
        let start = self
            .streams
            .first()
            .expect("At least one file in group")
            .range
            .start();
        let end = self
            .streams
            .last()
            .expect("At least one file in group")
            .range
            .end();
        start..=end
    }
}

impl ParquetFileWriterOutput {
    async fn commit_metadata(&self, metadata_db: &MetadataDb) -> Result<(), metadata_db::Error> {
        let location_id = self.location_id;
        let file_name = self.object_meta.location.filename().unwrap().to_string();
        let object_size = self.object_meta.size;
        let object_e_tag = self.object_meta.e_tag.clone();
        let object_version = self.object_meta.version.clone();
        let parquet_meta = serde_json::to_value(&self.parquet_meta).unwrap();
        let footer = &self.footer;

        metadata_db
            .register_file(
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
        metadata_db: &MetadataDb,
        duration: Duration,
    ) -> Result<(), metadata_db::Error> {
        metadata_db
            .upsert_gc_manifest(self.location_id, &self.parent_ids, duration)
            .await
    }
}
