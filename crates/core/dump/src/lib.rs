//! # Dump

use std::sync::Arc;

use amp_data_store::DataStore;
use common::{
    dataset_store::DatasetStore,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
};
use datasets_common::hash_reference::HashReference;
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};

pub mod block_ranges;
pub mod check;
pub mod compaction;
pub mod config;
mod derived_dataset;
pub mod metrics;
pub mod parquet_writer;
pub mod progress;
pub mod streaming_query;
pub mod tasks;

pub use self::{
    block_ranges::{EndBlock, ResolvedEndBlock},
    check::consistency_check,
    config::{
        CollectorConfig, CompactionAlgorithmConfig, CompactorConfig, ConfigDuration, ParquetConfig,
        SizeLimitConfig,
    },
    metrics::RECOMMENDED_METRICS_EXPORT_INTERVAL,
    progress::{
        NoOpProgressReporter, ProgressReporter, ProgressReporterExt, ProgressUpdate,
        SyncCompletedInfo, SyncFailedInfo, SyncStartedInfo,
    },
};
use crate::{
    compaction::{CollectorProperties, CompactorProperties, SegmentSizeLimit},
    config::Config,
    metrics::MetricsRegistry,
};

/// Dumps derived dataset tables. All tables must belong to the same dataset.
pub async fn dump_tables(
    ctx: Ctx,
    dataset: &HashReference,
    microbatch_max_interval: u64,
    end: EndBlock,
    writer: impl Into<Option<metadata_db::JobId>>,
    progress_reporter: Option<Arc<dyn ProgressReporter>>,
) -> Result<(), Error> {
    derived_dataset::dump(
        ctx,
        dataset,
        microbatch_max_interval,
        end,
        writer,
        progress_reporter,
    )
    .await
    .map_err(Error::DerivedDatasetDump)?;

    Ok(())
}

/// Errors that occur during dump_tables operations
///
/// This error type is used by the `dump_tables()` function to report issues encountered
/// when dumping derived dataset tables to Parquet files.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to dump derived dataset
    ///
    /// This occurs when the derived dataset dump operation fails. This wraps errors
    /// from the `derived_dataset::dump()` function which handles SQL query execution
    /// and Parquet file writing for derived datasets.
    ///
    /// Common causes:
    /// - Query environment creation failures
    /// - Consistency check failures
    /// - Manifest retrieval errors
    /// - Table dump failures
    /// - Parallel task execution failures
    #[error("Failed to dump derived dataset")]
    DerivedDatasetDump(#[source] derived_dataset::Error),
}

/// Dataset dump context
#[derive(Clone)]
pub struct Ctx {
    pub config: Config,
    pub metadata_db: MetadataDb,
    pub dataset_store: DatasetStore,
    pub data_store: DataStore,
    /// Shared notification multiplexer for streaming queries
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    /// Optional job-specific metrics registry
    pub metrics: Option<Arc<MetricsRegistry>>,
}

#[derive(Debug, Clone)]
pub struct WriterProperties {
    pub parquet: ParquetWriterProperties,
    pub compactor: CompactorProperties,
    pub collector: CollectorProperties,
    pub partition: SegmentSizeLimit,
    pub cache_size_mb: usize,
    pub max_row_group_bytes: usize,
}

pub fn parquet_opts(config: &ParquetConfig) -> Arc<WriterProperties> {
    // We have not done our own benchmarking, but the default 1_000_000 value for this adds about a
    // megabyte of storage per column, per row group. This analysis by InfluxData suggests that
    // smaller NDV values may be equally effective:
    // https://www.influxdata.com/blog/using-parquets-bloom-filters/
    let bloom_filter_ndv = 10_000;

    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    //
    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    let parquet = ParquetWriterProperties::builder()
        .set_compression(config.compression)
        .set_bloom_filter_ndv(bloom_filter_ndv)
        .set_bloom_filter_enabled(config.bloom_filters)
        .build();

    let collector = CollectorProperties::from(config);
    let compactor = CompactorProperties::from(config);
    let partition = SegmentSizeLimit::from(&config.target_size);
    let cache_size_mb = (config.cache_size_mb * 1024 * 1024) as usize;
    let max_row_group_bytes = (config.max_row_group_mb * 1024 * 1024) as usize;

    WriterProperties {
        parquet,
        compactor,
        collector,
        partition,
        cache_size_mb,
        max_row_group_bytes,
    }
    .into()
}
