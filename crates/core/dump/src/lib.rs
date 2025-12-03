//! # Dump

use std::sync::Arc;

use common::{
    catalog::physical::PhysicalTable,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
    store::Store as DataStore,
};
use dataset_store::{DatasetKind, DatasetStore};
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};

mod block_ranges;
mod check;
pub mod compaction;
pub mod config;
mod derived_dataset;
pub mod metrics;
mod parquet_writer;
mod raw_dataset;
mod raw_dataset_writer;
pub mod streaming_query;
mod tasks;

pub use self::{
    block_ranges::{EndBlock, ResolvedEndBlock},
    check::consistency_check,
    metrics::RECOMMENDED_METRICS_EXPORT_INTERVAL,
};
use crate::{
    compaction::{AmpCompactor, CollectorProperties, CompactorProperties, SegmentSizeLimit},
    config::Config,
    metrics::MetricsRegistry,
};

/// Dumps a set of tables. All tables must belong to the same dataset.
pub async fn dump_tables(
    ctx: Ctx,
    kind: DatasetKind,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    max_writers: u16,
    microbatch_max_interval: u64,
    end: EndBlock,
) -> Result<(), Error> {
    if kind.is_raw() {
        // Raw datasets (EvmRpc, EthBeacon, Firehose)
        raw_dataset::dump(ctx, tables, max_writers, end)
            .await
            .map_err(Error::RawDatasetDump)?;
    } else {
        // Derived datasets
        derived_dataset::dump(ctx, tables, microbatch_max_interval, end)
            .await
            .map_err(Error::DerivedDatasetDump)?;
    }

    Ok(())
}

/// Errors that occur during dump_tables operations
///
/// This error type is used by the `dump_tables()` function to report issues encountered
/// when dumping tables to Parquet files.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to dump raw dataset
    ///
    /// This occurs when the raw dataset dump operation fails. This wraps errors
    /// from the `raw_dataset::dump()` function which handles blockchain data
    /// extraction and Parquet file writing for raw datasets.
    ///
    /// Common causes:
    /// - Blockchain client connectivity issues
    /// - Consistency check failures
    /// - Invalid dataset kind
    /// - Partition task failures
    /// - Parquet file writing errors
    #[error("Failed to dump raw dataset")]
    RawDatasetDump(#[source] raw_dataset::Error),

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
    pub data_store: Arc<DataStore>,
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

pub fn parquet_opts(config: &common::config::ParquetConfig) -> Arc<WriterProperties> {
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
