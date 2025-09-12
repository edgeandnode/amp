//! # Dump

use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;

pub mod compaction;
mod core;
pub mod metrics;
mod parquet_writer;
pub mod streaming_query;
pub mod worker;

pub use core::*;

pub use metrics::RECOMMENDED_METRICS_EXPORT_INTERVAL;

use crate::compaction::CompactionProperties;

pub fn default_partition_size() -> u64 {
    4096 * 1024 * 1024 // 4 GB
}

pub fn parquet_opts(config: &common::config::ParquetConfig) -> ParquetWriterProperties {
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
    ParquetWriterProperties::builder()
        .set_compression(config.compression)
        .set_bloom_filter_ndv(bloom_filter_ndv)
        .set_bloom_filter_enabled(config.bloom_filters)
        .build()
}

pub fn compaction_opts(
    config: &common::config::CompactionConfig,
    parquet_writer_props: &ParquetWriterProperties,
) -> CompactionProperties {
    CompactionProperties::from_config(config, parquet_writer_props)
}
