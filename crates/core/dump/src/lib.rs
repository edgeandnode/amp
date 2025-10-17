//! # Dump

use std::sync::Arc;

use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;

pub mod compaction;
mod core;
pub mod metrics;
mod parquet_writer;
mod raw_dataset_writer;
pub mod streaming_query;

pub use core::*;

pub use metrics::RECOMMENDED_METRICS_EXPORT_INTERVAL;

use crate::compaction::{CollectorProperties, CompactorProperties, SegmentSizeLimit};

#[derive(Debug, Clone)]
pub struct WriterProperties {
    pub parquet: ParquetWriterProperties,
    pub compactor: CompactorProperties,
    pub collector: CollectorProperties,
    pub partition: SegmentSizeLimit,
    pub cache_size_mb: usize,
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

    WriterProperties {
        parquet,
        compactor,
        collector,
        partition,
        cache_size_mb,
    }
    .into()
}
