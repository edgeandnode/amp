//! # Dump

use common::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties as ParquetWriterProperties,
};

mod core;
mod metrics; // unused for now
mod parquet_writer;
pub mod worker;

pub use core::*;

pub fn default_partition_size() -> u64 {
    4096 * 1024 * 1024 // 4 GB
}

pub fn default_microbatch_max_interval() -> u64 {
    100_000
}

pub fn default_parquet_opts() -> ParquetWriterProperties {
    parquet_opts(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()), true)
}

pub fn parquet_opts(compression: Compression, bloom_filters: bool) -> ParquetWriterProperties {
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
        .set_compression(compression)
        .set_bloom_filter_ndv(bloom_filter_ndv)
        .set_bloom_filter_enabled(bloom_filters)
        .build()
}
