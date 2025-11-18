//! # Dump

use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use common::{
    BoxError, catalog::physical::PhysicalTable, config::Config,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
    store::Store as DataStore,
};
use dataset_store::{DatasetKind, DatasetStore};
use metadata_db::{MetadataDb, NotificationMultiplexerHandle};
use monitoring::telemetry::metrics::Meter;

mod block_ranges;
mod check;
pub mod compaction;
mod derived_dataset;
pub mod metrics;
mod parquet_writer;
mod raw_dataset;
mod raw_dataset_writer;
pub mod streaming_query;
mod tasks;

pub use block_ranges::{EndBlock, ResolvedEndBlock};
pub use check::*;
pub use metrics::RECOMMENDED_METRICS_EXPORT_INTERVAL;

use crate::compaction::{AmpCompactor, CollectorProperties, CompactorProperties, SegmentSizeLimit};

/// Dataset dump context
#[derive(Clone)]
pub struct Ctx {
    pub config: Arc<Config>,
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub data_store: Arc<DataStore>,
    /// Shared notification multiplexer for streaming queries
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    /// Optional meter for job metrics
    pub meter: Option<Meter>,
}

/// Dumps a set of tables. All tables must belong to the same dataset.
pub async fn dump_tables(
    ctx: Ctx,
    tables: &[(Arc<PhysicalTable>, Arc<AmpCompactor>)],
    max_writers: u16,
    microbatch_max_interval: u64,
    end: EndBlock,
    metrics: Option<Arc<metrics::MetricsRegistry>>,
) -> Result<(), BoxError> {
    let mut kinds = BTreeSet::new();
    for (t, _) in tables {
        kinds.insert(DatasetKind::from_str(&t.dataset().kind)?);
    }

    if kinds.iter().any(|k| k.is_raw()) {
        if !kinds.iter().all(|k| k.is_raw()) {
            return Err("Cannot mix raw and non-raw datasets in a same dump".into());
        }
        raw_dataset::dump(ctx, tables, max_writers, end, metrics).await
    } else {
        derived_dataset::dump(
            ctx,
            tables,
            microbatch_max_interval,
            max_writers,
            end,
            metrics,
        )
        .await
    }
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
