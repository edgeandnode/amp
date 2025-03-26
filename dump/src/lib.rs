mod job;
mod metrics; // unused for now
pub mod operator;
mod parquet_writer;
pub mod worker;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use common::catalog::physical::Catalog;
use common::catalog::physical::PhysicalDataset;
use common::catalog::physical::PhysicalTable;
use common::config::Config;
use common::meta_tables::scanned_ranges;
use common::multirange::MultiRange;
use common::parquet;
use common::parquet::basic::ZstdLevel;
use common::query_context::Error as CoreError;
use common::query_context::QueryContext;
use common::BlockNum;
use common::BlockStreamer as _;
use common::BoxError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::sql::TableReference;
use dataset_store::sql_datasets::is_incremental;
use dataset_store::sql_datasets::max_end_block;
use dataset_store::sql_datasets::SqlDataset;
use dataset_store::DatasetKind;
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use futures::TryFutureExt as _;
use futures::TryStreamExt;
use job::Job;
use log::info;
use log::warn;
use metadata_db::MetadataDb;
use object_store::ObjectMeta;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use parquet_writer::ParquetFileWriter;
use thiserror::Error;
use tracing::instrument;

pub async fn dump_dataset(
    dataset: &PhysicalDataset,
    dataset_store: &Arc<DatasetStore>,
    config: &Config,
    n_jobs: u16,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: u64,
    end_block: Option<u64>,
) -> Result<(), BoxError> {
    use common::meta_tables::scanned_ranges::scanned_ranges_by_table;

    let catalog = Catalog::new(vec![dataset.clone()]);
    let env = Arc::new(config.make_runtime_env()?);
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);
    let metadata_db = dataset_store.metadata_db.as_ref();

    // Ensure consistency before starting the dump procedure.
    consistency_check(dataset, &ctx, metadata_db).await?;

    // Query the scanned ranges, we might already have some ranges if this is not the first dump run
    // for this dataset.
    let scanned_ranges_by_table = scanned_ranges_by_table(&ctx, metadata_db).await.unwrap_or_default();
    for (table_name, multirange) in &scanned_ranges_by_table {
        if multirange.total_len() == 0 {
            continue;
        }

        info!(
            "table `{}` has scanned {} blocks in the ranges: {}",
            table_name,
            multirange.total_len(),
            multirange,
        );
    }

    let kind = DatasetKind::from_str(dataset.kind())?;
    match kind {
        DatasetKind::EvmRpc | DatasetKind::Firehose | DatasetKind::Substreams => {
            run_block_stream_jobs(
                n_jobs,
                ctx,
                &dataset.name(),
                dataset_store,
                scanned_ranges_by_table,
                partition_size,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
        DatasetKind::Sql | DatasetKind::Manifest => {
            if n_jobs > 1 {
                info!("n_jobs > 1 has no effect for SQL datasets");
            }

            let dataset = match kind {
                DatasetKind::Sql => dataset_store.load_sql_dataset(dataset.name()).await?,
                DatasetKind::Manifest => {
                    dataset_store.load_manifest_dataset(dataset.name()).await?
                }
                _ => unreachable!(),
            };

            dump_sql_dataset(
                ctx,
                dataset,
                config.data_store.clone(),
                dataset_store,
                &env,
                scanned_ranges_by_table,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
    }

    info!("dump of dataset {} completed successfully", dataset.name());

    Ok(())
}

async fn run_block_stream_jobs(
    n_jobs: u16,
    ctx: Arc<QueryContext>,
    dataset_name: &str,
    dataset_store: &DatasetStore,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: BlockNum,
    end: Option<BlockNum>,
) -> Result<(), BoxError> {
    let mut client = dataset_store.load_client(dataset_name).await?;

    let end = match end {
        Some(end) => end,
        None => client.latest_block(true).await?,
    };

    // This is the intersection of the `__scanned_ranges` for all tables. That is, a range is only
    // considered scanned if it is scanned for all tables.
    let scanned_ranges = {
        let mut scanned_ranges = scanned_ranges_by_table.clone().into_iter().map(|(_, r)| r);
        let first = scanned_ranges.next().ok_or("no tables")?;
        let intersection = scanned_ranges.fold(first, |acc, r| acc.intersection(&r));
        intersection
    };

    // Find the ranges of blocks that have not been scanned yet for at least one table.
    let ranges = scanned_ranges.complement(start, end);
    info!("dumping dataset {dataset_name} for ranges {ranges}");

    if ranges.total_len() == 0 {
        info!("no blocks to dump for {dataset_name}");
        return Ok(());
    }

    // Split them across the target number of jobs as to balance the number of blocks per job.
    let multiranges = ranges.split_and_partition(n_jobs as u64, 2000);
    let jobs = multiranges.into_iter().enumerate().map(|(i, multirange)| {
        Arc::new(Job {
            dataset_ctx: ctx.clone(),
            block_streamer: client.clone(),
            multirange,
            job_id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            scanned_ranges_by_table: scanned_ranges_by_table.clone(),
        })
    });

    // Spawn the jobs so they run in parallel, terminating early if any job fails.
    let mut join_handles = vec![];
    for job in jobs {
        let handle = tokio::spawn(job::run(job));

        // Stagger the start of each job by 1 second in an attempt to avoid client rate limits.
        tokio::time::sleep(Duration::from_secs(1)).await;

        join_handles.push(async { handle.err_into().await.and_then(|x| x) });
    }

    try_join_all(join_handles).await?;

    Ok(())
}

#[instrument(skip_all, err, fields(dataset = %dataset.name()))]
async fn dump_sql_dataset(
    dst_ctx: Arc<QueryContext>,
    dataset: SqlDataset,
    data_store: Arc<common::Store>,
    dataset_store: &Arc<DatasetStore>,
    env: &Arc<RuntimeEnv>,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    parquet_opts: &ParquetWriterProperties,
    start: BlockNum,
    end: Option<BlockNum>,
) -> Result<(), BoxError> {
    let physical_dataset = &dst_ctx.catalog().datasets()[0].clone();
    let mut matzn_tracker = MatznTracker::new();

    for (table, query) in &dataset.queries {
        let end = match end {
            Some(end) => end,
            None => {
                match max_end_block(&query, dataset_store.clone(), env.clone()).await? {
                    Some(end) => end,
                    None => {
                        // If the dependencies have synced nothing, we have nothing to do.
                        warn!("no blocks to dump for {table}, dependencies are empty");
                        continue;
                    }
                }
            }
        };

        let physical_table = {
            let mut tables = physical_dataset.tables();
            tables.find(|t| t.table_name() == table).unwrap()
        };
        let src_ctx = dataset_store
            .clone()
            .ctx_for_sql(&query, env.clone())
            .await?;
        let plan = src_ctx.plan_sql(query.clone()).await?;
        let is_incr = is_incremental(&plan)?;

        matzn_tracker
            .record(is_incr, &dst_ctx, dataset.name())
            .await?;

        if is_incr {
            let ranges_to_scan = scanned_ranges_by_table[table].complement(start, end);
            for (start, end) in ranges_to_scan.ranges {
                info!(
                    "dumping {} between blocks {start} and {end}",
                    physical_table.table_ref()
                );

                dump_sql_query(
                    dataset_store,
                    &query,
                    env,
                    start,
                    end,
                    physical_table,
                    parquet_opts,
                    dataset.name(),
                    &dst_ctx,
                )
                .await?;
            }
        } else {
            let Some(metadata_db) = dataset_store.metadata_db.as_ref() else {
                return Err("metadata_db is required for entire materialization".into());
            };
            let physical_table = PhysicalTable::next_revision(
                physical_table.table(),
                &data_store,
                dataset.name(),
                metadata_db,
            )
            .await?;
            info!(
                "dumping entire {} to {}",
                physical_table.table_ref(),
                physical_table.url()
            );
            dump_sql_query(
                dataset_store,
                &query,
                env,
                start,
                end,
                &physical_table,
                parquet_opts,
                dataset.name(),
                &dst_ctx,
            )
            .await?;
        }
    }

    Ok(())
}

async fn dump_sql_query(
    dataset_store: &Arc<DatasetStore>,
    query: &datafusion::sql::parser::Statement,
    env: &Arc<RuntimeEnv>,
    start: BlockNum,
    end: BlockNum,
    physical_table: &common::catalog::physical::PhysicalTable,
    parquet_opts: &ParquetWriterProperties,
    dataset_name: &str,
    dst_ctx: &QueryContext,
) -> Result<(), BoxError> {
    use dataset_store::sql_datasets::execute_query_for_range;

    let store = dataset_store.clone();
    let mut stream = execute_query_for_range(query.clone(), store, env.clone(), start, end).await?;
    let mut writer = ParquetFileWriter::new(physical_table.clone(), parquet_opts.clone(), start)?;
    while let Some(batch) = stream.try_next().await? {
        writer.write(&batch).await?;
    }
    let scanned_range = writer.close(end).await?.as_record_batch();
    let table_ref = TableReference::partial(dataset_name, scanned_ranges::TABLE_NAME);
    dst_ctx.meta_insert_into(table_ref, scanned_range).await?;
    Ok(())
}

pub fn default_partition_size() -> u64 {
    4096 * 1024 * 1024 // 4 GB
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

#[derive(Error, Debug)]
#[error("consistency check error: {0}")]
enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] CoreError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("dataset {0} is corrupted: {1}")]
    CorruptedDataset(String, BoxError),
}

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in `__scanned_ranges`.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in `__scanned_ranges` exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: `__scanned_ranges` does not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
async fn consistency_check(
    physical_dataset: &PhysicalDataset,
    ctx: &QueryContext,
    metadata_db: Option<&MetadataDb>
) -> Result<(), ConsistencyCheckError> {
    // See also: scanned-ranges-consistency

    use scanned_ranges::{filenames_for_table, ranges_for_table};
    use ConsistencyCheckError::CorruptedDataset;

    for table in physical_dataset.tables() {
        let dataset_name = table.catalog_schema().to_string();

        // Check that `__scanned_ranges` does not contain overlapping ranges.
        {
            let ranges = ranges_for_table(ctx, table.table_name(), metadata_db).await.unwrap_or_default();
            if let Err(e) = MultiRange::from_ranges(ranges) {
                return Err(CorruptedDataset(dataset_name, e.into()));
            }
        }

        let registered_files = {
            let f = filenames_for_table(&ctx, table.catalog_schema(), table.table_name()).await?;
            BTreeSet::from_iter(f.into_iter())
        };

        let store = table.object_store();
        let path = table.path();

        // Collect all stored files whose filename matches `is_dump_file`.
        let stored_files: BTreeMap<String, ObjectMeta> = table
            .parquet_files(true)
            .await
            .map_err(|e| ConsistencyCheckError::ObjectStoreError(e))?;

        for (filename, object_meta) in &stored_files {
            if !registered_files.contains(filename) {
                // This file was written by a dump job but it is not present in `__scanned_ranges`,
                // so it is an orphaned file. Delete it.
                warn!("Deleting orphaned file: {}", object_meta.location);
                store.delete(&object_meta.location).await?;
            }
        }

        // Check for files in `__scanned_ranges` that do not exist in the store.
        for filename in registered_files {
            if !stored_files.contains_key(&filename) {
                let err =
                    format!("file `{path}/{filename}` is registered in `__scanned_ranges` but is not in the data store")
                        .into();
                return Err(ConsistencyCheckError::CorruptedDataset(dataset_name, err));
            }
        }
    }
    Ok(())
}

// We work around some deficiencies in our metadata handling by deleting the
// scanned ranges for a table in its entirety. This struct encapsulates the
// logic for that and hides it from the casual reader because it's ugly
//
// We truncate __scanned_ranges for the first entire materialization we see,
// but we do not allow mixing entire and incremental materializations for
// safety
struct MatznTracker {
    had_entire: bool,
    had_incremental: bool,
    ranges_cleared: bool,
}

impl MatznTracker {
    fn new() -> Self {
        Self {
            had_entire: false,
            had_incremental: false,
            ranges_cleared: false,
        }
    }

    async fn record(
        &mut self,
        is_incremental: bool,
        ctx: &QueryContext,
        dataset_name: &str,
    ) -> Result<(), BoxError> {
        fn not_supported() -> Result<(), BoxError> {
            Err(
                "Currently, a dataset may not mix incremental and non-incremental \
                 queries. We're working on removing this restriction"
                    .into(),
            )
        }

        if is_incremental {
            if self.had_entire {
                return not_supported();
            }
            self.had_incremental = true;
        } else {
            if self.had_incremental {
                return not_supported();
            }
            self.had_entire = true;
            if !self.ranges_cleared {
                ctx.meta_truncate(dataset_name).await?;
                self.ranges_cleared = true;
            }
        }
        Ok(())
    }
}
