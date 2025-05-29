use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};

use common::{
    catalog::physical::{Catalog, PhysicalDataset, PhysicalTable},
    config::Config,
    metadata::block_ranges_by_table,
    multirange::MultiRange,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
    query_context::{Error as QueryError, QueryContext},
    BoxError,
};
use dataset_store::{DatasetKind, DatasetStore};
use metadata_db::LocationId;
use object_store::ObjectMeta;

mod block_ranges;
mod raw_dataset;
mod sql_dataset;

/// Dumps a dataset
pub async fn dump_dataset(
    dataset: &PhysicalDataset,
    dataset_store: &Arc<DatasetStore>,
    config: &Config,
    n_jobs: u16,
    partition_size: u64,
    input_batch_size_blocks: u64,
    parquet_opts: &ParquetWriterProperties,
    start: i64,
    end_block: Option<i64>,
) -> Result<(), BoxError> {
    let catalog = Catalog::new(dataset.tables().cloned().collect());
    let env = config.make_query_env()?;
    let query_ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    // Ensure consistency before starting the dump procedure.
    for table in dataset.tables() {
        consistency_check(table).await?;
    }

    // Query the block ranges, we might already have some ranges if this is not the first dump run
    // for this dataset.
    let block_ranges_by_table = block_ranges_by_table(&query_ctx).await?;
    for (table_name, multirange) in &block_ranges_by_table {
        if multirange.total_len() == 0 {
            continue;
        }

        tracing::info!(
            "table `{}` has scanned {} blocks in the ranges: {}",
            table_name,
            multirange.total_len(),
            multirange,
        );
    }

    let kind = DatasetKind::from_str(dataset.kind())?;
    match kind {
        DatasetKind::EvmRpc | DatasetKind::Firehose | DatasetKind::Substreams => {
            raw_dataset::dump(
                n_jobs,
                query_ctx,
                dataset.name(),
                dataset_store,
                block_ranges_by_table,
                partition_size,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
        DatasetKind::Sql | DatasetKind::Manifest => {
            if n_jobs > 1 {
                tracing::info!("n_jobs > 1 has no effect for SQL datasets");
            }

            let dataset = match kind {
                DatasetKind::Sql => dataset_store.load_sql_dataset(dataset.name()).await?,
                DatasetKind::Manifest => {
                    dataset_store.load_manifest_dataset(dataset.name()).await?
                }
                _ => unreachable!(),
            };

            sql_dataset::dump(
                query_ctx,
                dataset,
                config.data_store.clone(),
                dataset_store,
                &env,
                block_ranges_by_table,
                parquet_opts,
                start,
                end_block,
                input_batch_size_blocks,
            )
            .await?;
        }
    }

    tracing::info!("dump of dataset {} completed successfully", dataset.name());

    Ok(())
}

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in the metadata DB.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in the table exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: metadata entries do not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
async fn consistency_check(table: &PhysicalTable) -> Result<(), ConsistencyCheckError> {
    // See also: metadata-consistency

    let location_id = table.location_id();

    // Check that bock ranges do not contain overlapping ranges.
    {
        let ranges = table
            .ranges()
            .await
            .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?;
        if let Err(e) = MultiRange::from_ranges(ranges) {
            return Err(ConsistencyCheckError::CorruptedTable(location_id, e.into()));
        }
    }

    let registered_files = table
        .file_names()
        .await
        .map(BTreeSet::from_iter)
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?;

    let store = table.object_store();
    let path = table.path();

    // Collect all stored files whose filename matches `is_dump_file`.
    let stored_files: BTreeMap<String, ObjectMeta> = table
        .parquet_files()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?;

    for (filename, object_meta) in &stored_files {
        if !registered_files.contains(filename) {
            // This file was written by a dump job but it is not present in the metadata DB,
            // so it is an orphaned file. Delete it.
            tracing::warn!("Deleting orphaned file: {}", object_meta.location);
            store.delete(&object_meta.location).await?;
        }
    }

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains_key(&filename) {
            let err =
                    format!("file `{path}/{filename}` is registered in metadata DB but is not in the data store")
                        .into();
            return Err(ConsistencyCheckError::CorruptedTable(location_id, err));
        }
    }

    Ok(())
}

/// Error type for consistency checks
#[derive(Debug, thiserror::Error)]
#[error("consistency check error: {0}")]
enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("table {0} is corrupted: {1}")]
    CorruptedTable(LocationId, BoxError),
}
