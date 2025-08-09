pub mod job;
pub mod metrics;

use std::{str::FromStr, sync::Arc};

use common::{
    BoxError, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    manifest::Version,
    query_context::QueryEnv,
};
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use job::Job;
use metadata_db::{MetadataDb, TableId};

pub async fn dump_check(
    dataset_name: &str,
    dataset_version: Option<&str>,
    dataset_store: &Arc<DatasetStore>,
    metadata_db: Arc<MetadataDb>,
    env: &QueryEnv,
    batch_size: u64,
    n_jobs: u8,
    start: u64,
    end_block: u64,
) -> Result<(), BoxError> {
    let dataset_version = match dataset_version {
        Some(version) => Some(Version::from_str(version)?),
        None => None,
    };
    let dataset = dataset_store
        .load_dataset(&dataset_name, dataset_version.as_ref())
        .await?;
    let client = dataset_store.load_client(&dataset_name).await?;
    let total_blocks = end_block - start + 1;
    let mut tables = Vec::with_capacity(dataset.tables.len());
    let dataset_version = match dataset.kind.as_str() {
        "manifest" => dataset.dataset_version(),
        _ => None,
    };
    for table in Arc::new(dataset.clone()).resolved_tables() {
        let table_id = TableId {
            dataset: dataset_name,
            dataset_version: dataset_version.as_deref(),
            table: &table.name(),
        };
        let (url, location_id) = metadata_db
            .get_active_location(table_id)
            .await?
            .ok_or(format!("No active location for {table_id:?}"))?;
        let table = PhysicalTable::new(table.clone(), url, location_id, metadata_db.clone())?;
        tables.push(table.into());
    }
    let catalog = Catalog::new(tables, vec![]);
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    let jobs = {
        let mut jobs = vec![];
        let blocks_per_job = total_blocks.div_ceil(n_jobs as u64);
        let mut from = start;
        while from <= end_block {
            let to = (from + blocks_per_job).min(end_block);
            jobs.push(Job {
                dataset: dataset.clone(),
                block_streamer: client.clone(),
                start: from,
                end: to,
                batch_size,
                ctx: ctx.clone(),
            });
            from = to + 1;
        }
        jobs
    };

    // early return if any job errors out
    try_join_all(jobs.into_iter().map(job::run_job)).await?;

    println!("Validated successfully {total_blocks} blocks");

    Ok(())
}
