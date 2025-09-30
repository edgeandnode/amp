use std::sync::Arc;

use common::{
    BoxError, LogicalCatalog, QueryContext,
    catalog::physical::{Catalog, PhysicalTable},
    query_context::QueryEnv,
};
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use job::Job;
use metadata_db::{MetadataDb, TableId};

pub mod job;
pub mod metrics;

pub async fn dump_check(
    dataset_name: &str,
    dataset_version: Option<&str>,
    dataset_store: &Arc<DatasetStore>,
    metadata_db: MetadataDb,
    env: &QueryEnv,
    batch_size: u64,
    n_jobs: u8,
    start: u64,
    end_block: u64,
    metrics: Option<metrics::Metrics>,
) -> Result<(), BoxError> {
    let dataset_version = dataset_version.map(|v| v.parse()).transpose()?;
    let dataset = dataset_store
        .get_dataset(&dataset_name, dataset_version.as_ref())
        .await?
        .ok_or_else(|| format!("Dataset '{}' not found", dataset_name))?;
    let client = dataset_store
        .get_client(
            &dataset_name,
            dataset_version.as_ref(),
            false,
            metrics.as_ref().map(|m| &m.meter),
        )
        .await?
        .ok_or_else(|| format!("Client for dataset '{}' not found", dataset_name))?;
    let total_blocks = end_block - start + 1;
    let mut tables: Vec<Arc<PhysicalTable>> = Vec::with_capacity(dataset.tables.len());
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
    let logical = LogicalCatalog::from_tables(tables.iter().map(|t| t.table()));
    let catalog = Catalog::new(tables, logical);
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone(), false).await?);

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
                metrics: metrics.clone(),
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
