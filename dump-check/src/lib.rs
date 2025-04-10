pub mod job;
pub mod metrics;

use std::sync::Arc;

use common::{catalog::physical::Catalog, config::Config, BoxError, QueryContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use job::Job;
use metadata_db::MetadataDb;

pub async fn dump_check(
    dataset_name: &str,
    dataset_store: &Arc<DatasetStore>,
    config: &Config,
    metadata_db: Option<&MetadataDb>,
    env: &Arc<RuntimeEnv>,
    batch_size: u64,
    n_jobs: u8,
    start: u64,
    end_block: u64,
) -> Result<(), BoxError> {
    let dataset = dataset_store.load_dataset(&dataset_name).await?;
    let client = dataset_store.load_client(&dataset_name).await?;
    let total_blocks = end_block - start + 1;
    let catalog =
        Catalog::for_dataset(dataset.clone(), config.data_store.clone(), metadata_db).await?;
    let ctx = Arc::new(QueryContext::for_catalog(
        catalog,
        env.clone(),
        dataset_store.evm_rpc_dataset_providers().await?,
    )?);

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
