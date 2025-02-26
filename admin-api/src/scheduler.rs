use common::{catalog::physical::PhysicalDataset, config::Config, manifest::Manifest, BoxError};
use dataset_store::DatasetStore;
use dump::{
    operator::Operator,
    worker::{Action, WorkerAction, WORKER_ACTIONS_PG_CHANNEL},
};
use metadata_db::MetadataDb;
use rand::seq::SliceRandom as _;
use std::sync::Arc;
use tokio::select;

#[derive(Clone)]
pub enum Scheduler {
    /// Regular scheduler, uses the metadata db to coordinate with worker nodes.
    Full(FullScheduler),

    /// If no metadata db is configured, an ephemeral worker is used. It just invokes a dump run,
    /// which is not resumed on restart. This is to support local dev or testing environments that
    /// don't want to run PG.
    Ephemeral(Arc<Config>),
}

impl Scheduler {
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self::Full(FullScheduler {
            config,
            metadata_db,
        })
    }

    pub async fn schedule_dataset_dump(self, manifest: Manifest) -> Result<(), BoxError> {
        match self {
            Self::Full(scheduler) => scheduler.schedule_dataset_dump(manifest).await,
            Self::Ephemeral(config) => {
                let dataset_store = DatasetStore::new(config.clone(), None);
                let dataset = {
                    let dataset = dataset_store.load_dataset(&manifest.name).await?;
                    PhysicalDataset::from_dataset_at(dataset, config.data_store.clone(), None)
                        .await?
                };

                let join_handle = tokio::spawn(async move {
                    dump::dump_dataset(
                        &dataset,
                        &dataset_store.clone(),
                        &config.clone(),
                        1,
                        dump::default_partition_size(),
                        &dump::default_parquet_opts(),
                        0,
                        None,
                    )
                    .await
                });

                // Wait for a couple of seconds to see if the scheduler task errors
                select! {
                    res = join_handle => {
                        // The scheduler task completed quickly, return the error if any.
                        let () = res??;
                        Ok(())
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                    // The scheduler task did not complete, detach it and assume success.
                        Ok(())
                    }
                }
            }
        }
    }
}
#[derive(Clone)]
pub struct FullScheduler {
    config: Arc<Config>,
    metadata_db: MetadataDb,
}

impl FullScheduler {
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self {
            config,
            metadata_db,
        }
    }

    pub async fn schedule_dataset_dump(&self, dataset: Manifest) -> Result<(), BoxError> {
        // Scheduling procedure for a new `DumpDataset` operator:
        // 1. Choose a responsive node.
        // 2. Resolve the location for each table.
        // 3. Instantiate the operator description.
        // 4. Send a `Start` command through `worker_actions` for that node and operator description.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = self.metadata_db.live_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::thread_rng()) else {
            return Err("no available workers".into());
        };

        let data_store = self.config.data_store.clone();
        let dataset =
            PhysicalDataset::from_dataset_at(dataset.into(), data_store, Some(&self.metadata_db))
                .await?;
        let locations = dataset
            .tables()
            .map(|table| table.location_id().unwrap()) // Unwrap: The metadata db is configured.
            .collect();
        let action = WorkerAction {
            node_id: node_id.to_string(),
            operator: Operator::DumpDataset {
                dataset: dataset.name().to_string(),
                locations,
            },
            action: Action::Start,
        };
        self.metadata_db
            .notify(
                WORKER_ACTIONS_PG_CHANNEL,
                &serde_json::to_string(&action).unwrap(),
            )
            .await?;

        Ok(())
    }
}
