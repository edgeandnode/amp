use common::{
    catalog::physical::{PhysicalDataset, PhysicalTable},
    config::Config,
    manifest::Manifest,
    BoxError, Dataset,
};
use dataset_store::DatasetStore;
use dump::{
    operator::OperatorDesc,
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

    /// Schedule a dump for a new copy of a dataset.
    pub async fn schedule_dataset_dump(&self, dataset: Manifest) -> Result<(), BoxError> {
        // Scheduling procedure for a new `DumpDataset` operator:
        // 1. Choose a responsive node.
        // 2. Create a new location for each table.
        // 3. Register the operator in the metadata db.
        // 4. Send a `Start` command through `worker_actions` for that operator.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = self.metadata_db.active_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::thread_rng()) else {
            return Err("no available workers".into());
        };

        let dataset: Dataset = dataset.into();

        let mut locations = Vec::new();
        for table in dataset.tables_with_meta() {
            let physical_table = PhysicalTable::next_revision(
                &table,
                &self.config.data_store,
                &dataset.name,
                &self.metadata_db,
            )
            .await?;
            locations.push(physical_table.location_id().unwrap());
        }

        let operator_desc = serde_json::to_string(&OperatorDesc::DumpDataset {
            dataset: dataset.name,
        })?;

        let operator_id = self
            .metadata_db
            .schedule_operator(&node_id, &operator_desc, &locations)
            .await?;

        let action = WorkerAction {
            node_id: node_id.to_string(),
            operator_id,
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
