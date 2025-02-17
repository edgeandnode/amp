use common::{catalog::physical::PhysicalTable, config::Config, manifest::Manifest, BoxError};
use dataset_store::DatasetStore;
use metadata_db::{JobState, MetadataDb, WorkerAction};
use rand::seq::SliceRandom as _;
use std::sync::Arc;
use tokio::select;

#[derive(Clone)]
pub enum Scheduler {
    /// Regular scheduler, uses the metadata db to coordinate with worker nodes.
    Full(FullScheduler),

    /// If no metadata db is configured, the ephemeral scheduler is used. It just invokes a dump run.
    /// This is to support local dev or testing environments that don't want to run PG.
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

                let join_handle = tokio::spawn(async move {
                    dump::dump_dataset(
                        &manifest.name.clone(),
                        &dataset_store.clone(),
                        &config.clone(),
                        None,
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

    /// Schedule dump jobs to workers. Each table in the dataset will become a separate job.
    pub async fn schedule_dataset_dump(&self, dataset: Manifest) -> Result<(), BoxError> {
        // Scheduling procedure:
        // 1. Choose a responsive node.
        // 2. Create a location for each table.
        // 3. Create the job in the 'created' state.
        // 4. Sends a notification through `worker_actions`, with the node, location, current state and next state.
        //
        // The worker node should then receive the notification and starts the dump run for those tables.

        let candidates = self.metadata_db.live_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::thread_rng()) else {
            return Err("no available workers".into());
        };

        let mut locations = Vec::new();
        for table in dataset.tables() {
            let (_, location_id) = PhysicalTable::next_revision(
                &table,
                &self.config.data_store,
                &dataset.name,
                &self.metadata_db,
            )
            .await?;
            self.metadata_db.create_job(node_id, location_id).await?;
            let action = WorkerAction {
                node_id: node_id.to_string(),
                location: location_id,
                current_state: JobState::Created,
                next_state: JobState::Running,
            };
            self.metadata_db.notify_worker(action).await?;
            locations.push(location_id);
        }

        Ok(())
    }
}
