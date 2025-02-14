use common::{config::Config, BoxError};
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;
use std::sync::Arc;

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

    pub async fn schedule_dataset_dump(self, dataset_name: String) -> Result<(), BoxError> {
        match self {
            Self::Full(scheduler) => scheduler.schedule_dataset_dump(dataset_name).await,
            Self::Ephemeral(config) => {
                let dataset_store = DatasetStore::new(config.clone(), None);

                dump::dump_dataset(
                    &dataset_name,
                    &dataset_store,
                    &config,
                    None,
                    1,
                    dump::default_partition_size(),
                    &dump::default_parquet_opts(),
                    0,
                    None,
                )
                .await
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

    pub async fn schedule_dataset_dump(&self, dataset_name: String) -> Result<(), BoxError> {
        // Scheduling procedure:
        // 1. Create a location for each table and insert it in the metadata DB.
        // 2. Choose a responsive node.
        // 3. Sends a notification through `worker_actions`, with the node, location, current state and next state.
        // 4. The node receives the notification, and starts the dump run.
        todo!()
    }
}
