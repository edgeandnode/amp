use common::{
    catalog::physical::PhysicalTable, config::Config, manifest::Manifest, BoxError, Dataset,
};
use dump::{
    job::JobDesc,
    worker::{Action, WorkerAction, WORKER_ACTIONS_PG_CHANNEL},
};
use metadata_db::MetadataDb;
use rand::seq::IndexedRandom as _;
use std::sync::Arc;

#[derive(Clone)]
pub struct Scheduler {
    config: Arc<Config>,
    metadata_db: MetadataDb,
}

impl Scheduler {
    pub fn new(config: Arc<Config>, metadata_db: MetadataDb) -> Self {
        Self {
            config,
            metadata_db,
        }
    }

    /// Schedule a dump for a new copy of a dataset.
    pub async fn schedule_dataset_dump(&self, dataset: Manifest) -> Result<(), BoxError> {
        // Scheduling procedure for a new `DumpDataset` job:
        // 1. Choose a responsive node.
        // 2. Create a new location for each table.
        // 3. Register the job in the metadata db.
        // 4. Send a `Start` command through `worker_actions` for that job.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = self.metadata_db.active_workers().await?;
        let Some(node_id) = candidates.choose(&mut rand::rng()) else {
            return Err("no available workers".into());
        };

        let dataset: Dataset = dataset.into();

        let mut locations = Vec::new();
        for table in dataset.tables() {
            let physical_table = PhysicalTable::next_revision(
                &table,
                &self.config.data_store,
                &dataset.name,
                self.metadata_db.clone().into(),
            )
            .await?;
            locations.push(physical_table.location_id());
        }

        let job_desc = serde_json::to_string(&JobDesc::DumpDataset {
            dataset: dataset.name,
        })?;

        let job_id = self
            .metadata_db
            .schedule_job(&node_id, &job_desc, &locations)
            .await?;

        let action = WorkerAction {
            node_id: node_id.to_string(),
            job_id,
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
