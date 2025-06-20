use std::sync::Arc;

use common::{catalog::physical::PhysicalTable, config::Config, BoxError, Dataset};
use dump::worker::JobDesc;
use metadata_db::MetadataDb;
use rand::seq::IndexedRandom as _;

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
    pub async fn schedule_dataset_dump(
        &self,
        dataset: Dataset,
        end_block: Option<i64>,
    ) -> Result<(), BoxError> {
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

        let dataset_name = dataset.name.clone();

        let mut locations = Vec::new();
        let metadata_db = Arc::new(self.metadata_db.clone());
        for table in Arc::new(dataset).resolved_tables() {
            let physical_table = match PhysicalTable::get_active(&table, metadata_db.clone())
                .await?
            {
                Some(physical_table) => physical_table,
                None => {
                    let store = &self.config.data_store;
                    PhysicalTable::next_revision(&table, store, metadata_db.clone(), true).await?
                }
            };
            locations.push(physical_table.location_id());
        }

        let job_desc = serde_json::to_string(&JobDesc::DumpDataset {
            dataset: dataset_name,
            end_block,
        })?;

        self.metadata_db
            .schedule_job(node_id, &job_desc, &locations)
            .await?;

        Ok(())
    }
}
