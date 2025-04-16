use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use common::{
    catalog::physical::{PhysicalDataset, PhysicalTable},
    config::Config,
    BoxError,
};
use dataset_store::DatasetStore;
use metadata_db::{JobDatabaseId, MetadataDb};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{default_parquet_opts, default_partition_size, dump_dataset};

/// This is currently very simple, but the job abstraction is expected to become a central one.
///
/// Three kinds of jobs are expected to exist:
/// - Raw Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
///
/// Currently, the "dump job" is what have implemented so that's what we have here.
#[derive(Debug, Clone)]
pub enum Job {
    DumpDataset { dataset: PhysicalDataset },
}

impl fmt::Display for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Job::DumpDataset { dataset } => {
                write!(f, "DumpDataset({})", dataset.name())
            }
        }
    }
}

/// The logical descriptor of an job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobDesc {
    DumpDataset { dataset: String },
}

impl Job {
    /// Load a job from the database.
    #[instrument(skip(config, metadata_db), err)]
    pub async fn load(
        job_id: JobDatabaseId,
        config: Arc<Config>,
        metadata_db: MetadataDb,
    ) -> Result<Job, BoxError> {
        let raw_desc = metadata_db.job_desc(job_id).await?;
        let job_desc: JobDesc = serde_json::from_str(&raw_desc)
            .map_err(|e| format!("error parsing job descriptor `{}`: {}", raw_desc, e))?;
        let output_locations = metadata_db.output_locations(job_id).await?;

        match job_desc {
            JobDesc::DumpDataset { dataset } => {
                let dataset_store = DatasetStore::new(config.clone(), Some(metadata_db.clone()));
                let dataset = dataset_store.load_dataset(&dataset).await?;

                // Consistency check: All tables must be present in the job's output.
                let dataset_tables = dataset
                    .tables()
                    .into_iter()
                    .map(|t| t.name.to_string())
                    .collect::<BTreeSet<_>>();
                let job_tables = output_locations
                    .iter()
                    .map(|(_, tbl, _)| tbl.clone())
                    .collect::<BTreeSet<_>>();
                if dataset_tables != job_tables {
                    return Err(format!(
                        "Inconsistent job state: dataset tables and job output tables do not match: {:?} != {:?}",
                        dataset_tables, job_tables
                    ).into());
                }

                // Instantiate the physical tables.
                let mut output_locations_by_name = output_locations
                    .into_iter()
                    .map(|(id, tbl, url)| (tbl.clone(), (id, url)))
                    .collect::<BTreeMap<_, _>>();
                let mut physical_tables = vec![];
                for table in dataset.tables() {
                    // Unwrap: We checked consistency above.
                    let (id, url) = output_locations_by_name.remove(&table.name).unwrap();
                    physical_tables.push(PhysicalTable::new(
                        &dataset.name,
                        table.clone(),
                        url,
                        Some(id),
                    )?);
                }

                Ok(Job::DumpDataset {
                    dataset: PhysicalDataset::new(dataset, physical_tables),
                })
            }
        }
    }

    pub async fn run(&self, config: Arc<Config>, metadata_db: MetadataDb) -> Result<(), BoxError> {
        match self {
            Job::DumpDataset { dataset } => {
                debug!(
                    "Starting dump job for dataset {} with location ids {:?}",
                    dataset.name(),
                    dataset.location_ids()
                );

                let dataset_store = DatasetStore::new(config.clone(), Some(metadata_db.clone()));

                dump_dataset(
                    &dataset,
                    &dataset_store.clone(),
                    &config.clone(),
                    1,
                    100_000,
                    default_partition_size(),
                    &default_parquet_opts(),
                    0,
                    None,
                )
                .await
            }
        }
    }
}
