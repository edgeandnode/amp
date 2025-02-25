use std::{fmt, sync::Arc};

use common::{catalog::physical::PhysicalDataset, config::Config, BoxError};
use dataset_store::DatasetStore;
use metadata_db::{LocationId, MetadataDb};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{default_parquet_opts, default_partition_size, dump_dataset};

/// This is currently very simple, but the operator abstraction is expected to become a central one.
///
/// Three kinds of operators are expected to exist:
/// - External Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
///
/// Currently, the "dump operator" is what have implemented so that's what we have here. This
/// representation is for the `operator` column in the `jobs` table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    DumpDataset {
        // Dataset name.
        dataset: String,

        // One for each table in the dataset.
        locations: Vec<LocationId>,
    },
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::DumpDataset {
                dataset,
                locations: _,
            } => {
                write!(f, "DumpDataset({})", dataset)
            }
        }
    }
}

impl Operator {
    pub async fn run(&self, config: Arc<Config>, metadata_db: MetadataDb) -> Result<(), BoxError> {
        match self {
            Operator::DumpDataset { dataset, locations } => {
                debug!(
                    "Starting dump operator for dataset {} with location ids {:?}",
                    dataset, locations
                );

                let dataset_store = DatasetStore::new(config.clone(), Some(metadata_db.clone()));
                let dataset = {
                    let dataset = dataset_store.load_dataset(&dataset).await?;
                    PhysicalDataset::from_dataset_at(
                        dataset,
                        config.data_store.clone(),
                        Some(&metadata_db),
                    )
                    .await?
                };

                dump_dataset(
                    &dataset,
                    &dataset_store.clone(),
                    &config.clone(),
                    1,
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
