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
use metadata_db::{MetadataDb, OperatorDatabaseId};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{default_parquet_opts, default_partition_size, dump_dataset};

/// This is currently very simple, but the operator abstraction is expected to become a central one.
///
/// Three kinds of operators are expected to exist:
/// - External Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
///
/// Currently, the "dump operator" is what have implemented so that's what we have here.
#[derive(Debug, Clone)]
pub enum Operator {
    DumpDataset { dataset: PhysicalDataset },
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::DumpDataset { dataset } => {
                write!(f, "DumpDataset({})", dataset.name())
            }
        }
    }
}

/// The logical descriptor of an operator, as stored in the `descriptor` column of the `operators`
/// metadata DB table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperatorDesc {
    DumpDataset { dataset: String },
}

impl Operator {
    /// Load an operator from the database.
    #[instrument(skip(config, metadata_db), err)]
    pub async fn load(
        operator_id: OperatorDatabaseId,
        config: Arc<Config>,
        metadata_db: MetadataDb,
    ) -> Result<Operator, BoxError> {
        let operator_desc: OperatorDesc =
            serde_json::from_str(&metadata_db.operator_desc(operator_id).await?)?;
        let output_locations = metadata_db.output_locations(operator_id).await?;

        match operator_desc {
            OperatorDesc::DumpDataset { dataset } => {
                let dataset_store = DatasetStore::new(config.clone(), Some(metadata_db.clone()));
                let dataset = dataset_store.load_dataset(&dataset).await?;

                // Consistency check: All tables must be present in the operator's output.
                let dataset_tables = dataset
                    .tables_with_meta()
                    .into_iter()
                    .map(|t| t.name)
                    .collect::<BTreeSet<_>>();
                let operator_tables = output_locations
                    .iter()
                    .map(|(_, tbl, _)| tbl.clone())
                    .collect::<BTreeSet<_>>();
                if dataset_tables != operator_tables {
                    return Err(format!(
                        "Inconsistent operator state: dataset tables and operator output tables do not match: {:?} != {:?}",
                        dataset_tables, operator_tables
                    ).into());
                }

                // Instaantiate the physical tables.
                let mut output_locations_by_name = output_locations
                    .into_iter()
                    .map(|(id, tbl, url)| (tbl.clone(), (id, url)))
                    .collect::<BTreeMap<_, _>>();
                let mut physical_tables = vec![];
                for table in dataset.tables_with_meta() {
                    // Unwrap: We checked consistency above.
                    let (id, url) = output_locations_by_name.remove(&table.name).unwrap();
                    physical_tables.push(PhysicalTable::new(&dataset.name, table, url, Some(id))?);
                }

                Ok(Operator::DumpDataset {
                    dataset: PhysicalDataset::new(dataset, physical_tables),
                })
            }
        }
    }

    pub async fn run(&self, config: Arc<Config>, metadata_db: MetadataDb) -> Result<(), BoxError> {
        match self {
            Operator::DumpDataset { dataset } => {
                debug!(
                    "Starting dump operator for dataset {} with location ids {:?}",
                    dataset.name(),
                    dataset.location_ids()
                );

                let dataset_store = DatasetStore::new(config.clone(), Some(metadata_db.clone()));

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
