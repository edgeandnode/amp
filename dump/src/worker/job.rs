use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use common::{catalog::physical::PhysicalTable, BoxError};
use metadata_db::JobId;
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub use crate::core::Ctx as JobCtx;
use crate::{
    core::dump_tables, default_input_batch_size_blocks, default_parquet_opts,
    default_partition_size,
};

/// This is currently very simple, but the job abstraction is expected to become a central one.
///
/// Three kinds of jobs are expected to exist:
/// - Raw Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
///
/// Currently, the "dump job" is what have implemented so that's what we have here.
#[derive(Clone)]
pub enum Job {
    DumpTables {
        ctx: JobCtx,
        /// All tables must belong to the same dataset.
        tables: Vec<Arc<PhysicalTable>>,
        /// The end block to dump, or `None` for the latest block.
        end_block: Option<i64>,
    },
}

impl Job {
    /// Load a job from the database.
    #[instrument(skip(ctx), err)]
    pub async fn load(ctx: JobCtx, job_id: &JobId) -> Result<Job, BoxError> {
        let raw_desc = ctx
            .metadata_db
            .get_job(job_id)
            .await?
            .map(|j| j.desc.to_string())
            .ok_or_else(|| format!("job `{}` not found", job_id))?;
        let job_desc: JobDesc = serde_json::from_str(&raw_desc)
            .map_err(|e| format!("error parsing job descriptor `{}`: {}", raw_desc, e))?;
        let output_locations = ctx.metadata_db.output_locations(job_id).await?;

        match job_desc {
            JobDesc::DumpDataset { dataset, end_block } => {
                let dataset = ctx.dataset_store.load_dataset(&dataset).await?;

                // Consistency check: All tables must be present in the job's output.
                let dataset_tables = dataset
                    .tables()
                    .iter()
                    .map(|t| t.name().to_string())
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

                for table in Arc::new(dataset).resolved_tables() {
                    // Unwrap: We checked consistency above.
                    let (id, url) = output_locations_by_name.remove(table.name()).unwrap();
                    physical_tables.push(
                        PhysicalTable::new(table.clone(), url, id, ctx.metadata_db.clone())?.into(),
                    );
                }

                Ok(Job::DumpTables {
                    ctx,
                    tables: physical_tables,
                    end_block,
                })
            }
        }
    }

    pub async fn run(self) -> Result<(), BoxError> {
        match self {
            Job::DumpTables {
                ctx,
                ref tables,
                end_block,
            } => {
                dump_tables(
                    ctx.clone(),
                    tables,
                    1,
                    default_partition_size(),
                    default_input_batch_size_blocks(),
                    &default_parquet_opts(),
                    (0, end_block),
                )
                .await
            }
        }
    }
}

impl std::fmt::Display for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Job::DumpTables { tables, .. } => {
                write!(f, "DumpTables({})", tables.len())
            }
        }
    }
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// The logical descriptor of an job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobDesc {
    DumpDataset {
        dataset: String,
        end_block: Option<i64>,
    },
}
