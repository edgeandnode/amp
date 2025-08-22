use std::{str::FromStr, sync::Arc};

use common::{BoxError, catalog::physical::PhysicalTable, manifest::Version};
use metadata_db::JobId;
use serde::{Deserialize, Serialize};
use tracing::instrument;

pub use crate::core::Ctx;
use crate::{core::dump_tables, default_partition_size};

/// The kind of job is inferred from the location and associated dataset information.
///
/// Three kinds of jobs are expected to exist:
/// - Raw Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
#[derive(Clone)]
pub enum Job {
    DumpTables {
        ctx: Ctx,
        /// All tables must belong to the same dataset.
        tables: Vec<Arc<PhysicalTable>>,
        /// The end block to dump, or `None` for the latest block.
        end_block: Option<i64>,
    },
}

impl Job {
    /// Try to build a job from a job ID and descriptor.
    #[instrument(skip(ctx, job_id, job_desc), err)]
    pub async fn try_from_descriptor(
        ctx: Ctx,
        job_id: JobId,
        job_desc: JobDesc,
    ) -> Result<Job, BoxError> {
        let output_locations = ctx.metadata_db.output_locations(job_id).await?;
        match job_desc {
            JobDesc::Dump { end_block } => {
                let mut tables = vec![];
                for location in output_locations {
                    let dataset_version = Version::from_str(&location.dataset_version).ok();
                    let dataset = Arc::new(
                        ctx.dataset_store
                            .load_dataset(&location.dataset, dataset_version.as_ref())
                            .await?,
                    );
                    let mut resolved_tables = dataset.resolved_tables();
                    let Some(table) = resolved_tables.find(|t| t.name() == location.table) else {
                        return Err(format!(
                            "Table `{}` not found in dataset `{}`",
                            location.table, location.dataset
                        )
                        .into());
                    };
                    let loc = ctx
                        .metadata_db
                        .get_location_by_id(location.id)
                        .await?
                        .ok_or("Location not found")?;
                    let start_block = loc.start_block;
                    tables.push(
                        PhysicalTable::new(
                            table.clone(),
                            location.url,
                            location.id,
                            ctx.metadata_db.clone(),
                            start_block,
                        )?
                        .into(),
                    );
                }

                Ok(Job::DumpTables {
                    ctx,
                    tables,
                    end_block,
                })
            }
        }
    }

    pub async fn run(self) -> Result<(), BoxError> {
        match self {
            Job::DumpTables {
                ctx,
                tables,
                end_block,
            } => {
                dump_tables(
                    ctx.clone(),
                    &tables,
                    1,
                    default_partition_size(),
                    ctx.config.microbatch_max_interval,
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
    Dump { end_block: Option<i64> },
}
