//! Type definitions for worker jobs

use std::sync::Arc;

use common::{BoxError, catalog::physical::PhysicalTable};
pub use dump::Ctx;
use dump::{EndBlock, metrics};
pub use metadata_db::JobStatus;

use crate::JobCreationError;

mod id;
mod notif;

pub use self::{
    id::{JobId, JobIdFromStrError, JobIdI64ConvError, JobIdU64Error},
    notif::{Action, Notification},
};

/// The logical descriptor of a job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Descriptor {
    Dump { end_block: EndBlock },
}

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
        /// The end block configuration for the dump.
        end_block: EndBlock,
        /// Metrics registry.
        metrics: Option<Arc<metrics::MetricsRegistry>>,
        /// Meter for creating telemetry objects.
        meter: Option<monitoring::telemetry::metrics::Meter>,
    },
}

impl Job {
    /// Try to build a job from a job ID and descriptor.
    pub async fn try_from_descriptor(
        ctx: Ctx,
        job_id: JobId,
        job_desc: Descriptor,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Result<Job, JobCreationError> {
        let output_locations = ctx
            .metadata_db
            .output_locations(job_id)
            .await
            .map_err(JobCreationError::OutputLocationsFetchFailed)?;

        match job_desc {
            Descriptor::Dump { end_block } => {
                let mut tables = vec![];
                for location in output_locations {
                    let dataset_version = location.dataset_version.parse().ok();
                    let dataset = Arc::new(
                        ctx.dataset_store
                            .get_dataset(&location.dataset, dataset_version.as_ref())
                            .await
                            .map_err(|err| JobCreationError::DatasetFetchFailed(err.into()))?
                            .ok_or_else(|| JobCreationError::DatasetNotFound {
                                dataset: location.dataset.clone(),
                            })?,
                    );

                    let mut resolved_tables = dataset.resolved_tables();
                    let Some(table) = resolved_tables.find(|t| t.name() == location.table) else {
                        return Err(JobCreationError::TableNotFound {
                            table: location.table,
                            dataset: location.dataset,
                        });
                    };

                    tables.push(
                        PhysicalTable::new(
                            table.clone(),
                            location.url,
                            location.id,
                            ctx.metadata_db.clone(),
                        )
                        .map_err(JobCreationError::PhysicalTableCreationFailed)?
                        .into(),
                    );
                }

                Ok(Job::DumpTables {
                    ctx,
                    tables,
                    end_block,
                    metrics,
                    meter,
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
                metrics,
                meter,
            } => {
                dump::dump_tables(
                    ctx.clone(),
                    &tables,
                    1,
                    ctx.config.microbatch_max_interval,
                    end_block,
                    metrics,
                    meter.as_ref(),
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
