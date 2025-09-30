use std::sync::Arc;

use common::{BoxError, catalog::physical::PhysicalTable};
use metadata_db::JobId;
use tracing::instrument;

pub use crate::core::Ctx;
use crate::{core::dump_tables, default_partition_size, metrics};

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
        /// Metrics registry.
        metrics: Option<Arc<metrics::MetricsRegistry>>,
        /// Meter for creating telemetry objects.
        meter: Option<monitoring::telemetry::metrics::Meter>,
    },
}

impl Job {
    /// Try to build a job from a job ID and descriptor.
    #[instrument(skip(ctx, job_id, job_desc, metrics), err)]
    pub async fn try_from_descriptor(
        ctx: Ctx,
        job_id: JobId,
        job_desc: JobDesc,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Result<Job, BoxError> {
        let output_locations = ctx.metadata_db.output_locations(job_id).await?;
        match job_desc {
            JobDesc::Dump { end_block } => {
                let mut tables = vec![];
                for location in output_locations {
                    let dataset_version = location.dataset_version.parse().ok();
                    let dataset = Arc::new(
                        ctx.dataset_store
                            .get_dataset(&location.dataset, dataset_version.as_ref())
                            .await?
                            .ok_or_else(|| format!("Dataset '{}' not found", location.dataset))?,
                    );
                    let mut resolved_tables = dataset.resolved_tables();
                    let Some(table) = resolved_tables.find(|t| t.name() == location.table) else {
                        return Err(format!(
                            "Table `{}` not found in dataset `{}`",
                            location.table, location.dataset
                        )
                        .into());
                    };
                    tables.push(
                        PhysicalTable::new(
                            table.clone(),
                            location.url,
                            location.id,
                            ctx.metadata_db.clone(),
                        )?
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
                dump_tables(
                    ctx.clone(),
                    &tables,
                    1,
                    default_partition_size(),
                    ctx.config.microbatch_max_interval,
                    end_block,
                    metrics,
                    meter.as_ref(),
                    false,
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
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JobDesc {
    Dump { end_block: Option<i64> },
}
