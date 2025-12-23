//! Internal job implementation for the worker service.

use std::{future::Future, sync::Arc};

use common::{BoxError, catalog::physical::PhysicalTable};
use datasets_common::hash_reference::HashReference;
use dump::{Ctx, compaction::AmpCompactor, metrics::MetricsRegistry};
use metadata_db::LocationId;
use tracing::{Instrument, info_span};

use crate::{
    job::{JobDescriptor, JobId},
    service::WorkerJobCtx,
};

/// Create and run a worker job that dumps tables from a dataset.
///
/// This function performs initialization (fetching metadata and building physical tables)
/// and returns a future that executes the dump operation.
pub(super) async fn new(
    job_ctx: WorkerJobCtx,
    job_id: JobId,
    job_desc: JobDescriptor,
) -> Result<impl Future<Output = Result<(), BoxError>>, JobInitError> {
    let (end_block, max_writers, reference, dataset_kind) = match job_desc {
        JobDescriptor::Dump {
            end_block,
            max_writers,
            dataset_namespace,
            dataset_name,
            manifest_hash,
            dataset_kind,
        } => {
            let hash_reference = HashReference::new(dataset_namespace, dataset_name, manifest_hash);
            (end_block, max_writers, hash_reference, dataset_kind)
        }
    };

    let metrics = job_ctx
        .meter
        .as_ref()
        .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone())));

    // Create Ctx instance for job execution
    let ctx = Ctx {
        config: job_ctx.config.dump_config(),
        metadata_db: job_ctx.metadata_db.clone(),
        dataset_store: job_ctx.dataset_store.clone(),
        data_store: job_ctx.data_store.clone(),
        notification_multiplexer: job_ctx.notification_multiplexer.clone(),
        metrics,
    };

    // Get the dataset to access its tables
    let dataset = ctx
        .dataset_store
        .get_dataset(&reference)
        .await
        .map_err(|err| JobInitError::FetchDataset {
            reference: reference.clone(),
            source: err,
        })?;

    let metrics = ctx.metrics.clone();
    let opts = dump::parquet_opts(&ctx.config.parquet);

    // Create physical tables and compactors for each table in the dataset
    let mut tables: Vec<(Arc<PhysicalTable>, Arc<AmpCompactor>)> = vec![];
    let mut location_ids: Vec<LocationId> = vec![];
    for table in dataset.resolved_tables(reference.to_reference().into()) {
        // Try to get existing active physical table (handles retry case)
        let physical_table: Arc<PhysicalTable> =
            match PhysicalTable::get_active(ctx.data_store.clone(), table.clone())
                .await
                .map_err(JobInitError::GetActivePhysicalTable)?
            {
                // Reuse existing table (retry scenario)
                Some(pt) => pt,
                // Create new table (initial attempt)
                None => common::catalog::physical::register_new_table_revision(
                    ctx.data_store.clone(),
                    reference.clone(),
                    table,
                )
                .await
                .map_err(JobInitError::RegisterNewPhysicalTable)?,
            }
            .into();

        let compactor = AmpCompactor::start(
            ctx.metadata_db.clone(),
            ctx.data_store.clone(),
            opts.clone(),
            physical_table.clone(),
            metrics.clone(),
        )
        .into();

        location_ids.push(physical_table.location_id());
        tables.push((physical_table, compactor));
    }

    // Assign all physical tables to this job as the writer (locks them)
    metadata_db::physical_table::assign_job_writer(&ctx.metadata_db, &location_ids, job_id)
        .await
        .map_err(JobInitError::AssignJobWriter)?;

    let microbatch_max_interval = job_ctx.config.microbatch_max_interval;
    let fut = async move {
        dump::dump_tables(
            ctx,
            &reference,
            dataset_kind,
            &tables,
            max_writers,
            microbatch_max_interval,
            end_block,
        )
        .instrument(info_span!("dump_job", %job_id, dataset = %reference.short_display()))
        .await
        .map_err(|err| err.into())
    };
    Ok(fut)
}

/// Errors that occur during job initialization
///
/// This error type is used by the job initialization phase which fetches
/// metadata and builds physical tables before starting the actual dump.
#[derive(Debug, thiserror::Error)]
pub enum JobInitError {
    /// Failed to fetch dataset from dataset store
    ///
    /// This error occurs when retrieving a dataset by its hash reference fails.
    /// Common causes include:
    /// - Dataset store unavailable or unreachable
    /// - Manifest not cached or registered in the store
    /// - Dataset does not exist for the given hash reference
    #[error("Failed to fetch dataset {reference}")]
    FetchDataset {
        reference: HashReference,
        #[source]
        source: amp_dataset_store::GetDatasetError,
    },

    /// Failed to get or create active physical table
    ///
    /// This error occurs when querying for an active physical table fails.
    /// This typically happens due to database connection issues.
    ///
    /// Note: This wraps BoxError because PhysicalTable::get_active currently
    /// returns BoxError. This should be replaced with a concrete error type.
    #[error("Failed to get active physical table")]
    GetActivePhysicalTable(#[source] BoxError),

    /// Failed to register physical table revision
    ///
    /// This error occurs when registering a new physical table revision fails,
    /// typically due to storage configuration issues, database connection problems,
    /// or invalid URL construction.
    #[error("Failed to register new physical table")]
    RegisterNewPhysicalTable(#[source] common::catalog::physical::RegisterNewTableRevisionError),

    /// Failed to assign job writer
    ///
    /// This error occurs when assigning the job as the writer for physical
    /// table locations fails, typically due to database connection issues.
    #[error("Failed to assign job writer")]
    AssignJobWriter(#[source] metadata_db::Error),
}
