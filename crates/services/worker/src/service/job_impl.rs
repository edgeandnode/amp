//! Internal job implementation for the worker service.

use std::{future::Future, sync::Arc};

use amp_worker_core::{Ctx, ProgressReporter, metrics::MetricsRegistry};
use datasets_common::hash_reference::HashReference;
use tracing::{Instrument, info_span};

use crate::{
    events::WorkerProgressReporter,
    job::{JobDescriptor, JobId},
    kafka::proto,
    service::WorkerJobCtx,
};

/// Create and run a worker job that materializes tables from a dataset.
///
/// This function returns a future that executes the materialization operation.
/// Raw datasets are handled by `amp_worker_datasets_raw` and derived
/// datasets are handled by `amp_worker_datasets_derived`.
pub(super) fn new(
    job_ctx: WorkerJobCtx,
    job_id: JobId,
    job_desc: JobDescriptor,
) -> impl Future<Output = Result<(), JobError>> {
    let reference = match &job_desc {
        JobDescriptor::MaterializeRaw(desc) => HashReference::new(
            desc.dataset_namespace.clone(),
            desc.dataset_name.clone(),
            desc.manifest_hash.clone(),
        ),
        JobDescriptor::MaterializeDerived(desc) => HashReference::new(
            desc.dataset_namespace.clone(),
            desc.dataset_name.clone(),
            desc.manifest_hash.clone(),
        ),
    };

    let metrics = job_ctx
        .meter
        .as_ref()
        .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone(), *job_id)));

    // Create progress reporter for event streaming
    // Always create the reporter - NoOpEmitter will discard events if not needed
    let progress_reporter: Option<Arc<dyn ProgressReporter>> = {
        let dataset_info = proto::DatasetInfo {
            namespace: reference.namespace().to_string(),
            name: reference.name().to_string(),
            manifest_hash: reference.hash().to_string(),
        };
        Some(Arc::new(WorkerProgressReporter::new(
            job_id,
            dataset_info,
            job_ctx.event_emitter.clone(),
        )))
    };

    // Create Ctx instance for job execution
    let ctx = Ctx {
        config: job_ctx.config.dump_config(),
        metadata_db: job_ctx.metadata_db.clone(),
        dataset_store: job_ctx.dataset_store.clone(),
        data_store: job_ctx.data_store.clone(),
        notification_multiplexer: job_ctx.notification_multiplexer.clone(),
        metrics,
    };

    let writer: metadata_db::jobs::JobId = job_id.into();
    async move {
        match job_desc {
            JobDescriptor::MaterializeRaw(desc) => {
                amp_worker_datasets_raw::dump(
                    ctx,
                    &reference,
                    desc.max_writers,
                    desc.end_block,
                    writer,
                    progress_reporter,
                )
                .instrument(
                    info_span!("materialize_raw_job", %job_id, dataset = %format!("{reference:#}")),
                )
                .await
                .map_err(JobError::MaterializeRaw)?;
            }
            JobDescriptor::MaterializeDerived(desc) => {
                let microbatch_max_interval = job_ctx.config.microbatch_max_interval;
                amp_worker_datasets_derived::dump(
                    ctx,
                    &reference,
                    microbatch_max_interval,
                    desc.end_block,
                    writer,
                    progress_reporter,
                )
                .instrument(info_span!("materialize_derived_job", %job_id, dataset = %format!("{reference:#}")))
                .await
                .map_err(JobError::MaterializeDerived)?;
            }
        }

        Ok(())
    }
}

/// Errors from dataset materialization job execution.
///
/// Wraps the specific error types from raw and derived dataset materialization operations
/// to provide a unified error type for the worker job system.
#[derive(Debug, thiserror::Error)]
pub(crate) enum JobError {
    /// Raw dataset materialization operation failed
    ///
    /// This occurs when the raw dataset extraction and Parquet file writing
    /// process encounters an error. Common causes include blockchain client
    /// connectivity issues, consistency check failures, and partition task errors.
    #[error("Failed to materialize raw dataset")]
    MaterializeRaw(#[source] amp_worker_datasets_raw::Error),

    /// Derived dataset materialization operation failed
    ///
    /// This occurs when the derived dataset SQL query execution and Parquet
    /// file writing process encounters an error. Common causes include query
    /// environment creation failures, manifest retrieval errors, and table
    /// materialization failures.
    #[error("Failed to materialize derived dataset")]
    MaterializeDerived(#[source] amp_worker_datasets_derived::Error),
}

impl JobError {
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::MaterializeRaw(err) => err.is_fatal(),
            Self::MaterializeDerived(err) => err.is_fatal(),
        }
    }
}
