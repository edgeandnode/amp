//! Internal job implementation for the worker service.

use std::{future::Future, sync::Arc};

use amp_worker_core::{Ctx, ProgressReporter, metrics::MetricsRegistry};
use datasets_common::hash_reference::HashReference;
use datasets_derived::DerivedDatasetKind;
use tracing::{Instrument, info_span};

use crate::{
    events::WorkerProgressReporter,
    job::{JobDescriptor, JobId},
    kafka::proto,
    service::WorkerJobCtx,
};

/// Create and run a worker job that dumps tables from a dataset.
///
/// This function returns a future that executes the dump operation.
/// Raw datasets are handled by `amp_worker_datasets_raw` and derived
/// datasets are handled by `amp_worker_datasets_derived`.
pub(super) fn new(
    job_ctx: WorkerJobCtx,
    job_id: JobId,
    job_desc: JobDescriptor,
) -> impl Future<Output = Result<(), DumpError>> {
    let JobDescriptor::Dump {
        end_block,
        max_writers,
        dataset_namespace,
        dataset_name,
        manifest_hash,
        dataset_kind,
    } = job_desc;

    let reference = HashReference::new(
        dataset_namespace.clone(),
        dataset_name.clone(),
        manifest_hash.clone(),
    );

    let metrics = job_ctx
        .meter
        .as_ref()
        .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone())));

    // Create progress reporter for event streaming
    // Always create the reporter - NoOpEmitter will discard events if not needed
    let progress_reporter: Option<Arc<dyn ProgressReporter>> = {
        let dataset_info = proto::DatasetInfo {
            namespace: dataset_namespace.to_string(),
            name: dataset_name.to_string(),
            manifest_hash: manifest_hash.to_string(),
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

    let microbatch_max_interval = job_ctx.config.microbatch_max_interval;
    let writer: metadata_db::JobId = job_id.into();
    async move {
        if dataset_kind == DerivedDatasetKind {
            amp_worker_datasets_derived::dump(
                ctx,
                &reference,
                microbatch_max_interval,
                end_block,
                writer,
                progress_reporter,
            )
            .instrument(info_span!("dump_job", %job_id, dataset = %format!("{reference:#}")))
            .await
            .map_err(DumpError::Derived)?;
        } else {
            amp_worker_datasets_raw::dump(
                ctx,
                &reference,
                max_writers,
                end_block,
                writer,
                progress_reporter,
            )
            .instrument(info_span!("dump_job", %job_id, dataset = %format!("{reference:#}")))
            .await
            .map_err(DumpError::Raw)?;
        }

        Ok(())
    }
}

/// Errors from dataset dump job execution.
///
/// Wraps the specific error types from raw and derived dataset dump operations
/// to provide a unified error type for the worker job system.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DumpError {
    /// Raw dataset dump operation failed
    ///
    /// This occurs when the raw dataset extraction and Parquet file writing
    /// process encounters an error. Common causes include blockchain client
    /// connectivity issues, consistency check failures, and partition task errors.
    #[error("Failed to dump raw dataset")]
    Raw(#[source] amp_worker_datasets_raw::Error),

    /// Derived dataset dump operation failed
    ///
    /// This occurs when the derived dataset SQL query execution and Parquet
    /// file writing process encounters an error. Common causes include query
    /// environment creation failures, manifest retrieval errors, and table
    /// dump failures.
    #[error("Failed to dump derived dataset")]
    Derived(#[source] amp_worker_datasets_derived::Error),
}

impl DumpError {
    pub fn is_fatal(&self) -> bool {
        match self {
            Self::Raw(err) => err.is_fatal(),
            Self::Derived(err) => err.is_fatal(),
        }
    }
}
