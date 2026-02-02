//! Internal job implementation for the worker service.

use std::{future::Future, sync::Arc};

use datasets_common::hash_reference::HashReference;
use dump::{Ctx, Error as DumpError, ProgressCallback, metrics::MetricsRegistry};
use kafka_client::proto;
use tracing::{Instrument, info_span};

use crate::{
    events::WorkerProgressCallback,
    job::{JobDescriptor, JobId},
    service::WorkerJobCtx,
};

/// Create and run a worker job that dumps tables from a dataset.
///
/// This function returns a future that executes the dump operation.
pub(super) fn new(
    job_ctx: WorkerJobCtx,
    job_id: JobId,
    job_desc: JobDescriptor,
) -> impl Future<Output = Result<(), DumpError>> {
    let (end_block, max_writers, reference, dataset_kind, dataset_info) = match job_desc {
        JobDescriptor::Dump {
            end_block,
            max_writers,
            dataset_namespace,
            dataset_name,
            manifest_hash,
            dataset_kind,
        } => {
            let hash_reference = HashReference::new(
                dataset_namespace.clone(),
                dataset_name.clone(),
                manifest_hash.clone(),
            );
            let dataset_info = proto::DatasetInfo {
                namespace: dataset_namespace.to_string(),
                name: dataset_name.to_string(),
                manifest_hash: manifest_hash.to_string(),
            };
            (
                end_block,
                max_writers,
                hash_reference,
                dataset_kind,
                dataset_info,
            )
        }
    };

    let metrics = job_ctx
        .meter
        .as_ref()
        .map(|m| Arc::new(MetricsRegistry::new(m, reference.clone())));

    // Create progress callback for event streaming
    // Always create the callback - NoOpEmitter will discard events if not needed
    let progress_callback: Option<Arc<dyn ProgressCallback>> = Some(Arc::new(
        WorkerProgressCallback::new(job_id, dataset_info, job_ctx.event_emitter.clone()),
    ));

    // Create Ctx instance for job execution
    let ctx = Ctx {
        config: job_ctx.config.dump_config(),
        metadata_db: job_ctx.metadata_db.clone(),
        dataset_store: job_ctx.dataset_store.clone(),
        data_store: job_ctx.data_store.clone(),
        notification_multiplexer: job_ctx.notification_multiplexer.clone(),
        metrics,
        progress_callback,
    };

    let microbatch_max_interval = job_ctx.config.microbatch_max_interval;
    let writer: metadata_db::JobId = job_id.into();
    async move {
        dump::dump_tables(
            ctx,
            &reference,
            dataset_kind,
            max_writers,
            microbatch_max_interval,
            end_block,
            writer,
        )
        .instrument(info_span!("dump_job", %job_id, dataset = %format!("{reference:#}")))
        .await
    }
}
