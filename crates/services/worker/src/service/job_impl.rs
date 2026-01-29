//! Internal job implementation for the worker service.

use std::{future::Future, sync::Arc};

use datasets_common::hash_reference::HashReference;
use dump::{Ctx, Error as DumpError, metrics::MetricsRegistry};
use tracing::{Instrument, info_span};

use crate::{
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
