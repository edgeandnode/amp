use crate::service::Error;
use common::query_context::Error as CoreError;
use common::query_context::QueryContext;
use common::BlockNum;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use futures::channel::mpsc;
use futures::stream::{BoxStream, StreamExt};
use metadata_db::MetadataDb;
use std::sync::Arc;

pub type BlockRange = (BlockNum, BlockNum);

/// Creates a stream of block ranges for incremental processing
pub async fn create_range_stream(
    ctx: Arc<QueryContext>,
    plan: LogicalPlan,
    metadata_db: Arc<MetadataDb>,
) -> Result<BoxStream<'static, BlockRange>, Error> {
    // Assumption: The `ctx` contains only the tables that are relevant for the plan.
    let tables = ctx.catalog().tables().to_vec();

    // Get initial range
    let initial_range = {
        let synced_blocks = ctx
            .synced_blocks_for_plan(&plan)
            .await
            .map_err(|e| Error::CoreError(CoreError::DatasetError(e)))?;

        // We are not interested in gaps, so we take the first range
        let mut initial_range = synced_blocks.first();

        // This check is likely problematic, as there is nothing that guarantees that the writer will
        // always write from block 0 and commit block 0 first. But for now it's better to fail than to do
        // something inconsistent.
        if initial_range.is_some_and(|r| r.0 != 0) {
            return Err(Error::StreamingExecutionError(format!(
                "Initial range must start from block 0, but range is {:?}",
                initial_range
            )));
        }
    };

    // Set up change notifications
    let location = ctx.catalog().tables().iter().map(|t| t.location_id());
    let mut notification_streams = Vec::new();

    for location in locations {
        let channel = common::stream_helpers::change_tracking_pg_channel(location);
        let stream = metadata_db.listen(&channel).await.map_err(|e| {
            Error::StreamingExecutionError(format!(
                "Failed to listen on channel {}: {}",
                channel, e
            ))
        })?;
        notification_streams.push(stream.map(|_| ()));
    }

    let notifications = futures::stream::select_all(notification_streams);

    // Create the range stream
    let (tx, rx) = mpsc::unbounded();

    // Send initial ranges
    for range in initial_ranges {
        let _ = tx.unbounded_send(range);
    }

    // Spawn task to handle new ranges from notifications
    tokio::spawn(async move {
        let mut notifications = notifications;

        while let Some(()) = notifications.next().await {
            match ctx.max_end_block(&plan).await {
                Ok(Some(end)) => {
                    let (start, end) = match (current_end_block, Some(end)) {
                        (Some(start), Some(end)) if end > start => (start + 1, end),
                        (None, Some(end)) => (0, end),
                        _ => continue,
                    };

                    if tx.unbounded_send((start, end)).is_err() {
                        break; // Receiver dropped
                    }

                    current_end_block = Some(end);
                }
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("Failed to get max end block: {}", e);
                    continue;
                }
            }
        }
    });

    Ok(rx.boxed())
}

/// Executes a streaming query, which continuously listens for new blocks on the input tables and
/// emits incremental results. Currently not so complicated as only stateless queries are supported.
pub async fn execute_as_stream(
    ctx: Arc<QueryContext>,
    plan: LogicalPlan,
    metadata_db: Arc<MetadataDb>,
) -> Result<BoxStream<'static, Result<RecordBatch, DataFusionError>>, Error> {
    let range_stream = create_range_stream(ctx.clone(), plan.clone(), metadata_db).await?;

    let batch_stream = range_stream
        .then(move |range| {
            let ctx = ctx.clone();
            let plan = plan.clone();
            async move {
                let (start, end) = range;
                match dataset_store::sql_datasets::execute_plan_for_range(
                    plan, &ctx, start, end, false,
                )
                .await
                {
                    Ok(mut stream) => {
                        let mut batches = Vec::new();
                        while let Some(batch_result) = stream.next().await {
                            batches.push(batch_result);
                        }
                        futures::stream::iter(batches).left_stream()
                    }
                    Err(e) => {
                        eprintln!(
                            "Failed to execute plan for range [{}, {}]: {}",
                            start, end, e
                        );
                        futures::stream::empty().right_stream()
                    }
                }
            }
        })
        .flatten();

    Ok(batch_stream.boxed())
}
