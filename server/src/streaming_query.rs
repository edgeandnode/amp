use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use common::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::PhysicalTable,
    query_context::QueryContext,
    BlockNum, BoxError,
};
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{channel::mpsc, future::BoxFuture, stream::StreamExt, Stream, TryStreamExt as _};
use metadata_db::{LocationId, MetadataDb};
use tracing::{instrument, warn};

// Tracks watermarks for a set of tables
struct Watermarks {
    table_by_id: BTreeMap<LocationId, Arc<PhysicalTable>>,
    watermarks: BTreeMap<LocationId, Option<BlockNum>>,
}

impl Watermarks {
    async fn new(tables: &[Arc<PhysicalTable>]) -> Result<Self, BoxError> {
        let mut watermarks = BTreeMap::new();
        let mut table_by_id = BTreeMap::new();
        for table in tables {
            let watermark = table.watermark().await?;
            watermarks.insert(table.location_id(), watermark);
            table_by_id.insert(table.location_id(), table.clone());
        }
        Ok(Watermarks {
            watermarks,
            table_by_id,
        })
    }

    /// Updates and returns the watermark for a specific location.
    ///
    /// Errors if the new watermark is less than the current one.
    /// Panics if the location was not provided in the constructor.
    async fn update(&mut self, location: LocationId) -> Result<(), BoxError> {
        let table = self.table_by_id.get(&location).unwrap();
        let current = self.watermarks.get(&location).unwrap();
        let watermark = table.watermark().await?;
        if watermark < *current {
            return Err(format!(
                "New watermark {:?} is less than current {:?} for location {}",
                watermark,
                current,
                table.location_id()
            )
            .into());
        }
        self.watermarks.insert(location, watermark);
        Ok(())
    }

    /// Returns the minimum watermark across all tables
    fn common_watermark(&self) -> Option<BlockNum> {
        self.watermarks.values().min().and_then(|w| *w)
    }
}

pub type WatermarkStream =
    Pin<Box<dyn Stream<Item = Result<Option<BlockNum>, BoxError>> + Send + Sync + 'static>>;

/// Creates a stream of watermark updates for the tables in the context.
#[instrument(skip_all, err)]
pub async fn watermark_updates(
    ctx: Arc<QueryContext>,
    metadata_db: Arc<MetadataDb>,
) -> Result<WatermarkStream, BoxError> {
    let tables = ctx.catalog().tables().to_vec();

    // The most recent watermark we have seen for each input table
    let mut watermarks = Watermarks::new(&tables).await?;

    // Set up change notifications
    let locations = ctx.catalog().tables().iter().map(|t| t.location_id());
    let mut channel_to_location: BTreeMap<String, LocationId> = BTreeMap::new();
    let mut notification_streams = Vec::new();
    for location in locations {
        let channel = common::stream_helpers::change_tracking_pg_channel(location);
        let stream = metadata_db.listen(&channel).await?;
        notification_streams.push(stream.map_ok(|n| n.channel().to_string()));
        channel_to_location.insert(channel, location);
    }

    let notifications = futures::stream::select_all(notification_streams);

    // Create the stream channel. This is unbounded because we never want to put backpressure on the
    // PG notification queue.
    let (tx, rx) = mpsc::unbounded();

    // Send initial watermark
    tx.unbounded_send(Ok(watermarks.common_watermark()))
        .unwrap();

    // Spawn task to handle new ranges from notifications
    tokio::spawn(async move {
        let mut notifications = notifications;
        while let Some(Ok(channel)) = notifications.next().await {
            let location = channel_to_location.get(&*channel).unwrap();
            let watermark = watermarks.update(*location).await;

            let res = match watermark {
                Ok(()) => Ok(watermarks.common_watermark()),
                Err(e) => Err(e),
            };

            if tx.unbounded_send(res).is_err() {
                break; // Receiver dropped
            }
        }
        warn!("notification stream ended");
    });

    Ok(Box::pin(rx))
}

/// A streaming query that continuously listens for new blocks and emits incremental results.
///
/// This follows a 'microbatch' model where it processes data in chunks based on watermarks.
pub struct StreamingQuery {
    ctx: Arc<QueryContext>,
    plan: LogicalPlan,
    state: StreamState,
}

struct StreamState {
    watermark_stream: WatermarkStream,
    next_start: BlockNum,

    // Microbatch that needs to be started
    pending_microbatch: Option<BoxFuture<'static, Result<SendableRecordBatchStream, BoxError>>>,

    // Microbatch that has been started and is emitting results
    current_microbatch: Option<SendableRecordBatchStream>,
}

impl StreamingQuery {
    /// Creates a new streaming query. It is assumed that the `ctx` was built such that it contains
    /// only the tables relevant for the query.
    pub async fn new(
        ctx: Arc<QueryContext>,
        plan: LogicalPlan,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<Self, BoxError> {
        let watermark_stream = watermark_updates(ctx.clone(), metadata_db.clone()).await?;
        Ok(Self {
            ctx,
            plan,
            state: StreamState {
                watermark_stream,
                next_start: 0,
                current_microbatch: None,
                pending_microbatch: None,
            },
        })
    }

    pub fn as_record_batch_stream(self) -> SendableRecordBatchStream {
        let schema: SchemaRef = self.plan.schema().clone().as_ref().clone().into();
        let stream =
            RecordBatchStreamAdapter::new(schema, self.map_err(|e| DataFusionError::External(e)));
        Box::pin(stream)
    }
}

impl Stream for StreamingQuery {
    type Item = Result<RecordBatch, BoxError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First check if we have a pending microbatch to start
            if let Some(ref mut pending) = self.state.pending_microbatch {
                match ready!(pending.as_mut().poll(cx)) {
                    Ok(result_stream) => {
                        self.state.current_microbatch = Some(result_stream);
                        self.state.pending_microbatch = None;
                        // Continue to poll the new result stream
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }

            // If we have a current result stream, try to get the next batch from it
            if let Some(ref mut result_stream) = self.state.current_microbatch {
                match ready!(Pin::new(result_stream).poll_next(cx)) {
                    Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => {
                        // Current stream is exhausted, clear it and continue to get next watermark
                        self.state.current_microbatch = None;
                    }
                }
            }

            // No current result stream or it's exhausted, try to get next watermark
            match ready!(Pin::new(&mut self.state.watermark_stream).poll_next(cx)) {
                Some(Ok(Some(watermark))) => {
                    if watermark < self.state.next_start {
                        // Duplicate watermark, nothing to do
                        continue;
                    }

                    let start = self.state.next_start;
                    self.state.next_start = watermark + 1;

                    // Create a future for the async operation
                    let plan = self.plan.clone();
                    let ctx = self.ctx.clone();

                    let future = Box::pin(async move {
                        dataset_store::sql_datasets::execute_plan_for_range(
                            plan, &ctx, start, watermark, false,
                        )
                        .await
                    });

                    self.state.pending_microbatch = Some(future);
                    // Continue the loop to poll the pending execution
                }
                // Tables seem empty, lets wait for some data
                Some(Ok(None)) => {}
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    // Watermark stream ended, no more data ever?
                    return Poll::Ready(None);
                }
            }
        }
    }
}
