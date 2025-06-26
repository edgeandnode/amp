use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream, logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{
    FutureExt, Stream, TryStreamExt as _,
    stream::{self, StreamExt},
};
use metadata_db::{LocationId, MetadataDb};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::task::AbortOnDropHandle;
use tracing::{instrument, warn};

use crate::{
    BlockNum, BoxError,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    catalog::physical::PhysicalTable,
    query_context::QueryContext,
};

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
        let channel = crate::stream_helpers::change_tracking_pg_channel(location);
        let stream = metadata_db.listen(&channel).await?;
        notification_streams.push(stream.map_ok(|n| n.channel().to_string()));
        channel_to_location.insert(channel, location);
    }

    let notifications = futures::stream::select_all(notification_streams);

    // Create the stream channel. This is unbounded because we never want to put backpressure on the
    // PG notification queue.
    let (tx, rx) = mpsc::unbounded_channel();

    // Send initial watermark
    tx.send(Ok(watermarks.common_watermark())).unwrap();

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

            if tx.send(res).is_err() {
                break; // Receiver dropped
            }
        }
        warn!("notification stream ended");
    });

    Ok(Box::pin(UnboundedReceiverStream::new(rx)))
}

/// A handle to a streaming query that can be used to retrieve results as a stream.
///
/// Aborts the query task when dropped.
pub struct StreamingQueryHandle {
    rx: mpsc::Receiver<RecordBatch>,
    join_handle: AbortOnDropHandle<Result<(), BoxError>>,
    schema: SchemaRef,
}

impl StreamingQueryHandle {
    pub fn as_stream(self) -> impl Stream<Item = Result<RecordBatch, BoxError>> {
        let data_stream = ReceiverStream::new(self.rx);

        let join = self.join_handle;

        // If `tx` has been dropped then the query task has terminated. So we check if it has
        // terminated with errors, and if so send the error as the final item of the stream.
        let get_task_result = async move {
            // Unwrap: The task is known to have terminated.
            match join.now_or_never().unwrap() {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(Err(e)),
                Err(join_err) => Some(Err(
                    format!("Streaming task failed to join: {}", join_err).into()
                )),
            }
        };

        data_stream
            .map(Ok)
            .chain(stream::once(get_task_result).filter_map(|x| async { x }))
    }

    pub fn as_record_batch_stream(self) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let stream = RecordBatchStreamAdapter::new(
            schema,
            self.as_stream().map_err(DataFusionError::External),
        );
        Box::pin(stream)
    }
}

/// A streaming query that continuously listens for new blocks and emits incremental results.
///
/// This follows a 'microbatch' model where it processes data in chunks based on watermarks.
pub struct StreamingQuery {
    ctx: Arc<QueryContext>,
    plan: LogicalPlan,
    state: StreamState,
    tx: mpsc::Sender<RecordBatch>,
}

struct StreamState {
    watermark_stream: WatermarkStream,
    next_start: BlockNum,
}

impl StreamingQuery {
    /// Creates a new streaming query. It is assumed that the `ctx` was built such that it contains
    /// only the tables relevant for the query.
    ///
    /// The query execution loop will run in its own task.
    pub async fn spawn(
        ctx: Arc<QueryContext>,
        plan: LogicalPlan,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<StreamingQueryHandle, BoxError> {
        let watermark_stream = watermark_updates(ctx.clone(), metadata_db.clone()).await?;
        let schema: SchemaRef = plan.schema().clone().as_ref().clone().into();
        let (tx, rx) = mpsc::channel(10);
        let streaming_query = Self {
            ctx,
            plan,
            tx,
            state: StreamState {
                watermark_stream,
                next_start: 0,
            },
        };

        let join_handle = AbortOnDropHandle::new(tokio::spawn(streaming_query.execute()));

        Ok(StreamingQueryHandle {
            rx,
            join_handle,
            schema,
        })
    }

    /// The loop:
    /// 1. Get new input watermark
    /// 2. Execute microbatch up to that watermark
    /// 3. Send out results
    #[instrument(skip_all, err)]
    async fn execute(mut self) -> Result<(), BoxError> {
        loop {
            // Get the next watermark
            let watermark = {
                let Some(watermark) = self.state.watermark_stream.next().await else {
                    // Watermark stream ended, no more data ever?
                    return Ok(());
                };

                let watermark = watermark?;

                match watermark {
                    // Duplicate watermark, nothing to do
                    Some(watermark) if watermark < self.state.next_start => continue,

                    Some(watermark) => watermark,

                    // Tables seem empty, lets wait for some data
                    None => continue,
                }
            };

            let start = self.state.next_start;
            self.state.next_start = watermark + 1;

            // Start microbatch execution
            let mut stream = self
                .ctx
                .execute_plan_for_range(self.plan.clone(), start, watermark, false)
                .await?;

            // Drain the microbatch completely
            while let Some(item) = stream.next().await {
                let item = item?;
                let res = self.tx.send(item).await;
                if res.is_err() {
                    // Receiver dropped
                    return Ok(());
                }
            }
        }
    }
}
