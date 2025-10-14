//! Rust client library for Amp

mod decode;
pub mod stores;

use std::{
    ops::RangeInclusive,
    pin::Pin,
    task::{Context, Poll},
};

use arrow_flight::{FlightData, sql::client::FlightSqlServiceClient};
use async_stream::stream;
use bytes::Bytes;
use common::{
    BlockNum,
    arrow::array::RecordBatch,
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::{Stream, StreamExt as _, stream::BoxStream};
use serde::Deserialize;
pub use stores::{InMemoryResumeStore, ResumeStore};
use tonic::{Streaming, transport::Endpoint};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("{0}")]
    Arrow(#[from] common::arrow::error::ArrowError),
    #[error("gRPC status error: {0}")]
    Status(#[from] Box<tonic::Status>),
    #[error("server error: {0}")]
    Server(String),
    #[error("JSON deserialization error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::from(Box::new(value))
    }
}

pub struct ResultStream {
    decoder: decode::FlightDataDecoder<Streaming<FlightData>>,
}

#[derive(Clone, Debug)]
pub struct ResponseBatch {
    pub data: RecordBatch,
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Metadata {
    pub ranges: Vec<BlockRange>,
    pub ranges_complete: bool,
}

impl Stream for ResultStream {
    type Item = Result<ResponseBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.decoder).poll_next(cx)
    }
}

/// Arrow Flight client for connecting to amp server.
#[derive(Clone)]
pub struct SqlClient {
    client: FlightSqlServiceClient<tonic::transport::Channel>,
}

impl SqlClient {
    pub async fn new(endpoint: &str) -> Result<Self, Error> {
        let endpoint: Endpoint = endpoint.parse()?;
        let channel = endpoint.connect().await?;
        let client = FlightSqlServiceClient::new(channel);
        Ok(Self { client })
    }

    pub fn inner_client(&mut self) -> &mut FlightSqlServiceClient<tonic::transport::Channel> {
        &mut self.client
    }

    pub async fn query<S: ToString>(
        &mut self,
        sql: S,
        transaction_id: Option<Bytes>,
        resume_watermark: Option<&ResumeWatermark>,
    ) -> Result<ResultStream, Error> {
        self.client.set_header(
            "amp-resume",
            resume_watermark
                .map(|value| serde_json::to_string(value).unwrap())
                .unwrap_or_default(),
        );
        let flight_info = match self.client.execute(sql.to_string(), transaction_id).await {
            Ok(flight_info) => flight_info,
            Err(err) => {
                // Unset the amp-resume header after GetFlightInfo, since otherwise it gets
                // retained for subsequent requests.
                self.client.set_header("amp-resume", "");
                return Err(err.into());
            }
        };
        let ticket = flight_info
            .endpoint
            .into_iter()
            .next()
            .and_then(|endpoint| endpoint.ticket)
            .ok_or_else(|| Error::Server("FlightInfo missing ticket".to_string()))?;
        let flight_data = self.client.inner_mut().do_get(ticket).await?.into_inner();
        let decoder = decode::FlightDataDecoder::new(flight_data);
        Ok(ResultStream { decoder })
    }
}

#[derive(Clone, Debug)]
pub enum ResponseBatchWithReorg {
    /// Response record batch with associated block ranges.
    Batch {
        data: RecordBatch,
        metadata: Metadata,
    },
    /// Watermark used to indicate the fully processed ranges when resuming a dropped stream.
    Watermark(ResumeWatermark),
    /// Reorg marker, invalidating prior batches overlapping with the given ranges.
    Reorg {
        invalidation: Vec<InvalidationRange>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidationRange {
    pub network: String,
    pub numbers: RangeInclusive<BlockNum>,
}

impl InvalidationRange {
    /// Return true iff the given range overlaps with block numbers on the same network.
    pub fn invalidates(&self, range: &BlockRange) -> bool {
        (self.network == range.network)
            && !(*self.numbers.end() < range.start() || range.end() < *self.numbers.start())
    }
}

impl From<BlockRange> for InvalidationRange {
    fn from(value: BlockRange) -> Self {
        Self {
            network: value.network,
            numbers: value.numbers,
        }
    }
}

/// Transform a response batch stream to detect and signal blockchain reorganizations.
///
/// This function wraps a `ResultStream` and monitors the block ranges in consecutive
/// batches to detect chain reorganizations (reorgs). When a reorg is detected, a
/// `ResponseBatchWithReorg::Reorg` variant is emitted containing invalidation ranges
/// before yielding the next batch.
///
/// # Example
///
/// ```rust,no_run
/// use amp_client::{Error, ResponseBatchWithReorg, with_reorg};
/// use futures::StreamExt;
///
/// # async fn example(stream: amp_client::ResultStream) -> Result<(), Error> {
/// let mut reorg_stream = with_reorg(stream);
/// while let Some(result) = reorg_stream.next().await {
///     match result? {
///         ResponseBatchWithReorg::Batch { data, metadata } => {
///             println!("Received batch for block ranges: {:#?}", metadata.ranges);
///         }
///         ResponseBatchWithReorg::Watermark(watermark) => {
///             println!("Completed stream up to {:#?}", watermark);
///         }
///         ResponseBatchWithReorg::Reorg { invalidation } => {
///             println!("Reorg detected, invalidating ranges: {:#?}", invalidation);
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub fn with_reorg(
    mut result_stream: ResultStream,
) -> BoxStream<'static, Result<ResponseBatchWithReorg, Error>> {
    stream! {
        let mut prev_ranges: Vec<BlockRange> = vec![];
        while let Some(result) = result_stream.next().await {
            let batch = match result {
                Ok(batch) => batch,
                Err(err) => {
                    yield Err(err);
                    continue;
                }
            };
            let ranges = batch.metadata.ranges.clone();
            let invalidation: Vec<InvalidationRange> = ranges.iter().filter_map(|range| {
                let prev_range = prev_ranges.iter().find(|r| r.network == range.network)?;
                if (range != prev_range) && (range.start() <= prev_range.end()) {
                    return Some(InvalidationRange {
                        network: range.network.clone(),
                        numbers: range.start()..=BlockNum::max(range.end(), prev_range.end()),
                    });
                }
                None
            }).collect();
            if !invalidation.is_empty() {
                yield Ok(ResponseBatchWithReorg::Reorg { invalidation });
            }
            prev_ranges = ranges;
            if batch.metadata.ranges_complete {
                let watermark = ResumeWatermark::from_ranges(batch.metadata.ranges);
                yield Ok(ResponseBatchWithReorg::Watermark(watermark));
            } else {
                yield Ok(ResponseBatchWithReorg::Batch{
                    data: batch.data,
                    metadata: batch.metadata,
                });
            }
        }
    }
    .boxed()
}

/// Wrap a ResponseBatchWithReorg stream to automatically store resume watermarks.
///
/// This function intercepts `Watermark` events from the stream and stores them
/// in the provided `ResumeStore`, allowing for automatic watermark management
/// and query resumption.
///
/// # Arguments
/// * `id` - Unique identifier for this query (used to store/retrieve watermarks)
/// * `store` - The resume store implementation to use for persistence
/// * `stream` - The input stream of ResponseBatchWithReorg events
///
/// # Returns
/// A boxed stream that passes through all events unchanged while storing watermarks
///
/// # Example
///
/// ```rust,no_run
/// use amp_client::{
///     Error, InMemoryResumeStore, ResponseBatchWithReorg, ResumeStore, with_reorg,
///     with_resume_store,
/// };
/// use futures::StreamExt;
///
/// # async fn example(stream: amp_client::ResultStream) -> Result<(), Error> {
/// let mut store = InMemoryResumeStore::new();
/// let reorg_stream = with_reorg(stream);
/// let mut resumable_stream = with_resume_store("my_query", store.clone(), reorg_stream);
///
/// while let Some(result) = resumable_stream.next().await {
///     match result? {
///         ResponseBatchWithReorg::Batch { data, metadata } => {
///             println!("Received batch: {:#?}", metadata.ranges);
///         }
///         ResponseBatchWithReorg::Watermark(watermark) => {
///             // Watermark is automatically stored in the store
///             println!("Watermark stored: {:#?}", watermark);
///         }
///         ResponseBatchWithReorg::Reorg { invalidation } => {
///             println!("Reorg detected: {:#?}", invalidation);
///         }
///     }
/// }
///
/// // Later, retrieve the watermark for resumption
/// let watermark = store.get("my_query").await?;
/// # Ok(())
/// # }
/// ```
pub fn with_resume_store<S>(
    id: impl Into<String>,
    mut store: S,
    mut stream: BoxStream<'static, Result<ResponseBatchWithReorg, Error>>,
) -> BoxStream<'static, Result<ResponseBatchWithReorg, Error>>
where
    S: ResumeStore + 'static,
{
    let id = id.into();
    stream! {
        while let Some(result) = stream.next().await {
            match result {
                Ok(ResponseBatchWithReorg::Watermark(ref watermark)) => {
                    // NOTE: This is not transactionally safe. In case of a crash between storing
                    // the watermark and downstream processing completing, the watermark is saved
                    // but downstream state updates may be incomplete. On resume, the server will
                    // not re-emit this watermark, potentially leaving downstream in an inconsistent
                    // state.
                    //
                    // Trade-off: We prioritize resume capability (not losing stream position) over
                    // transactional consistency. This assumes downstream processing is idempotent
                    // and can handle replays gracefully.
                    //
                    // TODO: For full transactional safety, would need:
                    // - Two-phase commit between watermark storage and downstream state updates
                    // - Single transaction encompassing both operations
                    // - Write-ahead log for downstream operations

                    // Store the watermark first but yield watermark before propagating any errors.
                    let store_result = store.set_watermark(&id, watermark.clone()).await;

                    // Always yield the watermark so downstream consumers can act on it.
                    yield result;

                    // Propagate storage errors after yielding the watermark.
                    if let Err(e) = store_result {
                        yield Err(e);
                        break;
                    }
                }
                _ => {
                    // Pass through all other events unchanged.
                    yield result;
                }
            }
        }
    }
    .boxed()
}
