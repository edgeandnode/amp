//! AmpClient implementation

use std::{
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    task::{Context, Poll},
};

use arrow_flight::{FlightData, sql::client::FlightSqlServiceClient};
use async_stream::try_stream;
use common::{
    BlockNum,
    arrow::array::RecordBatch,
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::{Stream as FuturesStream, StreamExt, stream::BoxStream};
use serde::Deserialize;
use tonic::{Streaming, transport::Endpoint};

use crate::{
    cdc::CdcStreamBuilder,
    decode,
    error::{Error, ProtocolError},
    store::{BatchStore, StateStore},
    transactional::TransactionalStreamBuilder,
    validation::{validate_consecutiveness, validate_networks},
};

/// Metadata attached to each batch from the server.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Metadata {
    pub ranges: Vec<BlockRange>,
    pub ranges_complete: bool,
}

/// Response batch containing data and metadata.
#[derive(Clone, Debug)]
pub struct ResponseBatch {
    pub data: RecordBatch,
    pub metadata: Metadata,
}

/// Batch stream for non-streaming queries.
///
/// Returns `RecordBatch` directly since non-streaming queries have no metadata.
/// For streaming queries with metadata, use `client.stream()`.
pub struct BatchStream {
    inner: RawStream,
}

impl FuturesStream for BatchStream {
    type Item = Result<RecordBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(response_batch))) => Poll::Ready(Some(Ok(response_batch.data))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Raw stream from Flight SQL queries with metadata.
///
/// Returns `ResponseBatch` with metadata. Most users should use `client.stream()`
/// for streaming queries with reorg detection, or `client.query()` for batch queries.
pub struct RawStream {
    decoder: decode::FlightDataDecoder<Streaming<FlightData>>,
}

impl FuturesStream for RawStream {
    type Item = Result<ResponseBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.decoder).poll_next(cx)
    }
}

/// Protocol messages emitted by the stateless protocol stream.
#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    /// New data to process.
    Data {
        /// Data batch
        batch: RecordBatch,
        /// Block ranges covered by the data batch
        ranges: Vec<BlockRange>,
    },

    /// Reorg detected.
    Reorg {
        /// Previous block ranges before the reorg
        previous: Vec<BlockRange>,
        /// New block ranges after the reorg
        incoming: Vec<BlockRange>,
        /// Invalidation ranges for the reorg
        invalidation: Vec<InvalidationRange>,
    },

    /// Watermark (ranges completed).
    Watermark {
        /// Block ranges confirmed complete at this watermark
        ranges: Vec<BlockRange>,
    },
}

/// Interprets raw response batches into protocol messages.
///
/// Detects reorgs by comparing incoming block ranges against previous ranges.
///
/// # Example
///
/// ```rust,ignore
/// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").await?;
///
/// while let Some(msg) = stream.next().await {
///     match msg? {
///         ProtocolMessage::Data { batch, ranges } => {
///             // Process new data
///         }
///         ProtocolMessage::Reorg { previous, incoming, invalidation } => {
///             // Handle reorg
///         }
///         ProtocolMessage::Watermark { ranges } => {
///             // Watermark (ranges completed)
///         }
///     }
/// }
/// ```
pub struct ProtocolStream {
    stream: BoxStream<'static, Result<ProtocolMessage, Error>>,
}

impl ProtocolStream {
    /// Create a new protocol stream from a stream of response batches.
    ///
    /// # Arguments
    /// - `responses`: Stream of response batches from the server
    /// - `previous`: Initial previous ranges (from last watermark on resume, empty on first connect)
    pub fn new(
        mut responses: BoxStream<'static, Result<ResponseBatch, Error>>,
        previous: Vec<BlockRange>,
    ) -> Self {
        let stream = try_stream! {
            let mut previous = previous;

            while let Some(response) = responses.next().await {
                let batch = response?;
                let ranges = batch.metadata.ranges;

                // Validate network consistency (duplicates + stability)
                validate_networks(&previous, &ranges)?;

                // Detect reorg before validating consecutiveness
                let invalidation: Vec<InvalidationRange> = ranges.iter().filter_map(|i| {
                    let p = previous.iter().find(|p| p.network == i.network)?;
                    if (i != p) && (i.start() <= p.end()) {
                        return Some(InvalidationRange {
                            network: i.network.clone(),
                            numbers: i.start()..=BlockNum::max(i.end(), p.end()),
                        });
                    }
                    None
                }).collect();

                if !invalidation.is_empty() {
                    yield ProtocolMessage::Reorg {
                        previous: previous.clone(),
                        incoming: ranges.clone(),
                        invalidation,
                    };
                } else {
                    // Validate consecutiveness of block ranges
                    validate_consecutiveness(&previous, &ranges)?;
                }

                if batch.metadata.ranges_complete {
                    yield ProtocolMessage::Watermark {
                        ranges: ranges.clone(),
                    };
                } else {
                    yield ProtocolMessage::Data {
                        batch: batch.data,
                        ranges: ranges.clone(),
                    };
                }

                previous = ranges;
            }
        }
        .boxed();

        Self { stream }
    }
}

impl FuturesStream for ProtocolStream {
    type Item = Result<ProtocolMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Arrow Flight client for connecting to amp server.
#[derive(Clone)]
pub struct AmpClient {
    pub(crate) client: FlightSqlServiceClient<tonic::transport::Channel>,
}

impl AmpClient {
    /// Create a new AmpClient by connecting to the given endpoint.
    ///
    /// # Arguments
    /// - `endpoint`: gRPC endpoint URL (e.g., "http://localhost:1602")
    pub async fn from_endpoint(endpoint: &str) -> Result<Self, Error> {
        let endpoint: Endpoint = endpoint.parse().map_err(Error::Transport)?;
        let channel = endpoint.connect().await.map_err(Error::Transport)?;
        let client = FlightSqlServiceClient::new(channel);
        Ok(Self { client })
    }

    /// Create a new AmpClient from an existing FlightSqlServiceClient.
    ///
    /// Useful when you need custom channel configuration (TLS, interceptors, etc.)
    ///
    /// # Example
    /// ```rust,ignore
    /// use arrow_flight::sql::client::FlightSqlServiceClient;
    /// use tonic::transport::{Channel, Endpoint};
    ///
    /// let endpoint = Endpoint::from_static("http://localhost:1602")
    ///     .timeout(std::time::Duration::from_secs(60));
    /// let channel = endpoint.connect().await?;
    /// let flight_client = FlightSqlServiceClient::new(channel);
    /// let client = AmpClient::from_client(flight_client);
    /// ```
    pub fn from_client(client: FlightSqlServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }

    /// Start building a streaming query.
    ///
    /// Returns a `StreamBuilder` that can be awaited directly for a `ProtocolStream`,
    /// or configured with `.raw()` or `.transactional(store, retention)`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Protocol stream (default - stateless reorg detection)
    /// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").await?;
    ///
    /// // Raw stream (response batches)
    /// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").raw().await?;
    ///
    /// // Transactional stream (stateful with commits)
    /// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
    ///     .transactional(store, 128)
    ///     .await?;
    /// ```
    pub fn stream(&self, sql: impl Into<String>) -> StreamBuilder {
        StreamBuilder {
            client: self.clone(),
            sql: sql.into(),
        }
    }

    /// Execute a SQL query and return a stream of results.
    ///
    /// **Important**: This method is for **non-streaming queries only**. If your query includes
    /// `SETTINGS stream = true`, use `client.stream()` instead to get proper reorg detection
    /// and watermark handling.
    ///
    /// # Arguments
    /// - `sql`: SQL query string (should NOT include `SETTINGS stream = true`)
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut stream = client.query("SELECT * FROM eth.blocks LIMIT 10").await?;
    /// while let Some(batch) = stream.next().await {
    ///     // Process batch
    /// }
    /// ```
    pub async fn query<S: ToString>(&mut self, sql: S) -> Result<BatchStream, Error> {
        let inner = self.request(sql, None, false).await?;
        Ok(BatchStream { inner })
    }

    /// Execute a SQL query request with optional resume watermark.
    ///
    /// Most users should use `query()` or `stream()` API instead.
    ///
    /// # Arguments
    /// - `sql`: SQL query string
    /// - `watermark`: Optional watermark for resuming a stream
    /// - `streaming`: Whether to enable streaming mode
    pub async fn request<S: ToString>(
        &mut self,
        sql: S,
        watermark: Option<&ResumeWatermark>,
        streaming: bool,
    ) -> Result<RawStream, Error> {
        self.client.set_header(
            "amp-resume",
            watermark
                .map(|value| serde_json::to_string(value).unwrap())
                .unwrap_or_default(),
        );

        // Set amp-stream header to control streaming behavior
        self.client
            .set_header("amp-stream", if streaming { "true" } else { "" });

        let result = self.client.execute(sql.to_string(), None).await;

        // Unset headers after GetFlightInfo, since otherwise they get
        // retained for subsequent requests.
        self.client.set_header("amp-resume", "");
        self.client.set_header("amp-stream", "");

        let flight_info = result.map_err(Error::Arrow)?;

        let ticket = flight_info
            .endpoint
            .into_iter()
            .next()
            .and_then(|endpoint| endpoint.ticket)
            .ok_or(Error::Protocol(ProtocolError::MissingFlightTicket))?;

        let flight_data = self.client.inner_mut().do_get(ticket).await?.into_inner();
        let decoder = decode::FlightDataDecoder::new(flight_data);

        Ok(RawStream { decoder })
    }
}

/// Builder for creating streaming queries.
///
/// Default: awaiting directly returns a `ProtocolStream` (stateless reorg detection).
pub struct StreamBuilder {
    client: AmpClient,
    sql: String,
}

impl StreamBuilder {
    /// Create a raw stream returning response batches.
    pub fn raw(self) -> RawStreamBuilder {
        RawStreamBuilder {
            client: self.client,
            sql: self.sql,
        }
    }

    /// Create a transactional stream with state persistence.
    pub fn transactional(
        self,
        store: impl StateStore + 'static,
        retention: BlockNum,
    ) -> TransactionalStreamBuilder {
        TransactionalStreamBuilder::new(self.client, self.sql, Box::new(store), retention)
    }

    /// Create a CDC stream with batch content persistence.
    ///
    /// CDC streams store batch content to enable generating Delete events with
    /// original data for stateless forwarding consumers (Kafka, message queues, etc.).
    ///
    /// # Arguments
    /// - `state_store`: StateStore for watermark persistence
    /// - `batch_store`: BatchStore for batch persistence
    /// - `retention`: Retention window in blocks
    ///
    /// # Example
    /// ```rust,ignore
    /// use amp_client::{AmpClient, InMemoryStateStore, InMemoryBatchStore};
    ///
    /// let client = AmpClient::from_endpoint("http://localhost:1602").await?;
    /// let state_store = InMemoryStateStore::new();
    /// let batch_store = InMemoryBatchStore::new();
    /// let mut stream = client
    ///     .stream("SELECT * FROM eth.logs SETTINGS stream = true")
    ///     .cdc(state_store, batch_store, 128)
    ///     .await?;
    /// ```
    pub fn cdc(
        self,
        state_store: impl StateStore + 'static,
        batch_store: impl BatchStore + 'static,
        retention: BlockNum,
    ) -> CdcStreamBuilder {
        CdcStreamBuilder::new(
            self.client,
            self.sql,
            Box::new(state_store),
            Box::new(batch_store),
            retention,
        )
    }
}

impl IntoFuture for StreamBuilder {
    type Output = Result<ProtocolStream, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Get raw response stream with streaming enabled
            let raw = self.client.request(&self.sql, None, true).await?.boxed();

            // Create protocol stream (with reorg detection)
            Ok(ProtocolStream::new(raw, Vec::new()))
        })
    }
}

/// Builder for raw streams.
pub struct RawStreamBuilder {
    client: AmpClient,
    sql: String,
}

impl IntoFuture for RawStreamBuilder {
    type Output = Result<RawStream, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move { self.client.request(&self.sql, None, true).await })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidationRange {
    pub network: String,
    pub numbers: RangeInclusive<BlockNum>,
}

impl InvalidationRange {
    /// Return true if the given range overlaps with block numbers on the same network.
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
