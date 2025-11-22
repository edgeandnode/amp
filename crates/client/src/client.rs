//! AmpClient implementation

use std::{
    future::{Future, IntoFuture},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_flight::sql::client::FlightSqlServiceClient;
use async_stream::try_stream;
use common::{
    BlockNum,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::{Stream as FuturesStream, StreamExt, stream::BoxStream};
use serde::Deserialize;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::{
    cdc::CdcStreamBuilder,
    decode,
    error::{Error, ProtocolError},
    store::{BatchStore, StateStore},
    transactional::TransactionalStreamBuilder,
    validation::{validate_consecutiveness, validate_networks, validate_prev_hash},
};

/// Trait for types that expose an Arrow schema.
///
/// All client streams implement this trait to expose the Arrow schema
/// of the data they yield. The schema is guaranteed to be stable for the
/// lifetime of the stream.
pub trait HasSchema {
    /// Get the Arrow schema for this stream.
    fn schema(&self) -> SchemaRef;
}

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
    pub(crate) inner: BoxStream<'static, Result<ResponseBatch, Error>>,
    pub(crate) schema: SchemaRef,
}

impl HasSchema for RawStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl FuturesStream for RawStream {
    type Item = Result<ResponseBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
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
/// let stream = client.stream("SELECT * FROM eth.logs").await?;
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
    schema: SchemaRef,
}

impl ProtocolStream {
    /// Create a new protocol stream from a stream of response batches.
    ///
    /// # Arguments
    /// - `responses`: Stream of response batches from the server
    /// - `previous`: Initial previous ranges (from last watermark on resume, empty on first connect)
    /// - `schema`: Arrow schema for the stream
    pub fn new(
        mut responses: BoxStream<'static, Result<ResponseBatch, Error>>,
        previous: Vec<BlockRange>,
        schema: SchemaRef,
    ) -> Self {
        let stream = try_stream! {
            let mut previous = previous;

            while let Some(response) = responses.next().await {
                let batch = response?;
                let ranges = batch.metadata.ranges;

                // Validate prev_hash for all incoming ranges
                for range in &ranges {
                    validate_prev_hash(range)?;
                }

                // Validate network consistency (duplicates + stability)
                validate_networks(&previous, &ranges)?;

                // Validate consecutiveness with hash chain validation
                // This handles both normal progression and detects reorgs
                validate_consecutiveness(&previous, &ranges)?;

                // Detect reorgs from backwards jumps (start < prev.end + 1)
                // Reorgs are identified by backwards jumps with hash mismatches
                let invalidation: Vec<InvalidationRange> = ranges.iter().filter_map(|incoming| {
                    let prev = previous.iter().find(|p| p.network == incoming.network)?;

                    // Skip if ranges are identical (watermarks can repeat)
                    if incoming == prev {
                        return None;
                    }

                    // Detect backwards jump (reorg)
                    if incoming.start() < prev.end() + 1 {
                        return Some(InvalidationRange {
                            network: incoming.network.clone(),
                            numbers: incoming.start()..=BlockNum::max(incoming.end(), prev.end()),
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

        Self { stream, schema }
    }
}

impl HasSchema for ProtocolStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    /// - `endpoint`: gRPC endpoint URL (e.g., "http://localhost:1602" or "https://example.com")
    ///
    /// TLS is automatically enabled for HTTPS URLs using native root certificates.
    pub async fn from_endpoint(endpoint: &str) -> Result<Self, Error> {
        let mut endpoint: Endpoint = endpoint.parse().map_err(Error::Transport)?;

        if endpoint.uri().scheme_str() == Some("https") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(Error::Transport)?;
        }

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

    /// Set HTTP headers for all subsequent requests.
    ///
    /// This is useful for custom metadata headers. Headers persist across all requests made by this client.
    ///
    /// **Note**: For authentication, use [`set_token()`](Self::set_token) instead.
    ///
    /// # Example
    /// ```rust,ignore
    /// use http::header::{HeaderMap, HeaderName, HeaderValue};
    ///
    /// let mut client = AmpClient::from_endpoint("http://localhost:1602").await?;
    /// let mut headers = HeaderMap::new();
    /// headers.insert(
    ///     HeaderName::from_static("x-custom-header"),
    ///     HeaderValue::from_static("custom-value")
    /// );
    /// client.set_headers(&headers);
    /// ```
    pub fn set_headers(&mut self, headers: &http::HeaderMap) {
        for (name, value) in headers.iter() {
            self.client.set_header(
                name.as_str().to_string(),
                value.to_str().unwrap_or("").to_string(),
            );
        }
    }

    /// Set auth token for all subsequent requests.
    pub fn set_token(&mut self, token: impl Into<String>) {
        self.client.set_token(token.into());
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
    /// let stream = client.stream("SELECT * FROM eth.logs").await?;
    ///
    /// // Raw stream (response batches)
    /// let stream = client.stream("SELECT * FROM eth.logs").raw().await?;
    ///
    /// // Transactional stream (stateful with commits)
    /// let stream = client.stream("SELECT * FROM eth.logs")
    ///     .transactional(store, 128)
    ///     .await?;
    /// ```
    pub fn stream(&self, sql: impl Into<String>) -> StreamBuilder {
        StreamBuilder {
            client: self.clone(),
            sql: sql.into(),
        }
    }

    /// Execute a SQL batch query and return a stream of results.
    ///
    /// # Arguments
    /// - `sql`: SQL query string
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

        // Decode schema from FlightInfo
        let schema = Arc::new(
            flight_info
                .clone()
                .try_decode_schema()
                .map_err(|err| Error::Protocol(ProtocolError::InvalidSchema(err)))?,
        );

        let ticket = flight_info
            .endpoint
            .into_iter()
            .next()
            .and_then(|endpoint| endpoint.ticket)
            .ok_or(Error::Protocol(ProtocolError::MissingFlightTicket))?;

        // Manually inject authorization header into DoGet request as we are using the inner client
        let mut request = tonic::Request::new(ticket);
        if let Some(token) = self.client.token() {
            let auth_value = format!("Bearer {}", token);
            if let Ok(metadata_value) = auth_value.parse() {
                request
                    .metadata_mut()
                    .insert("authorization", metadata_value);
            }
        }

        let flight_data = self.client.inner_mut().do_get(request).await?.into_inner();
        let decoder = decode::FlightDataDecoder::new(flight_data);

        Ok(RawStream {
            inner: decoder.boxed(),
            schema,
        })
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
    ///     .stream("SELECT * FROM eth.logs")
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
            let raw = self.client.request(&self.sql, None, true).await?;
            let schema = raw.schema();
            let boxed = raw.boxed();

            // Create protocol stream (with reorg detection)
            Ok(ProtocolStream::new(boxed, Vec::new(), schema))
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
