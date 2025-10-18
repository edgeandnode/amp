//! AmpClient implementation

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use arrow_flight::{FlightData, sql::client::FlightSqlServiceClient};
use common::{
    arrow::array::RecordBatch,
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::Stream as FuturesStream;
use serde::Deserialize;
use tonic::{Streaming, transport::Endpoint};

use crate::{decode, error::Error, stream::StreamBuilder};

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
        let endpoint: Endpoint = endpoint.parse()?;
        let channel = endpoint.connect().await?;
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

    /// Start building a streaming query with reorg detection and automatic retry.
    ///
    /// Returns a `StreamBuilder` that can be awaited directly or configured with:
    /// - Initial state (watermark + ranges for resumption)
    /// - Retry policy for connection failures
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true").await?;
    ///
    /// while let Some(event) = stream.next().await {
    ///     match event? {
    ///         StreamEvent::Data { batch, ranges } => {
    ///             // Process data
    ///         }
    ///         StreamEvent::Watermark { watermark, ranges } => {
    ///             // Save checkpoint
    ///         }
    ///         StreamEvent::Rewind { invalidate, ranges } => {
    ///             // Handle invalidation
    ///         }
    ///     }
    /// }
    /// ```
    pub fn stream(&self, sql: impl Into<String>) -> StreamBuilder {
        StreamBuilder::new(self.clone(), sql)
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
        let inner = self.request(sql, None).await?;
        Ok(BatchStream { inner })
    }

    /// Execute a SQL query request with optional resume watermark.
    ///
    /// Most users should use `query()` or `stream()` API instead.
    ///
    /// # Arguments
    /// - `sql`: SQL query string
    /// - `watermark`: Optional watermark for resuming a stream
    pub async fn request<S: ToString>(
        &mut self,
        sql: S,
        watermark: Option<&ResumeWatermark>,
    ) -> Result<RawStream, Error> {
        self.client.set_header(
            "amp-resume",
            watermark
                .map(|value| serde_json::to_string(value).unwrap())
                .unwrap_or_default(),
        );

        let result = self.client.execute(sql.to_string(), None).await;

        // Unset the amp-resume header after GetFlightInfo, since otherwise it gets
        // retained for subsequent requests.
        self.client.set_header("amp-resume", "");

        let flight_info = match result {
            Ok(flight_info) => flight_info,
            Err(err) => return Err(err.into()),
        };

        let ticket = flight_info
            .endpoint
            .into_iter()
            .next()
            .and_then(|endpoint| endpoint.ticket)
            .ok_or_else(|| Error::Server("FlightInfo missing ticket".to_string()))?;

        let flight_data = self.client.inner_mut().do_get(ticket).await?.into_inner();
        let decoder = decode::FlightDataDecoder::new(flight_data);

        Ok(RawStream { decoder })
    }
}
