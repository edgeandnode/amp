//! Flight client fixture for test environments.
//!
//! This fixture provides a FlightClient for connecting to and querying Amp servers
//! via the Arrow Flight SQL protocol. It handles streaming responses and provides
//! convenient methods for running SQL queries and managing query streams.

use std::{collections::BTreeMap, sync::Arc};

use arrow_flight::{
    FlightData, decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient,
    sql::client::FlightSqlServiceClient,
};
use common::{
    BoxError,
    arrow::{
        array::RecordBatch, compute::concat_batches, datatypes::SchemaRef, ipc as arrow_ipc,
        json::writer::ArrayWriter,
    },
};
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tonic::transport::Channel;

/// Flight client fixture for connecting to Amp servers via Arrow Flight SQL.
///
/// This fixture wraps a FlightSqlServiceClient and provides convenient methods for
/// running SQL queries and managing streaming responses. It connects to the Flight
/// server endpoint provided by a DaemonServer fixture.
pub struct FlightClient {
    client: FlightSqlServiceClient<Channel>,
    streams: BTreeMap<String, ResponseStream>,
}

impl FlightClient {
    /// Create a new Flight client connected to the provided Flight server URL.
    pub async fn new(url: impl Into<String>) -> Result<Self, BoxError> {
        let flight_client = FlightServiceClient::connect(url.into()).await?;
        let client = FlightSqlServiceClient::new_from_inner(flight_client);

        Ok(Self {
            client,
            streams: Default::default(),
        })
    }

    /// Execute a SQL query and return the results as JSON along with batch count.
    pub async fn run_query(
        &mut self,
        query: &str,
        take: impl Into<Option<usize>>,
    ) -> Result<(serde_json::Value, usize), BoxError> {
        let take = take.into();

        tracing::debug!("Running query: {query}, take: {take:?}");
        let mut info = self.client.execute(query.to_string(), None).await?;
        let mut batches = self
            .client
            .do_get(
                info.endpoint[0]
                    .ticket
                    .take()
                    .expect("Flight query should return ticket"),
            )
            .await?;

        let mut buf = Vec::new();
        let mut writer = ArrayWriter::new(&mut buf);

        let mut remaining = take.unwrap_or(usize::MAX);
        let mut batch_count = 0;

        while let Some(batch_result) = batches.next().await {
            let batch = batch_result?;
            let row_count = batch.num_rows();
            batch_count += 1;

            if remaining >= row_count {
                writer.write(&batch)?;
                remaining -= row_count;
            } else {
                let truncated = batch.slice(0, remaining);
                writer.write(&truncated)?;
                break;
            }

            if remaining == 0 {
                break;
            }
        }

        writer.finish()?;

        Ok((serde_json::from_slice(&buf)?, batch_count))
    }

    /// Register a streaming query with the given name.
    pub async fn register_stream(&mut self, name: &str, query: &str) -> Result<(), BoxError> {
        let mut info = self.client.execute(query.to_string(), None).await?;
        let schema = arrow_ipc::convert::try_schema_from_ipc_buffer(&info.schema)
            .expect("Flight query should return valid schema");
        let stream = self
            .client
            .do_get(
                info.endpoint[0]
                    .ticket
                    .take()
                    .expect("Flight query should return ticket"),
            )
            .await?;
        self.streams.insert(
            name.to_string(),
            ResponseStream::new(stream, Arc::new(schema)),
        );
        Ok(())
    }

    /// Take a specified number of rows from a registered stream.
    /// Returns the JSON result and the number of batches consumed.
    pub async fn take_from_stream(
        &mut self,
        name: &str,
        n: usize,
    ) -> Result<(serde_json::Value, usize), BoxError> {
        let Some(stream) = self.streams.get_mut(name) else {
            return Err(Box::from(format!("Stream \"{name}\" not found")));
        };

        let mut buf = Vec::new();
        let mut writer = ArrayWriter::new(&mut buf);
        let (batch, batch_count) = stream.take(n).await?;
        writer.write(&batch)?;
        writer.finish()?;
        Ok((serde_json::from_slice(&buf)?, batch_count))
    }

    /// Execute a query and return a channel of raw FlightData messages for metadata extraction.
    ///
    /// This method provides access to the raw FlightData stream, including app_metadata fields
    /// that contain block range metadata. The returned receiver will receive all FlightData
    /// messages from the query execution.
    pub async fn execute_with_metadata_stream(
        &mut self,
        query: &str,
    ) -> Result<mpsc::UnboundedReceiver<FlightData>, BoxError> {
        tracing::debug!("Starting metadata stream for query: {query}");

        let info = self.client.execute(query.to_string(), None).await?;
        let ticket = info.endpoint[0]
            .ticket
            .clone()
            .expect("Flight query should return ticket");

        let response = self.client.inner_mut().do_get(ticket).await?;
        let mut flight_data_stream = response.into_inner();

        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(result) = flight_data_stream.next().await {
                match result {
                    Ok(flight_data) => {
                        if tx.send(flight_data).is_err() {
                            // Receiver has been dropped, exit the task
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Error in flight data stream: {}", err);
                        break;
                    }
                }
            }
            tracing::debug!("Flight data stream task completed");
        });

        Ok(rx)
    }
}

/// Streaming response handler for Arrow Flight queries.
///
/// This struct manages a streaming Flight response and provides methods to
/// incrementally consume batches of data while maintaining state across calls.
pub struct ResponseStream {
    stream: FlightRecordBatchStream,
    current_batch: RecordBatch,
}

impl ResponseStream {
    /// Create a new response stream wrapper.
    fn new(stream: FlightRecordBatchStream, schema: SchemaRef) -> Self {
        let current_batch = RecordBatch::new_empty(schema);
        Self {
            stream,
            current_batch,
        }
    }

    /// Take the first `n` rows from the stream, advancing the cursor.
    /// Returns the concatenated batch and the number of batches consumed.
    pub async fn take(&mut self, mut n: usize) -> Result<(RecordBatch, usize), BoxError> {
        let schema = self.current_batch.schema();
        let mut out_batches = Vec::new();
        let mut batch_count = 0;

        // Drain from the buffer we already hold
        if self.current_batch.num_rows() > 0 {
            batch_count += 1;
            if self.current_batch.num_rows() >= n {
                let slice = self.current_batch.slice(0, n);
                self.current_batch = self
                    .current_batch
                    .slice(n, self.current_batch.num_rows() - n);
                return Ok((slice, batch_count));
            }
            n -= self.current_batch.num_rows();
            out_batches.push(self.current_batch.clone());
            self.current_batch = RecordBatch::new_empty(schema.clone());
        }

        // Pull more batches until we have ≥ n rows
        while n > 0 {
            match self.stream.next().await {
                Some(Ok(batch)) => {
                    batch_count += 1;
                    if batch.num_rows() > n {
                        // keep remainder for future calls
                        out_batches.push(batch.slice(0, n));
                        self.current_batch = batch.slice(n, batch.num_rows() - n);
                        break;
                    }
                    n -= batch.num_rows();
                    out_batches.push(batch);
                }
                Some(Err(err)) => return Err(err.into()),
                None => break, // stream ended early
            }
        }

        // Stitch together what we collected
        let result_batch = match out_batches.len() {
            0 => RecordBatch::new_empty(schema),
            1 => out_batches.remove(0),
            _ => concat_batches(&schema, &out_batches)?,
        };

        Ok((result_batch, batch_count))
    }
}
