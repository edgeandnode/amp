use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use common::arrow::array::RecordBatch;
use common::arrow::compute::concat_batches;
use common::arrow::datatypes::SchemaRef;
use common::arrow::ipc as arrow_ipc;
use common::arrow::json::writer::ArrayWriter;
use common::BoxError;
use futures::stream::StreamExt;
use tonic::transport::Channel;

use crate::test_support::TestEnv;

pub(crate) struct TestClient {
    client: FlightSqlServiceClient<Channel>,
    streams: BTreeMap<String, ResponseStream>,
}

impl TestClient {
    pub async fn connect(test_env: &TestEnv) -> Result<Self, BoxError> {
        let addr = format!("grpc://{}", test_env.server_addrs.flight_addr);
        let flight_client = FlightServiceClient::connect(addr).await?;
        let client = FlightSqlServiceClient::new_from_inner(flight_client);
        Ok(Self {
            client,
            streams: BTreeMap::new(),
        })
    }

    pub async fn run_query(
        &mut self,
        query: &str,
        take: Option<usize>,
    ) -> Result<serde_json::Value, BoxError> {
        tracing::debug!("Running query: {query}, take: {take:?}");
        let mut info = self.client.execute(query.to_string(), None).await?;
        let mut batches = self
            .client
            .do_get(info.endpoint[0].ticket.take().unwrap())
            .await?;

        tracing::debug!("here we are");

        let mut buf = Vec::new();
        let mut writer = ArrayWriter::new(&mut buf);

        let mut remaining = take.unwrap_or(usize::MAX);

        while let Some(batch_result) = batches.next().await {
            tracing::debug!("now loopings");

            let batch = dbg!(batch_result?);
            let row_count = dbg!(batch.num_rows());
            tracing::debug!("now loopings {row_count} rows in batch");

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
        Ok(serde_json::from_slice(&buf)?)
    }

    pub async fn register_stream(&mut self, name: &str, query: &str) -> Result<(), BoxError> {
        let mut info = self.client.execute(query.to_string(), None).await?;
        let schema = arrow_ipc::convert::try_schema_from_ipc_buffer(&info.schema).unwrap();
        let stream = self
            .client
            .do_get(info.endpoint[0].ticket.take().unwrap())
            .await?;
        self.streams.insert(
            name.to_string(),
            ResponseStream::new(stream, Arc::new(schema)),
        );
        Ok(())
    }

    pub async fn take_from_stream(
        &mut self,
        name: &str,
        n: usize,
    ) -> Result<RecordBatch, BoxError> {
        if let Some(stream) = self.streams.get_mut(name) {
            stream.take(n).await
        } else {
            Err(Box::from(format!("Stream \"{name}\" not found")))
        }
    }
}

pub(crate) struct ResponseStream {
    stream: FlightRecordBatchStream,
    current_batch: RecordBatch,
}

impl ResponseStream {
    pub fn new(stream: FlightRecordBatchStream, schema: SchemaRef) -> Self {
        let current_batch = RecordBatch::new_empty(schema);
        Self {
            stream,
            current_batch,
        }
    }

    /// Return the first `n` rows, advancing the cursor.
    pub async fn take(&mut self, mut n: usize) -> Result<RecordBatch, BoxError> {
        let schema = self.current_batch.schema();
        let mut out_batches = Vec::new();

        // ── 1. Drain from the buffer we already hold ──────────────────────────────
        if self.current_batch.num_rows() > 0 {
            if self.current_batch.num_rows() >= n {
                let slice = self.current_batch.slice(0, n);
                self.current_batch = self
                    .current_batch
                    .slice(n, self.current_batch.num_rows() - n);
                return Ok(slice);
            }
            n -= self.current_batch.num_rows();
            out_batches.push(self.current_batch.clone());
            self.current_batch = RecordBatch::new_empty(schema.clone());
        }

        // ── 2. Pull more batches until we have ≥ n rows ───────────────────────────
        while n > 0 {
            match self.stream.next().await {
                Some(Ok(batch)) => {
                    if batch.num_rows() > n {
                        // keep remainder for future calls
                        out_batches.push(batch.slice(0, n));
                        self.current_batch = batch.slice(n, batch.num_rows() - n);
                        break;
                    }
                    n -= batch.num_rows();
                    out_batches.push(batch);
                }
                Some(Err(e)) => return Err(e.into()),
                None => break, // stream ended early
            }
        }

        // ── 3. Stitch together what we collected ─────────────────────────────────
        Ok(match out_batches.len() {
            0 => RecordBatch::new_empty(schema),
            1 => out_batches.remove(0),
            _ => concat_batches(&schema, &out_batches)?,
        })
    }
}
