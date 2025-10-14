use std::sync::Arc;

use amp_client::{ResponseBatchWithReorg, SqlClient};
use async_stream::stream;
use common::{
    BlockNum,
    arrow::array::RecordBatch,
    metadata::segments::{BlockRange, ResumeWatermark},
};
use futures::{StreamExt, stream::BoxStream};

use crate::{
    error::{Error, Result},
    state::StateStore,
    types::{DebeziumOp, DebeziumRecord},
};

/// A Debezium CDC-compliant streaming client for Amp.
///
/// Wraps Amp's Arrow Flight client and transforms streaming query results
/// into Debezium format with proper reorg handling.
///
/// # Configuration
///
/// The client requires:
/// - **Amp endpoint**: The Arrow Flight server URL
/// - **State store**: For tracking emitted batches and handling reorgs
///
/// Batches are treated as atomic units - during a reorg, all records from
/// affected batches are retracted together.
pub struct DebeziumClient<S: StateStore> {
    /// Underlying Amp SQL client
    amp_client: SqlClient,

    /// State store for tracking emitted batches
    store: S,
}

impl<S: StateStore + 'static> DebeziumClient<S> {
    /// Create a new builder for configuring the Debezium client.
    ///
    /// # Example
    /// ```no_run
    /// use amp_debezium_client::{DebeziumClient, InMemoryStore};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = DebeziumClient::builder()
    ///     .endpoint("http://localhost:1602")?
    ///     .store(InMemoryStore::new(64))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> DebeziumClientBuilder<S> {
        DebeziumClientBuilder::new()
    }

    /// Execute a streaming SQL query and return a Debezium CDC event stream.
    ///
    /// # Arguments
    /// * `query` - SQL query to execute (should include `SETTINGS stream = true`)
    /// * `resume_watermark` - Optional watermark to resume from
    ///
    /// # Returns
    /// A stream of Debezium CDC records with proper reorg handling
    ///
    /// # Example
    /// ```no_run
    /// # use amp_debezium_client::{DebeziumClient, InMemoryStore};
    /// # async fn example(client: DebeziumClient<InMemoryStore>) -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    ///
    /// let mut stream = client.stream(
    ///     "SELECT * FROM eth_rpc.logs WHERE address = '0x...' SETTINGS stream = true",
    ///     None
    /// ).await?;
    ///
    /// while let Some(record) = stream.next().await {
    ///     match record? {
    ///         record if record.op == amp_debezium_client::DebeziumOp::Create => {
    ///             println!("New record: {:?}", record.after);
    ///         }
    ///         record if record.op == amp_debezium_client::DebeziumOp::Delete => {
    ///             println!("Retracted record: {:?}", record.before);
    ///         }
    ///         _ => {}
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream(
        mut self,
        query: &str,
        resume_watermark: Option<&ResumeWatermark>,
    ) -> Result<BoxStream<'static, Result<DebeziumRecord>>> {
        // Execute the query through the Amp client
        let result_stream = self.amp_client.query(query, None, resume_watermark).await?;

        // Wrap with reorg detection
        let mut reorg_stream = amp_client::with_reorg(result_stream);

        // Transform into Debezium records
        Ok(stream! {
            while let Some(event) = reorg_stream.next().await {
                match event {
                    Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                        // Process batch and emit create records
                        // Pass all ranges from metadata
                        match self.handle_batch(data, metadata.ranges).await {
                            Ok(records) => {
                                for record in records {
                                    yield Ok(record);
                                }
                            }
                            Err(e) => {
                                yield Err(e);
                                break;
                            }
                        }
                    }
                    Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                        // Process reorg and emit delete records
                        match self.handle_reorg(invalidation).await {
                            Ok(records) => {
                                for record in records {
                                    yield Ok(record);
                                }
                            }
                            Err(e) => {
                                yield Err(e);
                                break;
                            }
                        }
                    }
                    Ok(ResponseBatchWithReorg::Watermark(watermark)) => {
                        // Prune old batches when watermark is received
                        // Batches are deleted only when ALL their ranges are safe to prune
                        // across all networks (conservative multi-network approach)
                        let watermarks: std::collections::BTreeMap<String, BlockNum> = watermark
                            .0
                            .into_iter()
                            .map(|(network, watermark)| (network, watermark.number))
                            .collect();

                        if let Err(e) = self.store.prune(&watermarks).await {
                            yield Err(e);
                            break;
                        }
                    }
                    Err(e) => {
                        yield Err(e.into());
                        break;
                    }
                }
            }
        }
        .boxed())
    }

    /// Handle a batch of records, converting to Debezium create events.
    async fn handle_batch(
        &mut self,
        batch: RecordBatch,
        ranges: Vec<BlockRange>,
    ) -> Result<Vec<DebeziumRecord>> {
        let batch_arc = Arc::new(batch.clone());

        // Store the batch with all its ranges for potential reorg handling
        use crate::types::StoredBatch;
        let stored_batch = StoredBatch {
            batch: batch_arc.clone(),
            ranges,
        };
        self.store.insert(stored_batch).await?;

        // Convert entire batch to JSON array once
        let json_rows = batch_to_json_array(&batch)?;

        // Emit create events for all rows
        let records = json_rows
            .into_iter()
            .map(|row_json| DebeziumRecord {
                before: None,
                after: Some(row_json),
                op: DebeziumOp::Create,
            })
            .collect();

        Ok(records)
    }

    /// Handle a reorg event, converting invalidated records to Debezium delete events.
    async fn handle_reorg(
        &mut self,
        invalidation: Vec<amp_client::InvalidationRange>,
    ) -> Result<Vec<DebeziumRecord>> {
        // Retrieve all affected batches at once
        let affected_batches = self.store.get_in_ranges(&invalidation).await?;
        let affected_records = affected_batches
            .iter()
            .map(|batch| batch_to_json_array(batch))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten();

        // Convert all records to delete events
        Ok(affected_records
            .map(|row_json| DebeziumRecord {
                before: Some(row_json),
                after: None,
                op: DebeziumOp::Delete,
            })
            .collect())
    }
}

/// Builder for configuring a DebeziumClient.
pub struct DebeziumClientBuilder<S: StateStore> {
    endpoint: Option<String>,
    store: Option<S>,
}

impl<S: StateStore> DebeziumClientBuilder<S> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            endpoint: None,
            store: None,
        }
    }

    /// Set the Amp server endpoint.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Result<Self> {
        self.endpoint = Some(endpoint.into());
        Ok(self)
    }

    /// Set the state store implementation.
    pub fn store(mut self, store: S) -> Self {
        self.store = Some(store);
        self
    }

    /// Build the DebeziumClient.
    pub async fn build(self) -> Result<DebeziumClient<S>> {
        let endpoint = self
            .endpoint
            .ok_or_else(|| Error::Config("endpoint is required".to_string()))?;

        let store = self
            .store
            .ok_or_else(|| Error::Config("store is required".to_string()))?;

        let amp_client = SqlClient::new(&endpoint).await?;

        Ok(DebeziumClient { amp_client, store })
    }
}

impl<S: StateStore> Default for DebeziumClientBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert an entire RecordBatch to a JSON array using arrow-json.
fn batch_to_json_array(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
    let mut buf = Vec::new();
    let mut writer = common::arrow::json::ArrayWriter::new(&mut buf);
    writer.write(batch)?;
    writer.finish()?;

    let json_data = writer.into_inner();
    let json_array: Vec<serde_json::Value> =
        serde_json::from_slice(&json_data).map_err(Error::Json)?;

    Ok(json_array)
}
