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
    stores::StateStore,
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
/// - **State store**: For tracking emitted batches and handling reorgs (defaults to InMemoryStore)
///
/// Batches are treated as atomic units - during a reorg, all records from
/// affected batches are retracted together.
pub struct DebeziumClient {
    /// Underlying Amp SQL client
    client: SqlClient,

    /// State store for tracking emitted batches
    store: Box<dyn StateStore>,
}

impl DebeziumClient {
    /// Create a new Debezium client.
    ///
    /// # Arguments
    /// * `client` - An Amp SQL client instance
    /// * `store` - Optional state store implementation. Defaults to `InMemoryStore` with 64-block reorg window.
    ///
    /// # Example - Default InMemoryStore
    /// ```no_run
    /// use amp_client::SqlClient;
    /// use amp_debezium_client::DebeziumClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let amp_client = SqlClient::new("http://localhost:1602").await?;
    /// let debezium_client = DebeziumClient::new(amp_client, None);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Example - Custom Store
    /// ```no_run
    /// use amp_client::SqlClient;
    /// use amp_debezium_client::{DebeziumClient, LmdbStore};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let store = LmdbStore::new("/path/to/db", 64)?;
    /// let amp_client = SqlClient::new("http://localhost:1602").await?;
    /// let debezium_client = DebeziumClient::new(amp_client, store);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: SqlClient, store: impl Into<Option<Box<dyn StateStore>>>) -> Self {
        let store = store
            .into()
            .unwrap_or_else(|| Box::new(crate::stores::InMemoryStore::new(64)));

        Self { client, store }
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
    /// # use amp_debezium_client::DebeziumClient;
    /// # async fn example(client: DebeziumClient) -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    ///
    /// let mut stream = client
    ///     .stream(
    ///         "SELECT * FROM eth_rpc.logs WHERE address = '0x...' SETTINGS stream = true",
    ///         None,
    ///     )
    ///     .await?;
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
        let result_stream = self.client.query(query, None, resume_watermark).await?;

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

/// Convert an entire RecordBatch to a JSON array using arrow-json.
fn batch_to_json_array(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
    let mut buf = Vec::new();
    let mut writer = common::arrow::json::LineDelimitedWriter::new(&mut buf);
    writer.write(batch)?;
    drop(writer);

    let json_str = String::from_utf8(buf)?;

    json_str
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(Error::Json))
        .collect()
}
