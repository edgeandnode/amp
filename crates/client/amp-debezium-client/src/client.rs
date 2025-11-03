use amp_client::{AmpClient, InMemoryStateStore, TransactionEvent};
use async_stream::try_stream;
use common::arrow::{array::RecordBatch, json::LineDelimitedWriter};
use futures::{StreamExt, stream::BoxStream};
use serde_json::{Value, from_str};

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
    /// Underlying Amp client
    client: AmpClient,

    /// State store for tracking emitted batches
    store: Box<dyn StateStore>,
}

impl DebeziumClient {
    /// Create a new Debezium client.
    ///
    /// # Arguments
    /// * `client` - An Amp client instance
    /// * `store` - Optional state store implementation. Defaults to `InMemoryStore`.
    ///
    /// # Example - Default InMemoryStore
    /// ```no_run
    /// use amp_client::AmpClient;
    /// use amp_debezium_client::DebeziumClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let amp_client = AmpClient::from_endpoint("http://localhost:1602").await?;
    /// let debezium_client = DebeziumClient::new(amp_client, None);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Example - Custom Store
    /// ```no_run
    /// use amp_client::AmpClient;
    /// use amp_debezium_client::DebeziumClient;
    /// # #[cfg(feature = "lmdb")]
    /// use amp_debezium_client::LmdbStore;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "lmdb")]
    /// # {
    /// let store = LmdbStore::new("/path/to/db")?;
    /// let amp_client = AmpClient::from_endpoint("http://localhost:1602").await?;
    /// let debezium_client = DebeziumClient::new(amp_client, store);
    /// # }
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: AmpClient, store: impl Into<Option<Box<dyn StateStore>>>) -> Self {
        let store = store
            .into()
            .unwrap_or_else(|| Box::new(crate::stores::InMemoryStore::new()));

        Self { client, store }
    }

    /// Execute a streaming SQL query and return a Debezium CDC event stream.
    ///
    /// # Arguments
    /// * `query` - SQL query to execute (should include `SETTINGS stream = true`)
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
    ///     .stream("SELECT * FROM eth_rpc.logs WHERE address = '0x...' SETTINGS stream = true")
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
    ) -> Result<BoxStream<'static, Result<Vec<DebeziumRecord>>>> {
        // TODO: Use a custom state store and generally refactor this all to allow for more flexibility
        let mut stream = self
            .client
            .stream(query)
            .transactional(InMemoryStateStore::new(), 128)
            .await?;

        let stream = try_stream! {
            while let Some(result) = stream.next().await {
                let (event, commit) = result?;

                match event {
                    TransactionEvent::Data { batch, id, .. } => {
                        self.store.append(batch.clone(), id).await?;
                        let records: Vec<DebeziumRecord> = batch_to_json_array(&batch)?
                            .into_iter()
                            .map(|row| DebeziumRecord {
                                before: None,
                                after: Some(row),
                                op: DebeziumOp::Create,
                            })
                            .collect();

                        commit.await?;
                        yield records;
                    }
                    TransactionEvent::Watermark { prune, .. } => {
                        if let Some(range) = prune {
                            self.store.prune(*range.end()).await?;
                        }
                        commit.await?;
                    }
                    TransactionEvent::Undo { invalidate, .. } => {
                        let retracted = self.store.retract(invalidate).await?;
                        let records = retracted
                            .into_iter()
                            .map(|batch| {
                                batch_to_json_array(&batch).map(|rows| {
                                    rows.into_iter().map(|row| DebeziumRecord {
                                        before: Some(row),
                                        after: None,
                                        op: DebeziumOp::Delete,
                                    })
                                })
                            })
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .flatten()
                            .collect();

                        commit.await?;
                        yield records;
                    }
                }
            }
        };

        Ok(stream.boxed())
    }
}

/// Convert an entire RecordBatch to a JSON array using arrow-json.
fn batch_to_json_array(batch: &RecordBatch) -> Result<Vec<Value>> {
    let mut buf = Vec::new();
    let mut writer = LineDelimitedWriter::new(&mut buf);
    writer.write(batch)?;
    drop(writer);

    String::from_utf8(buf)?
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| from_str(line).map_err(Error::Json))
        .collect()
}
