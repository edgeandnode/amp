use std::sync::Arc;

use amp_client::{ResponseBatchWithReorg, SqlClient};
use async_stream::stream;
use common::{arrow::array::RecordBatch, metadata::segments::ResumeWatermark};
use futures::{Stream, StreamExt};

use crate::{
    error::{Error, Result},
    primary_key::PrimaryKeyExtractor,
    state::StateStore,
    types::{DebeziumOp, DebeziumRecord, StoredRecord},
};

/// A Debezium CDC-compliant streaming client for Amp.
///
/// Wraps Amp's Arrow Flight client and transforms streaming query results
/// into Debezium format with proper reorg handling.
pub struct DebeziumClient<S: StateStore> {
    /// Underlying Amp SQL client
    amp_client: SqlClient,

    /// Primary key extractor
    pk_extractor: PrimaryKeyExtractor,

    /// State store for tracking emitted records
    state_store: S,
}

impl<S: StateStore> DebeziumClient<S> {
    /// Create a new builder for configuring the Debezium client.
    ///
    /// # Example
    /// ```no_run
    /// use amp_debezium::{DebeziumClient, InMemoryStore};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = DebeziumClient::builder()
    ///     .amp_endpoint("http://localhost:1602")?
    ///     .primary_keys(vec!["block_num".to_string(), "log_index".to_string()])
    ///     .state_store(InMemoryStore::new(64))
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
    /// # use amp_debezium::{DebeziumClient, InMemoryStore};
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
    ///         record if record.op == amp_debezium::DebeziumOp::Create => {
    ///             println!("New record: {:?}", record.after);
    ///         }
    ///         record if record.op == amp_debezium::DebeziumOp::Delete => {
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
    ) -> Result<impl Stream<Item = Result<DebeziumRecord>>> {
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
                        match self.handle_batch(data, metadata.ranges[0].network.clone()).await {
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
                    Ok(ResponseBatchWithReorg::Watermark(_watermark)) => {
                        // Watermark received - could be used for checkpointing
                        // For now, we just continue
                    }
                    Err(e) => {
                        yield Err(e.into());
                        break;
                    }
                }
            }
        })
    }

    /// Handle a batch of records, converting to Debezium create events.
    async fn handle_batch(
        &mut self,
        batch: RecordBatch,
        _network: String,
    ) -> Result<Vec<DebeziumRecord>> {
        let mut records = Vec::new();
        let batch_arc = Arc::new(batch.clone());

        for row_idx in 0..batch.num_rows() {
            // Extract primary key
            let key = self.pk_extractor.extract_key(&batch, row_idx)?;

            // Extract block number for state management
            let block_num = extract_block_num(&batch, row_idx)?;

            // Convert row to JSON
            let row_json = row_to_json(&batch, row_idx)?;

            // Store in state for potential reorg handling
            let stored_record = StoredRecord {
                key,
                batch: batch_arc.clone(),
                row_idx,
                block_num,
            };
            self.state_store.insert(stored_record).await?;

            // Create Debezium record
            let debezium_record = DebeziumRecord {
                before: None,
                after: Some(row_json),
                op: DebeziumOp::Create,
            };

            records.push(debezium_record);
        }

        Ok(records)
    }

    /// Handle a reorg event, converting invalidated records to Debezium delete events.
    async fn handle_reorg(
        &mut self,
        invalidation: Vec<amp_client::InvalidationRange>,
    ) -> Result<Vec<DebeziumRecord>> {
        let mut records = Vec::new();

        for range in invalidation {
            // Retrieve all records in the invalidation range
            let stored_records = self.state_store.get_in_range(&range).await?;

            for stored in stored_records {
                // Convert stored record to JSON
                let row_json = row_to_json(&stored.batch, stored.row_idx)?;

                // Create Debezium delete record
                let debezium_record = DebeziumRecord {
                    before: Some(row_json),
                    after: None,
                    op: DebeziumOp::Delete,
                };

                records.push(debezium_record);
            }
        }

        Ok(records)
    }
}

/// Builder for configuring a DebeziumClient.
pub struct DebeziumClientBuilder<S: StateStore> {
    endpoint: Option<String>,
    primary_keys: Option<Vec<String>>,
    state_store: Option<S>,
}

impl<S: StateStore> DebeziumClientBuilder<S> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            endpoint: None,
            primary_keys: None,
            state_store: None,
        }
    }

    /// Set the Amp server endpoint.
    pub fn amp_endpoint(mut self, endpoint: impl Into<String>) -> Result<Self> {
        self.endpoint = Some(endpoint.into());
        Ok(self)
    }

    /// Set the primary key column names.
    ///
    /// **Note**: The order of columns matters - keys will be hashed in the order provided.
    /// Different orderings will produce different composite keys.
    pub fn primary_keys(mut self, keys: Vec<String>) -> Self {
        self.primary_keys = Some(keys);
        self
    }

    /// Set the state store implementation.
    pub fn state_store(mut self, store: S) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Build the DebeziumClient.
    pub async fn build(self) -> Result<DebeziumClient<S>> {
        let endpoint = self
            .endpoint
            .ok_or_else(|| Error::Config("endpoint is required".to_string()))?;

        let primary_keys = self
            .primary_keys
            .ok_or_else(|| Error::Config("primary_keys are required".to_string()))?;

        let state_store = self
            .state_store
            .ok_or_else(|| Error::Config("state_store is required".to_string()))?;

        let amp_client = SqlClient::new(&endpoint).await?;

        let pk_extractor = PrimaryKeyExtractor::new(primary_keys);

        Ok(DebeziumClient {
            amp_client,
            pk_extractor,
            state_store,
        })
    }
}

impl<S: StateStore> Default for DebeziumClientBuilder<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract block number from a RecordBatch row.
///
/// Looks for a column named "block_num" or "_block_num_start".
fn extract_block_num(batch: &RecordBatch, row_idx: usize) -> Result<u64> {
    // Try common block number column names
    for col_name in ["block_num", "_block_num_start", "block_number"] {
        if let Some(column) = batch.column_by_name(col_name) {
            // Assume it's an Int64 column
            if let Some(arr) = column
                .as_any()
                .downcast_ref::<common::arrow::array::Int64Array>()
            {
                return Ok(arr.value(row_idx) as u64);
            }
        }
    }

    Err(Error::Config(
        "Could not find block number column (tried: block_num, _block_num_start, block_number)"
            .to_string(),
    ))
}

/// Convert a RecordBatch row to JSON.
fn row_to_json(batch: &RecordBatch, row_idx: usize) -> Result<serde_json::Value> {
    use serde_json::{Map, Value};

    let mut map = Map::new();

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let field_name = field.name().clone();

        // Convert Arrow value to JSON
        let json_value = arrow_value_to_json(column.as_ref(), row_idx)?;
        map.insert(field_name, json_value);
    }

    Ok(Value::Object(map))
}

/// Convert an Arrow array value at a specific row to a JSON value.
fn arrow_value_to_json(
    column: &dyn common::arrow::array::Array,
    row_idx: usize,
) -> Result<serde_json::Value> {
    use common::arrow::{
        array::{
            BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
            Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, TimeUnit},
    };
    use serde_json::Value;

    if column.is_null(row_idx) {
        return Ok(Value::Null);
    }

    match column.data_type() {
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Value::Bool(arr.value(row_idx)))
        }
        DataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt8 => {
            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt16 => {
            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt32 => {
            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::UInt64 => {
            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(serde_json::Number::from_f64(val as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(row_idx);
            Ok(serde_json::Number::from_f64(val)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Value::String(arr.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(Value::String(arr.value(row_idx).to_string()))
        }
        DataType::Binary => {
            let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let val = arr.value(row_idx);
            Ok(Value::String(format!("0x{}", hex::encode(val))))
        }
        DataType::LargeBinary => {
            let arr = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let val = arr.value(row_idx);
            Ok(Value::String(format!("0x{}", hex::encode(val))))
        }
        DataType::Date32 => {
            let arr = column.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(Value::Number(arr.value(row_idx).into()))
        }
        other => Err(Error::Config(format!(
            "Unsupported data type for JSON conversion: {:?}",
            other
        ))),
    }
}
