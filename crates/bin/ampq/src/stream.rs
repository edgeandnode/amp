//! Streaming data handling
//!
//! This module handles the connection to the Amp server and processes
//! streaming data events.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use amp_client::AmpClient;
use anyhow::Result;
use common::arrow::array::RecordBatch;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::app::{Row, RowType};

/// Represents a CDC row written to JSONL
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation")]
enum CdcRowJson {
    #[serde(rename = "insert")]
    Insert {
        transaction_id: String,
        timestamp: String,
        data: Value,
    },
    #[serde(rename = "delete")]
    Delete {
        transaction_id: String,
        timestamp: String,
        deleted_transaction_id: String,
        data: Value,
    },
}

/// Stream message sent from the streaming task to the UI
#[derive(Debug, Clone)]
pub enum StreamMessage {
    /// New data batch received
    Data(Vec<Row>),

    /// Connection status changed
    StatusChanged(StreamStatus),

    /// Reorg notification
    Reorg {
        transaction_ids: Vec<String>,
        #[allow(dead_code)]
        message: String,
    },

    /// Error occurred
    Error(String),
}

/// Stream connection status
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum StreamStatus {
    Connecting,
    Connected,
    Disconnected,
    Error(String),
}

/// Stream handler that manages the connection and data flow
pub struct StreamHandler {
    /// Receiver for stream messages
    receiver: mpsc::UnboundedReceiver<StreamMessage>,
}

impl StreamHandler {
    /// Create a new stream handler without starting a stream
    pub fn new() -> Self {
        let (_sender, receiver) = mpsc::unbounded_channel();
        Self { receiver }
    }

    /// Create a new stream handler and start streaming from the endpoint
    pub fn new_with_stream(
        endpoint: String,
        query: String,
        output_file: Option<PathBuf>,
        lmdb_path: Option<PathBuf>,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        // Spawn streaming task
        tokio::spawn(async move {
            if let Err(e) =
                start_stream_task(endpoint, query, sender.clone(), output_file, lmdb_path).await
            {
                tracing::error!("❌ Stream task failed: {:?}", e);
                let error_msg = format!("Stream error: {}", e);
                if let Err(send_err) = sender.send(StreamMessage::Error(error_msg)) {
                    tracing::error!("Failed to send error message to UI: {}", send_err);
                }
            }
        });

        Self { receiver }
    }

    /// Try to receive a message from the stream (non-blocking)
    pub fn try_recv(&mut self) -> Option<StreamMessage> {
        self.receiver.try_recv().ok()
    }
}

impl Default for StreamHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the streaming task with CDC
async fn start_stream_task(
    endpoint: String,
    query: String,
    sender: mpsc::UnboundedSender<StreamMessage>,
    output_file: Option<PathBuf>,
    lmdb_path: Option<PathBuf>,
) -> Result<()> {
    use amp_client::CdcEvent;

    tracing::info!("🔌 Starting stream task for endpoint: {}", endpoint);
    tracing::info!("📝 Query: {}", query);

    // Open output file if specified (always append mode for reconnection support)
    let mut jsonl_writer = if let Some(path) = output_file {
        tracing::info!("📄 Writing CDC events to: {}", path.display());
        let file = File::options().create(true).append(true).open(&path)?;
        tracing::info!("📄 Using append mode (reconnection-safe)");
        Some(BufWriter::new(file))
    } else {
        tracing::info!("📄 No output file specified");
        None
    };

    // Send connecting status
    if let Err(e) = sender.send(StreamMessage::StatusChanged(StreamStatus::Connecting)) {
        tracing::error!("Failed to send connecting status: {}", e);
        return Err(e.into());
    }

    // Connect to the server
    tracing::info!("🔗 Connecting to server at: {}", endpoint);
    let client = AmpClient::from_endpoint(&endpoint).await?;
    tracing::info!("✅ Connected to server successfully");

    // Send connected status
    if let Err(e) = sender.send(StreamMessage::StatusChanged(StreamStatus::Connected)) {
        tracing::error!("Failed to send connected status: {}", e);
        return Err(e.into());
    }

    // Create CDC stream with state stores (LMDB if path provided, otherwise in-memory)
    tracing::info!("🚀 Creating CDC stream");

    let mut stream = if let Some(lmdb_path) = lmdb_path {
        use amp_client::store::{LmdbBatchStore, LmdbStateStore, open_lmdb_env};

        tracing::info!("💾 Using LMDB storage at: {}", lmdb_path.display());

        // Create parent directory if it doesn't exist
        if let Some(parent) = lmdb_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open LMDB environment
        let env = open_lmdb_env(&lmdb_path)?;
        let state_store = LmdbStateStore::new(env.clone())?;
        let batch_store = LmdbBatchStore::new(env)?;

        tracing::info!("📡 Executing streaming query with persistent storage...");
        client
            .stream(&query)
            .cdc(state_store, batch_store, u64::MAX)
            .await?
    } else {
        use amp_client::{InMemoryBatchStore, InMemoryStateStore};

        tracing::info!("💾 Using in-memory storage (state will not persist)");
        let state_store = InMemoryStateStore::new();
        let batch_store = InMemoryBatchStore::new();

        tracing::info!("📡 Executing streaming query...");
        client
            .stream(&query)
            .cdc(state_store, batch_store, u64::MAX)
            .await?
    };

    tracing::info!("✅ CDC stream created successfully, waiting for events...");

    // Process CDC events
    let mut event_count = 0;
    let mut total_inserts = 0;
    let mut total_deletes = 0;

    loop {
        let result = stream.next().await;

        // Check if stream ended
        let Some(result) = result else {
            tracing::warn!("⚠️  Stream ended (returned None) - server may have closed connection");
            sender
                .send(StreamMessage::Error("Stream closed by server".to_string()))
                .ok();
            break;
        };

        match result {
            Ok((event, commit)) => {
                event_count += 1;

                match event {
                    CdcEvent::Insert { id, batch } => {
                        // Convert RecordBatch to rows with Insert type
                        let tx_id = format!("{:?}", id);
                        let rows = match convert_batch_to_rows(
                            &batch,
                            RowType::Insert,
                            Some(tx_id.clone()),
                        ) {
                            Ok(r) => r,
                            Err(e) => {
                                tracing::error!("Failed to convert batch to rows: {}", e);
                                // Continue without crashing - just log and skip this batch
                                continue;
                            }
                        };
                        total_inserts += rows.len();

                        tracing::info!(
                            "➕ Insert event #{}: {} rows (id: {:?}) | Total inserts: {}",
                            event_count,
                            rows.len(),
                            id,
                            total_inserts
                        );

                        // Write to JSONL file if configured (one line per row)
                        if let Some(ref mut writer) = jsonl_writer {
                            let timestamp = chrono::Utc::now().to_rfc3339();
                            match batch_to_json(&batch) {
                                Ok(json_rows) => {
                                    for row_data in json_rows {
                                        let row_json = CdcRowJson::Insert {
                                            transaction_id: tx_id.clone(),
                                            timestamp: timestamp.clone(),
                                            data: row_data,
                                        };
                                        if let Err(e) = write_json_line(writer, &row_json) {
                                            tracing::error!("Failed to write JSONL row: {}", e);
                                            // Continue processing - don't crash the stream
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to convert batch to JSON: {}", e);
                                    // Continue processing - don't crash the stream
                                }
                            }
                        }

                        // Send to UI - if channel is closed, stream should end
                        if sender.send(StreamMessage::Data(rows)).is_err() {
                            tracing::warn!("UI channel closed, ending stream");
                            break;
                        }

                        // Commit the event
                        if let Err(e) = commit.await {
                            tracing::error!("Failed to commit insert event: {}", e);
                            // This is a critical error - break the stream
                            sender
                                .send(StreamMessage::Error(format!("Commit failed: {}", e)))
                                .ok();
                            break;
                        }
                    }
                    CdcEvent::Delete { mut batches, id } => {
                        tracing::warn!(
                            "➖ Delete event #{}: Reorg/rewind (id: {:?})",
                            event_count,
                            id
                        );

                        // Collect all deleted transaction IDs and batches
                        let mut deleted_tx_ids = Vec::new();
                        let mut deleted_batches = Vec::new();
                        let mut row_count = 0;
                        while let Some(result) = batches.next().await {
                            match result {
                                Ok((batch_id, batch)) => {
                                    let tx_id = format!("{:?}", batch_id);
                                    let num_rows = batch.num_rows();
                                    deleted_tx_ids.push(tx_id.clone());
                                    deleted_batches.push((tx_id, batch));
                                    row_count += num_rows;
                                    total_deletes += num_rows;
                                }
                                Err(e) => {
                                    tracing::error!("Error loading delete batch: {}", e);
                                    // Continue loading other batches
                                }
                            }
                        }

                        tracing::info!(
                            "  Deleted {} rows from {} batches | Total deletes: {}",
                            row_count,
                            deleted_tx_ids.len(),
                            total_deletes
                        );

                        // Write to JSONL file if configured (one line per deleted row)
                        if let Some(ref mut writer) = jsonl_writer {
                            let timestamp = chrono::Utc::now().to_rfc3339();
                            for (deleted_tx_id, batch) in &deleted_batches {
                                match batch_to_json(batch) {
                                    Ok(json_rows) => {
                                        for row_data in json_rows {
                                            let row_json = CdcRowJson::Delete {
                                                transaction_id: format!("{:?}", id),
                                                timestamp: timestamp.clone(),
                                                deleted_transaction_id: deleted_tx_id.clone(),
                                                data: row_data,
                                            };
                                            if let Err(e) = write_json_line(writer, &row_json) {
                                                tracing::error!(
                                                    "Failed to write JSONL delete row: {}",
                                                    e
                                                );
                                                // Continue processing - don't crash the stream
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to convert delete batch to JSON: {}",
                                            e
                                        );
                                        // Continue processing - don't crash the stream
                                    }
                                }
                            }
                        }

                        if !deleted_tx_ids.is_empty() {
                            // Send to UI - if channel is closed, stream should end
                            if sender
                                .send(StreamMessage::Reorg {
                                    transaction_ids: deleted_tx_ids,
                                    message: format!("Reorg: {} rows deleted", row_count),
                                })
                                .is_err()
                            {
                                tracing::warn!("UI channel closed, ending stream");
                                break;
                            }
                        }

                        // Commit the event
                        if let Err(e) = commit.await {
                            tracing::error!("Failed to commit delete event: {}", e);
                            // This is a critical error - break the stream
                            sender
                                .send(StreamMessage::Error(format!("Commit failed: {}", e)))
                                .ok();
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "❌ Stream error after {} events (+{} -{} rows): {}",
                    event_count,
                    total_inserts,
                    total_deletes,
                    e
                );
                sender.send(StreamMessage::Error(format!("Stream error: {}", e)))?;
                break;
            }
        }
    }

    tracing::info!(
        "🏁 Stream ended normally. Total: {} events, {} inserts, {} deletes",
        event_count,
        total_inserts,
        total_deletes
    );

    if let Err(e) = sender.send(StreamMessage::StatusChanged(StreamStatus::Disconnected)) {
        tracing::warn!("Failed to send disconnected status (channel closed): {}", e);
    }

    Ok(())
}

/// Convert Arrow RecordBatch to UI Row format
fn convert_batch_to_rows(
    batch: &RecordBatch,
    row_type: RowType,
    transaction_id: Option<String>,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let schema = batch.schema();

    for row_idx in 0..batch.num_rows() {
        let mut data = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = format_arrow_value(column.as_ref(), row_idx)?;
            data.push((field.name().clone(), value));
        }

        rows.push(Row {
            data,
            row_type: row_type.clone(),
            transaction_id: transaction_id.clone(),
        });
    }

    Ok(rows)
}

/// Format an Arrow array value as a string
fn format_arrow_value(column: &dyn common::arrow::array::Array, row_idx: usize) -> Result<String> {
    use common::arrow::{
        array::{
            BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
            StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
            UInt64Array,
        },
        datatypes::{DataType, TimeUnit},
    };

    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }

    let value = match column.data_type() {
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt16 => {
            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt32 => {
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::LargeUtf8 => {
            let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Binary => {
            let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            format!("0x{}", hex::encode(bytes))
        }
        DataType::LargeBinary => {
            let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            format!("0x{}", hex::encode(bytes))
        }
        DataType::Date32 => {
            let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Date64 => {
            let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap();
                format_timestamp(array.value(row_idx), 1_000_000_000)
            }
            TimeUnit::Millisecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                format_timestamp(array.value(row_idx), 1_000_000)
            }
            TimeUnit::Microsecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                format_timestamp(array.value(row_idx), 1_000)
            }
            TimeUnit::Nanosecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                format_timestamp(array.value(row_idx), 1)
            }
        },
        // For complex types, try to extract values intelligently
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            // For lists, show array representation
            let sliced = column.slice(row_idx, 1);
            format_array_value(&sliced)
        }
        DataType::Struct(_) => {
            // For structs, show field values
            use common::arrow::array::StructArray;
            if let Some(struct_array) = column.as_any().downcast_ref::<StructArray>() {
                let values: Vec<String> = (0..struct_array.num_columns())
                    .map(|col_idx| {
                        let col = struct_array.column(col_idx);
                        format_arrow_value(col.as_ref(), row_idx)
                            .unwrap_or_else(|_| "?".to_string())
                    })
                    .collect();
                format!("{{{}}}", values.join(", "))
            } else {
                format!("{:?}", column.slice(row_idx, 1))
            }
        }
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            // For decimals, show numeric value with scale
            let sliced = column.slice(row_idx, 1);
            format!("{} (scale: {})", format_array_value(&sliced), scale)
        }
        // Fallback: try to extract a string representation
        _ => {
            // Try array cast to string
            let sliced = column.slice(row_idx, 1);
            format_array_value(&sliced)
        }
    };

    Ok(value)
}

/// Format an array value using Arrow's display formatting
fn format_array_value(array: &dyn common::arrow::array::Array) -> String {
    use common::arrow::util::display::{ArrayFormatter, FormatOptions};

    // Create formatter with default options
    let options = FormatOptions::default();
    match ArrayFormatter::try_new(array, &options) {
        Ok(formatter) => {
            // Format the first (and only) value
            formatter.value(0).to_string()
        }
        Err(_) => {
            // Fallback to debug representation
            format!("{:?}", array)
        }
    }
}

/// Format a timestamp value as a human-readable string
fn format_timestamp(value: i64, nanos_divisor: i64) -> String {
    use chrono::DateTime;

    let nanos = value * nanos_divisor;
    if let Some(dt) =
        DateTime::from_timestamp(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
    {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        value.to_string()
    }
}

/// Convert a RecordBatch to JSON representation
fn batch_to_json(batch: &RecordBatch) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let schema = batch.schema();

    for row_idx in 0..batch.num_rows() {
        let mut row_obj = serde_json::Map::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = arrow_value_to_json(column.as_ref(), row_idx)?;
            row_obj.insert(field.name().clone(), value);
        }

        rows.push(Value::Object(row_obj));
    }

    Ok(rows)
}

/// Convert an Arrow value to JSON
fn arrow_value_to_json(column: &dyn common::arrow::array::Array, row_idx: usize) -> Result<Value> {
    use common::arrow::{
        array::{
            BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array,
            UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::DataType,
    };

    if column.is_null(row_idx) {
        return Ok(Value::Null);
    }

    let value = match column.data_type() {
        DataType::Boolean => {
            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(array.value(row_idx))
        }
        DataType::Int8 => {
            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::Int16 => {
            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::Int32 => {
            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::Int64 => {
            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
            // JSON doesn't handle i64 well, so convert to string for large values
            let val = array.value(row_idx);
            if val.abs() < (1i64 << 53) {
                Value::Number(val.into())
            } else {
                Value::String(val.to_string())
            }
        }
        DataType::UInt8 => {
            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::UInt16 => {
            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::UInt32 => {
            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(array.value(row_idx).into())
        }
        DataType::UInt64 => {
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            // JSON doesn't handle u64 well, so convert to string for large values
            let val = array.value(row_idx);
            if val < (1u64 << 53) {
                Value::Number(val.into())
            } else {
                Value::String(val.to_string())
            }
        }
        DataType::Float32 => {
            let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = array.value(row_idx);
            if val.is_finite() {
                serde_json::Number::from_f64(val as f64)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(val.to_string()))
            } else {
                Value::String(val.to_string())
            }
        }
        DataType::Float64 => {
            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = array.value(row_idx);
            if val.is_finite() {
                serde_json::Number::from_f64(val)
                    .map(Value::Number)
                    .unwrap_or_else(|| Value::String(val.to_string()))
            } else {
                Value::String(val.to_string())
            }
        }
        DataType::Utf8 => {
            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(array.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(array.value(row_idx).to_string())
        }
        DataType::Binary => {
            let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            Value::String(format!("0x{}", hex::encode(bytes)))
        }
        DataType::LargeBinary => {
            let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let bytes = array.value(row_idx);
            Value::String(format!("0x{}", hex::encode(bytes)))
        }
        // For complex types, convert to string representation
        _ => {
            use common::arrow::util::display::{ArrayFormatter, FormatOptions};
            let options = FormatOptions::default();
            let sliced = column.slice(row_idx, 1);
            match ArrayFormatter::try_new(&sliced, &options) {
                Ok(formatter) => Value::String(formatter.value(0).to_string()),
                Err(_) => Value::String(format!("{:?}", sliced)),
            }
        }
    };

    Ok(value)
}

/// Write a JSON row to the file
fn write_json_line<W: Write>(writer: &mut W, row: &CdcRowJson) -> Result<()> {
    serde_json::to_writer(&mut *writer, row)?;
    writeln!(writer)?; // JSONL format requires newline after each JSON object
    writer.flush()?; // Flush to ensure data is written immediately
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_handler_creation() {
        let mut handler = StreamHandler::new();
        assert!(handler.try_recv().is_none());
    }
}
