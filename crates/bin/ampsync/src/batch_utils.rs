use std::sync::Arc;

use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use common::BoxError;

/// Convert nanosecond timestamps to microseconds in a RecordBatch for PostgreSQL compatibility.
///
/// PostgreSQL only supports microsecond precision, but Nozzle may return nanosecond timestamps.
/// This function converts any Timestamp(Nanosecond) columns to Timestamp(Microsecond) by
/// dividing the values by 1000.
pub fn convert_nanosecond_timestamps(batch: &RecordBatch) -> Result<RecordBatch, BoxError> {
    let schema = batch.schema();
    let mut needs_conversion = false;

    // Check if any fields need conversion
    for field in schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            needs_conversion = true;
            break;
        }
    }

    // If no conversion needed, return the original batch
    if !needs_conversion {
        return Ok(batch.clone());
    }

    // Build new schema and convert columns
    let mut new_fields = Vec::new();
    let mut new_columns = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                // Convert nanoseconds to microseconds
                let ns_array = batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or("Failed to downcast to TimestampNanosecondArray")?;

                // Create new array with microseconds (divide by 1000)
                let us_values: Vec<Option<i64>> = (0..ns_array.len())
                    .map(|row| {
                        if ns_array.is_null(row) {
                            None
                        } else {
                            Some(ns_array.value(row) / 1000)
                        }
                    })
                    .collect();

                let us_array =
                    TimestampMicrosecondArray::from(us_values).with_timezone_opt(tz.clone());

                // Create new field with microsecond precision
                new_fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    field.is_nullable(),
                )));

                new_columns.push(Arc::new(us_array) as _);
            }
            _ => {
                // Keep the field and column as-is
                new_fields.push(Arc::new((**field).clone()));
                new_columns.push(batch.column(i).clone());
            }
        }
    }

    // Create new schema and record batch
    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batch = RecordBatch::try_new(new_schema, new_columns)?;

    Ok(new_batch)
}
