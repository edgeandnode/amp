use std::{iter::once, sync::Arc};

use datafusion::arrow::{
    array::{
        BinaryArray, BooleanArray, FixedSizeBinaryBuilder, Int32Array, RecordBatch, UInt32Array,
        UInt64Array, UInt64Builder,
    },
    compute::concat_batches,
    error::ArrowError,
};

use crate::{
    Bytes, Bytes32, Bytes32ArrayType, EvmAddress, EvmCurrency, EvmCurrencyArrayType, TableRows,
    Timestamp, TimestampArrayType, EVM_CURRENCY_TYPE,
};

use super::arrow::array::Array;

pub fn rows_to_record_batch(rows: &TableRows) -> Result<RecordBatch, ArrowError> {
    let mut batches = Vec::with_capacity(rows.rows.len());
    for row in &rows.rows {
        batches.push(row.to_record_batch()?);
    }
    concat_batches(&rows.table.schema, batches.iter())
}

pub trait TableRow {
    /// Returns a record batch containing a single row.
    fn to_record_batch(&self) -> Result<RecordBatch, ArrowError>;
}

pub trait ScalarToArray {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError>;
}

impl ScalarToArray for u64 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(UInt64Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for Bytes32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Bytes32ArrayType::try_from_iter(once(self))?))
    }
}

impl ScalarToArray for Bytes {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(BinaryArray::from_iter_values(once(self))))
    }
}

impl ScalarToArray for Option<u64> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = UInt64Builder::new();
        builder.append_option(*self);
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for EvmAddress {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Bytes32ArrayType::try_from_iter(once(self))?))
    }
}

impl ScalarToArray for Option<Bytes32> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        match self {
            Some(bytes) => builder.append_value(bytes)?,
            None => builder.append_null(),
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for EvmCurrency {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let array =
            EvmCurrencyArrayType::from_iter_values(once(*self)).with_data_type(EVM_CURRENCY_TYPE);
        Ok(Arc::new(array))
    }
}

impl ScalarToArray for Option<EvmCurrency> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = EvmCurrencyArrayType::builder(1).with_data_type(EVM_CURRENCY_TYPE);
        builder.append_option(*self);
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for bool {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(BooleanArray::from_iter(once(Some(*self)))))
    }
}

impl ScalarToArray for u32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(UInt32Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for i32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Int32Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for Timestamp {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        // i64::MAX in nanoseconds is almost 300 years, so we're safe to cast.
        let nanos: i64 = self.0.as_nanos().try_into().map_err(|_| {
            ArrowError::ExternalError(
                format!("Timestamp out of range for Arrow's nanosecond precision",).into(),
            )
        })?;

        let array = TimestampArrayType::from_iter_values(once(nanos)).with_timezone_utc();
        Ok(Arc::new(array))
    }
}
