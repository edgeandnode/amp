pub use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
        TimestampNanosecondBuilder, UInt8Builder, UInt32Builder, UInt64Builder,
    },
    datatypes::{DECIMAL128_MAX_PRECISION, DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
};

use crate::{Timestamp, TimestampArrayType, timestamp_type};

#[derive(Debug)]
pub struct TimestampArrayBuilder(TimestampNanosecondBuilder);

impl TimestampArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(TimestampNanosecondBuilder::with_capacity(capacity).with_data_type(timestamp_type()))
    }

    pub fn append_value(&mut self, value: Timestamp) {
        // i64::MAX in nanoseconds is almost 300 years, so we're safe to cast.
        self.0.append_value(value.0.as_nanos() as i64)
    }

    pub fn finish(&mut self) -> TimestampArrayType {
        self.0.finish()
    }
}
