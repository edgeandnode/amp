use arrow::array::TimestampNanosecondBuilder;

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
