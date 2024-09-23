use datafusion::arrow::array::{
    Decimal128Array, Decimal128Builder, FixedSizeBinaryBuilder, TimestampNanosecondBuilder,
};

use crate::{
    timestamp_type, Bytes32ArrayType, EvmAddressArrayType, Timestamp, TimestampArrayType,
    EVM_CURRENCY_TYPE,
};

#[derive(Debug)]
pub struct Bytes32ArrayBuilder(FixedSizeBinaryBuilder);

impl Bytes32ArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(FixedSizeBinaryBuilder::with_capacity(capacity, 32))
    }

    pub fn append_value(&mut self, value: [u8; 32]) {
        // Unwrap: The length is fixed.
        self.0.append_value(value).unwrap()
    }

    pub fn append_option(&mut self, value: Option<[u8; 32]>) {
        match value {
            // Unwrap: The length is fixed.
            Some(value) => self.0.append_value(value).unwrap(),
            None => self.0.append_null(),
        }
    }

    pub fn finish(mut self) -> Bytes32ArrayType {
        self.0.finish()
    }
}

#[derive(Debug)]
pub struct EvmAddressArrayBuilder(FixedSizeBinaryBuilder);

impl EvmAddressArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(FixedSizeBinaryBuilder::with_capacity(capacity, 20))
    }

    pub fn append_value(&mut self, value: [u8; 20]) {
        // Unwrap: The length is fixed.
        self.0.append_value(value).unwrap()
    }

    pub fn append_option(&mut self, value: Option<[u8; 20]>) {
        match value {
            // Unwrap: The length is fixed.
            Some(value) => self.append_value(value),
            None => self.0.append_null(),
        }
    }

    pub fn finish(mut self) -> EvmAddressArrayType {
        self.0.finish()
    }
}

pub struct EvmCurrencyArrayBuilder(Decimal128Builder);

impl EvmCurrencyArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Decimal128Builder::with_capacity(capacity).with_data_type(EVM_CURRENCY_TYPE))
    }

    pub fn append_value(&mut self, value: i128) {
        self.0.append_value(value)
    }

    pub fn append_option(&mut self, value: Option<i128>) {
        match value {
            Some(value) => self.0.append_value(value),
            None => self.0.append_null(),
        }
    }

    pub fn finish(mut self) -> Decimal128Array {
        self.0.finish()
    }
}

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
