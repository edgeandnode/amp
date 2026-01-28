//! Raw dataset utilities and schema generation support.
//!
//! This crate provides utilities for working with raw datasets, including
//! build-time schema generation for documentation purposes.

use std::time::Duration;

use ::arrow::{
    array::TimestampNanosecondArray,
    datatypes::{DataType, TimeUnit},
};

pub mod arrow;
pub mod client;
pub mod evm;
pub mod manifest;
pub mod rows;
#[cfg(feature = "gen-schema")]
pub mod schema;

#[derive(Debug, Default, Copy, Clone)]
pub struct Timestamp(pub Duration);

// Note: We choose a 'nanosecond' precision for the timestamp, even though many blockchains expose
// only 'second' precision for block timestamps. A couple justifications for 'nanosecond':
// 1. We found that the `date_bin` scalar function in DataFusion expects a nanosecond precision
//    origin parameter. If we patch DataFusion to fix this, we can revisit this decision.
// 2. There is no performance hit for choosing higher precision, it's all i64 at the Arrow level.
// 3. It's the most precise timestamp, so it's maximally future-compatible with data sources that use
//    more precise timestamps.
pub fn timestamp_type() -> DataType {
    let timezone = Some("+00:00".into());
    DataType::Timestamp(TimeUnit::Nanosecond, timezone)
}

// Remember to call `.with_timezone_utc()` after creating a Timestamp array.
pub(crate) type TimestampArrayType = TimestampNanosecondArray;
