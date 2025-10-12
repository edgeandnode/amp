#![allow(clippy::redundant_closure_call)]

use std::{any::type_name, convert::identity, ops::Deref, sync::Arc};

use arrow_array::{self, Array, ArrowNativeTypeOp, OffsetSizeTrait};
use arrow_schema::{DataType, Field, TimeUnit};
use bytes::{BufMut, BytesMut};
use enum_dispatch::enum_dispatch;

use crate::{
    error::Error,
    pg_schema_mapper::{Column, PostgresType, TypeSize},
};

// PostgreSQL NUMERIC format constants
const NUMERIC_BASE: u64 = 10_000; // PostgreSQL uses base-10000 for NUMERIC type
const NUMERIC_BASE_I128: i128 = 10_000; // Same as NUMERIC_BASE but for i128 arithmetic
const NUMERIC_POS: i16 = 0x0000; // Sign value for positive NUMERIC
const NUMERIC_NEG: i16 = 0x4000; // Sign value for negative NUMERIC
const NUMERIC_HEADER_SIZE: usize = 8; // 4 i16 fields: ndigits, weight, sign, dscale
const MAX_U64_BASE10000_DIGITS: usize = 5; // u64::MAX requires at most 5 base-10000 digits
const MAX_I128_BASE10000_DIGITS: usize = 10; // i128::MAX requires at most 10 base-10000 digits

// JSONB format constants
const JSONB_VERSION: u8 = 1; // JSONB format version byte

#[inline]
fn downcast_checked<'a, T: 'static>(arr: &'a dyn Array, field: &str) -> Result<&'a T, Error> {
    match arr.as_any().downcast_ref::<T>() {
        Some(v) => Ok(v),
        None => Err(Error::mismatched_column_type(
            field,
            type_name::<T>(),
            arr.data_type(),
        )),
    }
}

#[enum_dispatch]
pub trait Encode: std::fmt::Debug {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error>;
    fn size_hint(&self) -> Result<usize, Error>;
}

#[enum_dispatch(Encode)]
#[derive(Debug)]
pub enum Encoder<'a> {
    Boolean(BooleanEncoder<'a>),
    UInt8(UInt8Encoder<'a>),
    UInt16(UInt16Encoder<'a>),
    UInt32(UInt32Encoder<'a>),
    UInt64(UInt64Encoder<'a>),
    Int8(Int8Encoder<'a>),
    Int16(Int16Encoder<'a>),
    Int32(Int32Encoder<'a>),
    Int64(Int64Encoder<'a>),
    Float16(Float16Encoder<'a>),
    Float32(Float32Encoder<'a>),
    Float64(Float64Encoder<'a>),
    Decimal128(Decimal128Encoder<'a>),
    TimestampMicrosecond(TimestampMicrosecondEncoder<'a>),
    TimestampMillisecond(TimestampMillisecondEncoder<'a>),
    TimestampSecond(TimestampSecondEncoder<'a>),
    Date32(Date32Encoder<'a>),
    Time32Millisecond(Time32MillisecondEncoder<'a>),
    Time32Second(Time32SecondEncoder<'a>),
    Time64Microsecond(Time64MicrosecondEncoder<'a>),
    DurationMicrosecond(DurationMicrosecondEncoder<'a>),
    DurationMillisecond(DurationMillisecondEncoder<'a>),
    DurationSecond(DurationSecondEncoder<'a>),
    Binary(BinaryEncoder<'a>),
    LargeBinary(LargeBinaryEncoder<'a>),
    FixedSizeBinary(FixedSizeBinaryEncoder<'a>),
    String(StringEncoder<'a>),
    LargeString(LargeStringEncoder<'a>),
    List(ListEncoder<'a>),
    LargeList(LargeListEncoder<'a>),
}

#[inline]
const fn type_size_fixed(size: TypeSize) -> usize {
    match size {
        TypeSize::Fixed(v) => v,
        _ => panic!("attempted to extract a fixed size for a variable sized type"),
    }
}

macro_rules! impl_encode {
    ($struct_name:ident, $field_size:expr, $transform:expr, $write:expr) => {
        impl<'a> Encode for $struct_name<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size as i32);
                    let v = self.arr.value(row);
                    let tv = $transform(v);
                    $write(buf, tv);
                }
                Ok(())
            }
            fn size_hint(&self) -> Result<usize, Error> {
                let null_count = self.arr.null_count();
                let item_count = self.arr.len();
                Ok((item_count - null_count) * $field_size + item_count)
            }
        }
    };
}

macro_rules! impl_encode_fallible {
    ($struct_name:ident, $field_size:expr, $transform:expr, $write:expr) => {
        impl<'a> Encode for $struct_name<'a> {
            fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
                if self.arr.is_null(row) {
                    buf.put_i32(-1)
                } else {
                    buf.put_i32($field_size as i32);
                    let v = self.arr.value(row);
                    let tv = $transform(&self.field, v)?;
                    $write(buf, tv);
                }
                Ok(())
            }
            fn size_hint(&self) -> Result<usize, Error> {
                let null_count = self.arr.null_count();
                let item_count = self.arr.len();
                Ok((item_count - null_count) * $field_size + item_count)
            }
        }
    };
}

#[derive(Debug)]
pub struct BooleanEncoder<'a> {
    arr: &'a arrow_array::BooleanArray,
}
impl_encode!(
    BooleanEncoder,
    type_size_fixed(PostgresType::Bool.size()),
    u8::from,
    BufMut::put_u8
);

#[derive(Debug)]
pub struct UInt8Encoder<'a> {
    arr: &'a arrow_array::UInt8Array,
}
impl_encode!(
    UInt8Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    i16::from,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct UInt16Encoder<'a> {
    arr: &'a arrow_array::UInt16Array,
}
impl_encode!(
    UInt16Encoder,
    type_size_fixed(PostgresType::Int4.size()),
    i32::from,
    BufMut::put_i32
);

#[derive(Debug)]
pub struct UInt32Encoder<'a> {
    arr: &'a arrow_array::UInt32Array,
}
impl_encode!(
    UInt32Encoder,
    type_size_fixed(PostgresType::Int8.size()),
    i64::from,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct UInt64Encoder<'a> {
    arr: &'a arrow_array::UInt64Array,
}
/// Efficiently count the number of base-10000 digits needed to represent a value.
/// For u64, max value is ~1.8e19, which requires at most 5 base-10000 digits.
#[inline]
fn count_base10000_digits(value: u64) -> usize {
    if value == 0 {
        return 0;
    }

    // Fast path using comparison instead of repeated division
    // NUMERIC_BASE^1 = 10,000
    // NUMERIC_BASE^2 = 100,000,000
    // NUMERIC_BASE^3 = 1,000,000,000,000
    // NUMERIC_BASE^4 = 10,000,000,000,000,000
    // NUMERIC_BASE^5 = 100,000,000,000,000,000,000 (exceeds u64::MAX)
    if value < 10_000 {
        return 1;
    }
    if value < 100_000_000 {
        return 2;
    }
    if value < 1_000_000_000_000 {
        return 3;
    }
    if value < 10_000_000_000_000_000 {
        return 4;
    }
    MAX_U64_BASE10000_DIGITS
}

/// Efficiently count base-10000 digits for i128 (used by Decimal128).
#[inline]
fn count_base10000_digits_i128(value: i128) -> usize {
    let abs_value = value.unsigned_abs();
    if abs_value == 0 {
        return 0;
    }

    // For i128, we need more thresholds
    // Using logarithmic search for better performance
    if abs_value < 10_000 {
        return 1;
    }
    if abs_value < 100_000_000 {
        return 2;
    }
    if abs_value < 1_000_000_000_000 {
        return 3;
    }
    if abs_value < 10_000_000_000_000_000 {
        return 4;
    }
    if abs_value < 100_000_000_000_000_000_000 {
        return 5;
    }
    if abs_value < 1_000_000_000_000_000_000_000_000 {
        return 6;
    }
    if abs_value < 10_000_000_000_000_000_000_000_000_000 {
        return 7;
    }
    if abs_value < 100_000_000_000_000_000_000_000_000_000_000 {
        return 8;
    }
    if abs_value < 1_000_000_000_000_000_000_000_000_000_000_000_000 {
        return 9;
    }

    // Fall back to division for very large numbers (rare)
    let mut ndigits = MAX_I128_BASE10000_DIGITS;
    let mut remaining = abs_value / 1_000_000_000_000_000_000_000_000_000_000_000_000;
    while remaining > 0 {
        ndigits += 1;
        remaining /= NUMERIC_BASE_I128 as u128;
    }
    ndigits
}

impl<'a> Encode for UInt64Encoder<'a> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1)
        } else {
            let value = self.arr.value(row);

            // PostgreSQL NUMERIC binary format (all fields as i16):
            // - ndigits (i16): number of base-10000 digits
            // - weight (i16): weight of first digit (NUMERIC_BASE^weight)
            // - sign (i16): NUMERIC_POS = 0x0000, NUMERIC_NEG = 0x4000, NUMERIC_NAN = 0xC000
            // - dscale (i16): display scale
            // - digits (i16[]): base-10000 digits

            // Handle zero as a special case
            if value == 0 {
                // Zero is encoded with ndigits=0, weight=0, sign=NUMERIC_POS, dscale=0
                buf.put_i32(NUMERIC_HEADER_SIZE as i32);
                buf.put_i16(0); // ndigits
                buf.put_i16(0); // weight
                buf.put_i16(NUMERIC_POS); // sign
                buf.put_i16(0); // dscale (scale is 0 for integers)
                return Ok(());
            }

            // Convert to base-10000 digits using stack array instead of Vec
            let mut digits = [0i16; MAX_U64_BASE10000_DIGITS];
            let mut ndigits = 0usize;
            let mut remaining = value;
            while remaining > 0 {
                digits[ndigits] = (remaining % NUMERIC_BASE) as i16;
                ndigits += 1;
                remaining /= NUMERIC_BASE;
            }

            let weight = (ndigits - 1) as i16; // Weight is position of first digit
            let dscale = 0i16; // Scale is 0 for integers

            // Calculate total size: header + (ndigits * 2 bytes)
            let size = NUMERIC_HEADER_SIZE + (ndigits * 2);
            buf.put_i32(size as i32);

            // Write header (all as i16)
            buf.put_i16(ndigits as i16);
            buf.put_i16(weight);
            buf.put_i16(NUMERIC_POS);
            buf.put_i16(dscale);

            // Write digits in reverse order (most significant first)
            for i in (0..ndigits).rev() {
                buf.put_i16(digits[i]);
            }
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        // Calculate size for binary NUMERIC format
        let mut total = 0;
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let value = self.arr.value(row);

                // Count base-10000 digits efficiently
                let ndigits = count_base10000_digits(value);

                // Size: length(4) + header + digits(ndigits * 2)
                total += 4 + NUMERIC_HEADER_SIZE + (ndigits * 2);
            } else {
                // Null value
                total += 4;
            }
        }
        Ok(total)
    }
}

#[derive(Debug)]
pub struct Int8Encoder<'a> {
    arr: &'a arrow_array::Int8Array,
}
impl_encode!(
    Int8Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    i16::from,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct Int16Encoder<'a> {
    arr: &'a arrow_array::Int16Array,
}
impl_encode!(
    Int16Encoder,
    type_size_fixed(PostgresType::Int2.size()),
    identity,
    BufMut::put_i16
);

#[derive(Debug)]
pub struct Int32Encoder<'a> {
    arr: &'a arrow_array::Int32Array,
}
impl_encode!(
    Int32Encoder,
    type_size_fixed(PostgresType::Int4.size()),
    identity,
    BufMut::put_i32
);

#[derive(Debug)]
pub struct Int64Encoder<'a> {
    arr: &'a arrow_array::Int64Array,
}
impl_encode!(
    Int64Encoder,
    type_size_fixed(PostgresType::Int8.size()),
    identity,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Float16Encoder<'a> {
    arr: &'a arrow_array::Float16Array,
}
impl_encode!(
    Float16Encoder,
    type_size_fixed(PostgresType::Float4.size()),
    f32::from,
    BufMut::put_f32
);

#[derive(Debug)]
pub struct Float32Encoder<'a> {
    arr: &'a arrow_array::Float32Array,
}
impl_encode!(
    Float32Encoder,
    type_size_fixed(PostgresType::Float4.size()),
    identity,
    BufMut::put_f32
);

#[derive(Debug)]
pub struct Float64Encoder<'a> {
    arr: &'a arrow_array::Float64Array,
}
impl_encode!(
    Float64Encoder,
    type_size_fixed(PostgresType::Float8.size()),
    identity,
    BufMut::put_f64
);

#[derive(Debug)]
pub struct Decimal128Encoder<'a> {
    arr: &'a arrow_array::Decimal128Array,
    /// Field name - included for consistency with other encoders but not currently used
    #[allow(dead_code)]
    field: String,
}

impl<'a> Encode for Decimal128Encoder<'a> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let value = self.arr.value(row);
            let scale = self.arr.scale() as i16;

            // PostgreSQL NUMERIC binary format (all fields as i16):
            // - ndigits (i16): number of base-10000 digits
            // - weight (i16): weight of first digit (NUMERIC_BASE^weight)
            // - sign (i16): NUMERIC_POS = 0x0000, NUMERIC_NEG = 0x4000, NUMERIC_NAN = 0xC000
            // - dscale (i16): display scale
            // - digits (i16[]): base-10000 digits

            // Handle zero as a special case
            if value == 0 {
                // Zero is encoded with ndigits=0, weight=0, sign=NUMERIC_POS, dscale=0
                buf.put_i32(NUMERIC_HEADER_SIZE as i32);
                buf.put_i16(0); // ndigits
                buf.put_i16(0); // weight
                buf.put_i16(NUMERIC_POS); // sign
                buf.put_i16(scale); // dscale
                return Ok(());
            }

            // Convert to absolute value and track sign
            let (abs_value, sign) = if value < 0 {
                (value.abs(), NUMERIC_NEG)
            } else {
                (value, NUMERIC_POS)
            };

            // Convert to base-10000 digits using stack array instead of Vec
            let mut digits = [0i16; MAX_I128_BASE10000_DIGITS];
            let mut ndigits = 0usize;
            let mut remaining = abs_value;
            while remaining > 0 {
                digits[ndigits] = (remaining % NUMERIC_BASE_I128) as i16;
                ndigits += 1;
                remaining /= NUMERIC_BASE_I128;
            }

            let weight = (ndigits - 1) as i16; // Weight is position of first digit
            let dscale = scale;

            // Calculate total size: header + (ndigits * 2 bytes)
            let size = NUMERIC_HEADER_SIZE + (ndigits * 2);
            buf.put_i32(size as i32);

            // Write header (all as i16)
            buf.put_i16(ndigits as i16);
            buf.put_i16(weight);
            buf.put_i16(sign);
            buf.put_i16(dscale);

            // Write digits in reverse order (most significant first)
            for i in (0..ndigits).rev() {
                buf.put_i16(digits[i]);
            }
        }
        Ok(())
    }

    fn size_hint(&self) -> Result<usize, Error> {
        // Calculate size for binary NUMERIC format
        let mut total = 0;
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let value = self.arr.value(row);

                // Count base-10000 digits efficiently
                let ndigits = count_base10000_digits_i128(value);

                // Size = 4 (length prefix) + header + (ndigits * 2)
                total += 4 + NUMERIC_HEADER_SIZE + (ndigits * 2);
            } else {
                total += 4; // 4 bytes for -1 (null indicator)
            }
        }
        Ok(total)
    }
}

const PG_BASE_TIMESTAMP_OFFSET_US: i64 = 946_684_800_000_000; // microseconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)
const PG_BASE_TIMESTAMP_OFFSET_MS: i64 = 946_684_800_000; // milliseconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)
const PG_BASE_TIMESTAMP_OFFSET_S: i64 = 946_684_800; // seconds between 2000-01-01 at midnight (Postgres's epoch) and 1970-01-01 (Arrow's / UNIX epoch)

#[inline(always)]
fn convert_arrow_timestamp_microseconds_to_pg_timestamp(
    _field: &str,
    timestamp_us: i64,
) -> Result<i64, Error> {
    // adjust the timestamp from microseconds since 1970-01-01 to microseconds since 2000-01-01 checking for overflows and underflow
    timestamp_us
        .checked_sub(PG_BASE_TIMESTAMP_OFFSET_US)
        .ok_or_else(|| Error::Encode {
            reason: "Underflow converting microseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
        })
}

/// Convert from Arrow timestamps (milliseconds since 1970-01-01) to Postgres timestamps (microseconds since 2000-01-01)
#[inline(always)]
fn convert_arrow_timestamp_milliseconds_to_pg_timestamp(
    _field: &str,
    timestamp_ms: i64,
) -> Result<i64, Error> {
    let timestamp_ms = timestamp_ms.checked_sub(PG_BASE_TIMESTAMP_OFFSET_MS).ok_or_else(|| Error::Encode {
        reason: "Underflow converting milliseconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
    })?;
    // convert to microseconds, checking for overflows
    timestamp_ms
        .checked_mul(1_000)
        .ok_or_else(|| Error::Encode {
            reason: "Overflow converting milliseconds to microseconds".to_string(),
        })
}

#[inline(always)]
fn convert_arrow_timestamp_seconds_to_pg_timestamp(
    _field: &str,
    timestamp_s: i64,
) -> Result<i64, Error> {
    let timestamp_s = timestamp_s.checked_sub(PG_BASE_TIMESTAMP_OFFSET_S).ok_or_else(|| Error::Encode {
        reason: "Underflow converting seconds since 1970-01-01 (Arrow) to microseconds since 2000-01-01 (Postgres)".to_string(),
    })?;
    // convert to microseconds, checking for overflows
    timestamp_s
        .checked_mul(1_000_000)
        .ok_or_else(|| Error::Encode {
            reason: "Overflow converting seconds to microseconds".to_string(),
        })
}

#[derive(Debug)]
pub struct TimestampMicrosecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMicrosecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMicrosecondEncoder,
    8, // Timestamp is always 8 bytes (i64)
    convert_arrow_timestamp_microseconds_to_pg_timestamp,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct TimestampMillisecondEncoder<'a> {
    arr: &'a arrow_array::TimestampMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampMillisecondEncoder,
    8, // Timestamp is always 8 bytes (i64)
    convert_arrow_timestamp_milliseconds_to_pg_timestamp,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct TimestampSecondEncoder<'a> {
    arr: &'a arrow_array::TimestampSecondArray,
    field: String,
}
impl_encode_fallible!(
    TimestampSecondEncoder,
    8, // Timestamp is always 8 bytes (i64)
    convert_arrow_timestamp_seconds_to_pg_timestamp,
    BufMut::put_i64
);

const PG_BASE_DATE_OFFSET: i32 = 10_957; // Number of days between PostgreSQL's epoch (2000-01-01) and Arrow's / UNIX epoch (1970-01-01)

#[inline(always)]
fn convert_arrow_date32_to_postgres_date(_field: &str, date: i32) -> Result<i32, Error> {
    // adjust the date from days since 1970-01-01 to days since 2000-01-01 checking for overflows and underflow
    date.checked_sub(PG_BASE_DATE_OFFSET).ok_or_else(|| Error::Encode {
        reason: "Underflow converting days since 1970-01-01 (Arrow) to days since 2000-01-01 (Postgres)".to_string(),
    })
}

#[derive(Debug)]
pub struct Date32Encoder<'a> {
    arr: &'a arrow_array::Date32Array,
    field: String,
}
impl_encode_fallible!(
    Date32Encoder,
    4,
    convert_arrow_date32_to_postgres_date,
    BufMut::put_i32
);

fn convert_arrow_time_seconds_to_postgres_time(_field: &str, time_s: i32) -> Result<i64, Error> {
    // convert to microseconds, checking for overflows
    let time_s = time_s as i64;
    time_s.checked_mul(1_000_000).ok_or_else(|| Error::Encode {
        reason: "Overflow converting seconds to microseconds".to_string(),
    })
}

fn convert_arrow_time_milliseconds_to_postgres_time(
    _field: &str,
    time_ms: i32,
) -> Result<i64, Error> {
    // convert to microseconds, checking for overflows
    let time_ms = time_ms as i64;
    time_ms.checked_mul(1_000).ok_or_else(|| Error::Encode {
        reason: "Overflow converting milliseconds to microseconds".to_string(),
    })
}

#[derive(Debug)]
pub struct Time32MillisecondEncoder<'a> {
    arr: &'a arrow_array::Time32MillisecondArray,
    field: String,
}
impl_encode_fallible!(
    Time32MillisecondEncoder,
    type_size_fixed(PostgresType::Time.size()),
    convert_arrow_time_milliseconds_to_postgres_time,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Time32SecondEncoder<'a> {
    arr: &'a arrow_array::Time32SecondArray,
    field: String,
}
impl_encode_fallible!(
    Time32SecondEncoder,
    type_size_fixed(PostgresType::Time.size()),
    convert_arrow_time_seconds_to_postgres_time,
    BufMut::put_i64
);

#[derive(Debug)]
pub struct Time64MicrosecondEncoder<'a> {
    arr: &'a arrow_array::Time64MicrosecondArray,
}
impl_encode!(Time64MicrosecondEncoder, 8, identity, BufMut::put_i64);

#[derive(Debug)]
pub struct DurationMicrosecondEncoder<'a> {
    arr: &'a arrow_array::DurationMicrosecondArray,
}
impl_encode!(DurationMicrosecondEncoder, 16, identity, write_duration);

const NUM_US_PER_MS: i64 = 1_000;
const NUM_US_PER_S: i64 = 1_000_000;

#[inline]
fn write_duration(buf: &mut BytesMut, duration_us: i64) {
    buf.put_i64(duration_us);
    buf.put_i32(0); // days
    buf.put_i32(0); // months
}

#[derive(Debug)]
pub struct DurationMillisecondEncoder<'a> {
    arr: &'a arrow_array::DurationMillisecondArray,
    field: String,
}
impl_encode_fallible!(
    DurationMillisecondEncoder,
    type_size_fixed(PostgresType::Interval.size()),
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_MS).map_err(|_| {
        Error::Encode {
            reason: "Overflow encoding millisecond duration as microseconds".to_string(),
        }
    }),
    write_duration
);

#[derive(Debug)]
pub struct DurationSecondEncoder<'a> {
    arr: &'a arrow_array::DurationSecondArray,
    field: String,
}

impl_encode_fallible!(
    DurationSecondEncoder,
    type_size_fixed(PostgresType::Interval.size()),
    |_: &str, v: i64| v.mul_checked(NUM_US_PER_S).map_err(|_| {
        Error::Encode {
            reason: "Overflow encoding seconds duration as microseconds".to_string(),
        }
    }),
    write_duration
);

#[derive(Debug)]
pub struct GenericBinaryEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericBinaryArray<T>,
    field: String,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericBinaryEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row);
            let len = v.len();
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(Error::field_too_large(&self.field, len)),
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let mut total = 0;
        for row in 0..self.arr.len() {
            // Each row: 4 bytes for length prefix + actual data length
            total += 4 + self.arr.value(row).len();
        }
        Ok(total)
    }
}

type BinaryEncoder<'a> = GenericBinaryEncoder<'a, i32>;
type LargeBinaryEncoder<'a> = GenericBinaryEncoder<'a, i64>;

#[derive(Debug)]
pub struct FixedSizeBinaryEncoder<'a> {
    arr: &'a arrow_array::FixedSizeBinaryArray,
    field: String,
}

impl<'a> Encode for FixedSizeBinaryEncoder<'a> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row);
            let len = v.len();
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(Error::field_too_large(&self.field, len)),
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let value_len = self.arr.value_length() as usize;
        let null_count = self.arr.null_count();
        let item_count = self.arr.len();
        Ok((item_count - null_count) * value_len + item_count * 4)
    }
}

#[derive(Debug)]
pub struct GenericStringEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericStringArray<T>,
    field: String,
    output: StringOutputType,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericStringEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let v = self.arr.value(row).as_bytes();

            // Validate JSON when encoding as JSONB
            if matches!(self.output, StringOutputType::Jsonb)
                && let Err(e) = serde_json::from_slice::<serde_json::Value>(v)
            {
                return Err(Error::invalid_json(&self.field, &e.to_string()));
            }

            let mut len = v.len();
            if matches!(self.output, StringOutputType::Jsonb) {
                len += 1;
            }
            match i32::try_from(len) {
                Ok(l) => buf.put_i32(l),
                Err(_) => return Err(Error::field_too_large(&self.field, len)),
            }
            if matches!(self.output, StringOutputType::Jsonb) {
                buf.put_u8(JSONB_VERSION)
            }
            buf.extend_from_slice(v);
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        let mut total = 0;
        let is_jsonb = matches!(self.output, StringOutputType::Jsonb);
        for row in 0..self.arr.len() {
            // Each row: 4 bytes for length prefix + actual string length + (1 byte for JSONB version if applicable)
            total += 4 + self.arr.value(row).len();
            if is_jsonb {
                total += 1; // JSONB format version byte
            }
        }
        Ok(total)
    }
}

type StringEncoder<'a> = GenericStringEncoder<'a, i32>;
type LargeStringEncoder<'a> = GenericStringEncoder<'a, i64>;

#[derive(Debug)]
pub struct GenericListEncoder<'a, T: OffsetSizeTrait> {
    arr: &'a arrow_array::GenericListArray<T>,
    field: String,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl<'a, T: OffsetSizeTrait> Encode for GenericListEncoder<'a, T> {
    fn encode(&self, row: usize, buf: &mut BytesMut) -> Result<(), Error> {
        if self.arr.is_null(row) {
            buf.put_i32(-1);
        } else {
            let val = self.arr.value(row);
            let inner_encoder = self.inner_encoder_builder.deref().try_new(val.as_ref())?;

            let base_idx = buf.len();
            buf.put_i32(0); // the total number of bytes this element takes up, insert later
            buf.put_i32(1); // num dimensions, we only support 1
            buf.put_i32((val.null_count() != 0) as i32); // nulls flag, true if any item is null
            let inner_tp_oid = self
                .inner_encoder_builder
                .deref()
                .schema()
                .data_type
                .oid()
                .unwrap();
            buf.put_i32(inner_tp_oid as i32);
            // put the dimension length
            buf.put_i32(val.len() as i32);
            // put the dimension lower bound, always 1
            buf.put_i32(1);

            for inner_row in 0..val.len() {
                inner_encoder.encode(inner_row, buf)?;
            }

            let total_len = buf.len() - base_idx - 4; // end - start - 4 bytes for the size i32 itself

            match i32::try_from(total_len) {
                Ok(v) => buf[base_idx..base_idx + 4].copy_from_slice(&v.to_be_bytes()),
                Err(_) => return Err(Error::field_too_large(&self.field, total_len)),
            };
        }
        Ok(())
    }
    fn size_hint(&self) -> Result<usize, Error> {
        // PostgreSQL array format overhead per non-null array:
        // - 4 bytes: total length (included in element size but written separately)
        // - 4 bytes: number of dimensions
        // - 4 bytes: nulls flag
        // - 4 bytes: element type OID
        // - 4 bytes: dimension length
        // - 4 bytes: dimension lower bound
        // = 24 bytes metadata per array
        const ARRAY_METADATA_SIZE: usize = 24;

        let mut total = 0;
        for row in 0..self.arr.len() {
            if !self.arr.is_null(row) {
                let val = self.arr.value(row);
                let inner_encoder = self.inner_encoder_builder.deref().try_new(val.as_ref())?;
                let inner_size = inner_encoder.size_hint()?;
                // Total: metadata + inner elements size
                total += ARRAY_METADATA_SIZE + inner_size;
            } else {
                // Null arrays: 4 bytes for -1
                total += 4;
            }
        }
        Ok(total)
    }
}

type ListEncoder<'a> = GenericListEncoder<'a, i32>;
type LargeListEncoder<'a> = GenericListEncoder<'a, i64>;

#[enum_dispatch]
pub trait BuildEncoder: std::fmt::Debug + PartialEq {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error>;
    fn schema(&self) -> Column;
    fn field(&self) -> Arc<Field>;
}

macro_rules! impl_encoder_builder_stateless {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, Error> {
                if !$check_data_type(field.data_type()) {
                    return Err(Error::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self { field })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                Ok($enum_name($encoder_name {
                    arr: downcast_checked(arr, &self.field.name())?,
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    data_type: $pg_data_type.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

macro_rules! impl_encoder_builder_stateless_with_field {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, Error> {
                if !$check_data_type(field.data_type()) {
                    return Err(Error::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self { field })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name {
                    arr,
                    field: field.to_string(),
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    data_type: $pg_data_type.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

macro_rules! impl_encoder_builder_stateless_with_variable_output {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $pg_data_type:expr, $allowed_pg_data_types:expr, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, Error> {
                if !$check_data_type(field.data_type()) {
                    return Err(Error::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self {
                    field,
                    output: $pg_data_type,
                })
            }
            pub fn new_with_output(field: Arc<Field>, output: PostgresType) -> Result<Self, Error> {
                if !$allowed_pg_data_types.contains(&output) {
                    return Err(Error::unsupported_encoding(
                        &field.name(),
                        &output,
                        &[PostgresType::Char, PostgresType::Int2],
                    ));
                }
                Ok(Self { field, output })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name { arr }))
            }
            fn schema(&self) -> Column {
                Column {
                    data_type: self.output.clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct BooleanEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    BooleanEncoderBuilder,
    Encoder::Boolean,
    BooleanEncoder,
    PostgresType::Bool,
    |dt: &DataType| matches!(dt, DataType::Boolean)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt8EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt8EncoderBuilder,
    Encoder::UInt8,
    UInt8Encoder,
    PostgresType::Int2,
    |dt: &DataType| matches!(dt, DataType::UInt8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt16EncoderBuilder,
    Encoder::UInt16,
    UInt16Encoder,
    PostgresType::Int4,
    |dt: &DataType| matches!(dt, DataType::UInt16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    UInt32EncoderBuilder,
    Encoder::UInt32,
    UInt32Encoder,
    PostgresType::Int8,
    |dt: &DataType| matches!(dt, DataType::UInt32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct UInt64EncoderBuilder {
    field: Arc<Field>,
}

impl UInt64EncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, Error> {
        if !matches!(field.data_type(), DataType::UInt64) {
            return Err(Error::FieldTypeNotSupported {
                encoder: "UInt64EncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self { field })
    }
}

impl BuildEncoder for UInt64EncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        Ok(Encoder::UInt64(UInt64Encoder {
            arr: downcast_checked(arr, self.field.name())?,
        }))
    }

    fn schema(&self) -> Column {
        Column {
            data_type: PostgresType::Numeric {
                precision: Some(20), // UInt64 max value ~1.8e19, needs precision of 20
                scale: Some(0),      // Integers have scale 0
            },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Int8EncoderBuilder {
    field: Arc<Field>,
    output: PostgresType,
}
impl_encoder_builder_stateless_with_variable_output!(
    Int8EncoderBuilder,
    Encoder::Int8,
    Int8Encoder,
    PostgresType::Int2,
    [PostgresType::Char, PostgresType::Int2],
    |dt: &DataType| matches!(dt, DataType::Int8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int16EncoderBuilder,
    Encoder::Int16,
    Int16Encoder,
    PostgresType::Int2,
    |dt: &DataType| matches!(dt, DataType::Int16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int32EncoderBuilder,
    Encoder::Int32,
    Int32Encoder,
    PostgresType::Int4,
    |dt: &DataType| matches!(dt, DataType::Int32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Int64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Int64EncoderBuilder,
    Encoder::Int64,
    Int64Encoder,
    PostgresType::Int8,
    |dt: &DataType| matches!(dt, DataType::Int64)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float16EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float16EncoderBuilder,
    Encoder::Float16,
    Float16Encoder,
    PostgresType::Float4,
    |dt: &DataType| matches!(dt, DataType::Float16)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float32EncoderBuilder,
    Encoder::Float32,
    Float32Encoder,
    PostgresType::Float4,
    |dt: &DataType| matches!(dt, DataType::Float32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Float64EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Float64EncoderBuilder,
    Encoder::Float64,
    Float64Encoder,
    PostgresType::Float8,
    |dt: &DataType| matches!(dt, DataType::Float64)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Decimal128EncoderBuilder {
    field: Arc<Field>,
}

impl Decimal128EncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, Error> {
        if !matches!(field.data_type(), DataType::Decimal128(_, _)) {
            return Err(Error::FieldTypeNotSupported {
                encoder: "Decimal128EncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self { field })
    }
}

impl BuildEncoder for Decimal128EncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let field_name = self.field.name();
        let arr = downcast_checked(arr, field_name)?;
        Ok(Encoder::Decimal128(Decimal128Encoder {
            arr,
            field: field_name.to_string(),
        }))
    }

    fn schema(&self) -> Column {
        // Extract precision and scale from the Arrow DataType
        let (precision, scale) = match self.field.data_type() {
            DataType::Decimal128(p, s) => (*p, *s),
            _ => {
                unreachable!("Decimal128EncoderBuilder should only be created for Decimal128 types")
            }
        };

        Column {
            data_type: PostgresType::Numeric {
                precision: Some(precision),
                scale: Some(scale),
            },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampMicrosecondEncoderBuilder {
    field: Arc<Field>,
}

impl TimestampMicrosecondEncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, Error> {
        if !matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return Err(Error::FieldTypeNotSupported {
                encoder: "TimestampMicrosecondEncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self { field })
    }
}

impl BuildEncoder for TimestampMicrosecondEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let field_name = self.field.name();
        let arr = downcast_checked(arr, field_name)?;
        Ok(Encoder::TimestampMicrosecond(TimestampMicrosecondEncoder {
            arr,
            field: field_name.to_string(),
        }))
    }

    fn schema(&self) -> Column {
        // Extract timezone from the Arrow DataType and convert Arc<str> to String
        let timezone = match self.field.data_type() {
            DataType::Timestamp(_, tz) => tz.as_ref().map(|s| s.to_string()),
            _ => unreachable!(
                "TimestampMicrosecondEncoderBuilder should only be created for Timestamp types"
            ),
        };

        Column {
            data_type: PostgresType::Timestamp { timezone },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampMillisecondEncoderBuilder {
    field: Arc<Field>,
}

impl TimestampMillisecondEncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, Error> {
        if !matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ) {
            return Err(Error::FieldTypeNotSupported {
                encoder: "TimestampMillisecondEncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self { field })
    }
}

impl BuildEncoder for TimestampMillisecondEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let field_name = self.field.name();
        let arr = downcast_checked(arr, field_name)?;
        Ok(Encoder::TimestampMillisecond(TimestampMillisecondEncoder {
            arr,
            field: field_name.to_string(),
        }))
    }

    fn schema(&self) -> Column {
        let timezone = match self.field.data_type() {
            DataType::Timestamp(_, tz) => tz.as_ref().map(|s| s.to_string()),
            _ => unreachable!(
                "TimestampMillisecondEncoderBuilder should only be created for Timestamp types"
            ),
        };

        Column {
            data_type: PostgresType::Timestamp { timezone },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampSecondEncoderBuilder {
    field: Arc<Field>,
}

impl TimestampSecondEncoderBuilder {
    pub fn new(field: Arc<Field>) -> Result<Self, Error> {
        if !matches!(field.data_type(), DataType::Timestamp(TimeUnit::Second, _)) {
            return Err(Error::FieldTypeNotSupported {
                encoder: "TimestampSecondEncoderBuilder".to_string(),
                tp: field.data_type().clone(),
                field: field.name().clone(),
            });
        }
        Ok(Self { field })
    }
}

impl BuildEncoder for TimestampSecondEncoderBuilder {
    fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
        let field_name = self.field.name();
        let arr = downcast_checked(arr, field_name)?;
        Ok(Encoder::TimestampSecond(TimestampSecondEncoder {
            arr,
            field: field_name.to_string(),
        }))
    }

    fn schema(&self) -> Column {
        let timezone = match self.field.data_type() {
            DataType::Timestamp(_, tz) => tz.as_ref().map(|s| s.to_string()),
            _ => unreachable!(
                "TimestampSecondEncoderBuilder should only be created for Timestamp types"
            ),
        };

        Column {
            data_type: PostgresType::Timestamp { timezone },
            nullable: self.field.is_nullable(),
        }
    }

    fn field(&self) -> Arc<Field> {
        self.field.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Date32EncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Date32EncoderBuilder,
    Encoder::Date32,
    Date32Encoder,
    PostgresType::Date,
    |dt: &DataType| matches!(dt, DataType::Date32)
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time32MillisecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Time32MillisecondEncoderBuilder,
    Encoder::Time32Millisecond,
    Time32MillisecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time32(TimeUnit::Millisecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time32SecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    Time32SecondEncoderBuilder,
    Encoder::Time32Second,
    Time32SecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time32(TimeUnit::Second))
);

#[derive(Debug, Clone, PartialEq)]
pub struct Time64MicrosecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    Time64MicrosecondEncoderBuilder,
    Encoder::Time64Microsecond,
    Time64MicrosecondEncoder,
    PostgresType::Time,
    |dt: &DataType| matches!(dt, DataType::Time64(TimeUnit::Microsecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationMicrosecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless!(
    DurationMicrosecondEncoderBuilder,
    Encoder::DurationMicrosecond,
    DurationMicrosecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Microsecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationMillisecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    DurationMillisecondEncoderBuilder,
    Encoder::DurationMillisecond,
    DurationMillisecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Millisecond))
);

#[derive(Debug, Clone, PartialEq)]
pub struct DurationSecondEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    DurationSecondEncoderBuilder,
    Encoder::DurationSecond,
    DurationSecondEncoder,
    PostgresType::Interval,
    |dt: &DataType| matches!(dt, DataType::Duration(TimeUnit::Second))
);

#[derive(Debug, Clone, PartialEq)]
enum StringOutputType {
    Text,
    Json,
    Jsonb,
}

impl StringOutputType {
    pub fn from_postgres_type(tp: PostgresType, field: &Field) -> Result<Self, Error> {
        match tp {
            PostgresType::Text => Ok(StringOutputType::Text),
            PostgresType::Json => Ok(StringOutputType::Json),
            PostgresType::Jsonb => Ok(StringOutputType::Jsonb),
            other => Err(Error::EncodingNotSupported {
                field: field.name().clone(),
                tp: other,
                allowed: vec![PostgresType::Text, PostgresType::Json, PostgresType::Jsonb],
            }),
        }
    }
    pub fn postgres_datatype(&self) -> PostgresType {
        match self {
            StringOutputType::Text => PostgresType::Text,
            StringOutputType::Json => PostgresType::Json,
            StringOutputType::Jsonb => PostgresType::Jsonb,
        }
    }
}

macro_rules! impl_encoder_builder_with_variable_output {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident, $check_data_type:expr) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, Error> {
                if !$check_data_type(field.data_type()) {
                    return Err(Error::FieldTypeNotSupported {
                        encoder: stringify!($struct_name).to_string(),
                        tp: field.data_type().clone(),
                        field: field.name().clone(),
                    });
                }
                Ok(Self {
                    field,
                    output: StringOutputType::Text,
                })
            }
            pub fn new_with_output(field: Arc<Field>, output: PostgresType) -> Result<Self, Error> {
                let output = StringOutputType::from_postgres_type(output, &field)?;
                Ok(Self { field, output })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                let field = self.field.name();
                let arr = downcast_checked(arr, &field)?;
                Ok($enum_name($encoder_name {
                    field: self.field.name().clone(),
                    arr,
                    output: self.output.clone(),
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    data_type: self.output.postgres_datatype().clone(),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct StringEncoderBuilder {
    field: Arc<Field>,
    output: StringOutputType,
}
impl_encoder_builder_with_variable_output!(
    StringEncoderBuilder,
    Encoder::String,
    StringEncoder,
    |dt: &DataType| matches!(dt, DataType::Utf8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeStringEncoderBuilder {
    field: Arc<Field>,
    output: StringOutputType,
}

impl_encoder_builder_with_variable_output!(
    LargeStringEncoderBuilder,
    Encoder::LargeString,
    LargeStringEncoder,
    |dt: &DataType| matches!(dt, DataType::LargeUtf8)
);

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    BinaryEncoderBuilder,
    Encoder::Binary,
    BinaryEncoder,
    PostgresType::Bytea,
    |dt: &DataType| matches!(dt, DataType::Binary)
);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeBinaryEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    LargeBinaryEncoderBuilder,
    Encoder::LargeBinary,
    LargeBinaryEncoder,
    PostgresType::Bytea,
    |dt: &DataType| matches!(dt, DataType::LargeBinary)
);

#[derive(Debug, Clone, PartialEq)]
pub struct FixedSizeBinaryEncoderBuilder {
    field: Arc<Field>,
}
impl_encoder_builder_stateless_with_field!(
    FixedSizeBinaryEncoderBuilder,
    Encoder::FixedSizeBinary,
    FixedSizeBinaryEncoder,
    PostgresType::Bytea,
    |dt: &DataType| matches!(dt, DataType::FixedSizeBinary(_))
);

macro_rules! impl_list_encoder_builder {
    ($struct_name:ident, $enum_name:expr, $encoder_name:ident) => {
        impl $struct_name {
            pub fn new(field: Arc<Field>) -> Result<Self, Error> {
                match &field.data_type() {
                    DataType::List(inner) => {
                        let inner_encoder_builder = EncoderBuilder::try_new(inner.clone())?;
                        Ok(Self {
                            field,
                            inner_encoder_builder: Arc::new(inner_encoder_builder),
                        })
                    }
                    _ => Err(Error::type_unsupported(
                        &field.name(),
                        field.data_type(),
                        format!("{:?} is not a list type", field.data_type()).as_str(),
                    )),
                }
            }
            pub fn new_with_inner(
                field: Arc<Field>,
                inner_encoder_builder: EncoderBuilder,
            ) -> Result<Self, Error> {
                Ok(Self {
                    field,
                    inner_encoder_builder: Arc::new(inner_encoder_builder),
                })
            }
        }
        impl BuildEncoder for $struct_name {
            fn try_new<'a, 'b: 'a>(&'b self, arr: &'a dyn Array) -> Result<Encoder<'a>, Error> {
                let field = self.field.name().clone();
                let arr = downcast_checked(arr, &field)?;
                let inner_encoder_builder = self.inner_encoder_builder.clone();
                Ok($enum_name($encoder_name {
                    arr,
                    field,
                    inner_encoder_builder,
                }))
            }
            fn schema(&self) -> Column {
                Column {
                    data_type: PostgresType::List(Box::new(
                        self.inner_encoder_builder.schema().clone(),
                    )),
                    nullable: self.field.is_nullable(),
                }
            }
            fn field(&self) -> Arc<Field> {
                self.field.clone()
            }
        }

        impl $struct_name {
            pub fn inner_encoder_builder(&self) -> EncoderBuilder {
                (*self.inner_encoder_builder).clone()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListEncoderBuilder {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
}

impl_list_encoder_builder!(ListEncoderBuilder, Encoder::List, ListEncoder);

#[derive(Debug, Clone, PartialEq)]
pub struct LargeListEncoderBuilder {
    field: Arc<Field>,
    inner_encoder_builder: Arc<EncoderBuilder>,
}
impl_list_encoder_builder!(
    LargeListEncoderBuilder,
    Encoder::LargeList,
    LargeListEncoder
);

#[enum_dispatch(BuildEncoder)]
#[derive(Debug, Clone, PartialEq)]
pub enum EncoderBuilder {
    Boolean(BooleanEncoderBuilder),
    UInt8(UInt8EncoderBuilder),
    UInt16(UInt16EncoderBuilder),
    UInt32(UInt32EncoderBuilder),
    UInt64(UInt64EncoderBuilder),
    Int8(Int8EncoderBuilder),
    Int16(Int16EncoderBuilder),
    Int32(Int32EncoderBuilder),
    Int64(Int64EncoderBuilder),
    Float16(Float16EncoderBuilder),
    Float32(Float32EncoderBuilder),
    Float64(Float64EncoderBuilder),
    Decimal128(Decimal128EncoderBuilder),
    TimestampMicrosecond(TimestampMicrosecondEncoderBuilder),
    TimestampMillisecond(TimestampMillisecondEncoderBuilder),
    TimestampSecond(TimestampSecondEncoderBuilder),
    Date32(Date32EncoderBuilder),
    Time32Millisecond(Time32MillisecondEncoderBuilder),
    Time32Second(Time32SecondEncoderBuilder),
    Time64Microsecond(Time64MicrosecondEncoderBuilder),
    DurationMicrosecond(DurationMicrosecondEncoderBuilder),
    DurationMillisecond(DurationMillisecondEncoderBuilder),
    DurationSecond(DurationSecondEncoderBuilder),
    String(StringEncoderBuilder),
    LargeString(LargeStringEncoderBuilder),
    Binary(BinaryEncoderBuilder),
    LargeBinary(LargeBinaryEncoderBuilder),
    FixedSizeBinary(FixedSizeBinaryEncoderBuilder),
    List(ListEncoderBuilder),
    LargeList(LargeListEncoderBuilder),
}

impl EncoderBuilder {
    pub fn try_new(field: Arc<Field>) -> Result<Self, Error> {
        let data_type = field.data_type();
        let res = match data_type {
            DataType::Boolean => Self::Boolean(BooleanEncoderBuilder { field }),
            DataType::UInt8 => Self::UInt8(UInt8EncoderBuilder { field }),
            DataType::UInt16 => Self::UInt16(UInt16EncoderBuilder { field }),
            DataType::UInt32 => Self::UInt32(UInt32EncoderBuilder { field }),
            DataType::UInt64 => Self::UInt64(UInt64EncoderBuilder { field }),
            // Note that rust-postgres encodes int8 to CHAR by default
            DataType::Int8 => Self::Int8(Int8EncoderBuilder {
                field,
                output: PostgresType::Int2,
            }),
            DataType::Int16 => Self::Int16(Int16EncoderBuilder { field }),
            DataType::Int32 => Self::Int32(Int32EncoderBuilder { field }),
            DataType::Int64 => Self::Int64(Int64EncoderBuilder { field }),
            DataType::Float16 => Self::Float16(Float16EncoderBuilder { field }),
            DataType::Float32 => Self::Float32(Float32EncoderBuilder { field }),
            DataType::Float64 => Self::Float64(Float64EncoderBuilder { field }),
            DataType::Decimal128(_, _) => Self::Decimal128(Decimal128EncoderBuilder { field }),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(Error::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ));
                }
                TimeUnit::Microsecond => {
                    Self::TimestampMicrosecond(TimestampMicrosecondEncoderBuilder { field })
                }
                TimeUnit::Millisecond => {
                    Self::TimestampMillisecond(TimestampMillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::TimestampSecond(TimestampSecondEncoderBuilder { field }),
            },
            DataType::Date32 => Self::Date32(Date32EncoderBuilder { field }),
            DataType::Time32(unit) => match unit {
                TimeUnit::Millisecond => {
                    Self::Time32Millisecond(Time32MillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::Time32Second(Time32SecondEncoderBuilder { field }),
                _ => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(Error::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ));
                }
                TimeUnit::Microsecond => {
                    Self::Time64Microsecond(Time64MicrosecondEncoderBuilder { field })
                }
                _ => unreachable!(),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Nanosecond => {
                    return Err(Error::type_unsupported(
                        field.name(),
                        data_type,
                        "Postgres does not support ns precision; convert to us",
                    ));
                }
                TimeUnit::Microsecond => {
                    Self::DurationMicrosecond(DurationMicrosecondEncoderBuilder { field })
                }
                TimeUnit::Millisecond => {
                    Self::DurationMillisecond(DurationMillisecondEncoderBuilder { field })
                }
                TimeUnit::Second => Self::DurationSecond(DurationSecondEncoderBuilder { field }),
            },
            DataType::Utf8 => Self::String(StringEncoderBuilder {
                field,
                output: StringOutputType::Text,
            }),
            DataType::LargeUtf8 => Self::LargeString(LargeStringEncoderBuilder {
                field,
                output: StringOutputType::Text,
            }),
            DataType::Binary => Self::Binary(BinaryEncoderBuilder { field }),
            DataType::LargeBinary => Self::LargeBinary(LargeBinaryEncoderBuilder { field }),
            DataType::FixedSizeBinary(_) => {
                Self::FixedSizeBinary(FixedSizeBinaryEncoderBuilder { field })
            }
            DataType::List(inner) => {
                if matches!(
                    inner.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) {
                    return Err(Error::type_unsupported(
                        field.name(),
                        data_type,
                        "nested lists are not supported",
                    ));
                }
                let inner = Self::try_new(inner.clone())?;
                Self::List(ListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            DataType::LargeList(inner) | DataType::FixedSizeList(inner, _) => {
                if matches!(
                    inner.data_type(),
                    DataType::List(_) | DataType::LargeList(_)
                ) {
                    return Err(Error::type_unsupported(
                        field.name(),
                        data_type,
                        "nested lists are not supported",
                    ));
                }
                let inner = Self::try_new(inner.clone())?;
                Self::LargeList(LargeListEncoderBuilder {
                    field,
                    inner_encoder_builder: Arc::new(inner),
                })
            }
            _ => {
                return Err(Error::type_unsupported(
                    field.name(),
                    data_type,
                    "unknown type",
                ));
            }
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Decimal128Array, FixedSizeBinaryArray, UInt64Array};
    use arrow_schema::{DataType, Field};
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_uint64_encoder() {
        let array = UInt64Array::from(vec![0, 100, u64::MAX / 2, i64::MAX as u64]);
        let field = Arc::new(Field::new("test", DataType::UInt64, false));
        let builder = UInt64EncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        encoder.encode(0, &mut buf).unwrap();
        encoder.encode(1, &mut buf).unwrap();
        encoder.encode(2, &mut buf).unwrap();
        encoder.encode(3, &mut buf).unwrap();

        assert!(!buf.is_empty());
    }

    #[test]
    fn test_uint64_encoder_large_value() {
        // Test that UInt64 can now handle the full u64 range using NUMERIC format
        let array = UInt64Array::from(vec![u64::MAX]);
        let field = Arc::new(Field::new("test", DataType::UInt64, false));
        let builder = UInt64EncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        // Should succeed now that we're using NUMERIC format
        encoder.encode(0, &mut buf).unwrap();

        // Verify non-zero buffer (means data was written)
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_decimal128_encoder_zero() {
        let array = Decimal128Array::from(vec![0])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let field = Arc::new(Field::new("test", DataType::Decimal128(38, 0), false));
        let builder = Decimal128EncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        encoder.encode(0, &mut buf).unwrap();

        // Zero: length(4) + ndigits(2) + weight(2) + sign(2) + dscale(2) = 12 bytes
        assert_eq!(buf.len(), 4 + 8);
    }

    #[test]
    fn test_decimal128_encoder_positive() {
        let array = Decimal128Array::from(vec![12345678])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let field = Arc::new(Field::new("test", DataType::Decimal128(38, 0), false));
        let builder = Decimal128EncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        encoder.encode(0, &mut buf).unwrap();

        assert!(buf.len() >= 4 + 8 + 2);
    }

    #[test]
    fn test_fixed_size_binary_encoder() {
        let values = vec![vec![0u8; 32], vec![255u8; 32], vec![42u8; 32]];
        let array = FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let field = Arc::new(Field::new("test", DataType::FixedSizeBinary(32), false));
        let builder = FixedSizeBinaryEncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        encoder.encode(0, &mut buf).unwrap();
        encoder.encode(1, &mut buf).unwrap();
        encoder.encode(2, &mut buf).unwrap();

        assert_eq!(buf.len(), 108);
    }

    #[test]
    fn test_nullable_fields() {
        let array = UInt64Array::from(vec![Some(100), None, Some(200)]);
        let field = Arc::new(Field::new("test", DataType::UInt64, true));
        let builder = UInt64EncoderBuilder { field };

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        encoder.encode(0, &mut buf).unwrap();
        encoder.encode(1, &mut buf).unwrap();
        encoder.encode(2, &mut buf).unwrap();

        // UInt64 now encodes as NUMERIC:
        // Row 0 (Some(100)): 4-byte length + 8-byte header + 2-byte digit = 14 bytes
        // Row 1 (None): 4-byte length (-1) = 4 bytes
        // Row 2 (Some(200)): 4-byte length + 8-byte header + 2-byte digit = 14 bytes
        // Total: 32 bytes
        assert_eq!(buf.len(), 32);
    }

    #[test]
    fn test_jsonb_validation_valid_json() {
        use arrow_array::StringArray;

        // Test various valid JSON values
        let array = StringArray::from(vec![
            r#"{"key": "value"}"#,
            r#"[1, 2, 3]"#,
            r#"null"#,
            r#"true"#,
            r#"123"#,
            r#""string""#,
        ]);
        let field = Arc::new(Field::new("json_field", DataType::Utf8, false));
        let builder = StringEncoderBuilder::new_with_output(field, PostgresType::Jsonb).unwrap();

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        // All valid JSON should encode successfully
        for row in 0..array.len() {
            encoder.encode(row, &mut buf).unwrap();
        }

        assert!(!buf.is_empty());
    }

    #[test]
    fn test_jsonb_validation_invalid_json() {
        use arrow_array::StringArray;

        // Test invalid JSON
        let array = StringArray::from(vec![r#"{"unclosed": true"#]);
        let field = Arc::new(Field::new("json_field", DataType::Utf8, false));
        let builder = StringEncoderBuilder::new_with_output(field, PostgresType::Jsonb).unwrap();

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        // Should fail with InvalidJson error
        let result = encoder.encode(0, &mut buf);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::InvalidJson { field, reason } => {
                assert_eq!(field, "json_field");
                assert!(reason.contains("EOF") || reason.contains("expected"));
            }
            _ => panic!("Expected InvalidJson error"),
        }
    }

    #[test]
    fn test_text_encoding_no_validation() {
        use arrow_array::StringArray;

        // Test that TEXT encoding doesn't validate JSON (allows invalid JSON)
        let array = StringArray::from(vec![r#"{"unclosed": true"#]);
        let field = Arc::new(Field::new("text_field", DataType::Utf8, false));
        let builder = StringEncoderBuilder::new(field).unwrap(); // Defaults to TEXT

        let encoder = builder.try_new(&array).unwrap();
        let mut buf = BytesMut::new();

        // Should succeed - TEXT doesn't validate
        encoder.encode(0, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
