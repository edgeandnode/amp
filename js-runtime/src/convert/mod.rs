mod v8_value;

use std::collections::BTreeMap;

use datafusion::{
    arrow::{
        array::Array,
        datatypes::{i256, Field},
    },
    common::scalar::ScalarStructBuilder,
    scalar::ScalarValue,
};
use num_traits::cast::ToPrimitive;
use v8_value::V8Value;

use crate::{exception::catch, BoxError};

pub trait FromV8: Sized {
    /// Converts a V8 value to a Rust value.
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError>;
}

pub trait ToV8 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError>;
}

impl FromV8 for i32 {
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError> {
        value.int32_value(scope).ok_or_else(|| {
            BoxError::from(format!(
                "value {} is not an i32",
                value.to_rust_string_lossy(scope)
            ))
        })
    }
}

impl FromV8 for () {
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError> {
        if value.is_undefined() {
            Ok(())
        } else {
            Err(BoxError::from(format!(
                "value {} is not undefined",
                value.to_rust_string_lossy(scope)
            )))
        }
    }
}

impl<R: FromV8> FromV8 for Option<R> {
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError> {
        if value.is_null_or_undefined() {
            return Ok(None);
        } else {
            Ok(Some(R::from_v8(scope, value)?))
        }
    }
}

impl ToV8 for bool {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::Boolean::new(scope, *self).into())
    }
}

impl ToV8 for f64 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::Number::new(scope, *self).into())
    }
}

impl ToV8 for i32 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::Integer::new(scope, *self).into())
    }
}

impl ToV8 for u32 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::Integer::new_from_unsigned(scope, *self).into())
    }
}

impl ToV8 for u64 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::BigInt::new_from_u64(scope, *self).into())
    }
}

impl ToV8 for i64 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::BigInt::new_from_i64(scope, *self).into())
    }
}

impl ToV8 for &str {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        Ok(v8::String::new(scope, self)
            .ok_or_else(|| {
                BoxError::from(format!(
                    "string has length {} which is too long",
                    self.len()
                ))
            })?
            .into())
    }
}

impl ToV8 for &[u8] {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        // Unwrap: The maximum length of a Uint8Array is larger than RAM

        let buffer = if self.is_empty() {
            v8::ArrayBuffer::new(scope, 0)
        } else {
            let store: v8::UniqueRef<_> = v8::ArrayBuffer::new_backing_store(scope, self.len());
            // SAFETY: raw memory copy into the v8 ArrayBuffer allocated above
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.as_ptr(),
                    store.data().unwrap().as_ptr() as *mut u8,
                    self.len(),
                )
            }
            v8::ArrayBuffer::with_backing_store(scope, &store.make_shared())
        };
        let array =
            v8::Uint8Array::new(scope, buffer, 0, self.len()).expect("Failed to create UintArray8");
        Ok(array.into())
    }
}

impl ToV8 for i128 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        let sign_bit = *self < 0;
        let magnitude = if sign_bit {
            self.wrapping_neg() as u128 // two's complement negation
        } else {
            *self as u128
        };

        let lo = (magnitude & 0xFFFF_FFFF_FFFF_FFFF) as u64;
        let hi = (magnitude >> 64) as u64;

        // Drop leading zeros if hi == 0
        let words = if hi == 0 { vec![lo] } else { vec![lo, hi] };

        // Unwrap: No known reason for this to fail
        let bigint = v8::BigInt::new_from_words(scope, sign_bit, &words).unwrap();
        Ok(bigint.into())
    }
}

impl ToV8 for i256 {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        let sign_bit = self < &i256::ZERO;
        let magnitude = if sign_bit { self.wrapping_neg() } else { *self };

        let mut words = [0u64; 4];

        for i in 0..4 {
            words[i] = ((magnitude >> (i as u8 * 64)) & i256::from_i128(0xFFFF_FFFF_FFFF_FFFF))
                .to_u64()
                .unwrap();
        }

        // Compute minimal word length (1–4), dropping leading zero words from the high end
        let mut len = 4;
        while len > 1 && words[len - 1] == 0 {
            len -= 1;
        }

        let bigint = v8::BigInt::new_from_words(scope, sign_bit, &words[..len]).unwrap();
        Ok(bigint.into())
    }
}

impl<T: ToV8> ToV8 for Option<T> {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        match self {
            Some(value) => value.to_v8(scope),
            None => Ok(v8::null(scope).into()),
        }
    }
}

impl<T: ToV8> ToV8 for &[T] {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        let elems = self
            .iter()
            .map(|e| e.to_v8(scope).into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(v8::Array::new_with_elements(scope, &elems).into())
    }
}

impl ToV8 for ScalarValue {
    fn to_v8<'s>(
        &self,
        scope: &mut v8::HandleScope<'s>,
    ) -> Result<v8::Local<'s, v8::Value>, BoxError> {
        match self {
            ScalarValue::Null => Ok(v8::null(scope).into()),
            ScalarValue::Boolean(b) => b.to_v8(scope),
            ScalarValue::Float32(f) => f.map(|f| f as f64).to_v8(scope),
            ScalarValue::Float64(f) => f.to_v8(scope),
            ScalarValue::Int8(i) => i.map(|i| i as i32).to_v8(scope),
            ScalarValue::Int16(i) => i.map(|i| i as i32).to_v8(scope),
            ScalarValue::Int32(i) => i.to_v8(scope),
            ScalarValue::Int64(i) => i.to_v8(scope),
            ScalarValue::UInt8(i) => i.map(|i| i as u32).to_v8(scope),
            ScalarValue::UInt16(i) => i.map(|i| i as u32).to_v8(scope),
            ScalarValue::UInt32(i) => i.to_v8(scope),
            ScalarValue::UInt64(i) => i.to_v8(scope),
            ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => {
                s.as_ref().map(|s| s.as_str()).to_v8(scope)
            }
            ScalarValue::Binary(b)
            | ScalarValue::BinaryView(b)
            | ScalarValue::FixedSizeBinary(_, b)
            | ScalarValue::LargeBinary(b) => b.as_ref().map(|b| b.as_slice()).to_v8(scope),
            ScalarValue::Decimal128(i, _, 0) => i.to_v8(scope),
            ScalarValue::Decimal256(i, _, 0) => i.to_v8(scope),
            ScalarValue::Struct(struct_array) => {
                // ScalarValue Struct should always have a single element
                assert_eq!(struct_array.len(), 1);

                let obj = v8::Object::new(scope);
                for (column, field) in struct_array.columns().iter().zip(struct_array.fields()) {
                    let sv = ScalarValue::try_from_array(column, 0)?;

                    // Unwrap: A field name would not exceed the max length of a v8 string
                    let key = v8::String::new(scope, field.name()).unwrap();
                    let value = sv.to_v8(scope)?;
                    obj.set(scope, key.into(), value);
                }
                Ok(obj.into())
            }

            ScalarValue::FixedSizeList(l) => {
                // ScalarValue FixedSizeList should always have a single element
                assert_eq!(l.len(), 1);
                let array = l.value(0);
                single_array_to_v8(&array, scope)
            }

            ScalarValue::List(l) => {
                // ScalarValue List should always have a single element
                assert_eq!(l.len(), 1);
                let array = l.value(0);
                single_array_to_v8(&array, scope)
            }

            ScalarValue::LargeList(l) => {
                // ScalarValue LargeList should always have a single element
                assert_eq!(l.len(), 1);
                let array = l.value(0);
                single_array_to_v8(&array, scope)
            }

            // Fractional decimals
            ScalarValue::Decimal128(_, _, _) | ScalarValue::Decimal256(_, _, _) => {
                Err(BoxError::from(format!(
                    "fractional Decimal128 or Decimal256 not yet supported in functions"
                )))
            }

            // TODOs
            ScalarValue::Float16(_)
            | ScalarValue::Map(_)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Time32Second(_)
            | ScalarValue::Time32Millisecond(_)
            | ScalarValue::Time64Microsecond(_)
            | ScalarValue::Time64Nanosecond(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::DurationSecond(_)
            | ScalarValue::DurationMillisecond(_)
            | ScalarValue::DurationMicrosecond(_)
            | ScalarValue::DurationNanosecond(_)
            | ScalarValue::Union(_, _, _)
            | ScalarValue::Dictionary(_, _) => Err(BoxError::from(format!(
                "{} not yet supported in functions",
                self.data_type()
            ))),
        }
    }
}

fn single_array_to_v8<'s>(
    array: &dyn Array,
    scope: &mut v8::HandleScope<'s>,
) -> Result<v8::Local<'s, v8::Value>, BoxError> {
    let mut sv_v8 = vec![];
    for i in 0..array.len() {
        let sv = ScalarValue::try_from_array(array, i)?;
        sv_v8.push(sv.to_v8(scope)?);
    }
    Ok(v8::Array::new_with_elements(scope, &sv_v8).into())
}

impl FromV8 for ScalarValue {
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError> {
        let v8_value = V8Value::new(scope, value)
            .map_err(|e| format!("unsupported return value from JS: {}", e))?;
        scalar_value_from_v8(scope, v8_value)
    }
}

fn scalar_value_from_v8<'s>(
    scope: &mut v8::HandleScope<'s>,
    v8_value: V8Value<'s>,
) -> Result<ScalarValue, BoxError> {
    match v8_value {
        V8Value::Undefined | V8Value::Null => Ok(ScalarValue::Null),
        V8Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(b))),
        V8Value::Int32(i) => Ok(ScalarValue::Int32(Some(i))),
        V8Value::Uint32(i) => Ok(ScalarValue::UInt32(Some(i))),
        V8Value::Number(n) => Ok(ScalarValue::Float64(Some(n))),
        V8Value::String(s) => Ok(ScalarValue::Utf8(Some(s))),

        // BigInt is converted to `Decimal256` if it fits. On overflow it is null.
        V8Value::BigInt(i) => {
            let mut words = vec![0u64; i.word_count()];
            let (sign, words) = i.to_words_array(&mut words);

            if words.len() > 4 {
                return Ok(ScalarValue::Null);
            }

            let mut value = i256::ZERO;
            for (index, &word) in words.iter().enumerate() {
                value = value | (i256::from_i128(word as i128) << (index * 64) as u8);
            }

            if value.is_negative() {
                // the magnitude should be positive, a negative value here is an overflow
                return Ok(ScalarValue::Null);
            }

            if sign {
                value = -value;
            }

            Ok(ScalarValue::Decimal256(Some(value), 76, 0))
        }

        // Objects are converted to `Struct`.
        V8Value::Object(o) => {
            let v8_values = {
                let s = &mut v8::TryCatch::new(scope);

                let res = o
                    .get_own_property_names(s, Default::default())
                    .and_then(|props| {
                        let mut v8_values: BTreeMap<String, V8Value> = BTreeMap::new();
                        for index in 0..props.length() {
                            let key = props.get_index(s, index)?.to_string(s)?;
                            let value = o.get(s, key.into())?;

                            // Ignore properties of unsupported types
                            match V8Value::new(s, value) {
                                Ok(v8_val) => v8_values.insert(key.to_rust_string_lossy(s), v8_val),
                                Err(_unsupported) => continue,
                            };
                        }
                        Some(v8_values)
                    });

                res.ok_or_else(|| catch(s))?
            };

            let mut builder = ScalarStructBuilder::new();
            for (key, value) in v8_values {
                // Recurse
                let value = scalar_value_from_v8(scope, value)?;
                let field = Field::new(key, value.data_type(), true);
                builder = builder.with_scalar(field, value);
            }

            Ok(builder.build()?)
        }
        V8Value::Array(local) => todo!(),
        V8Value::TypedArray(local) => todo!(),
    }
}
