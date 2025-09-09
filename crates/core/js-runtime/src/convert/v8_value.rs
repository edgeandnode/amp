use v8::{Array, BigInt, Local, Object, TypedArray};

use crate::BoxError;

#[derive(Debug)]
pub enum V8Value<'s> {
    Undefined,
    Null,
    Boolean(bool),
    Int32(i32),
    Uint32(u32),
    Number(f64),
    String(String),
    BigInt(Local<'s, BigInt>),
    Object(Local<'s, Object>),

    #[allow(dead_code)]
    Array(Local<'s, Array>),

    #[allow(dead_code)]
    TypedArray(Local<'s, TypedArray>),
}

impl<'s> V8Value<'s> {
    // Errors if the value is not of a supported type.
    pub fn new(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError> {
        if value.is_undefined() {
            Ok(V8Value::Undefined)
        } else if value.is_null() {
            Ok(V8Value::Null)
        } else if value.is_boolean() {
            Ok(V8Value::Boolean(value.boolean_value(scope)))
        } else if value.is_int32() {
            Ok(V8Value::Int32(value.int32_value(scope).unwrap()))
        } else if value.is_uint32() {
            Ok(V8Value::Uint32(value.uint32_value(scope).unwrap()))
        } else if value.is_number() {
            Ok(V8Value::Number(value.number_value(scope).unwrap()))
        } else if value.is_big_int() {
            Ok(V8Value::BigInt(value.to_big_int(scope).unwrap()))
        } else if value.is_string() {
            Ok(V8Value::String(value.to_rust_string_lossy(scope)))
        } else if value.is_array() {
            Ok(V8Value::Array(
                v8::Local::<v8::Array>::try_from(value.to_object(scope).unwrap()).unwrap(),
            ))
        } else if value.is_typed_array() {
            Ok(V8Value::TypedArray(
                v8::Local::<v8::TypedArray>::try_from(value.to_object(scope).unwrap()).unwrap(),
            ))
        } else if value.is_object() {
            Ok(V8Value::Object(value.to_object(scope).unwrap()))
        } else {
            Err(BoxError::from(format!(
                "Unsupported JS type {}",
                value.type_repr()
            )))
        }
    }
}
