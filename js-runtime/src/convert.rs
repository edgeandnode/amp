use crate::BoxError;

pub trait FromV8: Sized {
    /// Converts a V8 value to a Rust value.
    fn from_v8<'s>(
        scope: &mut v8::HandleScope<'s>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, BoxError>;
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
