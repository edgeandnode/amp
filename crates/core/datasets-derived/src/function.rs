//! User-defined function types for derived datasets.
//!
//! This module provides function representations used for derived datasets,
//! with Arrow type validation performed during deserialization so that a
//! successfully deserialized `Function` is always valid for the JS UDF runtime.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datasets_common::manifest::DataType;

/// User-defined function specification.
///
/// Defines a custom function with input/output types and implementation source.
/// Arrow type validation is performed during deserialization — a successfully
/// deserialized `Function` is guaranteed to have JS UDF-compatible types.
#[derive(Debug, Clone, serde::Serialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Function {
    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    /// Arrow data types for function input parameters
    pub input_types: Vec<DataType>,
    /// Arrow data type for function return value
    pub output_type: DataType,
    /// Function implementation source code and metadata
    pub source: FunctionSource,
}

impl<'de> serde::Deserialize<'de> for Function {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Internal deserialization struct to perform Arrow type validation
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Inner {
            input_types: Vec<DataType>,
            output_type: DataType,
            source: FunctionSource,
        }

        let Inner {
            input_types,
            output_type,
            source,
        } = serde::Deserialize::deserialize(deserializer)?;

        // Validate that all input types are supported
        for (index, input_type) in input_types.iter().enumerate() {
            validate_js_udf_input_type(input_type.as_arrow()).map_err(|err| {
                serde::de::Error::custom(format!(
                    "input parameter at index {index} uses an unsupported type: {err}"
                ))
            })?;
        }

        // Validate that the output type is supported
        validate_js_udf_output_type(output_type.as_arrow()).map_err(|err| {
            serde::de::Error::custom(format!("output uses an unsupported type: {err}"))
        })?;

        Ok(Self {
            input_types,
            output_type,
            source,
        })
    }
}

/// Source code and metadata for a user-defined function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FunctionSource {
    /// Function implementation source code
    pub source: Arc<str>,
    /// Filename where the function is defined
    pub filename: Arc<str>,
}

/// Errors from validating Arrow types for the JS UDF runtime.
#[derive(Debug, thiserror::Error)]
pub enum JsUdfTypeError {
    /// The Arrow data type is not supported by the JS UDF runtime.
    #[error("Arrow data type '{0:?}' is not supported by the JS UDF runtime")]
    UnsupportedType(ArrowDataType),

    /// Decimal type has a fractional scale, which the JS UDF runtime cannot represent.
    #[error(
        "Decimal type with scale {scale} is not supported; \
         only scale 0 (integer decimals) can be converted to BigInt"
    )]
    FractionalDecimal {
        /// The non-zero scale value
        scale: i8,
    },

    /// A field within a Struct has an unsupported type.
    #[error("field '{name}' has an unsupported type: {source}")]
    UnsupportedFieldType {
        /// Field name
        name: String,
        /// The underlying type error
        #[source]
        source: Box<JsUdfTypeError>,
    },

    /// The output type is not supported by the JS UDF runtime's FromV8 conversion.
    #[error("type '{0:?}' cannot be converted from JavaScript back to Arrow")]
    UnsupportedOutputType(ArrowDataType),

    /// A list element type is unsupported.
    #[error("list element has an unsupported type: {0}")]
    UnsupportedListElement(#[source] Box<JsUdfTypeError>),
}

/// Validates an Arrow data type for use as a JS UDF input parameter.
///
/// Accepts types that the js-runtime `ToV8` implementation can convert
/// from Arrow `ScalarValue` to V8 JavaScript values.
pub fn validate_js_udf_input_type(dt: &ArrowDataType) -> Result<(), JsUdfTypeError> {
    match dt {
        // Primitives
        ArrowDataType::Null
        | ArrowDataType::Boolean
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float32
        | ArrowDataType::Float64 => Ok(()),

        // String types
        ArrowDataType::Utf8 | ArrowDataType::Utf8View | ArrowDataType::LargeUtf8 => Ok(()),

        // Binary types
        ArrowDataType::Binary
        | ArrowDataType::BinaryView
        | ArrowDataType::LargeBinary
        | ArrowDataType::FixedSizeBinary(_) => Ok(()),

        // Decimal — only scale 0 (integer decimals that map to BigInt)
        ArrowDataType::Decimal128(_, 0) | ArrowDataType::Decimal256(_, 0) => Ok(()),
        ArrowDataType::Decimal128(_, scale) | ArrowDataType::Decimal256(_, scale) => {
            Err(JsUdfTypeError::FractionalDecimal { scale: *scale })
        }

        // Struct — validate each field recursively
        ArrowDataType::Struct(fields) => {
            for field in fields.iter() {
                validate_js_udf_input_type(field.data_type()).map_err(|source| {
                    JsUdfTypeError::UnsupportedFieldType {
                        name: field.name().clone(),
                        source: Box::new(source),
                    }
                })?;
            }
            Ok(())
        }

        // List types — validate element type recursively
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _) => validate_js_udf_input_type(field.data_type())
            .map_err(|e| JsUdfTypeError::UnsupportedListElement(Box::new(e))),

        other => Err(JsUdfTypeError::UnsupportedType(other.clone())),
    }
}

/// Validates an Arrow data type for use as a JS UDF output type.
///
/// Rejects types that the js-runtime `FromV8` implementation cannot convert
/// from V8 JavaScript values back to Arrow `ScalarValue`. List and binary
/// types are input-only (JS Array/TypedArray cannot be converted back).
pub fn validate_js_udf_output_type(dt: &ArrowDataType) -> Result<(), JsUdfTypeError> {
    match dt {
        // List types cannot be converted back from JS
        ArrowDataType::List(_)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::FixedSizeList(_, _) => {
            Err(JsUdfTypeError::UnsupportedOutputType(dt.clone()))
        }

        // Binary types cannot be converted back from JS
        ArrowDataType::Binary
        | ArrowDataType::BinaryView
        | ArrowDataType::LargeBinary
        | ArrowDataType::FixedSizeBinary(_) => {
            Err(JsUdfTypeError::UnsupportedOutputType(dt.clone()))
        }

        // Struct — validate each field recursively with output rules
        ArrowDataType::Struct(fields) => {
            for field in fields.iter() {
                validate_js_udf_output_type(field.data_type()).map_err(|source| {
                    JsUdfTypeError::UnsupportedFieldType {
                        name: field.name().clone(),
                        source: Box::new(source),
                    }
                })?;
            }
            Ok(())
        }

        // Everything else: delegate to input validator
        other => validate_js_udf_input_type(other),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Fields};

    use super::*;

    #[test]
    fn validate_js_udf_input_type_with_primitive_scalars_succeeds() {
        // Null
        assert!(
            validate_js_udf_input_type(&DataType::Null).is_ok(),
            "Null should be accepted"
        );

        // Boolean
        assert!(
            validate_js_udf_input_type(&DataType::Boolean).is_ok(),
            "Boolean should be accepted"
        );

        // Integer types
        assert!(
            validate_js_udf_input_type(&DataType::Int8).is_ok(),
            "Int8 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Int16).is_ok(),
            "Int16 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Int32).is_ok(),
            "Int32 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Int64).is_ok(),
            "Int64 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::UInt8).is_ok(),
            "UInt8 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::UInt16).is_ok(),
            "UInt16 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::UInt32).is_ok(),
            "UInt32 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::UInt64).is_ok(),
            "UInt64 should be accepted"
        );

        // Float types
        assert!(
            validate_js_udf_input_type(&DataType::Float32).is_ok(),
            "Float32 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Float64).is_ok(),
            "Float64 should be accepted"
        );

        // String types
        assert!(
            validate_js_udf_input_type(&DataType::Utf8).is_ok(),
            "Utf8 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Utf8View).is_ok(),
            "Utf8View should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::LargeUtf8).is_ok(),
            "LargeUtf8 should be accepted"
        );

        // Binary types
        assert!(
            validate_js_udf_input_type(&DataType::Binary).is_ok(),
            "Binary should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::BinaryView).is_ok(),
            "BinaryView should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::LargeBinary).is_ok(),
            "LargeBinary should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::FixedSizeBinary(32)).is_ok(),
            "FixedSizeBinary should be accepted"
        );

        // Decimal types (scale 0 only)
        assert!(
            validate_js_udf_input_type(&DataType::Decimal128(38, 0)).is_ok(),
            "Decimal128 with scale 0 should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&DataType::Decimal256(76, 0)).is_ok(),
            "Decimal256 with scale 0 should be accepted"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_fractional_decimal_fails() {
        //* Given
        let dt = DataType::Decimal128(38, 2);

        //* When
        let result = validate_js_udf_input_type(&dt);

        //* Then
        let err = result.expect_err("Decimal128 with scale 2 should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::FractionalDecimal { scale: 2 }),
            "expected FractionalDecimal, got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_valid_struct_succeeds() {
        //* Given
        let dt = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        //* When
        let result = validate_js_udf_input_type(&dt);

        //* Then
        assert!(
            result.is_ok(),
            "Struct with valid fields should be accepted"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_invalid_struct_field_fails() {
        //* Given
        let dt = DataType::Struct(Fields::from(vec![Field::new(
            "bad",
            DataType::Date32,
            false,
        )]));

        //* When
        let result = validate_js_udf_input_type(&dt);

        //* Then
        let err = result.expect_err("Struct with Date32 field should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::UnsupportedFieldType { ref name, .. } if name == "bad"),
            "expected UnsupportedFieldType for 'bad', got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_list_types_succeeds() {
        //* Given
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let large = DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true)));
        let fixed =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 3);

        //* Then
        assert!(
            validate_js_udf_input_type(&list).is_ok(),
            "List should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&large).is_ok(),
            "LargeList should be accepted"
        );
        assert!(
            validate_js_udf_input_type(&fixed).is_ok(),
            "FixedSizeList should be accepted"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_unsupported_list_element_fails() {
        //* Given
        let dt = DataType::List(Arc::new(Field::new("item", DataType::Date32, true)));

        //* When
        let result = validate_js_udf_input_type(&dt);

        //* Then
        let err = result.expect_err("List with Date32 element should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::UnsupportedListElement(_)),
            "expected UnsupportedListElement, got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_input_type_with_map_fails() {
        //* Given
        let dt = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );

        //* When
        let result = validate_js_udf_input_type(&dt);

        //* Then
        let err = result.expect_err("Map should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::UnsupportedType(_)),
            "expected UnsupportedType, got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_output_type_with_primitive_scalars_succeeds() {
        // Null
        assert!(
            validate_js_udf_output_type(&DataType::Null).is_ok(),
            "Null should be accepted"
        );

        // Boolean
        assert!(
            validate_js_udf_output_type(&DataType::Boolean).is_ok(),
            "Boolean should be accepted"
        );

        // Integer types
        assert!(
            validate_js_udf_output_type(&DataType::Int8).is_ok(),
            "Int8 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Int16).is_ok(),
            "Int16 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Int32).is_ok(),
            "Int32 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Int64).is_ok(),
            "Int64 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::UInt8).is_ok(),
            "UInt8 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::UInt16).is_ok(),
            "UInt16 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::UInt32).is_ok(),
            "UInt32 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::UInt64).is_ok(),
            "UInt64 should be accepted"
        );

        // Float types
        assert!(
            validate_js_udf_output_type(&DataType::Float32).is_ok(),
            "Float32 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Float64).is_ok(),
            "Float64 should be accepted"
        );

        // String types
        assert!(
            validate_js_udf_output_type(&DataType::Utf8).is_ok(),
            "Utf8 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Utf8View).is_ok(),
            "Utf8View should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::LargeUtf8).is_ok(),
            "LargeUtf8 should be accepted"
        );

        // Decimal types (scale 0 only)
        assert!(
            validate_js_udf_output_type(&DataType::Decimal128(38, 0)).is_ok(),
            "Decimal128 with scale 0 should be accepted"
        );
        assert!(
            validate_js_udf_output_type(&DataType::Decimal256(76, 0)).is_ok(),
            "Decimal256 with scale 0 should be accepted"
        );
    }

    #[test]
    fn validate_js_udf_output_type_with_fractional_decimal_fails() {
        //* Given
        let dt = DataType::Decimal128(38, 2);

        //* When
        let result = validate_js_udf_output_type(&dt);

        //* Then
        let err = result.expect_err("Decimal128 with scale 2 should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::FractionalDecimal { scale: 2 }),
            "expected FractionalDecimal, got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_output_type_with_valid_struct_succeeds() {
        //* Given
        let dt = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]));

        //* When
        let result = validate_js_udf_output_type(&dt);

        //* Then
        assert!(
            result.is_ok(),
            "Struct with valid fields should be accepted as output"
        );
    }

    #[test]
    fn validate_js_udf_output_type_with_invalid_struct_field_fails() {
        //* Given
        let dt = DataType::Struct(Fields::from(vec![Field::new(
            "bad",
            DataType::Date32,
            false,
        )]));

        //* When
        let result = validate_js_udf_output_type(&dt);

        //* Then
        let err = result.expect_err("Struct with Date32 field should be rejected as output");
        assert!(
            matches!(err, JsUdfTypeError::UnsupportedFieldType { ref name, .. } if name == "bad"),
            "expected UnsupportedFieldType for 'bad', got {err:?}"
        );
    }

    #[test]
    fn validate_js_udf_output_type_with_list_types_fails() {
        //* Given
        let cases = [
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
        ];

        //* Then
        for dt in &cases {
            let err = validate_js_udf_output_type(dt)
                .expect_err(&format!("{dt:?} should be rejected as output"));
            assert!(
                matches!(err, JsUdfTypeError::UnsupportedOutputType(_)),
                "{dt:?}: expected UnsupportedOutputType, got {err:?}"
            );
        }
    }

    #[test]
    fn validate_js_udf_output_type_with_binary_types_fails() {
        //* Given
        let cases = [
            DataType::Binary,
            DataType::BinaryView,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(32),
        ];

        //* Then
        for dt in &cases {
            let err = validate_js_udf_output_type(dt)
                .expect_err(&format!("{dt:?} should be rejected as output"));
            assert!(
                matches!(err, JsUdfTypeError::UnsupportedOutputType(_)),
                "{dt:?}: expected UnsupportedOutputType, got {err:?}"
            );
        }
    }

    #[test]
    fn validate_js_udf_output_type_with_map_fails() {
        //* Given
        let dt = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );

        //* When
        let result = validate_js_udf_output_type(&dt);

        //* Then
        let err = result.expect_err("Map should be rejected");
        assert!(
            matches!(err, JsUdfTypeError::UnsupportedType(_)),
            "expected UnsupportedType, got {err:?}"
        );
    }

    #[test]
    fn deserialize_with_valid_types_succeeds() {
        //* Given
        let json = make_json(&["Int32", "Utf8"], "Boolean");

        //* When
        let result = serde_json::from_str::<Function>(&json);

        //* Then
        let func = result.expect("should deserialize valid function");
        assert_eq!(func.input_types.len(), 2, "should have two input types");
    }

    #[test]
    fn deserialize_with_unsupported_input_type_fails() {
        //* Given
        let json = make_json(&["Date32"], "Int32");

        //* When
        let result = serde_json::from_str::<Function>(&json);

        //* Then
        let err = result.expect_err("should reject unsupported input type");
        let msg = err.to_string();
        assert!(
            msg.contains("input parameter at index 0"),
            "error should mention input index: {msg}"
        );
    }

    #[test]
    fn deserialize_with_unsupported_output_type_fails() {
        //* Given
        // Binary is valid as input but not as output
        let json = make_json(&["Int32"], "Binary");

        //* When
        let result = serde_json::from_str::<Function>(&json);

        //* Then
        let err = result.expect_err("should reject unsupported output type");
        let msg = err.to_string();
        assert!(
            msg.contains("output uses an unsupported type"),
            "error should mention output: {msg}"
        );
    }

    fn make_json(input_types: &[&str], output_type: &str) -> String {
        let inputs: Vec<String> = input_types.iter().map(|t| format!("\"{t}\"")).collect();
        format!(
            r#"{{
                "inputTypes": [{inputs}],
                "outputType": "{output_type}",
                "source": {{ "source": "function f() {{}}", "filename": "test.js" }}
            }}"#,
            inputs = inputs.join(", ")
        )
    }
}
