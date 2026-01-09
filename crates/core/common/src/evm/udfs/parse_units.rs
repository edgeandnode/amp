use std::{any::Any, str::FromStr, sync::Arc};

use bigdecimal::BigDecimal;
use datafusion::{
    arrow::datatypes::FieldRef,
    common::plan_err,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    scalar::ScalarValue,
};

use crate::arrow::{
    array::{Array, StringArray, StringBuilder},
    datatypes::{DataType, Field},
};

/// DataFusion UDF that converts a decimal/integer string to its smallest unit representation.
///
/// This function multiplies the value by 10^units, useful for converting
/// human-readable token amounts (like "1.5 ETH") to their raw values (wei).
///
/// # SQL Usage
///
/// ```ignore
/// // Convert 1.5 ETH to wei (18 decimals)
/// parse_units('1.5', 18)  // Returns "1500000000000000000"
///
/// // Convert 100 USDC to raw units (6 decimals)
/// parse_units('100', 6)   // Returns "100000000"
/// ```
///
/// # Arguments
///
/// * `value` - Decimal/Integer string (e.g., "1.5", "100", "0.001")
/// * `units` - Number of decimal places to shift (e.g., 18 for ETH, 6 for USDC)
///
/// # Returns
///
/// A string representation of the result without trailing zeros.
///
/// # Errors
///
/// Returns a planning error if:
/// - Value is not a valid decimal/integer string
/// - Units is null
/// - Input is null
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ParseUnits {
    signature: Signature,
}

impl Default for ParseUnits {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseUnits {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ParseUnits {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parse_units"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("DataFusion will never call this")
    }

    /// Validates input types at query planning time
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let arg_fields = &args.arg_fields;
        if arg_fields.len() != 2 {
            return plan_err!(
                "{}: expected 2 arguments, but got {}",
                self.name(),
                arg_fields.len()
            );
        }

        // Validate first arg is Utf8
        if !matches!(arg_fields[0].data_type(), DataType::Utf8) {
            return plan_err!(
                "{}: expected Utf8 for first argument, got {}",
                self.name(),
                arg_fields[0].data_type()
            );
        }

        // Validate second arg is integer type
        if !matches!(
            arg_fields[1].data_type(),
            DataType::Int64 | DataType::UInt64
        ) {
            return plan_err!(
                "{}: expected Int64 or UInt64 for second argument, got {}",
                self.name(),
                arg_fields[1].data_type()
            );
        }

        Ok(Field::new(self.name(), DataType::Utf8, true).into())
    }

    /// Executes the UDF at runtime, multiplying value by 10^units
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return plan_err!("expected 2 arguments, but got {}", args.len());
        }

        // Extract units value (must be scalar)
        let units = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(u))) => *u,
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(u))) => *u as i64,
            _ => return plan_err!("units must be a non-null integer scalar"),
        };

        // Handle scalar vs array for value
        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let value_str = match scalar {
                    ScalarValue::Utf8(Some(s)) => s,
                    _ => return plan_err!("value must be a non-null string"),
                };
                let value = BigDecimal::from_str(value_str).map_err(|e| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "failed to parse value: {}",
                        e
                    ))
                })?;
                let result = (value * BigDecimal::from(10).powi(units))
                    .normalized()
                    .to_plain_string();
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            ColumnarValue::Array(array) => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            datafusion::error::DataFusionError::Plan(
                                "expected StringArray".to_string(),
                            )
                        })?;

                let mut builder = StringBuilder::new();
                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let value_str = string_array.value(i);
                        let value = BigDecimal::from_str(value_str).map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "failed to parse value: {}",
                                e
                            ))
                        })?;
                        let result = (value * BigDecimal::from(10).powi(units))
                            .normalized()
                            .to_plain_string();
                        builder.append_value(&result);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invoke_parse_units(value: &str, units: i64) -> datafusion::error::Result<String> {
        let udf = ParseUnits::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(units))),
        ];
        let arg_fields = vec![
            Field::new("value", DataType::Utf8, false).into(),
            Field::new("units", DataType::Int64, false).into(),
        ];
        let return_field: FieldRef = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) = result else {
            panic!("expected Utf8, got {:?}", result);
        };
        Ok(s)
    }

    fn invoke_parse_units_array(
        values: Vec<Option<&str>>,
        units: i64,
    ) -> datafusion::error::Result<Vec<Option<String>>> {
        let udf = ParseUnits::new();

        // Build StringArray from values
        let mut builder = StringBuilder::new();
        for v in &values {
            match v {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        let string_array = Arc::new(builder.finish());

        let args = vec![
            ColumnarValue::Array(string_array),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(units))),
        ];
        let arg_fields = vec![
            Field::new("value", DataType::Utf8, true).into(),
            Field::new("units", DataType::Int64, false).into(),
        ];
        let return_field: FieldRef = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: values.len(),
            return_field,
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args)?;
        let ColumnarValue::Array(arr) = result else {
            panic!("expected Array, got {:?}", result);
        };
        let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        let mut results = Vec::new();
        for i in 0..string_arr.len() {
            if string_arr.is_null(i) {
                results.push(None);
            } else {
                results.push(Some(string_arr.value(i).to_string()));
            }
        }
        Ok(results)
    }

    #[test]
    fn parse_units_with_decimal_succeeds() {
        let result = invoke_parse_units("1.5", 18).unwrap();
        assert_eq!(result, "1500000000000000000");
    }

    #[test]
    fn parse_units_with_integer_succeeds() {
        let result = invoke_parse_units("100", 6).unwrap();
        assert_eq!(result, "100000000");
    }

    #[test]
    fn parse_units_with_many_decimal_places_succeeds() {
        let result = invoke_parse_units("1.376988483056381409", 18).unwrap();
        assert_eq!(result, "1376988483056381409");
    }

    #[test]
    fn parse_units_with_zero_units_succeeds() {
        let result = invoke_parse_units("123.456", 0).unwrap();
        assert_eq!(result, "123.456");
    }

    #[test]
    fn parse_units_with_small_decimal_succeeds() {
        let result = invoke_parse_units("0.1", 18).unwrap();
        assert_eq!(result, "100000000000000000");
    }

    #[test]
    fn parse_units_no_arguments_fails() {
        let udf = ParseUnits::new();
        let args: Vec<ColumnarValue> = vec![];
        let arg_fields: Vec<FieldRef> = vec![];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("expected 2 arguments")
        );
    }

    #[test]
    fn parse_units_invalid_string_fails() {
        let result = invoke_parse_units("not_a_number", 18);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("failed to parse"));
    }

    #[test]
    fn parse_units_negative_units_succeeds() {
        let result = invoke_parse_units("1.5", -18);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "0.0000000000000000015");
    }

    #[test]
    fn parse_units_array_succeeds() {
        let result =
            invoke_parse_units_array(vec![Some("1.5"), Some("100000"), Some("0.1")], 18).unwrap();
        assert_eq!(
            result,
            vec![
                Some("1500000000000000000".to_string()),
                Some("100000000000000000000000".to_string()),
                Some("100000000000000000".to_string()),
            ]
        );
    }

    #[test]
    fn parse_units_array_with_nulls_succeeds() {
        let result = invoke_parse_units_array(vec![Some("1.5"), None, Some("0.1")], 18).unwrap();
        assert_eq!(
            result,
            vec![
                Some("1500000000000000000".to_string()),
                None,
                Some("100000000000000000".to_string()),
            ]
        );
    }
}
