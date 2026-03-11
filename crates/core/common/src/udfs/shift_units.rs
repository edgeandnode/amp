use std::{any::Any, str::FromStr, sync::Arc};

use bigdecimal::BigDecimal;
use datafusion::{
    arrow::{
        array::{
            Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::FieldRef,
    },
    common::plan_err,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
        TypeSignature, Volatility,
    },
    scalar::ScalarValue,
};

use crate::arrow::{
    array::{Array, StringBuilder},
    datatypes::{DataType, Field},
};

const MAX_UNITS: i64 = 32;

/// DataFusion UDF that shifts the decimal point of a numeric value.
///
/// This function shifts the decimal point by the specified number of places:
/// - **Positive units**: Shifts right (multiplies by 10^units) - converts human-readable to raw
/// - **Negative units**: Shifts left (divides by 10^units) - converts raw to human-readable
///
/// # SQL Usage
///
/// ```ignore
/// // Convert 1.5 ETH to wei (shift right by 18 places)
/// shift_units('1.5', 18)  // Returns "1500000000000000000"
///
/// // Convert wei back to ETH (shift left by 18 places)
/// shift_units('1500000000000000000', -18)  // Returns "1.5"
///
/// // Convert 100 USDC to raw units (6 decimals)
/// shift_units('100', 6)   // Returns "100000000"
/// ```
///
/// # Arguments
///
/// * `value` - Decimal/Integer string (e.g., "1.5", "100", "0.001")
/// * `units` - Number of decimal places to shift (positive = right, negative = left)
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
pub struct ShiftUnits {
    signature: Signature,
}

impl Default for ShiftUnits {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ShiftUnits {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "shift_units"
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
        if !matches!(
            arg_fields[0].data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Utf8,
        ) {
            return plan_err!(
                "{}: expected numeric type or Utf8 for first argument, got {}",
                self.name(),
                arg_fields[0].data_type()
            );
        }

        // Validate second arg is integer type
        if !matches!(
            arg_fields[1].data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
        ) {
            return plan_err!(
                "{}: expected numeric type for second argument, got {}",
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
            return plan_err!(
                "{}: expected 2 arguments, but got {}",
                self.name(),
                args.len()
            );
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(value), ColumnarValue::Scalar(units)) => {
                let value = self.scalar_to_bigdecimal(value)?;
                let units = self.scalar_to_i64(units)?;
                let result = self.parse_value(value, units)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            (ColumnarValue::Array(value_arr), ColumnarValue::Scalar(units)) => {
                let units = self.scalar_to_i64(units)?;
                let values = self.array_to_bigdecimal(value_arr)?;
                let mut builder = StringBuilder::new();
                for value in values {
                    match value {
                        Some(value) => {
                            let result = self.parse_value(value, units)?;
                            builder.append_value(&result);
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (ColumnarValue::Scalar(value), ColumnarValue::Array(units_arr)) => {
                let value = self.scalar_to_bigdecimal(value)?;
                let mut builder = StringBuilder::new();
                let units_arr = self.array_to_i64(units_arr)?;
                for units in units_arr {
                    let result = self.parse_value(value.clone(), units.unwrap_or(1))?;
                    builder.append_value(&result);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (ColumnarValue::Array(value_arr), ColumnarValue::Array(units_arr)) => {
                let values = self.array_to_bigdecimal(value_arr)?;
                let units_arr = self.array_to_i64(units_arr)?;
                let mut builder = StringBuilder::new();
                for (value, units) in values.into_iter().zip(units_arr.into_iter()) {
                    match value {
                        Some(v) => {
                            let units = units.unwrap_or(1);
                            let result = self.parse_value(v, units)?;
                            builder.append_value(&result);
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

/// Macro to extract values from an array and convert to BigDecimal
macro_rules! extract_bigdecimal {
    ($self:expr, $array:expr, $len:expr, $result:expr, $array_type:ty, $convert:expr) => {{
        let arr = match $array.as_any().downcast_ref::<$array_type>() {
            Some(arr) => arr,
            None => return plan_err!("{}: expected {}", $self.name(), stringify!($array_type)),
        };
        for i in 0..$len {
            if arr.is_null(i) {
                $result.push(None);
            } else {
                let bd = $convert(arr.value(i));
                $result.push(Some(bd));
            }
        }
    }};
}

/// Macro to extract numeric values from an array and convert to i64
macro_rules! extract_i64 {
    ($self:expr, $array:expr, $len:expr, $result:expr, $array_type:ty) => {{
        let arr = match $array.as_any().downcast_ref::<$array_type>() {
            Some(arr) => arr,
            None => return plan_err!("{}: expected {}", $self.name(), stringify!($array_type)),
        };
        for i in 0..$len {
            if arr.is_null(i) {
                $result.push(None);
            } else {
                $result.push(Some(arr.value(i) as i64));
            }
        }
    }};
}

impl ShiftUnits {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }

    fn parse_value(&self, value: BigDecimal, units: i64) -> datafusion::error::Result<String> {
        if units == 0 {
            return Ok(value.normalized().to_plain_string());
        }
        // Sanity check to prevent malicious input.
        if units.abs() > MAX_UNITS {
            return plan_err!(
                "{}: units must be between -{} and {} (got {})",
                self.name(),
                MAX_UNITS,
                MAX_UNITS,
                units
            );
        }
        if units > 0 {
            let multiplier = BigDecimal::from(10).powi(units);
            Ok((value * multiplier).normalized().to_plain_string())
        } else {
            let divisor = BigDecimal::from(10).powi(-units);
            Ok((value / divisor).normalized().to_plain_string())
        }
    }

    fn scalar_to_bigdecimal(&self, scalar: &ScalarValue) -> datafusion::error::Result<BigDecimal> {
        match scalar {
            // Signed integers
            ScalarValue::Int8(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::Int16(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::Int32(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::Int64(Some(v)) => Ok(BigDecimal::from(*v)),
            // Unsigned integers
            ScalarValue::UInt8(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::UInt16(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::UInt32(Some(v)) => Ok(BigDecimal::from(*v)),
            ScalarValue::UInt64(Some(v)) => Ok(BigDecimal::from(*v)),
            // Floats
            ScalarValue::Float32(Some(v)) => BigDecimal::from_str(&v.to_string())
                .or_else(|e| plan_err!("{}: failed to parse float32: {}", self.name(), e)),
            ScalarValue::Float64(Some(v)) => BigDecimal::from_str(&v.to_string())
                .or_else(|e| plan_err!("{}: failed to parse float64: {}", self.name(), e)),
            // Decimal types
            ScalarValue::Decimal128(Some(v), _, scale) => {
                let bd = BigDecimal::from(*v);
                if *scale > 0 {
                    Ok(bd / BigDecimal::from(10).powi(*scale as i64))
                } else {
                    Ok(bd)
                }
            }
            ScalarValue::Decimal256(Some(v), _, scale) => {
                let bd = BigDecimal::from_str(&v.to_string()).or_else(|e| {
                    plan_err!("{}: failed to convert Decimal256: {}", self.name(), e)
                })?;
                if *scale > 0 {
                    Ok(bd / BigDecimal::from(10).powi(*scale as i64))
                } else {
                    Ok(bd)
                }
            }
            // String
            ScalarValue::Utf8(Some(s)) => BigDecimal::from_str(s)
                .or_else(|e| plan_err!("{}: failed to parse value: {}", self.name(), e)),
            _ => plan_err!("{}: value must be a non-null numeric type", self.name()),
        }
    }

    fn scalar_to_i64(&self, scalar: &ScalarValue) -> datafusion::error::Result<i64> {
        match scalar {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
            ScalarValue::Float32(Some(v)) => Ok(*v as i64),
            ScalarValue::Float64(Some(v)) => Ok(*v as i64),
            ScalarValue::Utf8(Some(v)) => v
                .parse::<i64>()
                .or_else(|e| plan_err!("{}: failed to parse integer: {}", self.name(), e)),
            _ => plan_err!("{}: expected numeric or string scalar", self.name()),
        }
    }

    /// Extracts all i64 values from an array, returning a Vec<Option<i64>>
    fn array_to_i64(&self, array: &Arc<dyn Array>) -> datafusion::error::Result<Vec<Option<i64>>> {
        let len = array.len();
        let mut result = Vec::with_capacity(len);

        match array.data_type() {
            // Signed integers
            DataType::Int8 => extract_i64!(self, array, len, result, Int8Array),
            DataType::Int16 => extract_i64!(self, array, len, result, Int16Array),
            DataType::Int32 => extract_i64!(self, array, len, result, Int32Array),
            DataType::Int64 => extract_i64!(self, array, len, result, Int64Array),
            // Unsigned integers
            DataType::UInt8 => extract_i64!(self, array, len, result, UInt8Array),
            DataType::UInt16 => extract_i64!(self, array, len, result, UInt16Array),
            DataType::UInt32 => extract_i64!(self, array, len, result, UInt32Array),
            DataType::UInt64 => extract_i64!(self, array, len, result, UInt64Array),
            // Floats
            DataType::Float32 => extract_i64!(self, array, len, result, Float32Array),
            DataType::Float64 => extract_i64!(self, array, len, result, Float64Array),
            // String
            DataType::Utf8 => {
                let arr = match array.as_any().downcast_ref::<StringArray>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected StringArray", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let val = arr.value(i).parse::<i64>().or_else(|e| {
                            plan_err!("{}: failed to parse integer: {}", self.name(), e)
                        })?;
                        result.push(Some(val));
                    }
                }
            }
            dt => {
                return plan_err!(
                    "{}: unsupported data type for integer conversion: {}",
                    self.name(),
                    dt
                );
            }
        }

        Ok(result)
    }

    /// Extracts all BigDecimal values from an array, returning a Vec<Option<BigDecimal>>
    fn array_to_bigdecimal(
        &self,
        array: &Arc<dyn Array>,
    ) -> datafusion::error::Result<Vec<Option<BigDecimal>>> {
        let len = array.len();
        let mut result = Vec::with_capacity(len);

        match array.data_type() {
            // Signed integers
            DataType::Int8 => {
                extract_bigdecimal!(self, array, len, result, Int8Array, BigDecimal::from)
            }
            DataType::Int16 => {
                extract_bigdecimal!(self, array, len, result, Int16Array, BigDecimal::from)
            }
            DataType::Int32 => {
                extract_bigdecimal!(self, array, len, result, Int32Array, BigDecimal::from)
            }
            DataType::Int64 => {
                extract_bigdecimal!(self, array, len, result, Int64Array, BigDecimal::from)
            }
            // Unsigned integers
            DataType::UInt8 => {
                extract_bigdecimal!(self, array, len, result, UInt8Array, BigDecimal::from)
            }
            DataType::UInt16 => {
                extract_bigdecimal!(self, array, len, result, UInt16Array, BigDecimal::from)
            }
            DataType::UInt32 => {
                extract_bigdecimal!(self, array, len, result, UInt32Array, BigDecimal::from)
            }
            DataType::UInt64 => {
                extract_bigdecimal!(self, array, len, result, UInt64Array, BigDecimal::from)
            }
            // Floats: from_str is faster and preserves intended precision
            // from_f64 adds spurious digits from exact binary representation
            DataType::Float32 => {
                let arr = match array.as_any().downcast_ref::<Float32Array>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected Float32Array", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let bd = BigDecimal::from_str(&arr.value(i).to_string()).or_else(|e| {
                            plan_err!(
                                "{}: failed to convert String to BigDecimal: {}",
                                self.name(),
                                e
                            )
                        })?;
                        result.push(Some(bd));
                    }
                }
            }
            DataType::Float64 => {
                let arr = match array.as_any().downcast_ref::<Float64Array>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected Float64Array", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let bd = BigDecimal::from_str(&arr.value(i).to_string()).or_else(|e| {
                            plan_err!(
                                "{}: failed to convert String to BigDecimal: {}",
                                self.name(),
                                e
                            )
                        })?;
                        result.push(Some(bd));
                    }
                }
            }
            // Decimal types
            DataType::Decimal128(_, scale) => {
                let arr = match array.as_any().downcast_ref::<Decimal128Array>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected Decimal128Array", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let v = arr.value(i);
                        let bd = BigDecimal::from(v);
                        if *scale > 0 {
                            result.push(Some(bd / BigDecimal::from(10).powi(*scale as i64)));
                        } else {
                            result.push(Some(bd));
                        }
                    }
                }
            }
            DataType::Decimal256(_, scale) => {
                let arr = match array.as_any().downcast_ref::<Decimal256Array>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected Decimal256Array", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let v = arr.value(i);
                        let bd = BigDecimal::from_str(&v.to_string()).or_else(|e| {
                            plan_err!("{}: failed to convert Decimal256: {}", self.name(), e)
                        })?;
                        if *scale > 0 {
                            result.push(Some(bd / BigDecimal::from(10).powi(*scale as i64)));
                        } else {
                            result.push(Some(bd));
                        }
                    }
                }
            }
            // String
            DataType::Utf8 => {
                let arr = match array.as_any().downcast_ref::<StringArray>() {
                    Some(arr) => arr,
                    None => return plan_err!("{}: expected StringArray", self.name()),
                };
                for i in 0..len {
                    if arr.is_null(i) {
                        result.push(None);
                    } else {
                        let bd = BigDecimal::from_str(arr.value(i)).or_else(|e| {
                            plan_err!(
                                "{}: failed to convert String to BigDecimal: {}",
                                self.name(),
                                e
                            )
                        })?;
                        result.push(Some(bd));
                    }
                }
            }
            dt => return plan_err!("{}: unsupported data type for value: {}", self.name(), dt),
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use super::*;

    #[test]
    fn shift_units_with_integer_succeeds() {
        //* Given
        let value = ScalarValue::Utf8(Some("100".into()));
        let units = 6;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Utf8, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with valid integer string");
        assert_eq!(shifted, "100000000");
    }

    #[test]
    fn shift_units_with_many_decimal_places_succeeds() {
        //* Given
        let value = ScalarValue::Utf8(Some("1.376988483056381409".into()));
        let units = 18;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Utf8, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with many decimal places");
        assert_eq!(shifted, "1376988483056381409");
    }

    #[test]
    fn shift_units_with_zero_units_succeeds() {
        //* Given
        let value = ScalarValue::Utf8(Some("123.456".into()));
        let units = 0;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Utf8, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with zero units");
        assert_eq!(shifted, "123.456");
    }

    #[test]
    fn shift_units_no_arguments_fails() {
        //* Given
        let udf = ShiftUnits::new();
        let args: Vec<ColumnarValue> = vec![];
        let arg_fields: Vec<FieldRef> = vec![];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let scalar_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(scalar_args);

        //* Then
        assert!(result.is_err(), "shift_units should fail with no arguments");
        let error = result.expect_err("should return argument count error");
        assert!(
            error.message().contains("expected 2 arguments"),
            "Error message should mention 'expected 2 arguments', got: {}",
            error.message()
        );
    }

    #[test]
    fn shift_units_invalid_string_fails() {
        //* Given
        let invalid_value = ScalarValue::Utf8(Some("not_a_number".into()));
        let units = 18;

        //* When
        let result = invoke_shift_units_typed(invalid_value, DataType::Utf8, units);

        //* Then
        assert!(
            result.is_err(),
            "shift_units should fail with invalid string input"
        );
        let error = result.expect_err("should return parse error");
        assert!(
            error.message().contains("failed to parse"),
            "Error message should mention 'failed to parse', got: {}",
            error.message()
        );
    }

    #[test]
    fn shift_units_negative_units_succeeds() {
        //* Given
        let value = ScalarValue::Utf8(Some("1.5".into()));
        let units = -18;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Utf8, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with negative units");
        assert_eq!(shifted, "0.0000000000000000015");
    }

    #[test]
    fn shift_units_array_succeeds() {
        //* Given
        let values = vec![Some("1.5"), Some("100000"), Some("0.1")];
        let units = 18;

        //* When
        let result = invoke_shift_units_array(values, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with array input");
        assert_eq!(
            shifted,
            vec![
                Some("1500000000000000000".to_string()),
                Some("100000000000000000000000".to_string()),
                Some("100000000000000000".to_string()),
            ]
        );
    }

    #[test]
    fn shift_units_array_with_nulls_succeeds() {
        //* Given
        let values = vec![Some("1.5"), None, Some("0.1")];
        let units = 18;

        //* When
        let result = invoke_shift_units_array(values, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with array containing nulls");
        assert_eq!(
            shifted,
            vec![
                Some("1500000000000000000".to_string()),
                None,
                Some("100000000000000000".to_string()),
            ]
        );
    }

    #[test]
    fn shift_units_int64_succeeds() {
        //* Given
        let value = ScalarValue::Int64(Some(10));
        let units = 3;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Int64, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with Int64 input");
        assert_eq!(shifted, "10000");
    }

    #[test]
    fn shift_units_float32_succeeds() {
        //* Given
        let value = ScalarValue::Float32(Some(10.0));
        let units = 3;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Float32, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with Float32 input");
        assert_eq!(shifted, "10000");
    }

    #[test]
    fn shift_units_float64_succeeds() {
        //* Given
        let value = ScalarValue::Float64(Some(10.0));
        let units = 3;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Float64, units);

        //* Then
        let shifted = result.expect("shift_units should succeed with Float64 input");
        assert_eq!(shifted, "10000");
    }

    #[test]
    fn shift_units_decimal128_succeeds() {
        //* Given
        let value = ScalarValue::Decimal128(Some(10), 38, 0);
        let units = 3;

        //* When
        let result = invoke_shift_units_typed(value, DataType::Decimal128(38, 0), units);

        //* Then
        let shifted = result.expect("shift_units should succeed with Decimal128 input");
        assert_eq!(shifted, "10000");
    }

    #[test]
    fn shift_units_null_value_fails() {
        //* Given
        let udf = ShiftUnits::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(18))),
        ];
        let arg_fields = vec![
            Field::new("value", DataType::Null, true).into(),
            Field::new("units", DataType::Int64, false).into(),
        ];
        let return_field: FieldRef = Field::new(udf.name(), DataType::Utf8, true).into();
        let scalar_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(scalar_args);

        //* Then
        assert!(result.is_err(), "shift_units should fail with null value");
        let error = result.expect_err("should return null value error");
        assert!(
            error
                .message()
                .contains("value must be a non-null numeric type"),
            "Error message should mention 'value must be a non-null numeric type', got: {}",
            error.message()
        );
    }

    #[test]
    fn shift_units_null_units_fails() {
        //* Given
        let udf = ShiftUnits::new();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("1.5".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Null),
        ];
        let arg_fields = vec![
            Field::new("value", DataType::Utf8, false).into(),
            Field::new("units", DataType::Null, true).into(),
        ];
        let return_field: FieldRef = Field::new(udf.name(), DataType::Utf8, true).into();
        let scalar_args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(scalar_args);

        //* Then
        assert!(result.is_err(), "shift_units should fail with null units");
        let error = result.expect_err("should return null units error");
        assert!(
            error
                .message()
                .contains("expected numeric or string scalar"),
            "Error message should mention 'expected numeric or string scalar', got: {}",
            error.message()
        );
    }

    #[test]
    fn shift_units_roundtrip_succeeds() {
        //* Given
        // shift_units(shift_units(value, -18), 18) should return original value
        let original = "28998036497399455000";

        //* When
        let shifted_left = invoke_shift_units_typed(
            ScalarValue::Utf8(Some(original.into())),
            DataType::Utf8,
            -18,
        )
        .expect("shift_units should succeed shifting left");
        let roundtrip =
            invoke_shift_units_typed(ScalarValue::Utf8(Some(shifted_left)), DataType::Utf8, 18)
                .expect("shift_units should succeed shifting right");

        //* Then
        assert_eq!(
            roundtrip, original,
            "roundtrip should return original value"
        );
    }

    fn invoke_shift_units_typed(
        value: ScalarValue,
        value_type: DataType,
        units: i64,
    ) -> datafusion::error::Result<String> {
        let udf = ShiftUnits::new();
        let args = vec![
            ColumnarValue::Scalar(value),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(units))),
        ];
        let arg_fields = vec![
            Field::new("value", value_type, false).into(),
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

    fn invoke_shift_units_array(
        values: Vec<Option<&str>>,
        units: i64,
    ) -> datafusion::error::Result<Vec<Option<String>>> {
        let udf = ShiftUnits::new();

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
}
