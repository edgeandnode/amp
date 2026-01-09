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

impl ShiftUnits {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }

    fn parse_value(value: BigDecimal, units: i64) -> datafusion::error::Result<String> {
        if units == 0 {
            return Ok(value.normalized().to_plain_string());
        }
        // Sanity check to prevent malicious input.
        if units.abs() > MAX_UNITS {
            return plan_err!(
                "units must be between -{} and {} (got {})",
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
            return plan_err!("expected 2 arguments, but got {}", args.len());
        }

        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(value), ColumnarValue::Scalar(units)) => {
                let value = scalar_to_bigdecimal(value)?;
                let units = scalar_to_i64(units)?;
                let result = Self::parse_value(value, units)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            (ColumnarValue::Array(value_arr), ColumnarValue::Scalar(units)) => {
                let units = scalar_to_i64(units)?;
                let values = array_to_bigdecimal(value_arr)?;
                let mut builder = StringBuilder::new();
                for value in values {
                    match value {
                        Some(value) => {
                            let result = Self::parse_value(value, units)?;
                            builder.append_value(&result);
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (ColumnarValue::Scalar(value), ColumnarValue::Array(units_arr)) => {
                let value = scalar_to_bigdecimal(value)?;
                let mut builder = StringBuilder::new();
                let units_arr = array_to_i64(units_arr)?;
                for units in units_arr {
                    let result = Self::parse_value(value.clone(), units.unwrap_or(1))?;
                    builder.append_value(&result);
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (ColumnarValue::Array(value_arr), ColumnarValue::Array(units_arr)) => {
                let values = array_to_bigdecimal(value_arr)?;
                let units_arr = array_to_i64(units_arr)?;
                let mut builder = StringBuilder::new();
                for (value, units) in values.into_iter().zip(units_arr.into_iter()) {
                    match value {
                        Some(v) => {
                            let units = units.unwrap_or(1);
                            let result = Self::parse_value(v, units)?;
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
    ($array:expr, $len:expr, $result:expr, $array_type:ty, $convert:expr) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    concat!("expected ", stringify!($array_type)).to_string(),
                )
            })?;
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
    ($array:expr, $len:expr, $result:expr, $array_type:ty) => {{
        let arr = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    concat!("expected ", stringify!($array_type)).to_string(),
                )
            })?;
        for i in 0..$len {
            if arr.is_null(i) {
                $result.push(None);
            } else {
                $result.push(Some(arr.value(i) as i64));
            }
        }
    }};
}

pub fn scalar_to_i64(scalar: &ScalarValue) -> datafusion::error::Result<i64> {
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
            .or_else(|e| plan_err!("failed to parse integer: {}", e)),
        _ => plan_err!("expected numeric or string scalar"),
    }
}

/// Extracts all i64 values from an array, returning a Vec<Option<i64>>
pub fn array_to_i64(array: &Arc<dyn Array>) -> datafusion::error::Result<Vec<Option<i64>>> {
    let len = array.len();
    let mut result = Vec::with_capacity(len);

    match array.data_type() {
        // Signed integers
        DataType::Int8 => extract_i64!(array, len, result, Int8Array),
        DataType::Int16 => extract_i64!(array, len, result, Int16Array),
        DataType::Int32 => extract_i64!(array, len, result, Int32Array),
        DataType::Int64 => extract_i64!(array, len, result, Int64Array),
        // Unsigned integers
        DataType::UInt8 => extract_i64!(array, len, result, UInt8Array),
        DataType::UInt16 => extract_i64!(array, len, result, UInt16Array),
        DataType::UInt32 => extract_i64!(array, len, result, UInt32Array),
        DataType::UInt64 => extract_i64!(array, len, result, UInt64Array),
        // Floats
        DataType::Float32 => extract_i64!(array, len, result, Float32Array),
        DataType::Float64 => extract_i64!(array, len, result, Float64Array),
        // String
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected StringArray".to_string())
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    result.push(None);
                } else {
                    let val = arr.value(i).parse::<i64>().map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "failed to parse integer: {}",
                            e
                        ))
                    })?;
                    result.push(Some(val));
                }
            }
        }
        dt => return plan_err!("unsupported data type for integer conversion: {}", dt),
    }

    Ok(result)
}

pub fn scalar_to_bigdecimal(scalar: &ScalarValue) -> datafusion::error::Result<BigDecimal> {
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
        ScalarValue::Float32(Some(v)) => BigDecimal::from_str(&v.to_string()).map_err(|_| {
            datafusion::error::DataFusionError::Plan("failed to parse float32".to_string())
        }),
        ScalarValue::Float64(Some(v)) => BigDecimal::from_str(&v.to_string()).map_err(|_| {
            datafusion::error::DataFusionError::Plan("failed to parse float64".to_string())
        }),
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
            let bd = BigDecimal::from_str(&v.to_string()).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "failed to convert Decimal256: {}",
                    e
                ))
            })?;
            if *scale > 0 {
                Ok(bd / BigDecimal::from(10).powi(*scale as i64))
            } else {
                Ok(bd)
            }
        }
        // String
        ScalarValue::Utf8(Some(s)) => BigDecimal::from_str(s).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!("failed to parse value: {}", e))
        }),
        _ => plan_err!("value must be a non-null numeric type"),
    }
}

/// Extracts all BigDecimal values from an array, returning a Vec<Option<BigDecimal>>
pub fn array_to_bigdecimal(
    array: &Arc<dyn Array>,
) -> datafusion::error::Result<Vec<Option<BigDecimal>>> {
    let len = array.len();
    let mut result = Vec::with_capacity(len);

    match array.data_type() {
        // Signed integers
        DataType::Int8 => {
            extract_bigdecimal!(array, len, result, Int8Array, BigDecimal::from)
        }
        DataType::Int16 => {
            extract_bigdecimal!(array, len, result, Int16Array, BigDecimal::from)
        }
        DataType::Int32 => {
            extract_bigdecimal!(array, len, result, Int32Array, BigDecimal::from)
        }
        DataType::Int64 => {
            extract_bigdecimal!(array, len, result, Int64Array, BigDecimal::from)
        }
        // Unsigned integers
        DataType::UInt8 => {
            extract_bigdecimal!(array, len, result, UInt8Array, BigDecimal::from)
        }
        DataType::UInt16 => {
            extract_bigdecimal!(array, len, result, UInt16Array, BigDecimal::from)
        }
        DataType::UInt32 => {
            extract_bigdecimal!(array, len, result, UInt32Array, BigDecimal::from)
        }
        DataType::UInt64 => {
            extract_bigdecimal!(array, len, result, UInt64Array, BigDecimal::from)
        }
        // Floats: from_str is faster and preserves intended precision
        // from_f64 adds spurious digits from exact binary representation
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected Float32Array".to_string())
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    result.push(None);
                } else {
                    let bd = BigDecimal::from_str(&arr.value(i).to_string()).map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "failed to convert String to BigDecimal: {}",
                            e
                        ))
                    })?;
                    result.push(Some(bd));
                }
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected Float64Array".to_string())
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    result.push(None);
                } else {
                    let bd = BigDecimal::from_str(&arr.value(i).to_string()).map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "failed to convert String to BigDecimal: {}",
                            e
                        ))
                    })?;
                    result.push(Some(bd));
                }
            }
        }
        // Decimal types
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected Decimal128Array".to_string())
                })?;
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
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected Decimal256Array".to_string())
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    result.push(None);
                } else {
                    let v = arr.value(i);
                    let bd = BigDecimal::from_str(&v.to_string()).map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "failed to convert Decimal256: {}",
                            e
                        ))
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
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan("expected StringArray".to_string())
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    result.push(None);
                } else {
                    let bd = BigDecimal::from_str(arr.value(i)).map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "failed to convert String to BigDecimal: {}",
                            e
                        ))
                    })?;
                    result.push(Some(bd));
                }
            }
        }
        dt => return plan_err!("unsupported data type for value: {}", dt),
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;

    use super::*;

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

    #[test]
    fn shift_units_with_integer_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Utf8(Some("100".into())), DataType::Utf8, 6)
                .unwrap();
        assert_eq!(result, "100000000");
    }

    #[test]
    fn shift_units_with_many_decimal_places_succeeds() {
        let result = invoke_shift_units_typed(
            ScalarValue::Utf8(Some("1.376988483056381409".into())),
            DataType::Utf8,
            18,
        )
        .unwrap();
        assert_eq!(result, "1376988483056381409");
    }

    #[test]
    fn shift_units_with_zero_units_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Utf8(Some("123.456".into())), DataType::Utf8, 0)
                .unwrap();
        assert_eq!(result, "123.456");
    }

    #[test]
    fn shift_units_no_arguments_fails() {
        let udf = ShiftUnits::new();
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
    fn shift_units_invalid_string_fails() {
        let result = invoke_shift_units_typed(
            ScalarValue::Utf8(Some("not_a_number".into())),
            DataType::Utf8,
            18,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("failed to parse"));
    }

    #[test]
    fn shift_units_negative_units_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Utf8(Some("1.5".into())), DataType::Utf8, -18);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "0.0000000000000000015");
    }

    #[test]
    fn shift_units_array_succeeds() {
        let result =
            invoke_shift_units_array(vec![Some("1.5"), Some("100000"), Some("0.1")], 18).unwrap();
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
    fn shift_units_array_with_nulls_succeeds() {
        let result = invoke_shift_units_array(vec![Some("1.5"), None, Some("0.1")], 18).unwrap();
        assert_eq!(
            result,
            vec![
                Some("1500000000000000000".to_string()),
                None,
                Some("100000000000000000".to_string()),
            ]
        );
    }

    #[test]
    fn shift_units_int64_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Int64(Some(10)), DataType::Int64, 3).unwrap();
        assert_eq!(result, "10000");
    }

    #[test]
    fn shift_units_float32_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Float32(Some(10.0)), DataType::Float32, 3)
                .unwrap();
        assert_eq!(result, "10000");
    }

    #[test]
    fn shift_units_float64_succeeds() {
        let result =
            invoke_shift_units_typed(ScalarValue::Float64(Some(10.0)), DataType::Float64, 3)
                .unwrap();
        assert_eq!(result, "10000");
    }

    #[test]
    fn shift_units_decimal128_succeeds() {
        let result = invoke_shift_units_typed(
            ScalarValue::Decimal128(Some(10), 38, 0),
            DataType::Decimal128(38, 0),
            3,
        )
        .unwrap();
        assert_eq!(result, "10000");
    }

    #[test]
    fn shift_units_null_value_fails() {
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
                .contains("value must be a non-null numeric type")
        );
    }

    #[test]
    fn shift_units_null_units_fails() {
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
                .contains("expected numeric or string scalar")
        );
    }

    #[test]
    fn shift_units_roundtrip_succeeds() {
        // shift_units(shift_units(value, -18), 18) should return original value
        let original = "28998036497399455000";
        let shifted_left = invoke_shift_units_typed(
            ScalarValue::Utf8(Some(original.into())),
            DataType::Utf8,
            -18,
        )
        .unwrap();
        let roundtrip =
            invoke_shift_units_typed(ScalarValue::Utf8(Some(shifted_left)), DataType::Utf8, 18)
                .unwrap();
        assert_eq!(roundtrip, original);
    }
}
