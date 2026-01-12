use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, FixedSizeBinaryArray, StringArray},
        datatypes::FieldRef,
    },
    common::plan_err,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
        TypeSignature, Volatility,
    },
};

use crate::{
    arrow::datatypes::{DataType, Field},
    plan,
};

/// DataFusion UDF that converts `FixedSizeBinary(20)` or `FixedSizeBinary(32)` to hex string.
///
/// This function simplifies SQL queries by converting binary addresses and hashes
/// to human-readable hex strings with `0x` prefix.
///
/// # SQL Usage
///
/// ```ignore
/// // FixedSizeBinary(20) -> '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
/// evm_decode_hex(address)
///
/// // FixedSizeBinary(32) -> '0xcb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff'
/// evm_decode_hex(tx_hash)
/// ```
///
/// # Arguments
///
/// * `binary` - FixedSizeBinary(20) or FixedSizeBinary(32)
///
/// # Errors
///
/// Returns a planning error if:
/// - Input is not FixedSizeBinary(20) or FixedSizeBinary(32)
///
/// # Nullability
///
/// Returns null if input is null.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmDecodeHex {
    signature: Signature,
}

impl Default for EvmDecodeHex {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmDecodeHex {
    /// Creates a new EvmDecodeHex UDF instance
    pub fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::FixedSizeBinary(20)]),
                TypeSignature::Exact(vec![DataType::FixedSizeBinary(32)]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmDecodeHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_decode_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("DataFusion will never call this")
    }

    /// Executes the UDF at runtime, converting binary to hex string with 0x prefix
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let input = args
            .args
            .first()
            .ok_or_else(|| plan!("{}: expected 1 argument, got 0", self.name()))?;

        match input {
            ColumnarValue::Array(array) => {
                // Handle array (column) input
                let binary_array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        plan!(
                            "{}: expected FixedSizeBinary array, got {}",
                            self.name(),
                            array.data_type()
                        )
                    })?;

                let result: StringArray = binary_array
                    .iter()
                    .map(|opt_bytes| {
                        opt_bytes.map(|bytes| format!("0x{}", alloy::hex::encode(bytes)))
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                plan_err!(
                    "{}: expected Array, but got {}",
                    self.name(),
                    scalar.data_type()
                )
            }
        }
    }

    /// Validates input type at query planning time
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let arg_fields = &args.arg_fields;
        if arg_fields.len() != 1 {
            return plan_err!(
                "{}: expected 1 argument, but got {}",
                self.name(),
                arg_fields.len()
            );
        }

        // Validate input type at planning time
        match arg_fields[0].data_type() {
            DataType::FixedSizeBinary(20) | DataType::FixedSizeBinary(32) => {}
            dt => {
                return plan_err!(
                    "{}: expected FixedSizeBinary(20) or FixedSizeBinary(32), got {}",
                    self.name(),
                    dt
                );
            }
        }

        Ok(Field::new(self.name(), DataType::Utf8, true).into())
    }
}

#[cfg(test)]
mod tests {
    use alloy::hex;

    use super::*;

    #[test]
    fn invoke_with_args_with_valid_address_succeeds() {
        //* Given
        let udf = EvmDecodeHex::new();
        let input_bytes = hex!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let array = FixedSizeBinaryArray::from(vec![Some(input_bytes.as_slice())]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let arg_fields = vec![Field::new("address", DataType::FixedSizeBinary(20), false).into()];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(args).unwrap();

        //* Then
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array, got {:?}", result);
        };
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            string_array.value(0),
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        );
    }

    #[test]
    fn invoke_with_args_with_valid_hash_succeeds() {
        //* Given
        let udf = EvmDecodeHex::new();
        let input_bytes = hex!("cb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff");
        let array = FixedSizeBinaryArray::from(vec![Some(input_bytes.as_slice())]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let arg_fields = vec![Field::new("tx_hash", DataType::FixedSizeBinary(32), false).into()];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(args).unwrap();

        //* Then
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array, got {:?}", result);
        };
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            string_array.value(0),
            "0xcb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff"
        );
    }

    #[test]
    fn invoke_with_args_with_null_input_returns_null() {
        //* Given
        let udf = EvmDecodeHex::new();
        let array = FixedSizeBinaryArray::from(vec![None]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let arg_fields = vec![Field::new("address", DataType::FixedSizeBinary(20), true).into()];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(args).unwrap();

        //* Then
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array, got {:?}", result);
        };
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(string_array.is_null(0));
    }

    #[test]
    fn invoke_with_args_with_invalid_type_fails() {
        //* Given
        let udf = EvmDecodeHex::new();
        let array = StringArray::from(vec![Some("not binary")]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let arg_fields = vec![Field::new("input", DataType::Utf8, false).into()];
        let return_field = Field::new(udf.name(), DataType::Utf8, true).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(args);

        //* Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("expected FixedSizeBinary array")
        );
    }

    #[test]
    fn invoke_with_args_with_no_arguments_fails() {
        //* Given
        let udf = EvmDecodeHex::new();
        let args: Vec<ColumnarValue> = vec![];
        let arg_fields: Vec<FieldRef> = vec![];
        let return_field = Field::new(udf.name(), DataType::Utf8, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = udf.invoke_with_args(args);

        //* Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("expected 1 argument, got 0")
        );
    }
}
