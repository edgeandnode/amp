use std::any::Any;

use datafusion::{
    arrow::datatypes::FieldRef,
    common::plan_err,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    scalar::ScalarValue,
};

use crate::{
    EVM_ADDRESS_TYPE,
    arrow::datatypes::{DataType, Field},
    plan,
};

/// DataFusion UDF that converts hex strings to `FixedSizeBinary(20)` or `FixedSizeBinary(32)`.
///
/// This function simplifies SQL queries by replacing verbose `arrow_cast` syntax
/// with an intuitive function call. Accepts hex strings with or without `0x` prefix.
/// Automatically determines output type based on input length.
///
/// # SQL Usage
///
/// ```ignore
/// // 20-byte address -> FixedSizeBinary(20)
/// evm_encode_hex('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
///
/// // 32-byte hash -> FixedSizeBinary(32)
/// evm_encode_hex('0xcb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff')
/// ```
///
/// # Arguments
///
/// * `hex_string` - Hex string (40 or 64 characters), with or without `0x` prefix
///
/// # Errors
///
/// Returns a planning error if:
/// - Input is not a valid hex string
/// - Decoded bytes are not exactly 20 or 32 bytes
/// - Input is null
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmEncodeHex {
    signature: Signature,
}

impl Default for EvmEncodeHex {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmEncodeHex {
    /// Creates a new EvmEncodeHex UDF instance
    pub fn new() -> Self {
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmEncodeHex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_encode_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("DataFusion will never call this")
    }

    /// Executes the UDF at runtime. Only scalar (literal) inputs supported.
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;

        // Extract the hex string from the first argument (must be a scalar UTF8 value)
        let hex_string = match args.first() {
            Some(ColumnarValue::Scalar(scalar)) => scalar,
            Some(v) => {
                return plan_err!(
                    "{}: expected scalar argument but got {}",
                    self.name(),
                    v.data_type()
                );
            }
            None => {
                return plan_err!("{}: expected 1 argument, got 0", self.name());
            }
        };
        let hex_str = match hex_string {
            ScalarValue::Utf8(Some(s)) => s,
            ScalarValue::Utf8(None) => return plan_err!("{}: input cannot be null", self.name()),
            v => return plan_err!("{}: expected Utf8, got {}", self.name(), v.data_type()),
        };

        // Decode hex string (handles optional "0x" prefix)
        let bytes = alloy::hex::decode(hex_str)
            .map_err(|e| plan!("{}: invalid hex string: {}", self.name(), e))?;

        // Only 20-byte addresses or 32-byte hashes are valid
        let len = bytes.len();
        if len != 20 && len != 32 {
            return plan_err!("{}: expected 20 or 32 bytes, got {}", self.name(), len);
        }
        Ok(ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
            len as i32,
            Some(bytes),
        )))
    }

    /// Validates literal hex string at query planning time, catching errors before execution
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let args = args.scalar_arguments;
        if args.len() != 1 {
            return plan_err!(
                "{}: expected 1 argument, but got {}",
                self.name(),
                args.len()
            );
        }

        // Validate the literal hex string at planning time
        if let Some(ScalarValue::Utf8(Some(hex_str))) = args[0] {
            // Validate hex format
            let bytes = alloy::hex::decode(hex_str)
                .map_err(|e| plan!("{}: invalid hex string: {}", self.name(), e))?;

            // Only 20-byte addresses or 32-byte hashes are valid
            let len = bytes.len();
            if len != 20 && len != 32 {
                return plan_err!("{}: expected 20 or 32 bytes, got {}", self.name(), len);
            }

            return Ok(
                Field::new(self.name(), DataType::FixedSizeBinary(len as i32), false).into(),
            );
        }

        // Default to 20 bytes if no literal value available at planning time
        Ok(Field::new(self.name(), EVM_ADDRESS_TYPE, false).into())
    }
}

#[cfg(test)]
mod tests {
    use alloy::hex;

    use super::*;

    #[test]
    fn invoke_with_args_with_valid_address_succeeds() {
        //* Given
        let evm_address = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(),
        )))];
        let arg_fields = vec![Field::new("evm_encode_hex", DataType::Utf8, false).into()];
        let return_field = Field::new(evm_address.name(), crate::EVM_ADDRESS_TYPE, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = evm_address.invoke_with_args(args).unwrap();

        //* Then
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };
        let expected_bytes = hex!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let exp = ScalarValue::FixedSizeBinary(20, Some(expected_bytes.to_vec()));
        assert_eq!(exp, result);
    }

    #[test]
    fn invoke_with_args_with_valid_hash_succeeds() {
        //* Given
        let udf = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "0xcb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff".to_string(),
        )))];
        let arg_fields = vec![Field::new("evm_encode_hex", DataType::Utf8, false).into()];
        let return_field = Field::new(udf.name(), DataType::FixedSizeBinary(32), false).into();
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
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };
        let expected_bytes =
            hex!("cb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff");
        let exp = ScalarValue::FixedSizeBinary(32, Some(expected_bytes.to_vec()));
        assert_eq!(exp, result);
    }

    #[test]
    fn invoke_with_args_with_valid_address_without_0x_succeeds() {
        //* Given
        let evm_address = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(),
        )))];
        let arg_fields = vec![Field::new("evm_encode_hex", DataType::Utf8, false).into()];
        let return_field = Field::new(evm_address.name(), crate::EVM_ADDRESS_TYPE, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = evm_address.invoke_with_args(args).unwrap();

        //* Then
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };
        let expected_bytes = hex!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let exp = ScalarValue::FixedSizeBinary(20, Some(expected_bytes.to_vec()));
        assert_eq!(exp, result);
    }

    #[test]
    fn invoke_with_args_with_valid_hash_without_0x_succeeds() {
        //* Given
        let udf = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "cb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff".to_string(),
        )))];
        let arg_fields = vec![Field::new("evm_encode_hex", DataType::Utf8, false).into()];
        let return_field = Field::new(udf.name(), DataType::FixedSizeBinary(32), false).into();
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
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };
        let expected_bytes =
            hex!("cb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff");
        let exp = ScalarValue::FixedSizeBinary(32, Some(expected_bytes.to_vec()));
        assert_eq!(exp, result);
    }

    #[test]
    fn invoke_with_args_with_invalid_length_fails() {
        //* Given
        let udf = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "0x1234".to_string(), // 2 bytes - not 20 or 32
        )))];
        let arg_fields = vec![Field::new("evm_encode_hex", DataType::Utf8, false).into()];
        let return_field = Field::new(udf.name(), crate::EVM_ADDRESS_TYPE, false).into();
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
                .contains("expected 20 or 32 bytes")
        );
    }

    #[test]
    fn invoke_with_args_with_empty_string_fails() {
        //* Given
        let evm_address = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "".to_string(), // Empty string
        )))];
        let arg_fields = vec![Field::new("address", DataType::Utf8, false).into()];
        let return_field = Field::new(evm_address.name(), crate::EVM_ADDRESS_TYPE, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = evm_address.invoke_with_args(args);

        //* Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("expected 20 or 32 bytes, got 0")
        );
    }

    #[test]
    fn invoke_with_args_with_null_address_fails() {
        //* Given
        let evm_address = EvmEncodeHex::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))];
        let arg_fields = vec![Field::new("address", DataType::Utf8, false).into()];
        let return_field = Field::new(evm_address.name(), crate::EVM_ADDRESS_TYPE, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = evm_address.invoke_with_args(args);

        //* Then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("input cannot be null")
        );
    }

    #[test]
    fn invoke_with_args_with_no_arguments_fails() {
        //* Given
        let evm_address = EvmEncodeHex::new();
        let args: Vec<ColumnarValue> = vec![]; // No arguments
        let arg_fields: Vec<FieldRef> = vec![];
        let return_field = Field::new(evm_address.name(), crate::EVM_ADDRESS_TYPE, false).into();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };

        //* When
        let result = evm_address.invoke_with_args(args);

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
