use std::any::Any;

use alloy::dyn_abi::DynSolType;
use datafusion::{
    arrow::{
        array::{ArrayBuilder, BinaryBuilder},
        datatypes::DataType,
    },
    common::plan_err,
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};

use super::{array_to_sol_value, num_rows, scalar_to_sol_value};
use crate::plan;

/// DataFusion UDF that ABI-encodes a value according to a Solidity type.
///
/// This function encodes a value into ABI-encoded binary format using the specified
/// Solidity type. It's the inverse of `evm_decode_type` and is useful for preparing
/// data for contract calls or encoding values for storage.
///
/// # SQL Usage
///
/// ```ignore
/// // Encode a uint256 value
/// evm_encode_type(12345, 'uint256')
///
/// // Encode an address
/// evm_encode_type(address_column, 'address')
///
/// // Encode a boolean
/// evm_encode_type(true, 'bool')
/// ```
///
/// # Arguments
///
/// * `value` - The value to encode (type must be compatible with the Solidity type)
/// * `type` - `Utf8` Solidity type string (e.g., "uint256", "address", "bool")
///
/// # Returns
///
/// `Binary` ABI-encoded representation of the value.
///
/// # Errors
///
/// Returns a planning error if:
/// - Type string is not a valid Solidity type
/// - Value cannot be converted to the specified Solidity type
/// - Type argument is not a string literal
/// - Number of arguments is not 2
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmEncodeType {
    signature: Signature,
}

impl Default for EvmEncodeType {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmEncodeType {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EvmEncodeType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_encode_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return plan_err!(
                "{}: expected at 2 arguments, but got {}",
                self.name(),
                args.len()
            );
        }
        let sol_ty = match &args[1] {
            ColumnarValue::Scalar(scalar) => scalar,
            v => {
                return plan_err!(
                    "{}: expected scalar argument for the Solidity type but got {}",
                    self.name(),
                    v.data_type()
                );
            }
        };
        let sol_ty = match sol_ty {
            ScalarValue::Utf8(Some(s)) => s,
            v => return plan_err!("expected Utf8, got {}", v.data_type()),
        };
        let sol_ty: DynSolType = sol_ty
            .parse()
            .map_err(|e| plan!("failed to parse Solidity type: {}", e))?;
        let value = &args[0];

        let mut builder = BinaryBuilder::new();
        for i in 0..num_rows(&args) {
            let sol_value = match value {
                ColumnarValue::Scalar(scalar) => scalar_to_sol_value(scalar.clone(), &sol_ty)?,
                ColumnarValue::Array(ary) => array_to_sol_value(ary, &sol_ty, i)?,
            };
            let encoded = sol_value.abi_encode();
            builder.append_value(encoded);
        }

        Ok(ColumnarValue::Array(ArrayBuilder::finish(&mut builder)))
    }
}
