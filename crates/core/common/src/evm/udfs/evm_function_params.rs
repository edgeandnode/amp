use std::{any::Any, str::FromStr, sync::Arc};

use alloy::{
    dyn_abi::{DynSolType, JsonAbiExt, Specifier as _},
    json_abi::Function as AlloyFunction,
};
use datafusion::{
    arrow::{
        array::{ArrayBuilder, BinaryBuilder},
        datatypes::FieldRef,
    },
    common::plan_err,
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarUDFImpl, Signature, Volatility,
        simplify::{ExprSimplifyResult, SimplifyInfo},
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use itertools::izip;

use super::sol_to_arrow_type;
use crate::{
    arrow::{
        array::{Array, BinaryArray, StructBuilder},
        datatypes::{DataType, Field, Fields},
    },
    evm::udfs::{FieldBuilder, array_to_sol_value, num_rows, scalar_to_sol_value},
    plan,
};

/// DataFusion UDF that decodes function call input data into structured parameters.
///
/// This function parses the input data of a contract function call using the function
/// signature. It validates the 4-byte selector and decodes the ABI-encoded parameters.
/// Returns NULL for rows where the selector doesn't match or decoding fails.
///
/// # SQL Usage
///
/// ```ignore
/// // Decode a transfer function call
/// evm_decode_params(input_data, 'transfer(address to, uint256 amount)')
///
/// // Decode a swap function call
/// evm_decode_params(
///     input_data,
///     'swap(uint256 amount0Out,uint256 amount1Out,address to,bytes data)'
/// )
/// ```
///
/// # Arguments
///
/// * `data` - `Binary` function call input data (including 4-byte selector)
/// * `signature` - `Utf8` Solidity function signature with named parameters
///
/// # Returns
///
/// A struct containing decoded function parameters. Field names come from the
/// signature. Returns a struct with NULL fields if selector doesn't match or
/// decoding fails.
///
/// # Errors
///
/// Returns a planning error if:
/// - Signature is not a valid Solidity function signature
/// - Function parameters are unnamed
/// - Signature is not provided as a string literal
/// - Number of arguments is not 2
///
/// # Nullability
///
/// Returns null struct fields if:
/// - Input data is null
/// - Input data is less than 4 bytes
/// - Function selector doesn't match
/// - ABI decoding fails
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmDecodeParams {
    signature: Signature,
}

impl Default for EvmDecodeParams {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmDecodeParams {
    pub fn new() -> Self {
        let signature = Signature::exact(
            vec![DataType::Binary, DataType::Utf8],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmDecodeParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_decode_params"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("DataFusion will never call this")
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 2 {
            return plan_err!(
                "{}: expected 2 arguments, but got {}",
                self.name(),
                args.len()
            );
        }
        let signature = match &args[1] {
            ColumnarValue::Scalar(scalar) => scalar,
            v => {
                return plan_err!(
                    "{}: expected scalar argument for the signature but got {}",
                    self.name(),
                    v.data_type()
                );
            }
        };

        let call = FunctionCall::try_from(signature).map_err(|e| e.context(self.name()))?;
        let result = match &args[0] {
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| plan!("expected binary array"))?;
                self.decode(array.iter(), &call)
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(data))) => {
                self.decode(std::iter::once(Some(data.as_slice())), &call)
            }
            ColumnarValue::Scalar(scalar) => {
                return plan_err!(
                    "{}: expected Binary scalar, but got {}",
                    self.name(),
                    scalar.data_type()
                );
            }
        };
        let ary = result.map_err(|e| e.context(self.name()))?;
        Ok(ColumnarValue::Array(ary))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let args = args.scalar_arguments;
        if args.len() != 2 {
            return plan_err!(
                "{}: expected 2 arguments, but got {}",
                self.name(),
                args.len()
            );
        }
        let signature = args[1];
        let signature = match signature {
            Some(scalar) => scalar,
            _ => {
                return plan_err!(
                    "{}: expected a string literal for the signature",
                    self.name()
                );
            }
        };
        let call = FunctionCall::try_from(signature).map_err(|e| e.context(self.name()))?;
        let fields = self.fields(&call).map_err(|e| e.context(self.name()))?;
        Ok(Field::new_struct(self.name(), fields, true).into())
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> datafusion::error::Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }
}

impl EvmDecodeParams {
    /// Decode the given data using the function signature.
    fn decode<'a>(
        &self,
        data: impl Iterator<Item = Option<&'a [u8]>>,
        call: &FunctionCall,
    ) -> Result<Arc<dyn Array>, DataFusionError> {
        let fields = self.fields(call)?;
        let mut builder = StructBuilder::from_fields(fields, 0);
        for data in data {
            match data {
                None => {
                    for (field, ty) in call.input_types.iter().enumerate() {
                        FieldBuilder::new(&mut builder, ty, field).append_null_value()?;
                    }
                    builder.append(false);
                }
                Some(data) if data.len() < 4 => {
                    tracing::trace!(
                        function_name=%call.alloy_function.name,
                        data_len=data.len(),
                        "failed to decode function params: data too short"
                    );
                    for (field, ty) in call.input_types.iter().enumerate() {
                        FieldBuilder::new(&mut builder, ty, field).append_null_value()?;
                    }
                    builder.append(false);
                }
                Some(data) => {
                    let selector = &data[..4];
                    if selector != call.alloy_function.selector() {
                        tracing::trace!(
                            function_name=%call.alloy_function.name,
                            "failed to decode function params: selector mismatch"
                        );
                        for (field, ty) in call.input_types.iter().enumerate() {
                            FieldBuilder::new(&mut builder, ty, field).append_null_value()?;
                        }
                        builder.append(false);
                        continue;
                    }
                    let decoded = call.alloy_function.abi_decode_input(&data[4..]);
                    match decoded {
                        Ok(decoded) => {
                            for (field, (param, ty)) in
                                izip!(decoded, &call.input_types).enumerate()
                            {
                                FieldBuilder::new(&mut builder, ty, field).append_value(param)?;
                            }
                            builder.append(true);
                        }
                        Err(e) => {
                            tracing::trace!(
                                function_name=%call.alloy_function.name,
                                error = %e,
                                "failed to decode function params"
                            );
                            for (field, ty) in call.input_types.iter().enumerate() {
                                FieldBuilder::new(&mut builder, ty, field).append_null_value()?;
                            }
                            builder.append(false);
                        }
                    }
                }
            }
        }

        Ok(ArrayBuilder::finish(&mut builder))
    }

    /// Return `Fields` for the given param names and types.
    fn fields(&self, call: &FunctionCall) -> Result<Fields, DataFusionError> {
        let mut fields = Vec::new();
        for (name, ty) in call.input_names.iter().zip(call.input_types.iter()) {
            let name = name.clone();
            let df = sol_to_arrow_type(ty)?;
            let field = Field::new(name, df, true);
            fields.push(field);
        }
        Ok(Fields::from(fields))
    }
}

/// DataFusion UDF that encodes values into function call input data.
///
/// This function creates ABI-encoded function call data including the 4-byte
/// selector. It's the inverse of `evm_decode_params` and is useful for preparing
/// transaction input data.
///
/// # SQL Usage
///
/// ```ignore
/// // Encode a transfer call
/// evm_encode_params(recipient_address, amount, 'transfer(address to, uint256 amount)')
///
/// // Encode an approve call
/// evm_encode_params(spender, max_uint256, 'approve(address spender, uint256 amount)')
/// ```
///
/// # Arguments
///
/// * `...values` - Values to encode (one per function parameter, in order)
/// * `signature` - `Utf8` Solidity function signature (must be last argument)
///
/// # Returns
///
/// `Binary` complete function call data with 4-byte selector followed by
/// ABI-encoded parameters.
///
/// # Errors
///
/// Returns a planning error if:
/// - Signature is not a valid Solidity function signature
/// - Number of value arguments doesn't match function parameters
/// - Values cannot be converted to the specified Solidity types
/// - Signature is not provided as a string literal
/// - No arguments provided
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmEncodeParams {
    signature: Signature,
}

impl Default for EvmEncodeParams {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmEncodeParams {
    pub fn new() -> Self {
        let signature = Signature::variadic_any(Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmEncodeParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_encode_params"
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
        if args.is_empty() {
            return plan_err!(
                "{}: expected at least 1 argument (Solidity function signature), but got {}",
                self.name(),
                args.len()
            );
        }
        let signature = match &args[args.len() - 1] {
            ColumnarValue::Scalar(scalar) => scalar,
            v => {
                return plan_err!(
                    "{}: expected scalar argument for the Solidity function signature but got {}",
                    self.name(),
                    v.data_type()
                );
            }
        };

        let call = FunctionCall::try_from(signature).map_err(|e| e.context(self.name()))?;
        let args = &args[..args.len() - 1];

        if args.len() != call.input_types.len() {
            return plan_err!(
                "{}: expected {} arguments for the function signature but got {}",
                self.name(),
                call.input_types.len(),
                args.len()
            );
        }

        let mut builder = BinaryBuilder::new();
        for i in 0..num_rows(args) {
            let mut sol_values = Vec::new();
            for (arg, sol_ty) in args.iter().zip(call.input_types.iter()) {
                let sol_value = match arg {
                    ColumnarValue::Scalar(scalar) => scalar_to_sol_value(scalar.clone(), sol_ty)?,
                    ColumnarValue::Array(ary) => array_to_sol_value(ary, sol_ty, i)?,
                };
                sol_values.push(sol_value);
            }
            let encoded = call
                .alloy_function
                .abi_encode_input(&sol_values)
                .map_err(|e| plan!("failed to encode function call params: {}", e))?;
            builder.append_value(encoded);
        }

        Ok(ColumnarValue::Array(ArrayBuilder::finish(&mut builder)))
    }
}

struct FunctionCall {
    alloy_function: AlloyFunction,
    input_names: Vec<String>,
    input_types: Vec<DynSolType>,
}

impl FunctionCall {
    fn new(alloy_function: AlloyFunction) -> Result<Self, DataFusionError> {
        let mut input_names = Vec::with_capacity(alloy_function.inputs.len());
        let mut input_types = Vec::with_capacity(alloy_function.inputs.len());

        for param in &alloy_function.inputs {
            let ty = param.resolve().unwrap();
            if param.name.is_empty() {
                return plan_err!(
                    "function {} has unnamed input parameter",
                    alloy_function.name
                );
            }
            input_types.push(ty);
            input_names.push(param.name.clone());
        }

        Ok(Self {
            alloy_function,
            input_names,
            input_types,
        })
    }
}

impl FromStr for FunctionCall {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let alloy_event =
            AlloyFunction::parse(s).or_else(|e| plan_err!("parse function signature: {}", e))?;
        Self::new(alloy_event)
    }
}

impl TryFrom<&ScalarValue> for FunctionCall {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        let s = match value {
            ScalarValue::Utf8(Some(s)) => s,
            v => return plan_err!("expected Utf8, got {}", v.data_type()),
        };
        s.parse()
    }
}
