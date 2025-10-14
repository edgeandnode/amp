use std::{any::Any, sync::Arc};

use alloy::dyn_abi::DynSolType;
use datafusion::{
    arrow::{
        array::{ArrayBuilder, make_builder},
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

use super::{append_null_value_to_builder, append_sol_value_to_builder, sol_to_arrow_type};
use crate::{
    arrow::{
        array::{Array, BinaryArray},
        datatypes::{DataType, Field},
    },
    plan,
};

#[derive(Debug)]
pub struct EvmDecodeType {
    signature: Signature,
}

impl Default for EvmDecodeType {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmDecodeType {
    pub fn new() -> Self {
        let signature = Signature::exact(
            vec![DataType::Binary, DataType::Utf8],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmDecodeType {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_decode_type"
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

        let result = match &args[0] {
            ColumnarValue::Array(array) => self.decode(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| plan!("expected binary array"))?
                    .iter(),
                array.len(),
                &sol_ty,
            ),
            ColumnarValue::Scalar(scalar_value) => match scalar_value {
                ScalarValue::Binary(Some(data)) => {
                    self.decode(std::iter::once(Some(data.as_slice())), 1, &sol_ty)
                }
                _ => {
                    return plan_err!(
                        "{}: expected Binary scalar, but got {}",
                        self.name(),
                        scalar_value.data_type()
                    );
                }
            },
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
        let sol_ty = args[1].ok_or_else(|| {
            plan!(
                "{}: expected scalar argument for the Solidity type",
                self.name()
            )
        })?;
        let sol_ty = match sol_ty {
            ScalarValue::Utf8(Some(s)) => s,
            v => return plan_err!("expected Utf8, got {}", v.data_type()),
        };
        let sol_ty: DynSolType = sol_ty
            .parse()
            .map_err(|e| plan!("failed to parse Solidity type: {}", e))?;
        let ty = sol_to_arrow_type(&sol_ty)?;
        Ok(Field::new(self.name(), ty, false).into())
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> datafusion::error::Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }
}

impl EvmDecodeType {
    fn decode<'a>(
        &self,
        data: impl Iterator<Item = Option<&'a [u8]>>,
        data_len: usize,
        sol_ty: &DynSolType,
    ) -> Result<Arc<dyn Array>, DataFusionError> {
        let ty = sol_to_arrow_type(sol_ty)?;
        let mut builder = make_builder(&ty, data_len);
        for data in data {
            match data {
                Some(data) => match sol_ty.abi_decode(data) {
                    Ok(sol_value) => append_sol_value_to_builder(&mut builder, sol_value)?,
                    Err(e) => {
                        tracing::trace!(
                            sol_ty=%sol_ty,
                            error=?e,
                            "failed to decode Solidity value"
                        );
                        append_null_value_to_builder(&mut builder, &ty)?;
                    }
                },
                None => {
                    append_null_value_to_builder(&mut builder, &ty)?;
                }
            }
        }
        Ok(ArrayBuilder::finish(&mut builder))
    }
}
