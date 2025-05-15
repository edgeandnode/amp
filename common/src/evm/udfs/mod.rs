use std::{any::Any, str::FromStr, sync::Arc};

use alloy::{
    dyn_abi::{DynSolType, DynSolValue, DynToken, Specifier as _},
    json_abi::Event as AlloyEvent,
    primitives::{ruint::FromUintError, Address, BigIntConversionError, Signed, B256, I256, U256},
};
use datafusion::{
    arrow::{
        array::{
            ArrayBuilder, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
            FixedSizeListArray, FixedSizeListBuilder, Int16Array, Int32Array, Int64Array,
            Int8Array, ListArray, ListBuilder, NullBuilder, StringArray, StructArray, UInt16Array,
            UInt32Array, UInt64Array, UInt8Array,
        },
        error::ArrowError,
    },
    common::{internal_err, plan_err},
    error::DataFusionError,
    logical_expr::{
        simplify::{ExprSimplifyResult, SimplifyInfo},
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
pub use eth_call::EthCall;
pub use evm_encode_type::EvmEncodeType;
pub use evm_function_data::{EvmDecodeFunctionData, EvmEncodeParams};
use itertools::izip;
use tracing::trace;

use crate::{
    arrow::{
        array::{
            Array, BinaryArray, BinaryBuilder, BooleanBuilder, Decimal128Builder,
            Decimal256Builder, FixedSizeBinaryBuilder, Int16Builder, Int32Builder, Int64Builder,
            Int8Builder, StringBuilder, StructBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
            UInt8Builder,
        },
        datatypes::{
            i256, validate_decimal256_precision, validate_decimal_precision, DataType, Field,
            Fields, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
        },
    },
    Bytes32ArrayType, BYTES32_TYPE,
};

mod eth_call;
mod evm_encode_type;
mod evm_function_data;

type Unsigned = alloy::primitives::Uint<256, 4>;

const DEC128_PREC: u8 = DECIMAL128_MAX_PRECISION;
const DEC256_PREC: u8 = DECIMAL256_MAX_PRECISION;

// This is `log2(10)*76 - 1`.
//
// Where 76 is DEC256_PREC, and 1 bit of precision is subtracted because we need
// to account for the sign bit.
//
// When the precision on a Solidity type is greater than this, such as uint256,
// we decode the value as a string instead of a decimal.
const DEC_256_MAX_BINARY_PREC: usize = 251;

// See comment for DEC_256_MAX_BINARY_PREC
const DEC_128_MAX_BINARY_PREC: usize = 125;

// Convenience to report an internal error. Same as DataFusion's
// `internal_err!`, but the error is not wrapped in an `Err`
#[macro_export]
macro_rules! internal {
    ($msg:expr) => {{
        datafusion::error::DataFusionError::Internal(format!("{}", $msg))
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        datafusion::error::DataFusionError::Internal(format!($fmt, $($arg)*))
    }}
}

// Convenience to report a plan error. Same as DataFusion's
// `plan_err!`, but the error is not wrapped in an `Err`.
#[macro_export]
macro_rules! plan {
    ($msg:expr) => {{
        datafusion::error::DataFusionError::Plan(format!("{}", $msg))
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        datafusion::error::DataFusionError::Plan(format!($fmt, $($arg)*))
    }}
}

struct AppendError(std::string::String);

impl From<DataFusionError> for AppendError {
    fn from(e: DataFusionError) -> Self {
        AppendError(e.to_string())
    }
}

impl From<BigIntConversionError> for AppendError {
    fn from(e: BigIntConversionError) -> Self {
        AppendError(e.to_string())
    }
}

impl<T> From<FromUintError<T>> for AppendError {
    fn from(e: FromUintError<T>) -> Self {
        AppendError(e.to_string())
    }
}

impl From<ArrowError> for AppendError {
    fn from(e: ArrowError) -> Self {
        AppendError(e.to_string())
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

struct FieldBuilder<'a> {
    builder: &'a mut StructBuilder,
    ty: &'a DynSolType,
    field: usize,
}

impl<'a> FieldBuilder<'a> {
    fn new(builder: &'a mut StructBuilder, ty: &'a DynSolType, field: usize) -> Self {
        Self { builder, ty, field }
    }

    fn append_null_value(&'a mut self) -> Result<(), DataFusionError> {
        let ty = sol_to_arrow_type(self.ty)?;
        let builder = &mut self.builder.field_builders_mut()[self.field];
        append_null_value_to_builder(builder, &ty)?;
        Ok(())
    }

    /// Convert `value` to a DF value and append it to the appropriate
    /// field. Most conversions are pretty straightforward, but conversion
    /// of large integers is a headache.
    ///
    ///  DF only provides up to 64 bit integers, but solidity supports
    /// signed and unsigned integers up to 256 bits. Arrow also provides
    /// `Decimal128` and `Decimal256` types. But since they are decimal
    /// types, they can't represent the full 128 or 256 bits of the original
    /// integer, and for `uint256` we can only represent roughly half the
    /// range as DF doesn't have any unsigned 256 bit integer and we have to
    /// resort to `i256`.
    ///
    /// To avoid lossy conversion, we convert `uint256` and `int256` to `Utf8`,
    /// leaving it up to the user to cast to an appropriate numeric type if
    /// they need to perform arithmetic operations.
    fn append_value(&'a mut self, value: DynSolValue) -> Result<(), DataFusionError> {
        let builder = &mut self.builder.field_builders_mut()[self.field];
        append_sol_value_to_builder(builder, value)
    }
}

struct Event {
    name: String,
    topic0: Option<B256>,
    topic_names: Vec<String>,
    topic_types: Vec<DynSolType>,
    body_names: Vec<String>,
    body_tuple: DynSolType,
}

impl Event {
    fn new(alloy_event: AlloyEvent) -> Result<Self, DataFusionError> {
        let topic0 = if alloy_event.anonymous {
            None
        } else {
            Some(alloy_event.selector())
        };

        let mut topic_types = Vec::with_capacity(alloy_event.inputs.len());
        let mut topic_names = Vec::with_capacity(alloy_event.inputs.len());
        let mut body_types = Vec::with_capacity(alloy_event.inputs.len());
        let mut body_names = Vec::with_capacity(alloy_event.inputs.len());

        for param in &alloy_event.inputs {
            let ty = param.resolve().unwrap();
            if param.indexed {
                topic_types.push(ty);
                topic_names.push(param.name.clone());
            } else {
                body_types.push(ty);
                body_names.push(param.name.clone());
            }
        }

        if topic_names.len() > 3 {
            return plan_err!(
                "event can only have 3 indexed parameters but has {}",
                topic_names.len()
            );
        }

        let body_tuple = DynSolType::Tuple(body_types);

        Ok(Self {
            name: alloy_event.name,
            topic0,
            topic_names,
            topic_types,
            body_names,
            body_tuple,
        })
    }

    /// Return `Fields` for the event. The fields are arranged so that the
    /// fields for the indexed attributes come first, followed by the fields
    /// for the non-indexed attributes.
    fn fields(&self) -> Result<Fields, DataFusionError> {
        let mut fields = Vec::new();
        let names = self.topic_names.iter().chain(&self.body_names);
        let DynSolType::Tuple(body_types) = &self.body_tuple else {
            return internal_err!("event: body_tuple is not a tuple");
        };
        let types = self.topic_types.iter().chain(body_types);
        for (name, ty) in names.zip(types) {
            let name = name.clone();
            let df = sol_to_arrow_type(ty)?;
            let field = Field::new(name, df, true);
            fields.push(field);
        }
        Ok(Fields::from(fields))
    }

    fn topic0(&self) -> Result<B256, DataFusionError> {
        match self.topic0 {
            Some(topic0) => Ok(topic0),
            None => plan_err!("anonymous event {} has no topic0", self.name),
        }
    }

    fn decode_topic(
        &self,
        builder: &mut StructBuilder,
        number: usize,
        value: Option<&[u8]>,
    ) -> Result<(), DataFusionError> {
        use DynSolType::*;

        if number > self.topic_names.len() {
            return Ok(());
        }

        let ty = &self.topic_types[number - 1];

        let mut field_builder = FieldBuilder::new(builder, ty, number - 1);
        match value {
            Some(value) => {
                let value = B256::from_slice(value);
                let value = match ty {
                    Address | Function | Bool | FixedBytes(_) | Int(_) | Uint(_) => ty
                        .detokenize(DynToken::Word(value))
                        .or_else(|e| plan_err!("failed to decode topic{}: {}", number, e))?,
                    _ => DynSolValue::FixedBytes(value, 32),
                };
                field_builder.append_value(value)?;
            }
            None => field_builder.append_null_value()?,
        }

        Ok(())
    }

    fn decode_body(
        &self,
        builder: &mut StructBuilder,
        data: Option<&[u8]>,
    ) -> Result<(), DataFusionError> {
        use DataFusionError::Execution;

        let DynSolType::Tuple(tys) = &self.body_tuple else {
            return internal_err!("expected a tuple type for body_tuple");
        };

        if tys.is_empty() {
            return Ok(());
        }

        let values = data
            .ok_or_else(|| Execution("expected non-null log data for event decoding".to_string()))
            .and_then(|data| {
                let decoded = self.body_tuple.abi_decode_sequence(data).map_err(|e| {
                    Execution(format!(
                        "failed to decode data field with {} bytes: {}",
                        data.len(),
                        e,
                    ))
                })?;
                match decoded {
                    DynSolValue::Tuple(values) => Ok(values),
                    // Unreachable: `body_tuple` is a `DynSolType::Tuple`
                    _ => unreachable!(),
                }
            });

        match values {
            Ok(values) => {
                for (number, (value, ty)) in values.into_iter().zip(tys).enumerate() {
                    let mut field_builder =
                        FieldBuilder::new(builder, ty, number + self.topic_types.len());
                    field_builder.append_value(value)?;
                }
            }
            Err(e) => {
                trace!(
                    "failed to decode event '{}{}', filling with nulls. Error: {}",
                    self.name,
                    self.body_tuple,
                    e
                );
                for (number, ty) in tys.iter().enumerate() {
                    let mut field_builder =
                        FieldBuilder::new(builder, ty, number + self.topic_types.len());
                    field_builder.append_null_value()?;
                }
            }
        }

        Ok(())
    }
}

impl FromStr for Event {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let alloy_event =
            AlloyEvent::parse(s).or_else(|e| plan_err!("parse event signature: {}", e))?;
        Self::new(alloy_event)
    }
}

impl TryFrom<&ScalarValue> for Event {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        let s = match value {
            ScalarValue::Utf8(Some(s)) => s,
            v => return plan_err!("expected Utf8, got {}", v.data_type()),
        };
        s.parse()
    }
}

pub fn decode(
    topic1: &Bytes32ArrayType,
    topic2: &Bytes32ArrayType,
    topic3: &Bytes32ArrayType,
    data: &BinaryArray,
    signature: &ScalarValue,
) -> Result<Arc<dyn Array>, DataFusionError> {
    let event = Event::try_from(signature)?;
    let fields = event.fields()?;
    if fields.is_empty() {
        let mut builder = NullBuilder::new();
        builder.append_nulls(topic1.len());
        return Ok(Arc::new(builder.finish()));
    }

    let mut builder = StructBuilder::from_fields(fields, topic1.len());
    for (t1, t2, t3, d) in izip!(topic1, topic2, topic3, data) {
        event.decode_topic(&mut builder, 1, t1)?;
        event.decode_topic(&mut builder, 2, t2)?;
        event.decode_topic(&mut builder, 3, t3)?;
        event.decode_body(&mut builder, d)?;
        builder.append(true);
    }

    let structs = builder.finish();
    Ok(Arc::new(structs))
}

#[derive(Debug)]
pub struct EvmDecode {
    signature: Signature,
}

impl EvmDecode {
    pub fn new() -> Self {
        let signature = Signature::exact(
            vec![
                BYTES32_TYPE,
                BYTES32_TYPE,
                BYTES32_TYPE,
                DataType::Binary,
                DataType::Utf8,
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("DataFusion will never call this")
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        if args.len() != 5 {
            return internal_err!(
                "{}: expected 5 arguments, but got {}",
                self.name(),
                args.len()
            );
        }

        let signature = match &args[4] {
            ColumnarValue::Scalar(scalar) => scalar,
            v => {
                return plan_err!(
                    "{}: expected scalar argument for the signature but got {}",
                    self.name(),
                    v.data_type()
                )
            }
        };

        let args: Vec<_> = ColumnarValue::values_to_arrays(&args[0..4])?;
        let topic1 = args[0].as_any().downcast_ref::<Bytes32ArrayType>().unwrap();
        let topic2 = args[1].as_any().downcast_ref::<Bytes32ArrayType>().unwrap();
        let topic3 = args[2].as_any().downcast_ref::<Bytes32ArrayType>().unwrap();
        let data = args[3].as_any().downcast_ref::<BinaryArray>().unwrap();
        let ary =
            decode(topic1, topic2, topic3, data, signature).map_err(|e| e.context(self.name()))?;
        Ok(ColumnarValue::Array(ary))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<Field> {
        let args = args.scalar_arguments;
        if args.len() != 5 {
            return internal_err!(
                "{}: expected 5 arguments, but got {}",
                self.name(),
                args.len()
            );
        }
        let signature = args[4];
        let signature = match signature {
            Some(scalar) => scalar,
            _ => {
                return plan_err!(
                    "{}: expected a string literal for the signature",
                    self.name()
                )
            }
        };
        let event = Event::try_from(signature).map_err(|e| e.context(self.name()))?;
        let fields = event.fields()?;
        if fields.is_empty() {
            return Ok(Field::new(self.name(), DataType::Null, true));
        }
        Ok(Field::new_struct(self.name(), fields, false))
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

#[derive(Debug)]
pub struct EvmTopic {
    signature: Signature,
}

impl EvmTopic {
    const RETURN_TYPE: DataType = DataType::FixedSizeBinary(32);

    pub fn new() -> Self {
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for EvmTopic {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "evm_topic"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        if arg_types.len() != 1 {
            return internal_err!(
                "{}: expected 1 argument but got {}",
                self.name(),
                arg_types.len()
            );
        }
        Ok(Self::RETURN_TYPE)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let args = args.args;
        let signature = match &args[0] {
            ColumnarValue::Scalar(scalar) => scalar,
            v => {
                return plan_err!(
                    "{}: expected scalar argument for the signature but got {}",
                    self.name(),
                    v.data_type()
                )
            }
        };
        let event = Event::try_from(signature).map_err(|e| e.context(self.name()))?;

        let topic0 = event.topic0()?;

        let value = ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(32, Some(topic0.to_vec())));
        Ok(value)
    }
}

fn append_sol_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    value: DynSolValue,
) -> Result<(), DataFusionError> {
    fn primitive_int<'a>(
        builder: &mut dyn ArrayBuilder,
        s: Signed<256, 4>,
        bits: usize,
    ) -> Result<(), AppendError> {
        match bits {
            8 => builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .ok_or_else(|| AppendError("failed to downcast to Int8Builder".to_string()))?
                .append_value(i8::try_from(s)?),
            16 => builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .ok_or_else(|| AppendError("failed to downcast to Int16Builder".to_string()))?
                .append_value(i16::try_from(s)?),
            n if n <= 32 => builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .ok_or_else(|| AppendError("failed to downcast to Int32Builder".to_string()))?
                .append_value(i32::try_from(s)?),
            n if n <= 64 => builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| AppendError("failed to downcast to Int64Builder".to_string()))?
                .append_value(i64::try_from(s)?),
            n if n <= DEC_128_MAX_BINARY_PREC => {
                let val = i128::try_from(s)?;
                validate_decimal_precision(val, DEC128_PREC)?;
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .ok_or_else(|| {
                        AppendError("failed to downcast to Decimal128Builder".to_string())
                    })?;
                builder.append_value(val);
            }
            n if n <= DEC_256_MAX_BINARY_PREC => {
                let val = i256::from_le_bytes(s.to_le_bytes());
                validate_decimal256_precision(val, DEC256_PREC)?;
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal256Builder>()
                    .ok_or_else(|| {
                        AppendError("failed to downcast to Decimal256Builder".to_string())
                    })?;
                builder.append_value(val);
            }
            n if n <= 256 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| AppendError("failed to downcast to StringBuilder".to_string()))?
                    .append_value(s.to_string());
            }
            _ => unreachable!("unexpected number of bits for int: {}", bits),
        };
        Ok(())
    }

    fn primitive_uint<'a>(
        builder: &mut dyn ArrayBuilder,
        u: Unsigned,
        bits: usize,
    ) -> Result<(), AppendError> {
        match bits {
            8 => builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .ok_or_else(|| AppendError("failed to downcast to UInt8Builder".to_string()))?
                .append_value(u8::try_from(u)?),
            16 => builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .ok_or_else(|| AppendError("failed to downcast to UInt16Builder".to_string()))?
                .append_value(u16::try_from(u)?),
            n if n <= 32 => builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .ok_or_else(|| AppendError("failed to downcast to UInt32Builder".to_string()))?
                .append_value(u32::try_from(u)?),
            n if n <= 64 => builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .ok_or_else(|| AppendError("failed to downcast to UInt64Builder".to_string()))?
                .append_value(u64::try_from(u)?),
            n if n <= DEC_128_MAX_BINARY_PREC => {
                let val = i128::try_from(u)?;
                validate_decimal_precision(val, DEC128_PREC)?;
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal128Builder>()
                    .ok_or_else(|| {
                        AppendError("failed to downcast to Decimal128Builder".to_string())
                    })?;
                builder.append_value(val);
            }
            n if n <= DEC_256_MAX_BINARY_PREC => {
                let val = i256::from_le_bytes(u.to_le_bytes());
                validate_decimal256_precision(val, DEC256_PREC)?;
                let builder = builder
                    .as_any_mut()
                    .downcast_mut::<Decimal256Builder>()
                    .ok_or_else(|| {
                        AppendError("failed to downcast to Decimal256Builder".to_string())
                    })?;
                builder.append_value(val);
            }
            n if n <= 256 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .ok_or_else(|| AppendError("failed to downcast to StringBuilder".to_string()))?
                    .append_value(u.to_string());
            }
            _ => unreachable!("unexpected number of bits for uint: {}", bits),
        };
        Ok(())
    }

    match value {
        DynSolValue::Bool(b) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| internal!("failed to downcast to BooleanBuilder"))?
            .append_value(b),
        DynSolValue::Int(s, bits) => match bits {
            n if n <= 256 => primitive_int(builder, s, bits)
                .or_else(|e| internal_err!("error converting int{}: {}", bits, e))?,
            _ => return internal_err!("unexpected number of bits for int{}", bits),
        },
        DynSolValue::Uint(u, bits) => match bits {
            n if n <= 256 => primitive_uint(builder, u, bits)
                .or_else(|e| internal_err!("error converting uint{}: {}", bits, e))?,
            _ => return internal_err!("unexpected number of bits for uint{}", bits),
        },
        DynSolValue::FixedBytes(b, _) => builder
            .as_any_mut()
            .downcast_mut::<FixedSizeBinaryBuilder>()
            .ok_or_else(|| plan!("failed to downcast to FixedSizeBinaryBuilder"))?
            .append_value(b)?,
        DynSolValue::Address(a) => builder
            .as_any_mut()
            .downcast_mut::<FixedSizeBinaryBuilder>()
            .ok_or_else(|| plan!("failed to downcast to FixedSizeBinaryBuilder"))?
            .append_value(a)?,
        DynSolValue::Bytes(b) => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| plan!("failed to downcast to BinaryBuilder"))?
            .append_value(b),
        DynSolValue::String(s) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| plan!("failed to downcast to StringBuilder"))?
            .append_value(s),
        DynSolValue::Array(a) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| plan!("failed to downcast to ListBuilder"))?;
            for value in a {
                append_sol_value_to_builder(builder.values(), value)?;
            }
            builder.append(true);
        }
        DynSolValue::FixedArray(a) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| plan!("failed to downcast to ListBuilder"))?;
            for value in a {
                append_sol_value_to_builder(builder.values(), value)?;
            }
            builder.append(true);
        }
        DynSolValue::Tuple(t) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| plan!("failed to downcast to ListBuilder"))?;
            for (value, builder) in t.into_iter().zip(builder.field_builders_mut()) {
                append_sol_value_to_builder(builder, value)?;
            }
            builder.append(true);
        }
        DynSolValue::Function(_) => {
            let type_name = value.sol_type_name().unwrap_or_default();
            return plan_err!("cannot convert {type_name} to arrow scalar");
        }
    };
    Ok(())
}

fn append_null_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    ty: &DataType,
) -> Result<(), DataFusionError> {
    match ty {
        DataType::Boolean => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or_else(|| internal!("failed to downcast to BooleanBuilder"))?
            .append_null(),
        DataType::Int8 => builder
            .as_any_mut()
            .downcast_mut::<Int8Builder>()
            .ok_or_else(|| internal!("failed to downcast to Int8Builder"))?
            .append_null(),
        DataType::Int16 => builder
            .as_any_mut()
            .downcast_mut::<Int16Builder>()
            .ok_or_else(|| internal!("failed to downcast to Int16Builder"))?
            .append_null(),
        DataType::Int32 => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .ok_or_else(|| internal!("failed to downcast to Int32Builder"))?
            .append_null(),
        DataType::Int64 => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or_else(|| internal!("failed to downcast to Int64Builder"))?
            .append_null(),
        DataType::UInt8 => builder
            .as_any_mut()
            .downcast_mut::<UInt8Builder>()
            .ok_or_else(|| internal!("failed to downcast to UInt8Builder"))?
            .append_null(),
        DataType::UInt16 => builder
            .as_any_mut()
            .downcast_mut::<UInt16Builder>()
            .ok_or_else(|| internal!("failed to downcast to UInt16Builder"))?
            .append_null(),
        DataType::UInt32 => builder
            .as_any_mut()
            .downcast_mut::<UInt32Builder>()
            .ok_or_else(|| internal!("failed to downcast to UInt32Builder"))?
            .append_null(),
        DataType::UInt64 => builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .ok_or_else(|| internal!("failed to downcast to UInt64Builder"))?
            .append_null(),
        DataType::Decimal128(DEC128_PREC, 0) => builder
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .ok_or_else(|| internal!("failed to downcast to Decimal128Builder"))?
            .append_null(),
        DataType::Decimal256(DEC256_PREC, 0) => builder
            .as_any_mut()
            .downcast_mut::<Decimal256Builder>()
            .ok_or_else(|| internal!("failed to downcast to Decimal256Builder"))?
            .append_null(),
        DataType::Binary => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .ok_or_else(|| internal!("failed to downcast to BinaryBuilder"))?
            .append_null(),
        DataType::FixedSizeBinary(_) => builder
            .as_any_mut()
            .downcast_mut::<FixedSizeBinaryBuilder>()
            .ok_or_else(|| internal!("failed to downcast to FixedSizeBinaryBuilder"))?
            .append_null(),
        DataType::Utf8 => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or_else(|| internal!("failed to downcast to StringBuilder"))?
            .append_null(),
        DataType::List(_) => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or_else(|| internal!("failed to downcast to ListBuilder"))?
            .append_null(),
        DataType::FixedSizeList(f, sz) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| internal!("failed to downcast to FixedSizeListBuilder"))?;
            for _ in 0..*sz {
                append_null_value_to_builder(builder.values(), f.data_type())?;
            }
            builder.append(false);
        }
        DataType::Struct(fields) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| internal!("failed to downcast to StructBuilder"))?;
            for (i, field) in fields.iter().enumerate() {
                append_null_value_to_builder(
                    &mut builder.field_builders_mut()[i],
                    field.data_type(),
                )?;
            }
            builder.append(false);
        }
        _ => return internal_err!("unexpected data type: {}", ty),
    };
    Ok(())
}

fn scalar_to_sol_value(
    arrow_value: ScalarValue,
    sol_type: &DynSolType,
) -> Result<DynSolValue, DataFusionError> {
    match arrow_value {
        ScalarValue::Boolean(Some(b)) => {
            if sol_type == &DynSolType::Bool {
                Ok(DynSolValue::Bool(b))
            } else {
                plan_err!("cannot convert boolean to solidity type {}", sol_type)
            }
        }
        ScalarValue::Int8(Some(i)) => int_to_sol_value(i.try_into().unwrap(), sol_type),
        ScalarValue::Int16(Some(i)) => int_to_sol_value(i.try_into().unwrap(), sol_type),
        ScalarValue::Int32(Some(i)) => int_to_sol_value(i.try_into().unwrap(), sol_type),
        ScalarValue::Int64(Some(i)) => int_to_sol_value(i.try_into().unwrap(), sol_type),
        ScalarValue::UInt8(Some(u)) => uint_to_sol_value(u.try_into().unwrap(), sol_type),
        ScalarValue::UInt16(Some(u)) => uint_to_sol_value(u.try_into().unwrap(), sol_type),
        ScalarValue::UInt32(Some(u)) => uint_to_sol_value(u.try_into().unwrap(), sol_type),
        ScalarValue::UInt64(Some(u)) => uint_to_sol_value(u.try_into().unwrap(), sol_type),
        ScalarValue::Decimal128(Some(d), _, scale)
            if scale <= 0 && matches!(sol_type, DynSolType::Int(_)) =>
        {
            int_to_sol_value(d.try_into().unwrap(), sol_type)
        }
        ScalarValue::Decimal128(Some(d), _, scale) if scale <= 0 => {
            uint_to_sol_value(d.try_into().unwrap(), sol_type)
        }
        ScalarValue::Decimal256(Some(d), _, scale)
            if scale <= 0 && matches!(sol_type, DynSolType::Int(_)) =>
        {
            int_to_sol_value(I256::from_le_bytes(d.to_le_bytes()), sol_type)
        }
        ScalarValue::Decimal256(Some(d), _, scale) if scale <= 0 => {
            uint_to_sol_value(U256::from_le_bytes(d.to_le_bytes()), sol_type)
        }
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => {
            if sol_type == &DynSolType::String {
                Ok(DynSolValue::String(s))
            } else {
                plan_err!("cannot convert string to solidity type {}", sol_type)
            }
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::LargeBinary(Some(b)) => {
            if sol_type == &DynSolType::Bytes {
                Ok(DynSolValue::Bytes(b))
            } else {
                plan_err!("cannot convert bytes to solidity type {}", sol_type)
            }
        }
        ScalarValue::FixedSizeBinary(_, Some(items)) if items.len() <= 32 => match sol_type {
            DynSolType::FixedBytes(len) if *len == items.len() => {
                let mut word = [0; 32];
                word[..items.len()].copy_from_slice(&items);
                Ok(DynSolValue::FixedBytes(
                    alloy::primitives::FixedBytes(word),
                    items.len(),
                ))
            }
            DynSolType::Address if items.len() == 20 => {
                Ok(DynSolValue::Address(Address::from_slice(&items)))
            }
            _ => plan_err!(
                "cannot convert fixed size binary to solidity type {}",
                sol_type
            ),
        },
        ScalarValue::List(items) => match sol_type {
            DynSolType::Array(ty) => {
                let items = items.value(0);
                let mut sol_items = Vec::new();
                for i in 0..items.len() {
                    let sol_value = array_to_sol_value(&items, ty, i)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::Array(sol_items))
            }
            _ => return plan_err!("cannot convert list to solidity type {}", sol_type),
        },
        ScalarValue::FixedSizeList(items) => match sol_type {
            DynSolType::FixedArray(ty, sz) => {
                let items = items.value(0);
                if *sz != items.len() {
                    return plan_err!(
                        "cannot convert fixed size list of size {} to solidity type {}",
                        items.len(),
                        sol_type
                    );
                }
                let mut sol_items = Vec::new();
                for i in 0..items.len() {
                    let sol_value = array_to_sol_value(&items, ty, i)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::FixedArray(sol_items))
            }
            _ => {
                return plan_err!(
                    "cannot convert fixed size list of size {} to solidity type {}",
                    items.len(),
                    sol_type
                )
            }
        },
        ScalarValue::Struct(items) => match sol_type {
            DynSolType::Tuple(tys) => {
                let columns = items.columns();
                if tys.len() != columns.len() {
                    return plan_err!(
                        "cannot convert struct of size {} to solidity type {}",
                        columns.len(),
                        sol_type
                    );
                }
                let mut sol_items = Vec::new();
                for (col, ty) in columns.iter().zip(tys) {
                    let sol_value = array_to_sol_value(col, &ty, 0)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::Tuple(sol_items))
            }
            _ => return plan_err!("cannot convert struct to solidity type {}", sol_type),
        },
        _ => {
            return plan_err!(
                "Unsupported type {} for Solidity type {}",
                arrow_value.data_type(),
                sol_type
            )
        }
    }
}

fn array_to_sol_value(
    ary: &Arc<dyn Array>,
    sol_type: &DynSolType,
    idx: usize,
) -> Result<DynSolValue, DataFusionError> {
    match ary.data_type() {
        DataType::Boolean => {
            if sol_type == &DynSolType::Bool {
                let value = ary
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(idx);
                Ok(DynSolValue::Bool(value))
            } else {
                plan_err!("cannot convert boolean to solidity type {}", sol_type)
            }
        }
        DataType::Int8 => {
            let value = ary.as_any().downcast_ref::<Int8Array>().unwrap().value(idx);
            int_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Int16 => {
            let value = ary
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(idx);
            int_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Int32 => {
            let value = ary
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(idx);
            int_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Int64 => {
            let value = ary
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx);
            int_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::UInt8 => {
            let value = ary
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::UInt16 => {
            let value = ary
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::UInt32 => {
            let value = ary
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::UInt64 => {
            let value = ary
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Decimal128(_, scale) if *scale <= 0 && matches!(sol_type, DynSolType::Int(_)) => {
            let value = ary
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(idx);
            int_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Decimal128(_, scale) if *scale <= 0 => {
            let value = ary
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(value.try_into().unwrap(), sol_type)
        }
        DataType::Decimal256(_, scale) if *scale <= 0 && matches!(sol_type, DynSolType::Int(_)) => {
            let value = ary
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(idx);
            int_to_sol_value(I256::from_le_bytes(value.to_le_bytes()), sol_type)
        }
        DataType::Decimal256(_, scale) if *scale <= 0 => {
            let value = ary
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(idx);
            uint_to_sol_value(U256::from_le_bytes(value.to_le_bytes()), sol_type)
        }
        DataType::Binary => {
            if sol_type == &DynSolType::Bytes {
                let value = ary
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(idx);
                Ok(DynSolValue::Bytes(value.to_vec()))
            } else {
                plan_err!("cannot convert bytes to solidity type {}", sol_type)
            }
        }
        DataType::FixedSizeBinary(_) => match sol_type {
            DynSolType::FixedBytes(len) => {
                let value = ary
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .value(idx);
                if value.len() == *len {
                    let mut word = [0; 32];
                    word[..value.len()].copy_from_slice(value);
                    Ok(DynSolValue::FixedBytes(
                        alloy::primitives::FixedBytes(word),
                        value.len(),
                    ))
                } else {
                    plan_err!(
                            "cannot convert fixed size binary of size {} to Solidity fixed bytes of size {}",
                            value.len(),
                            len
                        )
                }
            }
            DynSolType::Address => {
                let value = ary
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .value(idx);
                if value.len() == 20 {
                    Ok(DynSolValue::Address(Address::from_slice(value)))
                } else {
                    plan_err!("cannot convert fixed size binary of size {} to Solidity address, expected 20 bytes", value.len())
                }
            }
            _ => {
                return plan_err!(
                    "cannot convert fixed size binary to solidity type {}",
                    sol_type
                )
            }
        },
        DataType::Utf8 => {
            if sol_type == &DynSolType::String {
                let value = ary
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(idx);
                Ok(DynSolValue::String(value.to_string()))
            } else {
                plan_err!("cannot convert string to solidity type {}", sol_type)
            }
        }
        DataType::List(_) => match sol_type {
            DynSolType::Array(ty) => {
                let items = ary.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
                let mut sol_items = Vec::new();
                for i in 0..items.len() {
                    let sol_value = array_to_sol_value(&items, ty, i)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::Array(sol_items))
            }
            _ => return plan_err!("cannot convert list to solidity type {}", sol_type),
        },
        DataType::FixedSizeList(_, arrow_sz) => match sol_type {
            DynSolType::FixedArray(ty, sz) => {
                let items = ary
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(idx);
                if *sz != items.len() {
                    return plan_err!(
                        "cannot convert fixed size list of size {} to solidity type {}",
                        items.len(),
                        sol_type
                    );
                }
                let mut sol_items = Vec::new();
                for i in 0..items.len() {
                    let sol_value = array_to_sol_value(&items, ty, i)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::FixedArray(sol_items))
            }
            _ => {
                return plan_err!(
                    "cannot convert fixed size list of size {} to solidity type {}",
                    arrow_sz,
                    sol_type
                )
            }
        },
        DataType::Struct(_) => match sol_type {
            DynSolType::Tuple(tys) => {
                let items = ary.as_any().downcast_ref::<StructArray>().unwrap();
                let columns = items.columns();
                if tys.len() != columns.len() {
                    return plan_err!(
                        "cannot convert struct of size {} to solidity type {}",
                        columns.len(),
                        sol_type
                    );
                }
                let mut sol_items = Vec::new();
                for (col, ty) in columns.iter().zip(tys) {
                    let sol_value = array_to_sol_value(col, &ty, idx)?;
                    sol_items.push(sol_value);
                }
                Ok(DynSolValue::Tuple(sol_items))
            }
            _ => return plan_err!("cannot convert struct to solidity type {}", sol_type),
        },
        _ => {
            return plan_err!(
                "Unsupported type {} for Solidity type {}",
                ary.data_type(),
                sol_type
            )
        }
    }
}

fn sol_to_arrow_type(ty: &DynSolType) -> Result<DataType, DataFusionError> {
    let df = match ty {
        DynSolType::Bool => DataType::Boolean,
        DynSolType::Int(bits) => match *bits {
            8 => DataType::Int8,
            16 => DataType::Int16,
            n if n <= 32 => DataType::Int32,
            n if n <= 64 => DataType::Int64,
            n if n <= DEC_128_MAX_BINARY_PREC => DataType::Decimal128(DEC128_PREC, 0),
            n if n <= DEC_256_MAX_BINARY_PREC => DataType::Decimal256(DEC256_PREC, 0),
            n if n <= 256 => DataType::Utf8,
            _ => return internal_err!("unexpected number of bits for {}: {}", ty, bits),
        },
        DynSolType::Uint(bits) => match *bits {
            8 => DataType::UInt8,
            16 => DataType::UInt16,
            n if n <= 32 => DataType::UInt32,
            n if n <= 64 => DataType::UInt64,
            n if n <= DEC_128_MAX_BINARY_PREC => DataType::Decimal128(DEC128_PREC, 0),
            n if n <= DEC_256_MAX_BINARY_PREC => DataType::Decimal256(DEC256_PREC, 0),
            n if n <= 256 => DataType::Utf8,
            _ => return internal_err!("unexpected number of bits for {}: {}", ty, bits),
        },
        DynSolType::FixedBytes(bytes) => DataType::FixedSizeBinary(*bytes as i32),
        DynSolType::Address => DataType::FixedSizeBinary(20),
        DynSolType::Bytes => DataType::Binary,
        DynSolType::String => DataType::Utf8,
        DynSolType::Array(ty) => {
            DataType::List(Arc::new(Field::new("item", sol_to_arrow_type(ty)?, true)))
        }
        DynSolType::FixedArray(ty, sz) => DataType::FixedSizeList(
            Arc::new(Field::new("item", sol_to_arrow_type(ty)?, true)),
            *sz as i32,
        ),
        DynSolType::Tuple(tys) => {
            let mut fields = Vec::new();
            for (i, ty) in tys.iter().enumerate() {
                let field = Field::new(format!("c{i}"), sol_to_arrow_type(ty)?, true);
                fields.push(field);
            }
            DataType::Struct(Fields::from_iter(fields))
        }
        DynSolType::Function => {
            return plan_err!("cannot convert solidity type {} to arrow data type", ty)
        }
    };
    Ok(df)
}

fn int_to_sol_value(s: I256, sol_type: &DynSolType) -> Result<DynSolValue, DataFusionError> {
    let sz = match sol_type {
        DynSolType::Int(sz) => *sz,
        _ => return plan_err!("expected int type, got {}", sol_type),
    };
    if sz == 256 {
        // In this case, the value can definitely fit.
        return Ok(DynSolValue::Int(s, sz));
    }
    let sz_u64 = u64::try_from(sz).unwrap();
    let min = I256::try_from(2)
        .unwrap()
        .checked_pow((sz_u64 - 1).try_into().unwrap())
        .unwrap()
        .checked_neg()
        .unwrap();
    let max = I256::try_from(2)
        .unwrap()
        .checked_pow((sz_u64 - 1).try_into().unwrap())
        .unwrap()
        .checked_sub(1.try_into().unwrap())
        .unwrap();
    if s < min || s > max {
        return plan_err!(
            "cannot convert int to solidity int{}: {} not in [{}, {}]",
            sz_u64,
            s,
            min,
            max
        );
    }
    Ok(DynSolValue::Int(s, sz))
}

fn uint_to_sol_value(s: U256, sol_type: &DynSolType) -> Result<DynSolValue, DataFusionError> {
    let sz = match sol_type {
        DynSolType::Uint(sz) => *sz,
        _ => return plan_err!("expected uint type, got {}", sol_type),
    };
    if sz == 256 {
        // In this case, the value can definitely fit.
        return Ok(DynSolValue::Uint(s, sz));
    }
    let sz_u64 = u64::try_from(sz).unwrap();
    let limit = U256::try_from(2)
        .unwrap()
        .checked_pow(sz_u64.try_into().unwrap())
        .unwrap();
    if s >= limit {
        return plan_err!(
            "cannot convert uint to solidity uint{}: {} not in [0, {})",
            sz_u64,
            s,
            limit
        );
    }
    Ok(DynSolValue::Uint(s, sz))
}

fn num_rows(args: &[ColumnarValue]) -> usize {
    for arg in args {
        match arg {
            ColumnarValue::Array(array) => return array.len(),
            ColumnarValue::Scalar(_) => {}
        }
    }
    1
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use alloy::{
        hex,
        hex::FromHex as _,
        primitives::{Bytes, B256},
    };
    use datafusion::arrow::array::StringArray;

    use super::*;
    use crate::arrow::{
        array::{
            BinaryBuilder, Decimal256Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
            Int32Array, StructArray,
        },
        datatypes::i256,
    };

    fn parse_dec256<const N: usize>(vals: [&str; N]) -> Decimal256Array {
        // It's tempting to use Decimal256Array::from_iter_values but that
        // produces values with type Decimal(76, 10) which makes tests fail.
        let mut builder = Decimal256Builder::new()
            .with_precision_and_scale(DEC256_PREC, 0)
            .unwrap();
        for val in vals {
            let val = val.parse::<i256>().unwrap();
            builder.append_value(val);
        }
        builder.finish()
    }

    // Signature of a Uniswap v3 swap
    const SIG: &str =
            "Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)";

    // topic0: Swap event
    static TOPIC_0: LazyLock<B256> = LazyLock::new(|| {
        hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67").into()
    });

    // Transaction hashes for swaps from blocks 19644517 to 19644520. All
    // the data below is from logs of these transactions, in the same order.
    const _TXNS: [&str; 5] = [
        "0x188a6b9d93445f2c959258930856474748721b58eef76a49a265e08ef4588cd1",
        "0x7dfe031d6adc1e44adc8ab0ffb0a0cfdf0dbf216e2ffd645dbd3b032f8c484a7",
        "0xf29e151ea921af7f5b313f6834fefd4ac5295a5ba0f42ab9eccb80c4c46f40b3",
        "0x7c5125dc05dce3f9ee12883ff7f055284c31938a27a9e9fabc9a897d67e97da6",
        "0x5137bd503211dec3359d87825a15e1759d0e1e9a66556ee397212860f030643f",
    ];
    // topic1, topic2, and data for those transactions. topic3 is always empty
    #[rustfmt::skip]
        const CSV: [(&str,&str,&str);5] =
          [("0x000000000000000000000000e43ca1dee3f0fc1e2df73a0745674545f11a59f5",
            "0x00000000000000000000000010bff0723fa78a7c31260a9cbe7aa6ff470905d1",
            "0x000000000000000000000000000000000000000000000000000000003b9aca00\
             fffffffffffffffffffffffffffffffffffffffffffffffffbbe45052a28056300\
             0000000000000000000000000000000000446e1e52218e5b236f8735ca89440000\
             000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
             000000000000000000000000000000000000000000000000000002fb65"),
           ("0x0000000000000000000000001111111254eeb25477b68fb85ed929f73a960582",
            "0x0000000000000000000000001111111254eeb25477b68fb85ed929f73a960582",
            "0x0000000000000000000000000000000000000000000000000000000095ed66b5\
             fffffffffffffffffffffffffffffffffffffffffffffffff54af50c6b89978100\
             0000000000000000000000000000000000446e00ba767226fd3681ec499dd50000\
             000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
             000000000000000000000000000000000000000000000000000002fb65"),
           ("0x000000000000000000000000def1c0ded9bec7f1a1670819833240f027b25eff",
            "0x0000000000000000000000002bf39a1004ff433938a5f933a44b8dad377937f6",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffff486d4238\
             0000000000000000000000000000000000000000000000000d1f893781823b9f00\
             0000000000000000000000000000000000446e24fadfd4b87fa4adb40e37820000\
             000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
             000000000000000000000000000000000000000000000000000002fb65"),
           ("0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",
            "0x000000000000000000000000e1525583c72de1a3dada24f761007ba8a560e220",
            "0x000000000000000000000000000000000000000000000000000000011303cef5\
             ffffffffffffffffffffffffffffffffffffffffffffffffec5c0f7fe74d8dd700\
             0000000000000000000000000000000000446deeb2b3c9adc67ccca1b5a8b90000\
             000000000000000000000000000000000000000000005ca05c4528d95aa2000000\
             000000000000000000000000000000000000000000000000000002fb65"),
           ("0x000000000000000000000000767c8bb1574bee5d4fe35e27e0003c89d43c5121",
            "0x0000000000000000000000002d722c96f79d149dd21e9ef36f93fc12906ce9f8",
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffdd404a44ec\
             0000000000000000000000000000000000000000000000027c2bb0000000000000\
             00000000000000000000000000000000004474ca36a10f972d98019c42e0620000\
             000000000000000000000000000000000000000000005cbb7a5b248d7479000000\
             000000000000000000000000000000000000000000000000000002fb6d")];

    // Decoded data for TXNS from etherscan
    static SENDERS: LazyLock<FixedSizeBinaryArray> = LazyLock::new(|| {
        FixedSizeBinaryArray::try_from_iter(
            CSV.into_iter()
                .map(|(t1, _, _)| B256::from_hex(t1).unwrap()[12..32].to_vec()),
        )
        .expect("sender can be turned into FixedSizeBinaryArray")
    });
    static RECIPIENTS: LazyLock<FixedSizeBinaryArray> = LazyLock::new(|| {
        FixedSizeBinaryArray::try_from_iter(
            CSV.into_iter()
                .map(|(_, t2, _)| B256::from_hex(t2).unwrap()[12..32].to_vec()),
        )
        .expect("recipient can be turned into FixedSizeBinaryArray")
    });
    static AMOUNT0: LazyLock<StringArray> = LazyLock::new(|| {
        StringArray::from(vec![
            "1000000000",
            "2515363509",
            "-3079847368",
            "4613983989",
            "-149245246228",
        ])
    });
    static AMOUNT1: LazyLock<StringArray> = LazyLock::new(|| {
        StringArray::from(vec![
            "-306731836130196125",
            "-771534952448026751",
            "945625318260095903",
            "-1415239140885295657",
            "45840926746167214080",
        ])
    });
    static SQRT_PRICE_X96: LazyLock<Decimal256Array> = LazyLock::new(|| {
        parse_dec256([
            "1387928334765558613523830189754692",
            "1387919176344430097549424633421269",
            "1387930395673702867992718976366466",
            "1387913596232525690981973365205177",
            "1388456901914535021316446911389794",
        ])
    });
    static LIQUIDITY: LazyLock<Decimal256Array> = LazyLock::new(|| {
        parse_dec256([
            "6674436099870907042",
            "6674436099870907042",
            "6674436099870907042",
            "6674436099870907042",
            "6682069004008125561",
        ])
    });
    static TICK: LazyLock<Int32Array> = LazyLock::new(|| {
        Int32Array::from_iter(
            ["195429", "195429", "195429", "195429", "195437"]
                .into_iter()
                .map(|s| s.parse::<i32>().unwrap()),
        )
    });

    #[test]
    fn topic0_for_signature() {
        let event = Event::try_from(&ScalarValue::new_utf8(SIG)).unwrap();
        let topic0 = event.topic0().unwrap();
        assert_eq!(*TOPIC_0, topic0);
    }

    #[test]
    fn invoke_evm_topic() {
        let evm_topic = EvmTopic::new();
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            SIG.to_string(),
        )))];

        let arg_fields = vec![Field::new("signature", DataType::Utf8, false)];
        let return_field = Field::new(evm_topic.name(), EvmTopic::RETURN_TYPE, false);

        let args = ScalarFunctionArgs {
            args,
            arg_fields: arg_fields.iter().by_ref().collect(),
            number_rows: 1,
            return_field: &return_field,
        };
        let result = evm_topic.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };

        let exp = ScalarValue::FixedSizeBinary(32, Some(TOPIC_0.to_vec()));
        assert_eq!(exp, result);
    }

    #[test]
    fn invoke_evm_decode() {
        let evm_decode = EvmDecode::new();
        let topic1 = CSV
            .into_iter()
            .map(|(topic, _, _)| B256::from_hex(topic).unwrap().to_vec())
            .fold(FixedSizeBinaryBuilder::new(32), |mut builder, topic| {
                builder.append_value(topic).unwrap();
                builder
            })
            .finish();
        let topic2 = CSV
            .into_iter()
            .map(|(_, topic, _)| B256::from_hex(topic).unwrap().to_vec())
            .fold(FixedSizeBinaryBuilder::new(32), |mut builder, topic| {
                builder.append_value(topic).unwrap();
                builder
            })
            .finish();
        let topic3 = CSV
            .into_iter()
            .fold(FixedSizeBinaryBuilder::new(32), |mut builder, _| {
                builder.append_null();
                builder
            })
            .finish();
        let data = CSV
            .into_iter()
            .map(|(_, _, data)| Bytes::from_hex(data).unwrap().to_vec())
            .fold(BinaryBuilder::new(), |mut builder, topic| {
                builder.append_value(topic);
                builder
            })
            .finish();
        let args = vec![
            ColumnarValue::Array(Arc::new(topic1)),
            ColumnarValue::Array(Arc::new(topic2)),
            ColumnarValue::Array(Arc::new(topic3)),
            ColumnarValue::Array(Arc::new(data)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(SIG.to_string()))),
        ];

        let arg_fields = vec![
            Field::new("topic1", DataType::FixedSizeBinary(32), true),
            Field::new("topic2", DataType::FixedSizeBinary(32), true),
            Field::new("topic3", DataType::FixedSizeBinary(32), true),
            Field::new("data", DataType::Binary, true),
            Field::new("signature", DataType::Utf8, false),
        ];

        let args = ScalarFunctionArgs {
            args,
            arg_fields: arg_fields.iter().by_ref().collect(),
            number_rows: CSV.len(),
            return_field: &Field::new_struct(
                evm_decode.name(),
                Event::try_from(&ScalarValue::new_utf8(SIG))
                    .unwrap()
                    .fields()
                    .unwrap(),
                false,
            ),
        };
        let result = evm_decode.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array, got {:?}", result);
        };

        #[track_caller]
        fn cmp(act: &dyn Array, col: usize, exp: &dyn Array, name: &str) {
            let structs = act.as_any().downcast_ref::<StructArray>().unwrap();
            let act = structs.column(col);
            assert_eq!(exp, act, "field {}", name);
        }

        cmp(&result, 0, &*SENDERS, "sender");
        cmp(&result, 1, &*RECIPIENTS, "recipient");
        cmp(&result, 2, &*AMOUNT0, "amount0");
        cmp(&result, 3, &*AMOUNT1, "amount1");
        cmp(&result, 4, &*SQRT_PRICE_X96, "sqrt_price_x96");
        cmp(&result, 5, &*LIQUIDITY, "liquidity");
        cmp(&result, 6, &*TICK, "tick");
    }
}
