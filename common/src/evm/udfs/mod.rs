use std::{any::Any, str::FromStr, sync::Arc};

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
use alloy::{
    dyn_abi::{DynSolType, DynSolValue, DynToken, Specifier as _},
    json_abi::Event as AlloyEvent,
    primitives::{
        ruint::{mask, FromUintError},
        BigIntConversionError, Signed, B256,
    },
};
use datafusion::{
    arrow::array::ArrayBuilder,
    common::{internal_err, plan_err, ExprSchema},
    error::DataFusionError,
    logical_expr::{
        simplify::{ExprSimplifyResult, SimplifyInfo},
        ColumnarValue, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use itertools::izip;
use log::trace;

type Unsigned = alloy::primitives::Uint<256, 4>;

const DEC128_PREC: u8 = DECIMAL128_MAX_PRECISION;
const DEC256_PREC: u8 = DECIMAL256_MAX_PRECISION;

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

fn arrow_type(ty: &DynSolType) -> Result<DataType, DataFusionError> {
    use DataType as F;
    use DynSolType as S;
    let df = match ty {
        S::Bool => F::Boolean,
        S::Int(bits) => match *bits {
            8 => F::Int8,
            16 => F::Int16,
            n if n <= 32 => F::Int32,
            n if n <= 64 => F::Int64,
            n if n <= 126 => F::Decimal128(DEC128_PREC, 0),
            n if n <= 256 => F::Decimal256(DEC256_PREC, 0),
            _ => return internal_err!("unexpected number of bits for {}: {}", ty, bits),
        },
        S::Uint(bits) => match *bits {
            8 => F::UInt8,
            16 => F::UInt16,
            n if n <= 32 => F::UInt32,
            n if n <= 64 => F::UInt64,
            n if n <= 126 => F::Decimal128(DEC128_PREC, 0),
            n if n <= 256 => F::Decimal256(DEC256_PREC, 0),
            _ => return internal_err!("unexpected number of bits for {}: {}", ty, bits),
        },
        S::FixedBytes(bytes) => F::FixedSizeBinary(*bytes as i32),
        S::Address => F::FixedSizeBinary(20),
        S::Bytes => F::Binary,
        S::String => F::Utf8,
        S::Function | S::Array(_) | S::FixedArray(_, _) | S::Tuple(_) => {
            return plan_err!("cannot convert solidity type {} to arrow data type", ty)
        }
    };
    Ok(df)
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

    fn field<T>(&'a mut self) -> Result<&'a mut T, DataFusionError>
    where
        T: ArrayBuilder,
    {
        self.builder
            .field_builder(self.field)
            .ok_or_else(|| internal!("failed to get field builder for field {}", self.field))
    }

    fn append_null_value(&'a mut self) -> Result<(), DataFusionError> {
        let ty = arrow_type(&self.ty)?;
        match ty {
            DataType::Boolean => self.field::<BooleanBuilder>()?.append_null(),
            DataType::Int8 => self.field::<Int8Builder>()?.append_null(),
            DataType::Int16 => self.field::<Int16Builder>()?.append_null(),
            DataType::Int32 => self.field::<Int32Builder>()?.append_null(),
            DataType::Int64 => self.field::<Int64Builder>()?.append_null(),
            DataType::UInt8 => self.field::<UInt8Builder>()?.append_null(),
            DataType::UInt16 => self.field::<UInt16Builder>()?.append_null(),
            DataType::UInt32 => self.field::<UInt32Builder>()?.append_null(),
            DataType::UInt64 => self.field::<UInt64Builder>()?.append_null(),
            DataType::Decimal128(DEC128_PREC, 0) => {
                self.field::<Decimal128Builder>()?.append_null()
            }
            DataType::Decimal256(DEC256_PREC, 0) => {
                self.field::<Decimal256Builder>()?.append_null()
            }
            DataType::Binary => self.field::<BinaryBuilder>()?.append_null(),
            DataType::FixedSizeBinary(_) => self.field::<FixedSizeBinaryBuilder>()?.append_null(),
            DataType::Utf8 => self.field::<StringBuilder>()?.append_null(),
            _ => return internal_err!("unexpected data type: {}", ty),
        };
        Ok(())
    }

    /// Convert `value` to a DF value and append it to the appropriate
    /// field. Most conversions are pretty straightforward, but conversion
    /// of large integers is a headache.
    ///
    ///  DF only provides up to 64 bit integers, but solidity supports
    /// signed and unsigned integers up to 256 bits. For those, we use
    /// `Decimal128` or `Decimal256` types. But since they are decimal
    /// types, they can't represent the full 128 or 256 bits of the original
    /// integer, and for `uint256` we can only represent roughly half the
    /// range as DF doesn't have any unsigned 256 bit integer and we have to
    /// resort to `i256`. In cases where the conversion would not be
    /// possible without loss, we convert to `null`
    fn append_value(&'a mut self, value: DynSolValue) -> Result<(), DataFusionError> {
        fn primitive_int<'a>(
            builder: &'a mut FieldBuilder<'a>,
            s: Signed<256, 4>,
            bits: usize,
        ) -> Result<(), AppendError> {
            match bits {
                8 => builder
                    .field::<Int8Builder>()?
                    .append_value(i8::try_from(s)?),
                16 => builder
                    .field::<Int16Builder>()?
                    .append_value(i16::try_from(s)?),
                n if n <= 32 => builder
                    .field::<Int32Builder>()?
                    .append_value(i32::try_from(s)?),
                n if n <= 64 => builder
                    .field::<Int64Builder>()?
                    .append_value(i64::try_from(s)?),
                n if n <= 128 => {
                    let val = i128::try_from(s)?;
                    let builder = builder.field::<Decimal128Builder>()?;
                    match validate_decimal_precision(val, DEC128_PREC) {
                        Ok(_) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    }
                }
                n if n <= 256 => {
                    let val = i256::from_le_bytes(s.to_le_bytes());
                    let builder = builder.field::<Decimal256Builder>()?;
                    match validate_decimal256_precision(val, DEC256_PREC) {
                        Ok(_) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    }
                }
                _ => unreachable!("unexpected number of bits for int: {}", bits),
            };
            Ok(())
        }

        fn primitive_uint<'a>(
            builder: &'a mut FieldBuilder<'a>,
            u: Unsigned,
            bits: usize,
        ) -> Result<(), AppendError> {
            match bits {
                8 => builder
                    .field::<UInt8Builder>()?
                    .append_value(u8::try_from(u)?),
                16 => builder
                    .field::<UInt16Builder>()?
                    .append_value(u16::try_from(u)?),
                n if n <= 32 => builder
                    .field::<UInt32Builder>()?
                    .append_value(u32::try_from(u)?),
                n if n <= 64 => builder
                    .field::<UInt64Builder>()?
                    .append_value(u64::try_from(u)?),
                n if n <= 256 => {
                    // Some Voodoo to get 2^255 - 1, the maximum positive
                    // value an `i256` can represent. See the implementation
                    // of Uint in alloy_core for where this all comes from
                    const LIMBS: usize = 4;
                    const BITS: usize = 256;
                    const MASK: u64 = mask(BITS - 1);
                    const MAX_SIGNED: Unsigned = {
                        let mut limbs = [u64::MAX; LIMBS];
                        limbs[LIMBS - 1] &= MASK;
                        Unsigned::from_limbs(limbs)
                    };
                    let field_builder = builder.field::<Decimal256Builder>()?;
                    if u > MAX_SIGNED {
                        field_builder.append_null()
                    } else {
                        let val = i256::from_le_bytes(u.to_le_bytes());
                        match validate_decimal256_precision(val, DEC256_PREC) {
                            Ok(_) => field_builder.append_value(val),
                            Err(_) => field_builder.append_null(),
                        }
                    }
                }
                _ => unreachable!("unexpected number of bits for uint: {}", bits),
            };
            Ok(())
        }

        use DynSolValue::*;
        match value {
            Bool(b) => self.field::<BooleanBuilder>()?.append_value(b),
            Int(s, bits) => match bits {
                n if n <= 256 => primitive_int(self, s, bits)
                    .or_else(|e| internal_err!("error converting int{}: {}", bits, e))?,
                _ => return internal_err!("unexpected number of bits for int{}", bits),
            },
            Uint(u, bits) => match bits {
                n if n <= 256 => primitive_uint(self, u, bits)
                    .or_else(|e| internal_err!("error converting uint{}: {}", bits, e))?,
                _ => return internal_err!("unexpected number of bits for uint{}", bits),
            },
            FixedBytes(b, _) => self.field::<FixedSizeBinaryBuilder>()?.append_value(b)?,
            Address(a) => self.field::<FixedSizeBinaryBuilder>()?.append_value(a)?,
            Bytes(b) => self.field::<BinaryBuilder>()?.append_value(b),
            String(s) => self.field::<StringBuilder>()?.append_value(s),
            Function(_) | Tuple(_) | Array(_) | FixedArray(_) => {
                return plan_err!("cannot convert function to arrow scalar")
            }
        };
        Ok(())
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
            let df = arrow_type(ty)?;
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

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
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

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> datafusion::error::Result<DataType> {
        if args.len() != 5 {
            return internal_err!(
                "{}: expected at 5 arguments, but got {}",
                self.name(),
                args.len()
            );
        }
        let signature = &args[4];
        let signature = match signature {
            Expr::Literal(scalar) => scalar,
            _ => {
                return plan_err!(
                    "{}: expected a string literal for the signature",
                    self.name()
                )
            }
        };
        let event = Event::try_from(signature).map_err(|e| e.context(self.name()))?;
        let fields = event.fields()?;
        Ok(DataType::Struct(fields))
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
        Ok(DataType::FixedSizeBinary(32))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
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

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use crate::arrow::{
        array::{
            BinaryBuilder, Decimal256Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
            Int32Array, StructArray,
        },
        datatypes::i256,
    };
    use alloy::{
        hex,
        hex::FromHex as _,
        primitives::{Bytes, B256},
    };

    use super::*;

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
    static AMOUNT0: LazyLock<Decimal256Array> = LazyLock::new(|| {
        parse_dec256([
            "1000000000",
            "2515363509",
            "-3079847368",
            "4613983989",
            "-149245246228",
        ])
    });
    static AMOUNT1: LazyLock<Decimal256Array> = LazyLock::new(|| {
        parse_dec256([
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

        let result = evm_topic.invoke_batch(&args, 1).unwrap();
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

        let result = evm_decode.invoke_batch(&args, CSV.len()).unwrap();
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
