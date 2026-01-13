use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{array::NullBuilder, datatypes::FieldRef},
    common::{internal_err, plan_err},
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        simplify::{ExprSimplifyResult, SimplifyInfo},
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use itertools::izip;

use crate::{
    BYTES32_TYPE, Bytes32ArrayType,
    arrow::{
        array::{Array, BinaryArray, StructBuilder},
        datatypes::{DataType, Field},
    },
    evm::udfs::Event,
};

/// DataFusion UDF that decodes EVM event logs into structured data.
///
/// This function parses Solidity event logs using the event signature to extract
/// indexed and non-indexed parameters. Indexed reference types (string, bytes, arrays)
/// are stored as their keccak256 hash in topics rather than the actual data.
///
/// # SQL Usage
///
/// ```ignore
/// // Decode a Uniswap V3 Swap event
/// evm_decode_log(
///     topic1, topic2, topic3, data,
///     'Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)'
/// )
///
/// // Decode an ERC20 Transfer event
/// evm_decode_log(
///     topic1, topic2, topic3, data,
///     'Transfer(address indexed from,address indexed to,uint256 value)'
/// )
/// ```
///
/// # Arguments
///
/// * `topic1` - `FixedSizeBinary(32)` first indexed topic (after topic0)
/// * `topic2` - `FixedSizeBinary(32)` second indexed topic
/// * `topic3` - `FixedSizeBinary(32)` third indexed topic
/// * `data` - `Binary` non-indexed event data
/// * `signature` - `Utf8` Solidity event signature (e.g., "Transfer(address indexed,address indexed,uint256)")
///
/// # Returns
///
/// A struct containing decoded event parameters. Field types are determined by the
/// Solidity types in the signature. Returns `Null` type if the event has no parameters.
///
/// # Errors
///
/// Returns a planning error if:
/// - Signature is not a valid Solidity event signature
/// - Signature is not provided as a string literal
/// - Number of arguments is not 5
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmDecodeLog {
    name: &'static str,
    signature: Signature,
}

impl Default for EvmDecodeLog {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmDecodeLog {
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
        Self {
            name: "evm_decode_log",
            signature,
        }
    }

    pub fn with_deprecated_name(self) -> Self {
        Self {
            name: "evm_decode",
            ..self
        }
    }
}

impl ScalarUDFImpl for EvmDecodeLog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
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
                );
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
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
                );
            }
        };
        let event = Event::try_from(signature).map_err(|e| e.context(self.name()))?;
        let fields = event.fields()?;
        if fields.is_empty() {
            return Ok(Field::new(self.name(), DataType::Null, true).into());
        }
        Ok(Field::new_struct(self.name(), fields, false).into())
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> datafusion::error::Result<ExprSimplifyResult> {
        Ok(ExprSimplifyResult::Original(args))
    }
}

fn decode(
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

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use alloy::{
        hex,
        hex::FromHex as _,
        primitives::{B256, Bytes},
    };
    use datafusion::arrow::array::{Decimal256Builder, StringArray};

    use super::*;
    use crate::{
        arrow::{
            array::{
                BinaryBuilder, Decimal256Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
                Int32Array, StructArray,
            },
            datatypes::i256,
        },
        evm::udfs::DEC256_PREC,
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
    const SIG: &str = "Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)";

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
    fn invoke_evm_decode_log() {
        let evm_decode_log = EvmDecodeLog::new();
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
            Field::new("topic1", DataType::FixedSizeBinary(32), true).into(),
            Field::new("topic2", DataType::FixedSizeBinary(32), true).into(),
            Field::new("topic3", DataType::FixedSizeBinary(32), true).into(),
            Field::new("data", DataType::Binary, true).into(),
            Field::new("signature", DataType::Utf8, false).into(),
        ];

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: CSV.len(),
            return_field: Field::new_struct(
                evm_decode_log.name(),
                Event::try_from(&ScalarValue::new_utf8(SIG))
                    .unwrap()
                    .fields()
                    .unwrap(),
                false,
            )
            .into(),
            config_options: Default::default(),
        };
        let result = evm_decode_log.invoke_with_args(args).unwrap();
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

    #[test]
    fn evm_decode_log_indexed_reference_types() {
        // Regression test for indexed reference type handling.
        // When reference types (string, bytes, arrays, tuples) are indexed in Solidity events,
        // they are stored as their keccak256 hash (bytes32) in the topics, not as the actual data.
        const EVENT_SIG: &str = "DataStored(string indexed key, bytes indexed data, uint256 value)";

        let evm_decode_log = EvmDecodeLog::new();

        // Build topic arrays for a single DataStored event
        // topic0 would be the event signature hash (not needed for decode)
        // topic1: keccak256("myKey") - indexed string stored as hash
        let mut topic1_builder = FixedSizeBinaryBuilder::new(32);
        let topic1_value = hex!("a2e0a8d4e5b3c2e1e5c8d0f2a1b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1");
        topic1_builder.append_value(topic1_value).unwrap();
        let topic1 = topic1_builder.finish();

        // topic2: keccak256(someBytes) - indexed bytes stored as hash
        let mut topic2_builder = FixedSizeBinaryBuilder::new(32);
        let topic2_value = hex!("b3f1c9e5d7a8f2b4c6e0d8f1a3b5c7e9d1f3a5b7c9e1f3a5b7c9e1f3a5b7c9e1");
        topic2_builder.append_value(topic2_value).unwrap();
        let topic2 = topic2_builder.finish();

        // topic3: null (only 2 indexed params)
        let mut topic3_builder = FixedSizeBinaryBuilder::new(32);
        topic3_builder.append_null();
        let topic3 = topic3_builder.finish();

        // data: value (non-indexed uint256)
        let mut data_builder = BinaryBuilder::new();
        data_builder.append_value(hex!(
            "000000000000000000000000000000000000000000000000000000000000002a"
        ));
        let data = data_builder.finish();

        let args = vec![
            ColumnarValue::Array(Arc::new(topic1)),
            ColumnarValue::Array(Arc::new(topic2)),
            ColumnarValue::Array(Arc::new(topic3)),
            ColumnarValue::Array(Arc::new(data)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(EVENT_SIG.to_string()))),
        ];

        let arg_fields = vec![
            Field::new("topic1", DataType::FixedSizeBinary(32), true).into(),
            Field::new("topic2", DataType::FixedSizeBinary(32), true).into(),
            Field::new("topic3", DataType::FixedSizeBinary(32), true).into(),
            Field::new("data", DataType::Binary, true).into(),
            Field::new("signature", DataType::Utf8, false).into(),
        ];

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Field::new_struct(
                evm_decode_log.name(),
                Event::try_from(&ScalarValue::new_utf8(EVENT_SIG))
                    .unwrap()
                    .fields()
                    .unwrap(),
                false,
            )
            .into(),
            config_options: Default::default(),
        };

        let result = evm_decode_log.invoke_with_args(args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array, got {:?}", result);
        };

        // Verify the result is a struct array with the correct schema
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        // Check that we have 3 fields: key, data, value
        assert_eq!(struct_array.num_columns(), 3, "expected 3 fields");

        // Verify field types - this is the critical part of the test
        let fields = struct_array.fields();

        // The indexed string "key" should be FixedSizeBinary(32) because it's stored as a hash
        assert_eq!(fields[0].name(), "key", "first field should be 'key'");
        assert_eq!(
            fields[0].data_type(),
            &DataType::FixedSizeBinary(32),
            "indexed string should be FixedSizeBinary(32) (keccak256 hash)"
        );

        // The indexed bytes "data" should be FixedSizeBinary(32) because it's stored as a hash
        assert_eq!(fields[1].name(), "data", "second field should be 'data'");
        assert_eq!(
            fields[1].data_type(),
            &DataType::FixedSizeBinary(32),
            "indexed bytes should be FixedSizeBinary(32) (keccak256 hash)"
        );

        // The non-indexed uint256 "value" should be Utf8 (as uint256 is too large for Decimal)
        assert_eq!(fields[2].name(), "value", "third field should be 'value'");
        assert_eq!(
            fields[2].data_type(),
            &DataType::Utf8,
            "value should be Utf8 (uint256 is too large for Decimal types)"
        );

        // Verify the actual values
        let key_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(key_array.value(0), topic1_value, "key hash should match");

        let data_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(data_array.value(0), topic2_value, "data hash should match");

        let value_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value_array.value(0), "42", "value should match");
    }
}
