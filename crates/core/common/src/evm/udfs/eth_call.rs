use std::{any::Any, str::FromStr};

use alloy::{
    eips::BlockNumberOrTag,
    hex,
    network::{AnyNetwork, Ethereum},
    primitives::{Address, Bytes, TxKind},
    providers::Provider,
    rpc::{json_rpc::ErrorPayload, types::TransactionInput},
    transports::RpcError,
};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayBuilder, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, Int64Array,
            StringArray, StringBuilder, StructBuilder, UInt64Array,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{internal_err, plan_err, utils::quote_identifier},
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        async_udf::AsyncScalarUDFImpl,
    },
};
use itertools::izip;

use crate::plan;

type TransactionRequest = <Ethereum as alloy::network::Network>::TransactionRequest;

/// DataFusion UDF that executes an `eth_call` against an Ethereum JSON-RPC endpoint.
///
/// This async UDF performs read-only contract calls at a specified block, returning
/// the call result or error message. It's commonly used for querying contract state,
/// simulating transactions, or reading view functions without sending a transaction.
///
/// # SQL Usage
///
/// ```ignore
/// // Call a contract function at the latest block
/// eth_call(NULL, 0x1234...address, 0xabcd...calldata, 'latest')
///
/// // Call with a specific sender address at block 19000000
/// eth_call(0xsender...addr, 0xcontract...addr, 0xcalldata, '19000000')
///
/// // Query token balance (balanceOf call)
/// eth_call(NULL, token_address, balanceOf_calldata, 'latest')
/// ```
///
/// # Arguments
///
/// * `from` - `FixedSizeBinary(20)` sender address (optional, can be NULL)
/// * `to` - `FixedSizeBinary(20)` target contract address (required)
/// * `input` - `Binary` encoded function call data (optional)
/// * `block` - `Utf8` or `UInt64` or `Int64` block number or tag ("latest", "pending", "earliest")
///
/// # Returns
///
/// A struct with two fields:
/// * `data` - `Binary` the return data from the call (NULL on error)
/// * `message` - `Utf8` error message if the call reverted (NULL on success)
///
/// # Errors
///
/// Returns a planning error if:
/// - `to` address is NULL
/// - `block` is NULL
/// - `block` is not a valid integer or block tag
/// - Address conversion fails
#[derive(Debug, Clone)]
pub struct EthCall {
    name: String,
    client: alloy::providers::RootProvider<AnyNetwork>,
    signature: Signature,
    fields: Fields,
}

impl PartialEq for EthCall {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature && self.fields == other.fields
    }
}

impl Eq for EthCall {}

impl std::hash::Hash for EthCall {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.fields.hash(state);
    }
}

impl EthCall {
    pub fn new(
        sql_table_ref_schema: &str,
        client: alloy::providers::RootProvider<AnyNetwork>,
    ) -> Self {
        // Create UDF name with quoted schema to match how DataFusion's query planner
        // resolves qualified function references (e.g., "_/anvil_rpc@0.0.0".eth_call)
        let name = format!("{}.eth_call", quote_identifier(sql_table_ref_schema));

        EthCall {
            name,
            client,
            signature: Signature {
                type_signature: TypeSignature::Any(4),
                volatility: Volatility::Volatile,
                parameter_names: Some(vec![
                    "from".to_string(),
                    "to".to_string(),
                    "input_data".to_string(),
                    "block".to_string(),
                ]),
            },
            fields: Fields::from_iter([
                Field::new("data", DataType::Binary, true),
                Field::new("message", DataType::Utf8, true),
            ]),
        }
    }
}

impl ScalarUDFImpl for EthCall {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Struct(self.fields.clone()))
    }

    /// Since this is an async UDF, the `invoke_with_args` method will not be called.
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        unreachable!("is only called as async UDF");
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for EthCall {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        let name = self.name().to_string();
        let client = self.client.clone();
        let fields = self.fields.clone();
        // Decode the arguments.
        let args: Vec<_> = ColumnarValue::values_to_arrays(&args.args)?;
        let [from, to, input_data, block] = args.as_slice() else {
            return internal_err!("{}: expected 4 arguments, but got {}", name, args.len());
        };

        // from: Optional, only accepts address
        let from = match from.data_type() {
            DataType::Null => {
                let from_len = from.len();
                &FixedSizeBinaryArray::new_null(20, from_len)
            }
            DataType::FixedSizeBinary(20) => from
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap(),
            _ => return plan_err!("{}: 'from' address is not a valid address", name),
        };
        // to: Required, only accepts address
        let to = match to.data_type() {
            DataType::FixedSizeBinary(20) => {
                to.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap()
            }
            _ => return plan_err!("{}: 'to' address is not a valid address", name),
        };
        // input_data: Optional, only accepts binary
        let input_data = match input_data.data_type() {
            DataType::Null => {
                let input_data_len = input_data.len();
                &BinaryArray::new_null(input_data_len)
            }
            DataType::Binary => input_data.as_any().downcast_ref::<BinaryArray>().unwrap(),
            _ => return plan_err!("{}: input data is not a valid data", name),
        };
        // block: Required, accepts block number (UInt64/Int64) or tag (string)
        let parse_block_str = |s: &str| -> Result<BlockNumberOrTag, DataFusionError> {
            u64::from_str(s)
                .map(BlockNumberOrTag::Number)
                .or_else(|_| BlockNumberOrTag::from_str(s))
                .map_err(|_| {
                    plan!("block is not a valid integer, \"0x\" prefixed hex integer, or tag")
                })
        };

        let block_null_err = || plan!("'block' is NULL");
        let downcast_err = |expected| plan!("Failed to downcast block to {}", expected);

        let blocks: Vec<BlockNumberOrTag> = match block.data_type() {
            DataType::Utf8 => block
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| downcast_err("StringArray"))?
                .iter()
                .map(|b| parse_block_str(b.ok_or_else(block_null_err)?))
                .collect::<Result<_, _>>()?,
            DataType::UInt64 => block
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| downcast_err("UInt64Array"))?
                .iter()
                .map(|b| b.ok_or_else(block_null_err).map(BlockNumberOrTag::Number))
                .collect::<Result<_, _>>()?,
            DataType::Int64 => block
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| downcast_err("Int64Array"))?
                .iter()
                .map(|b| {
                    b.ok_or_else(block_null_err).and_then(|n| {
                        if n < 0 {
                            plan_err!("block number cannot be negative: {}", n)
                        } else {
                            Ok(BlockNumberOrTag::Number(n as u64))
                        }
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => {
                return plan_err!(
                    "{}: 'block' is not a valid block number or tag: {}",
                    name,
                    block.data_type()
                );
            }
        };

        // Make the eth_call requests.
        let mut result_builder = StructBuilder::from_fields(fields, from.len());
        for (from, to, input_data, block) in izip!(from, to, input_data, blocks) {
            let Some(to) = to else {
                return plan_err!("to address is NULL");
            };
            let result = eth_call_retry(
                &client,
                block,
                TransactionRequest {
                    // `eth_call` only requires the following fields
                    // (https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_call)
                    from: match from {
                        Some(from) => Some(Address::new(from.try_into().map_err(|_| {
                            DataFusionError::Execution(format!(
                                "invalid from address: {}",
                                hex::encode(from)
                            ))
                        })?)),
                        None => None,
                    },
                    to: Some(TxKind::Call(Address::new(to.try_into().map_err(|_| {
                        DataFusionError::Execution(format!(
                            "invalid to address: {}",
                            hex::encode(to)
                        ))
                    })?))),
                    gas: None,
                    gas_price: None,
                    value: None,
                    input: TransactionInput {
                        input: input_data.map(Bytes::copy_from_slice),
                        data: None,
                    },
                    // `eth_call` does not require any other fields.
                    ..Default::default()
                },
            )
            .await;
            // Build the response row.
            match result {
                Ok(bytes) => {
                    result_builder
                        .field_builder::<BinaryBuilder>(0)
                        .unwrap()
                        .append_value(&bytes);
                    result_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_null();
                    result_builder.append(true);
                }
                Err(EthCallRetryError::RpcError(resp)) => {
                    match resp.data {
                        Some(data) => {
                            match hex::decode(
                                data.get().trim_start_matches('"').trim_end_matches('"'),
                            ) {
                                Ok(data) => result_builder
                                    .field_builder::<BinaryBuilder>(0)
                                    .unwrap()
                                    .append_value(data),
                                Err(_) => {
                                    result_builder
                                        .field_builder::<BinaryBuilder>(0)
                                        .unwrap()
                                        .append_null();
                                }
                            }
                        }
                        None => result_builder
                            .field_builder::<BinaryBuilder>(0)
                            .unwrap()
                            .append_null(),
                    }
                    if !resp.message.is_empty() {
                        result_builder
                            .field_builder::<StringBuilder>(1)
                            .unwrap()
                            .append_value(resp.message)
                    } else {
                        result_builder
                            .field_builder::<StringBuilder>(1)
                            .unwrap()
                            .append_null()
                    }
                    result_builder.append(true);
                }
                Err(EthCallRetryError::RetriesFailed) => {
                    result_builder
                        .field_builder::<BinaryBuilder>(0)
                        .unwrap()
                        .append_null();
                    result_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value("unexpected rpc error");
                    result_builder.append(true);
                }
            }
        }
        Ok(ColumnarValue::Array(ArrayBuilder::finish(
            &mut result_builder,
        )))
    }
}

async fn eth_call_retry(
    client: &alloy::providers::RootProvider<AnyNetwork>,
    block: BlockNumberOrTag,
    req: TransactionRequest,
) -> Result<Bytes, EthCallRetryError> {
    for _ in 0..3 {
        let result = client.call(req.clone().into()).block(block.into()).await;
        match result {
            Ok(bytes) => {
                return Ok(bytes);
            }
            Err(RpcError::ErrorResp(resp)) if [3, -32000].contains(&resp.code) => {
                return Err(EthCallRetryError::RpcError(resp));
            }
            other => {
                tracing::info!("unexpected RPC error: {other:?}, retrying");
            }
        }
    }
    tracing::info!("RPC error: retries failed for request {req:?}");
    Err(EthCallRetryError::RetriesFailed)
}

enum EthCallRetryError {
    RpcError(ErrorPayload),
    RetriesFailed,
}
