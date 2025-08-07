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
            Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray,
            StringArray, StringBuilder, StructBuilder,
        },
        datatypes::{DataType, Field, Fields},
    },
    common::{internal_err, plan_err},
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        async_udf::AsyncScalarUDFImpl,
    },
};
use itertools::izip;

type TransactionRequest = <Ethereum as alloy::network::Network>::TransactionRequest;

#[derive(Debug, Clone)]
pub struct EthCall {
    name: String,
    client: alloy::providers::RootProvider<AnyNetwork>,
    signature: Signature,
    fields: Fields,
}

impl EthCall {
    pub fn new(dataset_name: &str, client: alloy::providers::RootProvider<AnyNetwork>) -> Self {
        EthCall {
            name: format!("{dataset_name}.eth_call"),
            client,
            signature: Signature {
                type_signature: TypeSignature::Exact(vec![
                    // from (optional)
                    DataType::FixedSizeBinary(20),
                    // to
                    DataType::FixedSizeBinary(20),
                    // input data (optional)
                    DataType::Binary,
                    // block
                    DataType::Utf8,
                ]),
                volatility: Volatility::Volatile,
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
        _option: &ConfigOptions,
    ) -> Result<ArrayRef, DataFusionError> {
        let name = self.name().to_string();
        let client = self.client.clone();
        let fields = self.fields.clone();
        // Decode the arguments.
        let args: Vec<_> = ColumnarValue::values_to_arrays(&args.args)?;
        let [from, to, input_data, block] = args.as_slice() else {
            return internal_err!("{}: expected 4 arguments, but got {}", name, args.len());
        };
        let from = from
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let to = to.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let input_data = input_data.as_any().downcast_ref::<BinaryArray>().unwrap();
        let block = block.as_any().downcast_ref::<StringArray>().unwrap();

        // Make the eth_call requests.
        let mut result_builder = StructBuilder::from_fields(fields, from.len());
        for (from, to, input_data, block) in izip!(from, to, input_data, block) {
            let Some(to) = to else {
                return plan_err!("to address is NULL");
            };
            let Some(block) = block else {
                return plan_err!("block is NULL");
            };
            let block = match u64::from_str(block) {
                Ok(block) => BlockNumberOrTag::Number(block),
                Err(_) => match BlockNumberOrTag::from_str(block) {
                    Ok(block) => block,
                    Err(_) => {
                        return plan_err!(
                            "block is not a valid integer, \"0x\" prefixed hex integer, or tag"
                        );
                    }
                },
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
                        input: input_data.map(|i| alloy::primitives::Bytes::copy_from_slice(i)),
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
                        .append_value(bytes.to_vec());
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
                        .append_value(format!("unexpected rpc error"));
                    result_builder.append(true);
                }
            }
        }
        Ok(ArrayBuilder::finish(&mut result_builder))
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
