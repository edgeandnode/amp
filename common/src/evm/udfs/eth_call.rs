use std::{any::Any, future::IntoFuture, str::FromStr, thread};

use alloy::{
    eips::BlockNumberOrTag,
    hex,
    primitives::{Address, Bytes, TxKind},
    providers::Provider,
    rpc::{
        json_rpc::ErrorPayload,
        types::{TransactionInput, TransactionRequest},
    },
    transports::RpcError,
};
use datafusion::{
    arrow::{
        array::{
            Array, ArrayBuilder, BinaryArray, BinaryBuilder, Decimal128Array, FixedSizeBinaryArray,
            StringArray, StringBuilder, StructBuilder, UInt64Array,
        },
        datatypes::{DataType, Field, Fields, DECIMAL128_MAX_PRECISION},
    },
    common::{internal_err, plan_err},
    error::DataFusionError,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
};
use itertools::izip;

#[derive(Debug, Clone)]
pub struct EthCall {
    name: String,
    client: alloy::providers::ReqwestProvider,
    signature: Signature,
    fields: Fields,
}

impl EthCall {
    pub fn new(table_name: &str, client: alloy::providers::ReqwestProvider) -> Self {
        EthCall {
            name: format!("{table_name}.eth_call"),
            client,
            signature: Signature::exact(
                vec![
                    // from (optional)
                    DataType::FixedSizeBinary(20),
                    // to
                    DataType::FixedSizeBinary(20),
                    // gas (optional)
                    DataType::UInt64,
                    // gas price (optional)
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    // value (optional)
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                    // input data (optional)
                    DataType::Binary,
                    // block
                    DataType::Utf8,
                ],
                Volatility::Volatile,
            ),
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(self.fields.clone()))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let name = self.name().to_string();
        let client = self.client.clone();
        let fields = self.fields.clone();
        // Dedicated thread required to spawn nested tokio runtime.
        thread::spawn(move || {
            // Decode the arguments.
            let args: Vec<_> = ColumnarValue::values_to_arrays(&args.args)?;
            let [from, to, gas, gas_price, value, input_data, block] = args.as_slice() else {
                return internal_err!("{}: expected 7 arguments, but got {}", name, args.len());
            };
            let from = from
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let to = to.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let gas = gas.as_any().downcast_ref::<UInt64Array>().unwrap();
            let gas_price = gas_price
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let value = value.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let input_data = input_data.as_any().downcast_ref::<BinaryArray>().unwrap();
            let block = block.as_any().downcast_ref::<StringArray>().unwrap();

            // Make the eth_call requests.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let mut result_builder = StructBuilder::from_fields(fields, from.len());
            for (from, to, gas, gas_price, value, input_data, block) in
                izip!(from, to, gas, gas_price, value, input_data, block)
            {
                let Some(to) = to else {
                    return plan_err!("from address is NULL");
                };
                let Some(block) = block else {
                    return plan_err!("block is NULL");
                };
                let block = match u64::from_str(block) {
                    Ok(block) => BlockNumberOrTag::Number(block),
                    Err(_) => match BlockNumberOrTag::from_str(block) {
                        Ok(block) => block,
                        Err(_) => {
                            return plan_err!("block is not a valid integer, \"0x\" prefixed hex integer, or tag");
                        }
                    },
                };
                let result = eth_call_retry(&rt, &client, block, &TransactionRequest {
                    from: match from {
                        Some(from) => Some(Address::new(from.try_into().map_err(|_| DataFusionError::Execution(format!("invalid from address: {}", hex::encode(from))))?)),
                        None => None,
                    },
                    to: Some(TxKind::Call(Address::new(to.try_into().map_err(|_| DataFusionError::Execution(format!("invalid to address: {}", hex::encode(to))))?))),
                    gas_price: match gas_price {
                        Some(gas_price) => Some(gas_price.try_into().map_err(|_| DataFusionError::Execution(format!("invalid gas price: {}", gas_price)))?),
                        None => None,
                    },
                    max_fee_per_gas: None,
                    max_priority_fee_per_gas: None,
                    max_fee_per_blob_gas: None,
                    gas,
                    value: match value {
                        Some(value) => Some(value.try_into().map_err(|_| DataFusionError::Execution(format!("invalid value: {}", value)))?),
                        None => None,
                    },
                    input: TransactionInput {
                        input: input_data.map(|i| alloy::primitives::Bytes::copy_from_slice(i)),
                        data: None,
                    },
                    nonce: None,
                    chain_id: None,
                    access_list: None,
                    transaction_type: None,
                    blob_versioned_hashes: None,
                    sidecar: None,
                    authorization_list: None,
                });
                // Build the response row.
                match result  {
                    Ok(bytes) => {
                        result_builder.field_builder::<BinaryBuilder>(0).unwrap().append_value(bytes.to_vec());
                        result_builder.field_builder::<StringBuilder>(1).unwrap().append_null();
                        result_builder.append(true);
                    }
                    Err(EthCallRetryError::RpcError(resp)) => {
                        match resp.data {
                            Some(data) => {
                                match hex::decode(data.get().trim_start_matches('"').trim_end_matches('"')) {
                                    Ok(data) => {
                                        result_builder.field_builder::<BinaryBuilder>(0).unwrap().append_value(data)
                                    }
                                    Err(_) => {
                                        result_builder.field_builder::<BinaryBuilder>(0).unwrap().append_null();
                                    }
                                }
                            }
                            None => {
                                result_builder.field_builder::<BinaryBuilder>(0).unwrap().append_null()
                            }
                        }
                        if !resp.message.is_empty() {
                            result_builder.field_builder::<StringBuilder>(1).unwrap().append_value(resp.message)
                        } else {
                            result_builder.field_builder::<StringBuilder>(1).unwrap().append_null()
                        }
                        result_builder.append(true);
                    }
                    Err(EthCallRetryError::RetriesFailed) => {
                        result_builder.field_builder::<BinaryBuilder>(0).unwrap().append_null();
                        result_builder.field_builder::<StringBuilder>(1).unwrap().append_value(format!("unexpected rpc error"));
                        result_builder.append(true);
                    },
                }
            }
            Ok(ColumnarValue::Array(ArrayBuilder::finish(
                &mut result_builder,
            )))
        })
        .join()
        .unwrap()
    }
}

fn eth_call_retry(
    rt: &tokio::runtime::Runtime,
    client: &alloy::providers::ReqwestProvider,
    block: BlockNumberOrTag,
    req: &TransactionRequest,
) -> Result<Bytes, EthCallRetryError> {
    for _ in 0..3 {
        let result = rt.block_on(client.call(req).block(block.into()).into_future());
        match result {
            Ok(bytes) => {
                return Ok(bytes);
            }
            Err(RpcError::ErrorResp(resp)) if [3, -32000].contains(&resp.code) => {
                return Err(EthCallRetryError::RpcError(resp));
            }
            other => {
                log::info!("unexpected RPC error: {other:?}, retrying");
            }
        }
    }
    log::info!("RPC error: retries failed for request {req:?}");
    Err(EthCallRetryError::RetriesFailed)
}

enum EthCallRetryError {
    RpcError(ErrorPayload),
    RetriesFailed,
}
