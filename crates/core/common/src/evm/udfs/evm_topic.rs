use std::any::Any;

use datafusion::{
    common::{internal_err, plan_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};

use crate::{arrow::datatypes::DataType, evm::udfs::Event};

/// DataFusion UDF that computes the topic0 (event selector) for a Solidity event signature.
///
/// This function calculates the keccak256 hash of the event signature, which is used
/// as topic0 in EVM event logs to identify the event type. This is useful for filtering
/// logs by event type or validating event signatures.
///
/// # SQL Usage
///
/// ```ignore
/// // Get topic0 for Transfer event
/// evm_topic('Transfer(address indexed from,address indexed to,uint256 value)')
/// // Returns 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
///
/// // Get topic0 for Swap event
/// evm_topic('Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)')
/// // Returns 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
/// ```
///
/// # Arguments
///
/// * `signature` - `Utf8` Solidity event signature (e.g., "Transfer(address,address,uint256)")
///
/// # Returns
///
/// `FixedSizeBinary(32)` the keccak256 hash of the canonical event signature.
///
/// # Errors
///
/// Returns a planning error if:
/// - Signature is not a valid Solidity event signature
/// - Signature is not provided as a scalar string literal
/// - Event is anonymous (has no topic0)
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EvmTopic {
    signature: Signature,
}

impl Default for EvmTopic {
    fn default() -> Self {
        Self::new()
    }
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
                );
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

    use alloy::{hex, primitives::B256};

    use super::*;
    use crate::arrow::datatypes::Field;

    // Signature of a Uniswap v3 swap
    const SIG: &str = "Swap(address indexed sender,address indexed recipient,int256 amount0,int256 amount1,uint160 sqrtPriceX96,uint128 liquidity,int24 tick)";

    // topic0: Swap event
    static TOPIC_0: LazyLock<B256> = LazyLock::new(|| {
        hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67").into()
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

        let arg_fields = vec![Field::new("signature", DataType::Utf8, false).into()];
        let return_field = Field::new(evm_topic.name(), EvmTopic::RETURN_TYPE, false).into();

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Default::default(),
        };
        let result = evm_topic.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected ScalarValue, got {:?}", result);
        };

        let exp = ScalarValue::FixedSizeBinary(32, Some(TOPIC_0.to_vec()));
        assert_eq!(exp, result);
    }
}
