use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow::datatypes::{DataType, Field, Schema};
use common::arrow::error::ArrowError;
use common::arrow_helpers::ScalarToArray as _;
use common::{
    Bytes, Bytes32, EvmAddress as Address, EvmCurrency, Table, BYTES32_TYPE,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE,
};

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: schema(),
    }
}

pub const TABLE_NAME: &'static str = "calls";

/// Represents successful calls.
///
/// The Firehose call model is much richer, and there is more we can easily add here.
#[derive(Debug, Default)]
pub struct Call {
    pub(crate) block_num: u64,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,
    pub(crate) index: u32,

    pub(crate) parent_index: u32,
    pub(crate) depth: u32,
    pub(crate) caller: Address,
    pub(crate) address: Address,
    pub(crate) value: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,
    pub(crate) gas_consumed: u64,
    pub(crate) return_data: Bytes,
    pub(crate) input: Bytes,
    pub(crate) executed_code: bool,
    pub(crate) selfdestruct: bool,
    pub(crate) begin_ordinal: u64,
    pub(crate) end_ordinal: u64,
}

impl Call {
    pub fn to_arrow(&self) -> Result<RecordBatch, ArrowError> {
        let Call {
            block_num,
            tx_index,
            tx_hash,
            index,
            parent_index,
            depth,
            caller,
            address,
            value,
            gas_limit,
            gas_consumed,
            return_data,
            input,
            executed_code,
            selfdestruct,
            begin_ordinal,
            end_ordinal,
        } = self;

        let columns = vec![
            block_num.to_arrow()?,
            tx_index.to_arrow()?,
            tx_hash.to_arrow()?,
            index.to_arrow()?,
            parent_index.to_arrow()?,
            depth.to_arrow()?,
            caller.to_arrow()?,
            address.to_arrow()?,
            value.to_arrow()?,
            gas_limit.to_arrow()?,
            gas_consumed.to_arrow()?,
            return_data.to_arrow()?,
            input.to_arrow()?,
            executed_code.to_arrow()?,
            selfdestruct.to_arrow()?,
            begin_ordinal.to_arrow()?,
            end_ordinal.to_arrow()?,
        ];

        RecordBatch::try_new(Arc::new(schema()), columns)
    }
}

pub fn schema() -> Schema {
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let index = Field::new("index", DataType::UInt32, false);
    let parent_index = Field::new("parent_index", DataType::UInt32, false);
    let depth = Field::new("depth", DataType::UInt32, false);
    let caller = Field::new("caller", ADDRESS_TYPE, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let value = Field::new("value", EVM_CURRENCY_TYPE, true);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let gas_consumed = Field::new("gas_consumed", DataType::UInt64, false);
    let return_data = Field::new("return_data", DataType::Binary, false);
    let input = Field::new("input", DataType::Binary, false);
    let executed_code = Field::new("executed_code", DataType::Boolean, false);
    let selfdestruct = Field::new("selfdestruct", DataType::Boolean, false);
    let begin_ordinal = Field::new("begin_ordinal", DataType::UInt64, false);
    let end_ordinal = Field::new("end_ordinal", DataType::UInt64, false);

    let fields = vec![
        block_num,
        tx_index,
        tx_hash,
        index,
        parent_index,
        depth,
        caller,
        address,
        value,
        gas_limit,
        gas_consumed,
        return_data,
        input,
        executed_code,
        selfdestruct,
        begin_ordinal,
        end_ordinal,
    ];

    Schema::new(fields)
}

#[test]
fn default_to_arrow() {
    let call = Call::default();
    let batch = call.to_arrow().unwrap();
    assert_eq!(batch.num_columns(), 17);
    assert_eq!(batch.num_rows(), 1);
}
