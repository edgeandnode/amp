use std::sync::{Arc, LazyLock};

use common::{
    BLOCK_NUM, BYTES32_TYPE, BoxError, Bytes32, Bytes32ArrayBuilder,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE, EvmAddress as Address,
    EvmAddressArrayBuilder, EvmCurrency, EvmCurrencyArrayBuilder, RawTableRows, SPECIAL_BLOCK_NUM,
    Table, Timestamp, TimestampArrayBuilder,
    arrow::{
        array::{
            ArrayRef, BinaryBuilder, BooleanBuilder, Int32Builder, UInt32Builder, UInt64Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    Table::new(TABLE_NAME.to_string(), SCHEMA.clone(), network)
}

pub const TABLE_NAME: &'static str = "calls";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let special_block_num = Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false);
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new(BLOCK_NUM, DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let index = Field::new("index", DataType::UInt32, false);
    let parent_index = Field::new("parent_index", DataType::UInt32, false);
    let depth = Field::new("depth", DataType::UInt32, false);
    let call_type = Field::new("call_type", DataType::Int32, false);
    let caller = Field::new("caller", ADDRESS_TYPE, false);
    let address = Field::new("address", ADDRESS_TYPE, false);
    let value = Field::new("value", EVM_CURRENCY_TYPE, true);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let gas_consumed = Field::new("gas_consumed", DataType::UInt64, false);
    let return_data = Field::new("return_data", DataType::Binary, false);
    let input = Field::new("input", DataType::Binary, false);
    let selfdestruct = Field::new("selfdestruct", DataType::Boolean, false);
    let executed_code = Field::new("executed_code", DataType::Boolean, false);
    let begin_ordinal = Field::new("begin_ordinal", DataType::UInt64, false);
    let end_ordinal = Field::new("end_ordinal", DataType::UInt64, false);

    let fields = vec![
        special_block_num,
        block_hash,
        block_num,
        timestamp,
        tx_index,
        tx_hash,
        index,
        parent_index,
        depth,
        call_type,
        caller,
        address,
        value,
        gas_limit,
        gas_consumed,
        return_data,
        input,
        selfdestruct,
        executed_code,
        begin_ordinal,
        end_ordinal,
    ];

    Schema::new(fields)
}

/// Represents successful calls.
///
/// The Firehose call model is much richer, and there is more we can easily add here.
#[derive(Debug, Default)]
pub struct Call {
    pub(crate) block_hash: Bytes32,
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,
    pub(crate) index: u32,

    pub(crate) parent_index: u32,
    pub(crate) depth: u32,
    pub(crate) call_type: i32,
    pub(crate) caller: Address,
    pub(crate) address: Address,
    pub(crate) value: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,
    pub(crate) gas_consumed: u64,
    pub(crate) return_data: Vec<u8>,
    pub(crate) input: Vec<u8>,
    pub(crate) selfdestruct: bool,

    // Firehose specific.
    pub(crate) executed_code: bool,
    pub(crate) begin_ordinal: u64,
    pub(crate) end_ordinal: u64,
}

pub(crate) struct CallRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    index: UInt32Builder,
    parent_index: UInt32Builder,
    depth: UInt32Builder,
    call_type: Int32Builder,
    caller: EvmAddressArrayBuilder,
    address: EvmAddressArrayBuilder,
    value: EvmCurrencyArrayBuilder,
    gas_limit: UInt64Builder,
    gas_consumed: UInt64Builder,
    return_data: BinaryBuilder,
    input: BinaryBuilder,
    selfdestruct: BooleanBuilder,
    executed_code: BooleanBuilder,
    begin_ordinal: UInt64Builder,
    end_ordinal: UInt64Builder,
}

impl CallRowsBuilder {
    pub fn with_capacity(
        count: usize,
        total_return_data_size: usize,
        total_input_size: usize,
    ) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            block_hash: Bytes32ArrayBuilder::with_capacity(count),
            block_num: UInt64Builder::with_capacity(count),
            timestamp: TimestampArrayBuilder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_hash: Bytes32ArrayBuilder::with_capacity(count),
            index: UInt32Builder::with_capacity(count),
            parent_index: UInt32Builder::with_capacity(count),
            depth: UInt32Builder::with_capacity(count),
            call_type: Int32Builder::with_capacity(count),
            caller: EvmAddressArrayBuilder::with_capacity(count),
            address: EvmAddressArrayBuilder::with_capacity(count),
            value: EvmCurrencyArrayBuilder::with_capacity(count),
            gas_limit: UInt64Builder::with_capacity(count),
            gas_consumed: UInt64Builder::with_capacity(count),
            return_data: BinaryBuilder::with_capacity(count, total_return_data_size),
            input: BinaryBuilder::with_capacity(count, total_input_size),
            selfdestruct: BooleanBuilder::with_capacity(count),
            executed_code: BooleanBuilder::with_capacity(count),
            begin_ordinal: UInt64Builder::with_capacity(count),
            end_ordinal: UInt64Builder::with_capacity(count),
        }
    }

    pub fn append(&mut self, call: &Call) {
        let Call {
            block_hash,
            block_num,
            timestamp,
            tx_index,
            tx_hash,
            index,
            parent_index,
            depth,
            call_type,
            caller,
            address,
            value,
            gas_limit,
            gas_consumed,
            return_data,
            input,
            selfdestruct,
            executed_code,
            begin_ordinal,
            end_ordinal,
        } = call;

        self.special_block_num.append_value(*block_num);
        self.block_hash.append_value(*block_hash);
        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.tx_index.append_value(*tx_index);
        self.tx_hash.append_value(*tx_hash);
        self.index.append_value(*index);
        self.parent_index.append_value(*parent_index);
        self.depth.append_value(*depth);
        self.call_type.append_value(*call_type);
        self.caller.append_value(*caller);
        self.address.append_value(*address);
        self.value.append_option(*value);
        self.gas_limit.append_value(*gas_limit);
        self.gas_consumed.append_value(*gas_consumed);
        self.return_data.append_value(return_data);
        self.input.append_value(input);
        self.selfdestruct.append_value(*selfdestruct);
        self.executed_code.append_value(*executed_code);
        self.begin_ordinal.append_value(*begin_ordinal);
        self.end_ordinal.append_value(*end_ordinal);
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<RawTableRows, BoxError> {
        let Self {
            mut special_block_num,
            block_hash,
            mut block_num,
            mut timestamp,
            mut tx_index,
            tx_hash,
            mut index,
            mut parent_index,
            mut depth,
            mut call_type,
            caller,
            address,
            value,
            mut gas_limit,
            mut gas_consumed,
            mut return_data,
            mut input,
            mut selfdestruct,
            mut executed_code,
            mut begin_ordinal,
            mut end_ordinal,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(block_hash.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(index.finish()),
            Arc::new(parent_index.finish()),
            Arc::new(depth.finish()),
            Arc::new(call_type.finish()),
            Arc::new(caller.finish()),
            Arc::new(address.finish()),
            Arc::new(value.finish()),
            Arc::new(gas_limit.finish()),
            Arc::new(gas_consumed.finish()),
            Arc::new(return_data.finish()),
            Arc::new(input.finish()),
            Arc::new(selfdestruct.finish()),
            Arc::new(executed_code.finish()),
            Arc::new(begin_ordinal.finish()),
            Arc::new(end_ordinal.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}

#[test]
fn default_to_arrow() {
    let call = Call::default();
    let rows = {
        let mut builder =
            CallRowsBuilder::with_capacity(1, call.return_data.len(), call.input.len());
        builder.append(&call);
        builder
            .build(BlockRange {
                numbers: call.block_num..=call.block_num,
                network: "test_network".to_string(),
                hash: call.block_hash.into(),
                prev_hash: None,
            })
            .unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 21);
    assert_eq!(rows.rows.num_rows(), 1);
}
