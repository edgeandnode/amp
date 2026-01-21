use std::sync::{Arc, LazyLock};

use common::{
    BoxResult, RawTableRows, SPECIAL_BLOCK_NUM,
    arrow::{
        array::{ArrayRef, ListBuilder, UInt8Builder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};
use datasets_common::dataset::Table;
use serde::Deserialize;
use solana_clock::Slot;

pub const TABLE_NAME: &str = "instructions";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(name, SCHEMA.clone(), network, vec!["slot".to_string()])
}

/// Prefer using the pre-computed [SCHEMA].
fn schema() -> Schema {
    let fields = vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("program_id_index", DataType::UInt8, false),
        Field::new(
            "accounts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ),
        Field::new(
            "data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            false,
        ),
        // Inner instruction fields. Present only if this is an inner instruction.
        Field::new("inner_index", DataType::UInt32, true),
        Field::new("inner_stack_height", DataType::UInt32, true),
    ];

    Schema::new(fields)
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct Instruction {
    pub(crate) slot: Slot,
    pub(crate) tx_index: u32,

    pub(crate) program_id_index: u8,
    pub(crate) accounts: Vec<u8>,
    pub(crate) data: Vec<u8>,

    /// If the fields below are present, this instruction is an inner instruction.
    /// Otherwise, it is a message instruction.
    ///
    /// ## Reference
    ///
    /// <https://solana.com/docs/rpc/json-structures#inner-instructions>
    pub(crate) inner_index: Option<u32>,
    pub(crate) inner_stack_height: Option<u32>,
}

pub(crate) struct InstructionRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    tx_index: UInt32Builder,
    program_id_index: UInt8Builder,
    accounts: ListBuilder<UInt8Builder>,
    data: ListBuilder<UInt8Builder>,
    inner_index: UInt32Builder,
    inner_stack_height: UInt32Builder,
}

impl InstructionRowsBuilder {
    /// Creates a new [InstructionRowsBuilder] with enough capacity to hold `capacity` instructions.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(capacity),
            slot: UInt64Builder::with_capacity(capacity),
            tx_index: UInt32Builder::with_capacity(capacity),
            program_id_index: UInt8Builder::with_capacity(capacity),
            accounts: ListBuilder::with_capacity(UInt8Builder::new(), capacity),
            data: ListBuilder::with_capacity(UInt8Builder::new(), capacity),
            inner_index: UInt32Builder::with_capacity(capacity),
            inner_stack_height: UInt32Builder::with_capacity(capacity),
        }
    }

    pub(crate) fn append(&mut self, instruction: &Instruction) {
        let Instruction {
            slot,
            tx_index,
            program_id_index,
            accounts,
            data,
            inner_index,
            inner_stack_height,
        } = instruction;

        self.special_block_num.append_value(*slot);
        self.slot.append_value(*slot);
        self.tx_index.append_value(*tx_index);
        self.program_id_index.append_value(*program_id_index);
        self.accounts.values().append_slice(accounts);
        self.accounts.append(true);
        self.data.values().append_slice(data);
        self.data.append(true);
        self.inner_index.append_option(*inner_index);
        self.inner_stack_height.append_option(*inner_stack_height);
    }

    pub(crate) fn build(self, range: BlockRange) -> BoxResult<RawTableRows> {
        let Self {
            mut special_block_num,
            mut slot,
            mut tx_index,
            mut inner_index,
            mut program_id_index,
            mut accounts,
            mut data,
            mut inner_stack_height,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(program_id_index.finish()),
            Arc::new(accounts.finish()),
            Arc::new(data.finish()),
            Arc::new(inner_index.finish()),
            Arc::new(inner_stack_height.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}
