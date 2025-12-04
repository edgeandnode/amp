use std::sync::{Arc, LazyLock};

use common::{
    BYTES32_TYPE, BoxResult, Bytes32ArrayBuilder, RawTableRows, SPECIAL_BLOCK_NUM, Table,
    arrow::{
        array::{ArrayRef, Int64Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};
use solana_clock::Slot;

pub const TABLE_NAME: &str = "block_headers";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(name, SCHEMA.clone(), network, vec!["slot".to_string()]).unwrap()
}

/// Prefer using the pre-computed [SCHEMA].
fn schema() -> Schema {
    let fields = vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("block_hash", BYTES32_TYPE, false),
        Field::new("previous_block_hash", BYTES32_TYPE, false),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("block_time", DataType::Int64, true),
    ];

    Schema::new(fields)
}

/// A Solana block header.
#[derive(Debug, Default, Clone)]
pub(crate) struct BlockHeader {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,

    pub(crate) block_hash: [u8; 32],
    pub(crate) previous_block_hash: [u8; 32],

    pub(crate) block_height: Option<u64>,
    pub(crate) block_time: Option<i64>,
}

impl BlockHeader {
    pub(crate) fn from_of_block(block: crate::extractor::DecodedBlock) -> Self {
        Self {
            slot: block.slot,
            parent_slot: block.parent_slot,
            block_hash: block.blockhash,
            previous_block_hash: block.prev_blockhash,
            block_height: block.block_height,
            block_time: Some(block.blocktime as i64),
        }
    }

    pub(crate) fn empty(slot: Slot) -> Self {
        Self {
            slot,
            ..Default::default()
        }
    }
}

/// A builder for converting [BlockHeader]s into [RawTableRows].
pub(crate) struct BlockHeaderRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    parent_slot: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    previous_block_hash: Bytes32ArrayBuilder,
    block_height: UInt64Builder,
    block_time: Int64Builder,
}

impl BlockHeaderRowsBuilder {
    /// Creates a new [BlockHeaderRowsBuilder].
    pub(crate) fn new() -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(1),
            slot: UInt64Builder::with_capacity(1),
            parent_slot: UInt64Builder::with_capacity(1),
            block_hash: Bytes32ArrayBuilder::with_capacity(1),
            previous_block_hash: Bytes32ArrayBuilder::with_capacity(1),
            block_height: UInt64Builder::with_capacity(1),
            block_time: Int64Builder::with_capacity(1),
        }
    }

    /// Appends a [BlockHeader] to the builder.
    pub(crate) fn append(&mut self, header: &BlockHeader) {
        let BlockHeader {
            slot,
            parent_slot,
            block_hash,
            previous_block_hash,
            block_height,
            block_time,
        } = header;

        self.special_block_num.append_value(*slot);
        self.slot.append_value(*slot);
        self.parent_slot.append_value(*parent_slot);
        self.block_hash.append_value(*block_hash);
        self.previous_block_hash.append_value(*previous_block_hash);
        self.block_height.append_option(*block_height);
        self.block_time.append_option(*block_time);
    }

    /// Builds the [RawTableRows] from the appended data.
    pub(crate) fn build(self, range: BlockRange) -> BoxResult<RawTableRows> {
        let Self {
            mut special_block_num,
            mut slot,
            mut parent_slot,
            block_hash,
            previous_block_hash,
            mut block_height,
            mut block_time,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(parent_slot.finish()),
            Arc::new(block_hash.finish()),
            Arc::new(previous_block_hash.finish()),
            Arc::new(block_height.finish()),
            Arc::new(block_time.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}
