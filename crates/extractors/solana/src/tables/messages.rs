use std::sync::{Arc, LazyLock};

use common::{
    BYTES32_TYPE, BoxResult, Bytes32ArrayBuilder, RawTableRows, SPECIAL_BLOCK_NUM, Table,
    arrow::{
        array::{
            ArrayRef, ListBuilder, StringBuilder, StructBuilder, UInt8Builder, UInt32Builder,
            UInt64Builder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};
use solana_clock::Slot;

pub const TABLE_NAME: &str = "messages";

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
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("num_required_signatures", DataType::UInt8, false),
        Field::new("num_readonly_signed_accounts", DataType::UInt8, false),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt8, false),
        Field::new(
            "instructions",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("program_id_index", DataType::UInt8, false),
                    Field::new(
                        "accounts",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                        false,
                    ),
                    Field::new("data", DataType::Utf8, false),
                    Field::new("stack_height", DataType::UInt32, true),
                ])),
                true,
            ))),
            false,
        ),
        Field::new(
            "account_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("recent_block_hash", BYTES32_TYPE, false),
    ];

    Schema::new(fields)
}

/// A Solana message.
#[derive(Debug, Default, Clone)]
pub(crate) struct Message {
    pub(crate) slot: Slot,
    pub(crate) tx_index: u32,

    pub(crate) num_required_signatures: u8,
    pub(crate) num_readonly_signed_accounts: u8,
    pub(crate) num_readonly_unsigned_accounts: u8,

    pub(crate) instructions: Vec<Instruction>,

    pub(crate) account_keys: Vec<String>,
    pub(crate) recent_block_hash: [u8; 32],
}

impl Message {
    pub(crate) fn from_of_message(
        slot: u64,
        tx_index: u32,
        message: solana_sdk::message::VersionedMessage,
    ) -> Message {
        Self {
            slot,
            tx_index,
            num_required_signatures: message.header().num_required_signatures,
            num_readonly_signed_accounts: message.header().num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: message.header().num_readonly_unsigned_accounts,
            instructions: message
                .instructions()
                .iter()
                .map(|inst| Instruction {
                    program_id_index: inst.program_id_index,
                    accounts: inst.accounts.clone(),
                    data: bs58::encode(&inst.data).into_string(),
                    stack_height: None,
                })
                .collect(),
            account_keys: message
                .static_account_keys()
                .iter()
                .map(|key| key.to_string())
                .collect(),
            recent_block_hash: message.recent_blockhash().to_bytes(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Instruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
    pub stack_height: Option<u32>,
}

/// A builder for converting [Message]s into [RawTableRows].
pub(crate) struct MessageRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    tx_index: UInt32Builder,
    num_required_signatures: UInt8Builder,
    num_readonly_signed_accounts: UInt8Builder,
    num_readonly_unsigned_accounts: UInt8Builder,
    instructions: ListBuilder<StructBuilder>,
    account_keys: ListBuilder<StringBuilder>,
    recent_block_hash: Bytes32ArrayBuilder,
}

impl MessageRowsBuilder {
    /// Creates a new [MessageRowBuilder] with enough capacity for data in `message`.
    pub(crate) fn with_capacity(count: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            slot: UInt64Builder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            num_required_signatures: UInt8Builder::with_capacity(count),
            num_readonly_signed_accounts: UInt8Builder::with_capacity(count),
            num_readonly_unsigned_accounts: UInt8Builder::with_capacity(count),
            instructions: ListBuilder::with_capacity(
                StructBuilder::new(
                    Fields::from(vec![
                        Field::new("program_id_index", DataType::UInt8, false),
                        Field::new(
                            "accounts",
                            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                            false,
                        ),
                        Field::new("data", DataType::Utf8, false),
                        Field::new("stack_height", DataType::UInt32, true),
                    ]),
                    vec![
                        Box::new(UInt8Builder::new()),
                        Box::new(ListBuilder::new(UInt8Builder::new())),
                        Box::new(StringBuilder::new()),
                        Box::new(UInt32Builder::new()),
                    ],
                ),
                count,
            ),
            account_keys: ListBuilder::with_capacity(StringBuilder::new(), count),
            recent_block_hash: Bytes32ArrayBuilder::with_capacity(count),
        }
    }

    /// Appends a [Message] to the builder.
    pub(crate) fn append(&mut self, message: &Message) {
        let Message {
            slot,
            tx_index,
            num_required_signatures,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
            instructions,
            account_keys,
            recent_block_hash,
        } = message;

        self.special_block_num.append_value(*slot);
        self.slot.append_value(*slot);
        self.tx_index.append_value(*tx_index);
        self.num_required_signatures
            .append_value(*num_required_signatures);
        self.num_readonly_signed_accounts
            .append_value(*num_readonly_signed_accounts);
        self.num_readonly_unsigned_accounts
            .append_value(*num_readonly_unsigned_accounts);
        for inst in instructions {
            let struct_builder = self.instructions.values();
            let program_id_index_builder = struct_builder
                .field_builder::<UInt8Builder>(0)
                .expect("program_id_index builder");
            program_id_index_builder.append_value(inst.program_id_index);
            let accounts_builder = struct_builder
                .field_builder::<ListBuilder<UInt8Builder>>(1)
                .expect("accounts builder");
            for account in &inst.accounts {
                accounts_builder.values().append_value(*account);
            }
            accounts_builder.append(true);
            let data_builder = struct_builder
                .field_builder::<StringBuilder>(2)
                .expect("data builder");
            data_builder.append_value(&inst.data);
            let stack_height_builder = struct_builder
                .field_builder::<UInt32Builder>(3)
                .expect("stack_height builder");
            if let Some(stack_height) = inst.stack_height {
                stack_height_builder.append_value(stack_height);
            } else {
                stack_height_builder.append_null();
            }
            struct_builder.append(true);
        }
        self.instructions.append(true);
        for key in account_keys {
            self.account_keys.values().append_value(key);
        }
        self.account_keys.append(true);
        self.recent_block_hash.append_value(*recent_block_hash);
    }

    /// Builds the [RawTableRows] from the appended data.
    pub(crate) fn build(self, range: BlockRange) -> BoxResult<RawTableRows> {
        let Self {
            mut special_block_num,
            mut slot,
            mut tx_index,
            mut num_required_signatures,
            mut num_readonly_signed_accounts,
            mut num_readonly_unsigned_accounts,
            mut instructions,
            mut account_keys,
            recent_block_hash,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(num_required_signatures.finish()),
            Arc::new(num_readonly_signed_accounts.finish()),
            Arc::new(num_readonly_unsigned_accounts.finish()),
            Arc::new(instructions.finish()),
            Arc::new(account_keys.finish()),
            Arc::new(recent_block_hash.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}
