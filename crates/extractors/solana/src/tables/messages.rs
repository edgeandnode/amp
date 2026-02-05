use std::sync::{Arc, LazyLock};

use datasets_common::{
    block_range::BlockRange,
    dataset::{SPECIAL_BLOCK_NUM, Table},
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, DataType, Field, Fields, ListBuilder, Schema, SchemaRef, StringBuilder,
        StructBuilder, UInt8Builder, UInt32Builder, UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};
use solana_clock::Slot;

use crate::{rpc_client, tables::BASE58_ENCODED_HASH_LEN};

pub const TABLE_NAME: &str = "messages";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(name, SCHEMA.clone(), network, vec!["slot".to_string()])
}

/// Prefer using the pre-computed [SCHEMA].
fn schema() -> Schema {
    let fields = vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new("header", header_dtype(), false),
        Field::new("address_table_lookups", address_table_lookups_dtype(), true),
        Field::new(
            "account_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("recent_block_hash", DataType::Utf8, false),
    ];

    Schema::new(fields)
}

fn header_dtype() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("num_required_signatures", DataType::UInt8, false),
        Field::new("num_readonly_signed_accounts", DataType::UInt8, false),
        Field::new("num_readonly_unsigned_accounts", DataType::UInt8, false),
    ]))
}

fn address_table_lookups_dtype() -> DataType {
    fn address_table_lookup_dtype() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("account_key", DataType::Utf8, false),
            Field::new(
                "writable_indexes",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                false,
            ),
            Field::new(
                "readonly_indexes",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                false,
            ),
        ]))
    }

    DataType::List(Arc::new(Field::new(
        "item",
        address_table_lookup_dtype(),
        true,
    )))
}

#[derive(Debug, Default, PartialEq)]
pub struct Message {
    pub slot: Slot,
    pub tx_index: u32,

    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
    pub instructions: Vec<super::instructions::Instruction>,
    pub address_table_lookups: Option<Vec<AddressTableLookup>>,
    pub account_keys: Vec<String>,
    pub recent_block_hash: String,
}

impl Message {
    pub(crate) fn from_of1_message(
        slot: Slot,
        tx_index: u32,
        message: solana_sdk::message::VersionedMessage,
    ) -> Self {
        let instructions = message
            .instructions()
            .iter()
            .cloned()
            .map(|inst| super::instructions::Instruction {
                slot,
                tx_index,
                program_id_index: inst.program_id_index,
                accounts: inst.accounts,
                data: inst.data,
                inner_index: None,
                inner_stack_height: None,
            })
            .collect();
        let address_table_lookups = message.address_table_lookups().as_ref().map(|atls| {
            atls.iter()
                .cloned()
                .map(|atl| AddressTableLookup {
                    account_key: atl.account_key.to_string(),
                    writable_indexes: atl.writable_indexes,
                    readonly_indexes: atl.readonly_indexes,
                })
                .collect()
        });

        Self {
            slot,
            tx_index,
            num_required_signatures: message.header().num_required_signatures,
            num_readonly_signed_accounts: message.header().num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: message.header().num_readonly_unsigned_accounts,
            instructions,
            address_table_lookups,
            account_keys: message
                .static_account_keys()
                .iter()
                .map(|key| key.to_string())
                .collect(),
            recent_block_hash: message.recent_blockhash().to_string(),
        }
    }

    pub(crate) fn from_rpc_message(
        slot: Slot,
        tx_index: u32,
        message: rpc_client::UiRawMessage,
    ) -> Self {
        let instructions = message
            .instructions
            .iter()
            .map(|inst| {
                let data = bs58::decode(&inst.data)
                    .into_vec()
                    .expect("invalid base-58 string");
                super::instructions::Instruction {
                    slot,
                    tx_index,
                    program_id_index: inst.program_id_index,
                    accounts: inst.accounts.clone(),
                    data,
                    inner_index: None,
                    inner_stack_height: None,
                }
            })
            .collect();
        let address_table_lookups = message.address_table_lookups.as_ref().map(|atls| {
            atls.iter()
                .cloned()
                .map(|atl| AddressTableLookup {
                    account_key: atl.account_key,
                    writable_indexes: atl.writable_indexes,
                    readonly_indexes: atl.readonly_indexes,
                })
                .collect()
        });

        Self {
            slot,
            tx_index,
            num_required_signatures: message.header.num_required_signatures,
            num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
            instructions,
            address_table_lookups,
            account_keys: message.account_keys.clone(),
            recent_block_hash: message.recent_blockhash.clone(),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct AddressTableLookup {
    pub account_key: String,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

/// A builder for converting [Message]s into [TableRows].
pub(crate) struct MessageRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    tx_index: UInt32Builder,
    header: StructBuilder,
    address_table_lookups: ListBuilder<StructBuilder>,
    account_keys: ListBuilder<StringBuilder>,
    recent_block_hash: StringBuilder,
}

impl MessageRowsBuilder {
    /// Creates a new [MessageRowsBuilder] with enough capacity to hold `capacity` messages.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        fn address_table_lookup_builder() -> StructBuilder {
            StructBuilder::new(
                Fields::from(vec![
                    Field::new("account_key", DataType::Utf8, false),
                    Field::new(
                        "writable_indexes",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                        false,
                    ),
                    Field::new(
                        "readonly_indexes",
                        DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                        false,
                    ),
                ]),
                vec![
                    Box::new(StringBuilder::new()),
                    Box::new(ListBuilder::new(UInt8Builder::new())),
                    Box::new(ListBuilder::new(UInt8Builder::new())),
                ],
            )
        }

        fn header_builder() -> StructBuilder {
            StructBuilder::new(
                Fields::from(vec![
                    Field::new("num_required_signatures", DataType::UInt8, false),
                    Field::new("num_readonly_signed_accounts", DataType::UInt8, false),
                    Field::new("num_readonly_unsigned_accounts", DataType::UInt8, false),
                ]),
                vec![
                    Box::new(UInt8Builder::new()),
                    Box::new(UInt8Builder::new()),
                    Box::new(UInt8Builder::new()),
                ],
            )
        }

        Self {
            special_block_num: UInt64Builder::with_capacity(capacity),
            slot: UInt64Builder::with_capacity(capacity),
            tx_index: UInt32Builder::with_capacity(capacity),
            header: header_builder(),
            address_table_lookups: ListBuilder::with_capacity(
                address_table_lookup_builder(),
                capacity,
            ),
            account_keys: ListBuilder::with_capacity(StringBuilder::new(), capacity),
            recent_block_hash: StringBuilder::with_capacity(
                capacity,
                capacity * BASE58_ENCODED_HASH_LEN,
            ),
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
            instructions: _,
            address_table_lookups,
            account_keys,
            recent_block_hash,
        } = message;

        self.special_block_num.append_value(*slot);
        self.slot.append_value(*slot);
        self.tx_index.append_value(*tx_index);

        self.header
            .field_builder::<UInt8Builder>(0)
            .expect("num_required_signatures builder")
            .append_value(*num_required_signatures);
        self.header
            .field_builder::<UInt8Builder>(1)
            .expect("num_readonly_signed_accounts builder")
            .append_value(*num_readonly_signed_accounts);
        self.header
            .field_builder::<UInt8Builder>(2)
            .expect("num_readonly_unsigned_accounts builder")
            .append_value(*num_readonly_unsigned_accounts);
        self.header.append(true);

        if let Some(atls) = address_table_lookups {
            for atl in atls {
                let struct_builder = self.address_table_lookups.values();
                let account_key_builder = struct_builder
                    .field_builder::<StringBuilder>(0)
                    .expect("account_key builder");
                account_key_builder.append_value(&atl.account_key);
                let writable_indexes_builder = struct_builder
                    .field_builder::<ListBuilder<UInt8Builder>>(1)
                    .expect("writable_indexes builder");
                for index in &atl.writable_indexes {
                    writable_indexes_builder.values().append_value(*index);
                }
                writable_indexes_builder.append(true);
                let readonly_indexes_builder = struct_builder
                    .field_builder::<ListBuilder<UInt8Builder>>(2)
                    .expect("readonly_indexes builder");
                for index in &atl.readonly_indexes {
                    readonly_indexes_builder.values().append_value(*index);
                }
                readonly_indexes_builder.append(true);
                struct_builder.append(true);
            }
            self.address_table_lookups.append(true);
        } else {
            self.address_table_lookups.append(false);
        }
        for key in account_keys {
            self.account_keys.values().append_value(key);
        }
        self.account_keys.append(true);
        self.recent_block_hash.append_value(recent_block_hash);
    }

    /// Builds the [TableRows] from the appended data.
    pub(crate) fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            mut slot,
            mut tx_index,
            mut header,
            mut address_table_lookups,
            mut account_keys,
            mut recent_block_hash,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(header.finish()),
            Arc::new(address_table_lookups.finish()),
            Arc::new(account_keys.finish()),
            Arc::new(recent_block_hash.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}
