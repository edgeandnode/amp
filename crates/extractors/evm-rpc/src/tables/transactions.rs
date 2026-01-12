use std::sync::{Arc, LazyLock};

use common::{
    BYTES32_TYPE, BoxError, Bytes32, Bytes32ArrayBuilder, EVM_ADDRESS_TYPE as ADDRESS_TYPE,
    EVM_CURRENCY_TYPE, EvmAddress as Address, EvmAddressArrayBuilder, EvmCurrency,
    EvmCurrencyArrayBuilder, RawTableRows, SPECIAL_BLOCK_NUM, Table, Timestamp,
    TimestampArrayBuilder,
    arrow::{
        array::{
            ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Int32Builder,
            ListBuilder, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(
        name,
        SCHEMA.clone(),
        network,
        vec!["block_num".to_string(), "timestamp".to_string()],
    )
}

pub const TABLE_NAME: &str = "transactions";

/// Prefer using the pre-computed SCHEMA
fn schema() -> Schema {
    let special_block_num = Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false);
    let block_hash = Field::new("block_hash", BYTES32_TYPE, false);
    let block_num = Field::new("block_num", DataType::UInt64, false);
    let timestamp = Field::new("timestamp", common::timestamp_type(), false);
    let tx_index = Field::new("tx_index", DataType::UInt32, false);
    let tx_hash = Field::new("tx_hash", BYTES32_TYPE, false);
    let to = Field::new("to", ADDRESS_TYPE, true);
    let nonce = Field::new("nonce", DataType::UInt64, false);
    let gas_price = Field::new("gas_price", EVM_CURRENCY_TYPE, true);
    let gas_limit = Field::new("gas_limit", DataType::UInt64, false);
    let value = Field::new("value", DataType::Utf8, false);
    let input = Field::new("input", DataType::Binary, false);
    let r = Field::new("r", BYTES32_TYPE, false);
    let s = Field::new("s", BYTES32_TYPE, false);
    let v_parity = Field::new("v_parity", DataType::Boolean, false);
    let chain_id = Field::new("chain_id", DataType::UInt64, true);
    let gas_used = Field::new("gas_used", DataType::UInt64, false);
    let r#type = Field::new("type", DataType::Int32, false);
    let max_fee_per_gas = Field::new("max_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let max_priority_fee_per_gas = Field::new("max_priority_fee_per_gas", EVM_CURRENCY_TYPE, true);
    let max_fee_per_blob_gas = Field::new("max_fee_per_blob_gas", EVM_CURRENCY_TYPE, true);
    let from = Field::new("from", ADDRESS_TYPE, false);
    let status = Field::new("status", DataType::Boolean, false);

    // Access list: List<Struct<address: FixedSizeBinary(20), storage_keys: List<FixedSizeBinary(32)>>>
    let storage_key_type = Field::new("item", BYTES32_TYPE, false);
    let storage_keys_field = Field::new(
        "storage_keys",
        DataType::List(Arc::new(storage_key_type)),
        false,
    );
    let access_list_item_fields = vec![
        Field::new("address", ADDRESS_TYPE, false),
        storage_keys_field,
    ];
    let access_list_item_type = DataType::Struct(access_list_item_fields.into());
    let access_list = Field::new(
        "access_list",
        DataType::List(Arc::new(Field::new("item", access_list_item_type, false))),
        true, // nullable - Legacy transactions don't have access lists
    );

    let fields = vec![
        special_block_num,
        block_hash,
        block_num,
        timestamp,
        tx_index,
        tx_hash,
        to,
        nonce,
        gas_price,
        gas_limit,
        value,
        input,
        r,
        s,
        v_parity,
        chain_id,
        gas_used,
        r#type,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        max_fee_per_blob_gas,
        from,
        status,
        access_list,
    ];

    Schema::new(fields)
}

#[derive(Debug, Default)]
pub(crate) struct Transaction {
    pub(crate) block_hash: Bytes32,
    pub(crate) block_num: u64,
    pub(crate) timestamp: Timestamp,
    pub(crate) tx_index: u32,
    pub(crate) tx_hash: Bytes32,

    pub(crate) to: Option<Address>,
    pub(crate) nonce: u64,

    pub(crate) gas_price: Option<EvmCurrency>,
    pub(crate) gas_limit: u64,

    pub(crate) value: String,

    // Input data the transaction will receive for EVM execution.
    pub(crate) input: Vec<u8>,

    // Elliptic curve parameters.
    pub(crate) r: Bytes32,
    pub(crate) s: Bytes32,
    pub(crate) v_parity: bool,

    pub(crate) chain_id: Option<u64>,

    // The total amount of gas unit used for the whole execution of the transaction.
    pub(crate) receipt_cumulative_gas_used: u64,

    pub(crate) r#type: i32,
    pub(crate) max_fee_per_gas: EvmCurrency,
    pub(crate) max_priority_fee_per_gas: Option<EvmCurrency>,
    pub(crate) max_fee_per_blob_gas: Option<EvmCurrency>,
    pub(crate) from: Address,

    pub(crate) status: bool,

    // EIP-2930, EIP-1559, EIP-4844, EIP-7702 access list
    // Each item contains an address and a list of storage keys
    pub(crate) access_list: Option<Vec<(Address, Vec<[u8; 32]>)>>,
}

pub(crate) struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    block_hash: Bytes32ArrayBuilder,
    block_num: UInt64Builder,
    timestamp: TimestampArrayBuilder,
    tx_index: UInt32Builder,
    tx_hash: Bytes32ArrayBuilder,
    to: EvmAddressArrayBuilder,
    nonce: UInt64Builder,
    gas_price: EvmCurrencyArrayBuilder,
    gas_limit: UInt64Builder,
    value: StringBuilder,
    input: BinaryBuilder,
    r: Bytes32ArrayBuilder,
    s: Bytes32ArrayBuilder,
    v_parity: BooleanBuilder,
    chain_id: UInt64Builder,
    gas_used: UInt64Builder,
    r#type: Int32Builder,
    max_fee_per_gas: EvmCurrencyArrayBuilder,
    max_priority_fee_per_gas: EvmCurrencyArrayBuilder,
    max_fee_per_blob_gas: EvmCurrencyArrayBuilder,
    from: EvmAddressArrayBuilder,
    status: BooleanBuilder,
    access_list: ListBuilder<StructBuilder>,
}

impl TransactionRowsBuilder {
    pub(crate) fn with_capacity(count: usize, total_input_size: usize) -> Self {
        // Helper function to create the StructBuilder for access list items
        fn access_list_item_builder() -> StructBuilder {
            // Create the inner ListBuilder manually to control nullability
            let inner_field = Field::new("item", BYTES32_TYPE, false);
            let storage_keys_builder =
                ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(0, 32))
                    .with_field(inner_field);

            StructBuilder::new(
                Fields::from(vec![
                    Field::new("address", ADDRESS_TYPE, false),
                    Field::new(
                        "storage_keys",
                        DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
                        false,
                    ),
                ]),
                vec![
                    Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)) as _,
                    Box::new(storage_keys_builder) as _,
                ],
            )
        }

        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            block_hash: Bytes32ArrayBuilder::with_capacity(count),
            block_num: UInt64Builder::with_capacity(count),
            timestamp: TimestampArrayBuilder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_hash: Bytes32ArrayBuilder::with_capacity(count),
            to: EvmAddressArrayBuilder::with_capacity(count),
            nonce: UInt64Builder::with_capacity(count),
            gas_price: EvmCurrencyArrayBuilder::with_capacity(count),
            gas_limit: UInt64Builder::with_capacity(count),
            value: StringBuilder::new(),
            input: BinaryBuilder::with_capacity(count, total_input_size),
            r: Bytes32ArrayBuilder::with_capacity(count),
            s: Bytes32ArrayBuilder::with_capacity(count),
            v_parity: BooleanBuilder::with_capacity(count),
            chain_id: UInt64Builder::with_capacity(count),
            gas_used: UInt64Builder::with_capacity(count),
            r#type: Int32Builder::with_capacity(count),
            max_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            max_priority_fee_per_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            max_fee_per_blob_gas: EvmCurrencyArrayBuilder::with_capacity(count),
            from: EvmAddressArrayBuilder::with_capacity(count),
            status: BooleanBuilder::with_capacity(count),
            access_list: {
                let access_list_item_field = Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("address", ADDRESS_TYPE, false),
                        Field::new(
                            "storage_keys",
                            DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
                            false,
                        ),
                    ])),
                    false,
                );
                ListBuilder::with_capacity(access_list_item_builder(), count)
                    .with_field(access_list_item_field)
            },
        }
    }

    pub(crate) fn append(&mut self, tx: &Transaction) {
        let Transaction {
            block_hash,
            block_num,
            timestamp,
            tx_index,
            tx_hash,
            to,
            nonce,
            gas_price,
            gas_limit,
            value,
            input,
            r,
            s,
            v_parity,
            chain_id,
            receipt_cumulative_gas_used,
            r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
            from,
            status,
            access_list,
        } = tx;

        self.special_block_num.append_value(*block_num);
        self.block_hash.append_value(*block_hash);
        self.block_num.append_value(*block_num);
        self.timestamp.append_value(*timestamp);
        self.tx_index.append_value(*tx_index);
        self.tx_hash.append_value(*tx_hash);
        self.to.append_option(*to);
        self.nonce.append_value(*nonce);
        self.gas_price.append_option(*gas_price);
        self.gas_limit.append_value(*gas_limit);
        self.value.append_value(value);
        self.input.append_value(input);
        self.r.append_value(*r);
        self.s.append_value(*s);
        self.v_parity.append_value(*v_parity);
        self.chain_id.append_option(*chain_id);
        self.gas_used.append_value(*receipt_cumulative_gas_used);
        self.r#type.append_value(*r#type);
        self.max_fee_per_gas.append_value(*max_fee_per_gas);
        self.max_priority_fee_per_gas
            .append_option(*max_priority_fee_per_gas);
        self.max_fee_per_blob_gas
            .append_option(*max_fee_per_blob_gas);
        self.from.append_value(*from);
        self.status.append_value(*status);

        if let Some(access_list) = access_list {
            for (address, storage_keys) in access_list {
                let struct_builder = self.access_list.values();

                // Append address field (index 0)
                struct_builder
                    .field_builder::<FixedSizeBinaryBuilder>(0)
                    .unwrap()
                    .append_value(address)
                    .unwrap();

                // Append storage_keys field (index 1) - this is a List<FixedSizeBinary>
                let storage_keys_builder = struct_builder
                    .field_builder::<ListBuilder<FixedSizeBinaryBuilder>>(1)
                    .unwrap();
                for key in storage_keys {
                    storage_keys_builder.values().append_value(key).unwrap();
                }
                storage_keys_builder.append(true);

                struct_builder.append(true);
            }
            self.access_list.append(true);
        } else {
            // Legacy transactions don't have access lists
            self.access_list.append(false);
        }
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<RawTableRows, BoxError> {
        let Self {
            mut special_block_num,
            block_hash,
            mut block_num,
            mut timestamp,
            mut tx_index,
            tx_hash,
            to,
            mut nonce,
            gas_price,
            mut gas_limit,
            mut value,
            mut input,
            r,
            s,
            mut v_parity,
            mut chain_id,
            mut gas_used,
            mut r#type,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
            from,
            mut status,
            mut access_list,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(block_hash.finish()),
            Arc::new(block_num.finish()),
            Arc::new(timestamp.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(to.finish()),
            Arc::new(nonce.finish()),
            Arc::new(gas_price.finish()),
            Arc::new(gas_limit.finish()),
            Arc::new(value.finish()),
            Arc::new(input.finish()),
            Arc::new(r.finish()),
            Arc::new(s.finish()),
            Arc::new(v_parity.finish()),
            Arc::new(chain_id.finish()),
            Arc::new(gas_used.finish()),
            Arc::new(r#type.finish()),
            Arc::new(max_fee_per_gas.finish()),
            Arc::new(max_priority_fee_per_gas.finish()),
            Arc::new(max_fee_per_blob_gas.finish()),
            Arc::new(from.finish()),
            Arc::new(status.finish()),
            Arc::new(access_list.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}

#[test]
fn default_to_arrow() {
    let tx = Transaction::default();
    let rows = {
        let mut builder = TransactionRowsBuilder::with_capacity(1, tx.input.len());
        builder.append(&tx);
        builder
            .build(BlockRange {
                numbers: tx.block_num..=tx.block_num,
                network: "test_network".to_string(),
                hash: tx.block_hash.into(),
                prev_hash: None,
            })
            .unwrap()
    };
    assert_eq!(rows.rows.num_columns(), 24);
    assert_eq!(rows.rows.num_rows(), 1);
}
