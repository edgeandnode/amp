use std::sync::{Arc, LazyLock};

use common::{
    BYTES32_TYPE, BlockRange, BoxError, Bytes32, Bytes32ArrayBuilder,
    EVM_ADDRESS_TYPE as ADDRESS_TYPE, EVM_CURRENCY_TYPE, EvmAddress as Address,
    EvmAddressArrayBuilder, EvmCurrency, EvmCurrencyArrayBuilder, SPECIAL_BLOCK_NUM, Timestamp,
    TimestampArrayBuilder,
    arrow::{
        array::{
            ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Int32Builder,
            ListBuilder, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
};
use datasets_common::dataset::Table;
use datasets_raw::rows::TableRows;

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

/// EIP-7702 authorization tuple: (chain_id, address, nonce, y_parity, r, s)
type AuthorizationTuple = (u64, [u8; 20], u64, bool, [u8; 32], [u8; 32]);

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
    let state_root = Field::new("state_root", BYTES32_TYPE, true);
    let access_list = Field::new(
        "access_list",
        DataType::List(Arc::new(Field::new(
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
        ))),
        true, // nullable - Legacy transactions don't have access lists
    );
    let blob_versioned_hashes = Field::new(
        "blob_versioned_hashes",
        DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
        true, // nullable - only EIP-4844 transactions have blob versioned hashes
    );
    let authorization_list = Field::new(
        "authorization_list",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("chain_id", DataType::UInt64, false),
                Field::new("address", ADDRESS_TYPE, false),
                Field::new("nonce", DataType::UInt64, false),
                Field::new("y_parity", DataType::Boolean, false),
                Field::new("r", BYTES32_TYPE, false),
                Field::new("s", BYTES32_TYPE, false),
            ])),
            false,
        ))),
        true, // nullable - only EIP-7702 transactions have authorization lists
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
        state_root,
        access_list,
        blob_versioned_hashes,
        authorization_list,
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

    // State root from transaction receipt (pre-Byzantium only, post-Byzantium will be None)
    pub(crate) state_root: Option<Bytes32>,

    // EIP-2930, EIP-1559, EIP-4844, EIP-7702 access list
    // Each item contains an address and a list of storage keys
    pub(crate) access_list: Option<Vec<(Address, Vec<[u8; 32]>)>>,

    // EIP-4844 blob versioned hashes
    // List of KZG commitment versioned hashes for blob transactions
    pub(crate) blob_versioned_hashes: Option<Vec<[u8; 32]>>,

    // EIP-7702 authorization list
    // Each authorization tuple: (chain_id, address, nonce, y_parity, r, s)
    pub(crate) authorization_list: Option<Vec<AuthorizationTuple>>,
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
    state_root: Bytes32ArrayBuilder,
    access_list: ListBuilder<StructBuilder>,
    blob_versioned_hashes: ListBuilder<FixedSizeBinaryBuilder>,
    authorization_list: ListBuilder<StructBuilder>,
}

impl TransactionRowsBuilder {
    pub(crate) fn with_capacity(count: usize, total_input_size: usize) -> Self {
        let access_list_fields = Fields::from(vec![
            Field::new("address", ADDRESS_TYPE, false),
            Field::new(
                "storage_keys",
                DataType::List(Arc::new(Field::new("item", BYTES32_TYPE, false))),
                false,
            ),
        ]);
        let authorization_list_fields = Fields::from(vec![
            Field::new("chain_id", DataType::UInt64, false),
            Field::new("address", ADDRESS_TYPE, false),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("y_parity", DataType::Boolean, false),
            Field::new("r", BYTES32_TYPE, false),
            Field::new("s", BYTES32_TYPE, false),
        ]);
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
            state_root: Bytes32ArrayBuilder::with_capacity(count),
            // This is verbose, because we need to manually set the inner fields as non-nullable.
            access_list: {
                ListBuilder::with_capacity(
                    StructBuilder::new(
                        access_list_fields.clone(),
                        vec![
                            Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)),
                            Box::new(
                                ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(0, 32))
                                    .with_field(Field::new("item", BYTES32_TYPE, false)),
                            ),
                        ],
                    ),
                    count,
                )
                .with_field(Field::new(
                    "item",
                    DataType::Struct(access_list_fields),
                    false,
                ))
            },
            blob_versioned_hashes: ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(0, 32))
                .with_field(Field::new("item", BYTES32_TYPE, false)),
            authorization_list: ListBuilder::with_capacity(
                StructBuilder::new(
                    authorization_list_fields.clone(),
                    vec![
                        Box::new(UInt64Builder::with_capacity(0)), // chain_id
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 20)), // address
                        Box::new(UInt64Builder::with_capacity(0)), // nonce
                        Box::new(BooleanBuilder::with_capacity(0)), // y_parity
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)), // r
                        Box::new(FixedSizeBinaryBuilder::with_capacity(0, 32)), // s
                    ],
                ),
                count,
            )
            .with_field(Field::new(
                "item",
                DataType::Struct(authorization_list_fields),
                false,
            )),
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
            state_root,
            access_list,
            blob_versioned_hashes,
            authorization_list,
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
        self.state_root.append_option(*state_root);

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

        // Append blob_versioned_hashes (EIP-4844 only)
        if let Some(hashes) = blob_versioned_hashes {
            for hash in hashes {
                self.blob_versioned_hashes
                    .values()
                    .append_value(hash)
                    .unwrap();
            }
            self.blob_versioned_hashes.append(true);
        } else {
            // Non-EIP-4844 transactions don't have blob versioned hashes
            self.blob_versioned_hashes.append(false);
        }

        // Append authorization_list (EIP-7702 only)
        if let Some(auths) = authorization_list {
            for (chain_id, address, nonce, y_parity, r, s) in auths {
                let struct_builder = self.authorization_list.values();

                // Field 0: chain_id
                struct_builder
                    .field_builder::<UInt64Builder>(0)
                    .unwrap()
                    .append_value(*chain_id);

                // Field 1: address (20 bytes)
                struct_builder
                    .field_builder::<FixedSizeBinaryBuilder>(1)
                    .unwrap()
                    .append_value(address)
                    .unwrap();

                // Field 2: nonce
                struct_builder
                    .field_builder::<UInt64Builder>(2)
                    .unwrap()
                    .append_value(*nonce);

                // Field 3: y_parity
                struct_builder
                    .field_builder::<BooleanBuilder>(3)
                    .unwrap()
                    .append_value(*y_parity);

                // Field 4: r (32 bytes)
                struct_builder
                    .field_builder::<FixedSizeBinaryBuilder>(4)
                    .unwrap()
                    .append_value(r)
                    .unwrap();

                // Field 5: s (32 bytes)
                struct_builder
                    .field_builder::<FixedSizeBinaryBuilder>(5)
                    .unwrap()
                    .append_value(s)
                    .unwrap();

                struct_builder.append(true);
            }
            self.authorization_list.append(true);
        } else {
            // Non-EIP-7702 transactions don't have authorization lists
            self.authorization_list.append(false);
        }
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<TableRows, BoxError> {
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
            state_root,
            mut access_list,
            mut blob_versioned_hashes,
            mut authorization_list,
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
            Arc::new(state_root.finish()),
            Arc::new(access_list.finish()),
            Arc::new(blob_versioned_hashes.finish()),
            Arc::new(authorization_list.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns).map_err(Into::into)
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
    assert_eq!(rows.rows.num_columns(), 27);
    assert_eq!(rows.rows.num_rows(), 1);
}
