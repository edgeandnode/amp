use std::sync::{Arc, LazyLock};

use common::{
    BoxResult, RawTableRows, SPECIAL_BLOCK_NUM, Table,
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    metadata::segments::BlockRange,
};
use solana_clock::Slot;

use crate::rpc_client::{UiTransaction, UiTransactionStatusMeta};

pub const TABLE_NAME: &str = "transactions";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: String) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(name, SCHEMA.clone(), network, vec!["slot".to_string()]).unwrap()
}

/// Prefer using the pre-computed [SCHEMA].
fn schema() -> Schema {
    let fields = vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new(
            "tx_signatures",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("status", DataType::Boolean, true),
        Field::new("fee", DataType::UInt64, true),
        Field::new(
            "pre_balances",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new(
            "post_balances",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        ),
        Field::new("slot", DataType::UInt64, false),
    ];

    Schema::new(fields)
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Transaction {
    // Tx data.
    pub(crate) tx_index: u32,
    pub(crate) tx_signatures: Vec<String>,

    pub(crate) tx_meta: Option<TransactionMeta>,

    // Block data.
    pub(crate) slot: u64,
}

impl Transaction {
    pub(crate) fn from_rpc_transaction(
        slot: Slot,
        tx_index: u32,
        rpc_tx: &UiTransaction,
        rpc_tx_meta: Option<&UiTransactionStatusMeta>,
    ) -> Self {
        let tx_meta = rpc_tx_meta.map(|meta| TransactionMeta {
            status: meta.err.is_none(),
            fee: meta.fee,
            pre_balances: meta.pre_balances.clone(),
            post_balances: meta.post_balances.clone(),
        });

        Self {
            tx_index,
            tx_signatures: rpc_tx.signatures.clone(),
            tx_meta,
            slot,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TransactionMeta {
    status: bool,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

pub(crate) struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    tx_index: UInt32Builder,
    tx_signatures: ListBuilder<StringBuilder>,
    status: BooleanBuilder,
    fee: UInt64Builder,
    pre_balances: ListBuilder<UInt64Builder>,
    post_balances: ListBuilder<UInt64Builder>,

    slot: UInt64Builder,
}

impl TransactionRowsBuilder {
    pub(crate) fn with_capacity(count: usize) -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_signatures: ListBuilder::with_capacity(StringBuilder::new(), count),
            status: BooleanBuilder::with_capacity(count),
            fee: UInt64Builder::with_capacity(count),
            pre_balances: ListBuilder::with_capacity(UInt64Builder::new(), count),
            post_balances: ListBuilder::with_capacity(UInt64Builder::new(), count),
            slot: UInt64Builder::with_capacity(count),
        }
    }

    pub(crate) fn append(&mut self, tx: &Transaction) {
        let Transaction {
            tx_index,
            tx_signatures,
            tx_meta,
            slot,
        } = tx;

        self.special_block_num.append_value(*slot);
        self.tx_index.append_value(*tx_index);
        for sig in tx_signatures {
            self.tx_signatures.values().append_value(sig);
        }
        self.tx_signatures.append(true);
        if let Some(meta) = tx_meta {
            self.status.append_value(meta.status);
            self.fee.append_value(meta.fee);
            for pre_balance in &meta.pre_balances {
                self.pre_balances.values().append_value(*pre_balance);
            }
            self.pre_balances.append(true);
            for post_balance in &meta.post_balances {
                self.post_balances.values().append_value(*post_balance);
            }
            self.post_balances.append(true);
        } else {
            self.status.append_null();
            self.fee.append_null();
            self.pre_balances.append(false);
            self.post_balances.append(false);
        }
        self.slot.append_value(*slot);
    }

    pub(crate) fn build(self, range: BlockRange) -> BoxResult<RawTableRows> {
        let Self {
            mut special_block_num,
            mut tx_index,
            mut tx_signatures,
            mut status,
            mut fee,
            mut pre_balances,
            mut post_balances,
            mut slot,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(tx_index.finish()),
            Arc::new(tx_signatures.finish()),
            Arc::new(status.finish()),
            Arc::new(fee.finish()),
            Arc::new(pre_balances.finish()),
            Arc::new(post_balances.finish()),
            Arc::new(slot.finish()),
        ];

        RawTableRows::new(table(range.network.clone()), range, columns)
    }
}
