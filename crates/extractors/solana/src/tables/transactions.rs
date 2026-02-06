use std::sync::{Arc, LazyLock};

use anyhow::Context;
use base64::Engine;
use datasets_common::{
    block_num::RESERVED_BLOCK_NUM_COLUMN_NAME, block_range::BlockRange, dataset::Table,
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, BooleanBuilder, DataType, Field, Fields, Float64Builder, Int64Builder,
        ListBuilder, Schema, SchemaRef, StringBuilder, StructBuilder, UInt8Builder, UInt32Builder,
        UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};
use solana_clock::Slot;

use crate::{of1_client, rpc_client, tables};

pub const TABLE_NAME: &str = "transactions";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table(network: NetworkId) -> Table {
    let name = TABLE_NAME.parse().expect("table name is valid");
    Table::new(name, SCHEMA.clone(), network, vec!["slot".to_string()])
}

/// Prefer using the pre-computed [SCHEMA].
fn schema() -> Schema {
    let fields = vec![
        Field::new(RESERVED_BLOCK_NUM_COLUMN_NAME, DataType::UInt64, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("tx_index", DataType::UInt32, false),
        Field::new(
            "signatures",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        // Transaction status meta fields.
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
        Field::new(
            "log_messages",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("pre_token_balances", token_balances_dtype(), true),
        Field::new("post_token_balances", token_balances_dtype(), true),
        Field::new(
            "rewards",
            DataType::List(Arc::new(Field::new("item", reward_dtype(), true))),
            true,
        ),
        Field::new(
            "loaded_addresses_writable",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "loaded_addresses_readonly",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("return_data_program_id", DataType::Utf8, true),
        Field::new(
            "return_data_data",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            true,
        ),
        Field::new("compute_units_consumed", DataType::UInt64, true),
        Field::new("cost_units", DataType::UInt64, true),
    ];

    Schema::new(fields)
}

fn token_balances_dtype() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![
            Field::new("account_index", DataType::UInt8, false),
            Field::new("mint", DataType::Utf8, false),
            Field::new("ui_token_amount", ui_token_amount_dtype(), false),
            Field::new("owner", DataType::Utf8, true),
            Field::new("program_id", DataType::Utf8, true),
        ])),
        true,
    )))
}

fn ui_token_amount_dtype() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("ui_amount", DataType::Float64, true),
        Field::new("decimals", DataType::UInt8, false),
        Field::new("amount", DataType::Utf8, false),
        Field::new("ui_amount_string", DataType::Utf8, false),
    ]))
}

fn reward_dtype() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("lamports", DataType::Int64, false),
        Field::new("post_balance", DataType::UInt64, false),
        Field::new("reward_type", DataType::Utf8, true),
        Field::new("commission", DataType::UInt8, true),
    ]))
}

#[derive(Debug, Default, PartialEq)]
pub struct Transaction {
    pub slot: Slot,
    pub index: u32,

    pub signatures: Vec<String>,
    pub transaction_status_meta: Option<TransactionStatusMeta>,
}

impl Transaction {
    pub(crate) fn from_of1_transaction(
        slot: Slot,
        tx_index: u32,
        of1_tx_signatures: Vec<solana_sdk::signature::Signature>,
        of1_tx_meta: Option<of1_client::DecodedTransactionStatusMeta>,
    ) -> anyhow::Result<Self> {
        let signatures = of1_tx_signatures.iter().map(|s| s.to_string()).collect();
        let transaction_status_meta = of1_tx_meta
            .map(|meta| match meta {
                of1_client::DecodedTransactionStatusMeta::Proto(proto_meta) => {
                    TransactionStatusMeta::from_proto_meta(slot, tx_index, proto_meta)
                }

                of1_client::DecodedTransactionStatusMeta::Bincode(stored_meta) => Ok(
                    TransactionStatusMeta::from_stored_meta(slot, tx_index, stored_meta),
                ),
            })
            .transpose()?;

        Ok(Self {
            slot,
            index: tx_index,
            signatures,
            transaction_status_meta,
        })
    }

    pub(crate) fn from_rpc_transaction(
        slot: Slot,
        tx_index: u32,
        rpc_tx_signatures: Vec<String>,
        rpc_tx_meta: Option<rpc_client::UiTransactionStatusMeta>,
    ) -> anyhow::Result<Self> {
        let transaction_status_meta = rpc_tx_meta
            .map(|rpc_meta| TransactionStatusMeta::from_rpc_meta(slot, tx_index, rpc_meta))
            .transpose()?;

        Ok(Self {
            slot,
            index: tx_index,
            signatures: rpc_tx_signatures,
            transaction_status_meta,
        })
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct TransactionStatusMeta {
    // `true` if the transaction succeeded, `false` otherwise.
    pub status: bool,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<Vec<tables::instructions::Instruction>>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub rewards: Option<Vec<tables::block_rewards::Reward>>,
    pub loaded_addresses: Option<LoadedAddresses>,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

impl TransactionStatusMeta {
    pub(crate) fn from_stored_meta(
        slot: u64,
        tx_index: u32,
        stored_tx_meta: solana_storage_proto::StoredTransactionStatusMeta,
    ) -> Self {
        let rpc_tx_meta = crate::rpc_client::TransactionStatusMeta::from(stored_tx_meta);
        let inner_instructions = rpc_tx_meta
            .inner_instructions
            .map(|inner_instructions_vec| {
                inner_instructions_vec
                    .into_iter()
                    .map(|inner_instructions| {
                        inner_instructions
                            .instructions
                            .into_iter()
                            .map(|inst| tables::instructions::Instruction {
                                slot,
                                tx_index,
                                program_id_index: inst.instruction.program_id_index,
                                accounts: inst.instruction.accounts,
                                data: inst.instruction.data,
                                inner_index: Some(inner_instructions.index as u32),
                                inner_stack_height: inst.stack_height,
                            })
                            .collect()
                    })
                    .collect()
            });

        let pre_token_balances = rpc_tx_meta
            .pre_token_balances
            .map(|pre_token_balances| pre_token_balances.into_iter().map(From::from).collect());
        let post_token_balances = rpc_tx_meta
            .post_token_balances
            .map(|post_token_balances| post_token_balances.into_iter().map(From::from).collect());

        let rewards = rpc_tx_meta
            .rewards
            .map(|rewards| rewards.into_iter().map(Into::into).collect());

        let loaded_addresses = LoadedAddresses {
            writable: rpc_tx_meta
                .loaded_addresses
                .writable
                .iter()
                .map(|a| a.to_string())
                .collect(),
            readonly: rpc_tx_meta
                .loaded_addresses
                .readonly
                .iter()
                .map(|a| a.to_string())
                .collect(),
        };

        let return_data = rpc_tx_meta
            .return_data
            .map(|return_data| TransactionReturnData {
                program_id: return_data.program_id.to_string(),
                data: return_data.data,
            });

        Self {
            status: rpc_tx_meta.status.is_ok(),
            fee: rpc_tx_meta.fee,
            pre_balances: rpc_tx_meta.pre_balances,
            post_balances: rpc_tx_meta.post_balances,
            inner_instructions,
            log_messages: rpc_tx_meta.log_messages,
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_addresses: Some(loaded_addresses),
            return_data,
            compute_units_consumed: rpc_tx_meta.compute_units_consumed,
            cost_units: rpc_tx_meta.cost_units,
        }
    }

    pub(crate) fn from_proto_meta(
        slot: Slot,
        tx_index: u32,
        of_tx_meta: solana_storage_proto::confirmed_block::TransactionStatusMeta,
    ) -> anyhow::Result<Self> {
        let inner_instructions = of_tx_meta
            .inner_instructions
            .into_iter()
            .map(|inner| {
                inner
                    .instructions
                    .into_iter()
                    .map(|inst| tables::instructions::Instruction {
                        slot,
                        tx_index,
                        program_id_index: inst.program_id_index as u8,
                        accounts: inst.accounts,
                        data: inst.data,
                        inner_index: Some(inner.index),
                        inner_stack_height: inst.stack_height,
                    })
                    .collect()
            })
            .collect();

        let pre_token_balances = of_tx_meta
            .pre_token_balances
            .into_iter()
            .map(TransactionTokenBalance::from)
            .collect();
        let post_token_balances = of_tx_meta
            .post_token_balances
            .into_iter()
            .map(TransactionTokenBalance::from)
            .collect();

        let rewards: Vec<tables::block_rewards::Reward> = of_tx_meta
            .rewards
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
            .context("converting of1 tx rewards")?;

        let loaded_addresses = LoadedAddresses {
            writable: of_tx_meta
                .loaded_writable_addresses
                .iter()
                .map(|a| bs58::encode(&a).into_string())
                .collect(),
            readonly: of_tx_meta
                .loaded_readonly_addresses
                .iter()
                .map(|a| bs58::encode(&a).into_string())
                .collect(),
        };

        let return_data = of_tx_meta.return_data.map(|return_data| {
            let program_id = bs58::encode(&return_data.program_id).into_string();
            TransactionReturnData {
                program_id,
                data: return_data.data,
            }
        });

        Ok(TransactionStatusMeta {
            status: of_tx_meta.err.is_none(),
            fee: of_tx_meta.fee,
            pre_balances: of_tx_meta.pre_balances,
            post_balances: of_tx_meta.post_balances,
            inner_instructions: Some(inner_instructions),
            log_messages: Some(of_tx_meta.log_messages),
            pre_token_balances: Some(pre_token_balances),
            post_token_balances: Some(post_token_balances),
            rewards: Some(rewards),
            loaded_addresses: Some(loaded_addresses),
            return_data,
            compute_units_consumed: of_tx_meta.compute_units_consumed,
            cost_units: of_tx_meta.cost_units,
        })
    }

    pub(crate) fn from_rpc_meta(
        slot: Slot,
        tx_index: u32,
        rpc_meta: rpc_client::UiTransactionStatusMeta,
    ) -> anyhow::Result<Self> {
        let inner_instructions = rpc_meta
            .inner_instructions
            .map(|rpc_inner_instructions| {
                let mut inner_instructions = Vec::with_capacity(rpc_inner_instructions.len());

                for rpc_inner_instruction_array in rpc_inner_instructions {
                    let mut inner_instruction_array =
                        Vec::with_capacity(rpc_inner_instruction_array.instructions.len());

                    for rpc_inner_instruction in rpc_inner_instruction_array.instructions {
                        let rpc_client::UiInstruction::Compiled(compiled) = rpc_inner_instruction
                        else {
                            anyhow::bail!("unexpected inner instruction format; slot {slot}, transaction index {tx_index}");
                        };
                        let data = bs58::decode(&compiled.data)
                            .into_vec()
                            .context("decoding base58 inner instruction data")?;
                        let instruction = tables::instructions::Instruction {
                            slot,
                            tx_index,
                            program_id_index: compiled.program_id_index,
                            accounts: compiled.accounts,
                            data,
                            inner_index: Some(rpc_inner_instruction_array.index as u32),
                            inner_stack_height: compiled.stack_height,
                        };

                        inner_instruction_array.push(instruction);
                    }

                    inner_instructions.push(inner_instruction_array);
                }

                Ok(inner_instructions)
            })
            .transpose()?;

        let pre_token_balances = rpc_meta.pre_token_balances.map(|pre_token_balances| {
            pre_token_balances
                .into_iter()
                .map(TransactionTokenBalance::from)
                .collect()
        });
        let post_token_balances = rpc_meta.post_token_balances.map(|post_token_balances| {
            post_token_balances
                .into_iter()
                .map(TransactionTokenBalance::from)
                .collect()
        });

        let rewards = rpc_meta
            .rewards
            .map(|rewards| rewards.into_iter().map(Into::into).collect());

        let loaded_addresses = rpc_meta
            .loaded_addresses
            .map(|loaded_addresses| LoadedAddresses {
                writable: loaded_addresses.writable,
                readonly: loaded_addresses.readonly,
            });

        let return_data = rpc_meta
            .return_data
            .map(|return_data| {
                let (data, encoding) = return_data.data;
                let data = match encoding {
                    rpc_client::UiReturnDataEncoding::Base64 => base64::prelude::BASE64_STANDARD
                        .decode(data)
                        .context("decoding base64 return data")?,
                };
                anyhow::Ok(TransactionReturnData {
                    program_id: return_data.program_id,
                    data,
                })
            })
            .transpose()?;

        Ok(Self {
            status: rpc_meta.err.is_none(),
            fee: rpc_meta.fee,
            pre_balances: rpc_meta.pre_balances,
            post_balances: rpc_meta.post_balances,
            inner_instructions,
            log_messages: rpc_meta.log_messages.into(),
            pre_token_balances,
            post_token_balances,
            rewards,
            loaded_addresses,
            return_data,
            compute_units_consumed: rpc_meta.compute_units_consumed.into(),
            cost_units: rpc_meta.cost_units.into(),
        })
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct TransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: TokenAmount,
    pub owner: Option<String>,
    pub program_id: Option<String>,
}

impl From<rpc_client::UiTransactionTokenBalance> for TransactionTokenBalance {
    fn from(value: rpc_client::UiTransactionTokenBalance) -> Self {
        Self {
            account_index: value.account_index,
            mint: value.mint,
            ui_token_amount: TokenAmount {
                ui_amount: value.ui_token_amount.ui_amount,
                decimals: value.ui_token_amount.decimals,
                amount: value.ui_token_amount.amount,
                ui_amount_string: value.ui_token_amount.ui_amount_string,
            },
            owner: value.owner.map(|owner| owner),
            program_id: value.program_id.map(|pid| pid),
        }
    }
}

impl From<rpc_client::TransactionTokenBalance> for TransactionTokenBalance {
    fn from(value: rpc_client::TransactionTokenBalance) -> Self {
        let ui_token_amount = TokenAmount {
            ui_amount: value.ui_token_amount.ui_amount,
            decimals: value.ui_token_amount.decimals,
            amount: value.ui_token_amount.amount,
            ui_amount_string: value.ui_token_amount.ui_amount_string,
        };
        Self {
            account_index: value.account_index,
            mint: value.mint,
            ui_token_amount,
            owner: Some(value.owner),
            program_id: Some(value.program_id),
        }
    }
}

impl From<solana_storage_proto::confirmed_block::TokenBalance> for TransactionTokenBalance {
    fn from(value: solana_storage_proto::confirmed_block::TokenBalance) -> Self {
        let ui_token_amount = value
            .ui_token_amount
            .map(|token_amount| TokenAmount {
                ui_amount: Some(token_amount.ui_amount),
                decimals: token_amount.decimals as u8,
                amount: token_amount.amount,
                ui_amount_string: token_amount.ui_amount_string,
            })
            .unwrap_or_default();

        Self {
            account_index: value.account_index as u8,
            mint: value.mint,
            ui_token_amount,
            owner: Some(value.owner),
            program_id: Some(value.program_id),
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct TokenAmount {
    pub ui_amount: Option<f64>,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

#[derive(Debug, Default, PartialEq)]
pub struct LoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}

#[derive(Debug, Default, PartialEq)]
pub struct TransactionReturnData {
    pub program_id: String,
    pub data: Vec<u8>,
}

pub(crate) struct TransactionRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    tx_index: UInt32Builder,

    tx_signatures: ListBuilder<StringBuilder>,
    status: BooleanBuilder,
    fee: UInt64Builder,
    pre_balances: ListBuilder<UInt64Builder>,
    post_balances: ListBuilder<UInt64Builder>,
    log_messages: ListBuilder<StringBuilder>,
    pre_token_balances: ListBuilder<StructBuilder>,
    post_token_balances: ListBuilder<StructBuilder>,
    rewards: ListBuilder<StructBuilder>,
    loaded_addresses_writable: ListBuilder<StringBuilder>,
    loaded_addresses_readonly: ListBuilder<StringBuilder>,
    return_data_program_id: StringBuilder,
    return_data_data: ListBuilder<UInt8Builder>,
    compute_units_consumed: UInt64Builder,
    cost_units: UInt64Builder,
}

impl TransactionRowsBuilder {
    /// Creates a new [TransactionRowsBuilder] with enough capacity to hold `count` transactions.
    pub(crate) fn with_capacity(count: usize) -> Self {
        fn ui_token_amount_builder() -> StructBuilder {
            StructBuilder::new(
                Fields::from(vec![
                    Field::new("ui_amount", DataType::Float64, true),
                    Field::new("decimals", DataType::UInt8, false),
                    Field::new("amount", DataType::Utf8, false),
                    Field::new("ui_amount_string", DataType::Utf8, false),
                ]),
                vec![
                    Box::new(Float64Builder::new()),
                    Box::new(UInt8Builder::new()),
                    Box::new(StringBuilder::new()),
                    Box::new(StringBuilder::new()),
                ],
            )
        }

        fn token_balances_builder() -> StructBuilder {
            StructBuilder::new(
                Fields::from(vec![
                    Field::new("account_index", DataType::UInt8, false),
                    Field::new("mint", DataType::Utf8, false),
                    Field::new("ui_token_amount", ui_token_amount_dtype(), false),
                    Field::new("owner", DataType::Utf8, true),
                    Field::new("program_id", DataType::Utf8, true),
                ]),
                vec![
                    Box::new(UInt8Builder::new()),
                    Box::new(StringBuilder::new()),
                    Box::new(ui_token_amount_builder()),
                    Box::new(StringBuilder::new()),
                    Box::new(StringBuilder::new()),
                ],
            )
        }

        fn reward_builder() -> StructBuilder {
            StructBuilder::new(
                Fields::from(vec![
                    Field::new("pubkey", DataType::Utf8, false),
                    Field::new("lamports", DataType::Int64, false),
                    Field::new("post_balance", DataType::UInt64, false),
                    Field::new("reward_type", DataType::Utf8, true),
                    Field::new("commission", DataType::UInt8, true),
                ]),
                vec![
                    Box::new(StringBuilder::new()),
                    Box::new(Int64Builder::new()),
                    Box::new(UInt64Builder::new()),
                    Box::new(StringBuilder::new()),
                    Box::new(UInt8Builder::new()),
                ],
            )
        }

        Self {
            special_block_num: UInt64Builder::with_capacity(count),
            slot: UInt64Builder::with_capacity(count),
            tx_index: UInt32Builder::with_capacity(count),
            tx_signatures: ListBuilder::with_capacity(StringBuilder::new(), count),
            status: BooleanBuilder::with_capacity(count),
            fee: UInt64Builder::with_capacity(count),
            pre_balances: ListBuilder::with_capacity(UInt64Builder::new(), count),
            post_balances: ListBuilder::with_capacity(UInt64Builder::new(), count),
            log_messages: ListBuilder::with_capacity(StringBuilder::new(), count),
            pre_token_balances: ListBuilder::with_capacity(token_balances_builder(), count),
            post_token_balances: ListBuilder::with_capacity(token_balances_builder(), count),
            rewards: ListBuilder::with_capacity(reward_builder(), count),
            loaded_addresses_writable: ListBuilder::with_capacity(StringBuilder::new(), count),
            loaded_addresses_readonly: ListBuilder::with_capacity(StringBuilder::new(), count),
            return_data_program_id: StringBuilder::with_capacity(count, 0),
            return_data_data: ListBuilder::with_capacity(UInt8Builder::new(), count),
            compute_units_consumed: UInt64Builder::with_capacity(count),
            cost_units: UInt64Builder::with_capacity(count),
        }
    }

    pub(crate) fn append(&mut self, tx: &Transaction) {
        self.special_block_num.append_value(tx.slot);
        self.slot.append_value(tx.slot);
        self.tx_index.append_value(tx.index);

        for sig in &tx.signatures {
            self.tx_signatures.values().append_value(sig);
        }
        self.tx_signatures.append(true);

        if let Some(meta) = tx.transaction_status_meta.as_ref() {
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

            if let Some(log_messages) = &meta.log_messages {
                for log in log_messages {
                    self.log_messages.values().append_value(log);
                }
                self.log_messages.append(true);
            } else {
                self.log_messages.append_null();
            }

            if let Some(pre_token_balances) = &meta.pre_token_balances {
                for balance in pre_token_balances {
                    let struct_builder = self.pre_token_balances.values();

                    struct_builder
                        .field_builder::<UInt8Builder>(0)
                        .unwrap()
                        .append_value(balance.account_index);
                    struct_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value(&balance.mint);

                    {
                        let struct_builder =
                            struct_builder.field_builder::<StructBuilder>(2).unwrap();
                        struct_builder
                            .field_builder::<Float64Builder>(0)
                            .unwrap()
                            .append_option(balance.ui_token_amount.ui_amount);
                        struct_builder
                            .field_builder::<UInt8Builder>(1)
                            .unwrap()
                            .append_value(balance.ui_token_amount.decimals);
                        struct_builder
                            .field_builder::<StringBuilder>(2)
                            .unwrap()
                            .append_value(&balance.ui_token_amount.amount);
                        struct_builder
                            .field_builder::<StringBuilder>(3)
                            .unwrap()
                            .append_value(&balance.ui_token_amount.ui_amount_string);

                        struct_builder.append(true);
                    }

                    struct_builder
                        .field_builder::<StringBuilder>(3)
                        .unwrap()
                        .append_option(balance.owner.clone());
                    struct_builder
                        .field_builder::<StringBuilder>(4)
                        .unwrap()
                        .append_option(balance.program_id.clone());

                    struct_builder.append(true);
                }
                self.pre_token_balances.append(true);
            } else {
                self.pre_token_balances.append_null();
            }

            if let Some(post_token_balances) = &meta.post_token_balances {
                for balance in post_token_balances {
                    let struct_builder = self.post_token_balances.values();

                    struct_builder
                        .field_builder::<UInt8Builder>(0)
                        .unwrap()
                        .append_value(balance.account_index);
                    struct_builder
                        .field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value(&balance.mint);

                    {
                        let struct_builder =
                            struct_builder.field_builder::<StructBuilder>(2).unwrap();
                        struct_builder
                            .field_builder::<Float64Builder>(0)
                            .unwrap()
                            .append_option(balance.ui_token_amount.ui_amount);
                        struct_builder
                            .field_builder::<UInt8Builder>(1)
                            .unwrap()
                            .append_value(balance.ui_token_amount.decimals);
                        struct_builder
                            .field_builder::<StringBuilder>(2)
                            .unwrap()
                            .append_value(&balance.ui_token_amount.amount);
                        struct_builder
                            .field_builder::<StringBuilder>(3)
                            .unwrap()
                            .append_value(&balance.ui_token_amount.ui_amount_string);

                        struct_builder.append(true);
                    }

                    struct_builder
                        .field_builder::<StringBuilder>(3)
                        .unwrap()
                        .append_option(balance.owner.clone());
                    struct_builder
                        .field_builder::<StringBuilder>(4)
                        .unwrap()
                        .append_option(balance.program_id.clone());

                    struct_builder.append(true);
                }

                self.post_token_balances.append(true);
            } else {
                self.post_token_balances.append_null();
            }

            if let Some(rewards) = &meta.rewards {
                for reward in rewards {
                    let struct_builder = self.rewards.values();

                    struct_builder
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(&reward.pubkey);
                    struct_builder
                        .field_builder::<Int64Builder>(1)
                        .unwrap()
                        .append_value(reward.lamports);
                    struct_builder
                        .field_builder::<UInt64Builder>(2)
                        .unwrap()
                        .append_value(reward.post_balance);
                    struct_builder
                        .field_builder::<StringBuilder>(3)
                        .unwrap()
                        .append_option(reward.reward_type.as_ref().map(|typ| typ.to_string()));
                    struct_builder
                        .field_builder::<UInt8Builder>(4)
                        .unwrap()
                        .append_option(reward.commission);

                    struct_builder.append(true);
                }

                self.rewards.append(true);
            } else {
                self.rewards.append_null();
            }

            if let Some(loaded_addresses) = &meta.loaded_addresses {
                for addr in &loaded_addresses.writable {
                    self.loaded_addresses_writable.values().append_value(addr);
                }
                self.loaded_addresses_writable.append(true);
                for addr in &loaded_addresses.readonly {
                    self.loaded_addresses_readonly.values().append_value(addr);
                }
                self.loaded_addresses_readonly.append(true);
            } else {
                self.loaded_addresses_writable.append_null();
                self.loaded_addresses_readonly.append_null();
            }

            if let Some(return_data) = &meta.return_data {
                self.return_data_program_id
                    .append_value(&return_data.program_id);
                self.return_data_data
                    .values()
                    .append_slice(&return_data.data);
                self.return_data_data.append(true);
            } else {
                self.return_data_program_id.append_null();
                self.return_data_data.append_null();
            }

            self.compute_units_consumed
                .append_option(meta.compute_units_consumed);
            self.cost_units.append_option(meta.cost_units);
        } else {
            self.status.append_null();
            self.fee.append_null();
            self.pre_balances.append(false);
            self.post_balances.append(false);
            self.log_messages.append_null();
            self.pre_token_balances.append_null();
            self.post_token_balances.append_null();
            self.rewards.append_null();
            self.loaded_addresses_writable.append_null();
            self.loaded_addresses_readonly.append_null();
            self.return_data_program_id.append_null();
            self.return_data_data.append_null();
            self.compute_units_consumed.append_null();
            self.cost_units.append_null();
        }
    }

    pub(crate) fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            mut slot,
            mut tx_index,
            mut tx_signatures,
            mut status,
            mut fee,
            mut pre_balances,
            mut post_balances,
            mut log_messages,
            mut pre_token_balances,
            mut post_token_balances,
            mut rewards,
            mut loaded_addresses_writable,
            mut loaded_addresses_readonly,
            mut return_data_program_id,
            mut return_data_data,
            mut compute_units_consumed,
            mut cost_units,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(tx_index.finish()),
            Arc::new(tx_signatures.finish()),
            Arc::new(status.finish()),
            Arc::new(fee.finish()),
            Arc::new(pre_balances.finish()),
            Arc::new(post_balances.finish()),
            Arc::new(log_messages.finish()),
            Arc::new(pre_token_balances.finish()),
            Arc::new(post_token_balances.finish()),
            Arc::new(rewards.finish()),
            Arc::new(loaded_addresses_writable.finish()),
            Arc::new(loaded_addresses_readonly.finish()),
            Arc::new(return_data_program_id.finish()),
            Arc::new(return_data_data.finish()),
            Arc::new(compute_units_consumed.finish()),
            Arc::new(cost_units.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}
