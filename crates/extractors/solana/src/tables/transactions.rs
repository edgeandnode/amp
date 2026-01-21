use std::sync::{Arc, LazyLock};

use base64::Engine;
use common::{
    BlockRange, BoxResult, SPECIAL_BLOCK_NUM,
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
            StructBuilder, UInt8Builder, UInt32Builder, UInt64Builder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    },
};
use datasets_common::dataset::Table;
use datasets_raw::rows::TableRows;
use serde::Deserialize;
use solana_clock::Slot;

use crate::rpc_client;

pub const TABLE_NAME: &str = "transactions";

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

#[derive(Debug, Default, Clone)]
pub(crate) struct Transaction {
    // Tx data.
    pub(crate) tx_index: u32,
    pub(crate) tx_signatures: Vec<String>,

    pub(crate) tx_meta: Option<TransactionStatusMeta>,

    // Block data.
    pub(crate) slot: u64,
}

impl Transaction {
    pub(crate) fn from_of1_transaction(
        slot: Slot,
        tx_index: u32,
        tx_meta: TransactionStatusMeta,
        of_tx: solana_sdk::transaction::VersionedTransaction,
    ) -> Self {
        Self {
            tx_index,
            tx_signatures: of_tx.signatures.iter().map(|s| s.to_string()).collect(),
            tx_meta: Some(tx_meta),
            slot,
        }
    }

    pub(crate) fn from_rpc_transaction(
        slot: Slot,
        tx_index: u32,
        rpc_tx: &rpc_client::UiTransaction,
        rpc_tx_meta: Option<&rpc_client::UiTransactionStatusMeta>,
    ) -> Self {
        let tx_meta = rpc_tx_meta.map(|meta| {
            let inner_instructions = meta.inner_instructions.as_ref().map(|inners| {
                inners
                    .iter()
                    .map(|inner| {
                        inner
                            .instructions
                            .iter()
                            .cloned()
                            .map(|inst| {
                                let rpc_client::UiInstruction::Compiled(compiled) = inst else {
                                    unreachable!(
                                        "the format of inner instructions has already been verified"
                                    )
                                };
                                let data = bs58::decode(&compiled.data)
                                    .into_vec()
                                    .expect("invalid base-58 string");
                                super::instructions::Instruction {
                                    slot,
                                    tx_index,
                                    program_id_index: compiled.program_id_index,
                                    accounts: compiled.accounts,
                                    data,
                                    inner_index: Some(inner.index as u32),
                                    inner_stack_height: compiled.stack_height,
                                }
                            })
                            .collect()
                    })
                    .collect()
            });

            let pre_token_balances = meta.pre_token_balances.as_ref().map(|pre_token_balances| {
                pre_token_balances
                    .iter()
                    .cloned()
                    .map(TransactionTokenBalance::from)
                    .collect()
            });
            let post_token_balances =
                meta.post_token_balances
                    .as_ref()
                    .map(|post_token_balances| {
                        post_token_balances
                            .iter()
                            .cloned()
                            .map(TransactionTokenBalance::from)
                            .collect()
                    });

            let rewards = meta.rewards.as_ref().map(|rewards| {
                rewards
                    .iter()
                    .map(|reward| Reward {
                        pubkey: reward.pubkey.clone(),
                        lamports: reward.lamports,
                        post_balance: reward.post_balance,
                        reward_type: reward.reward_type.map(Into::into),
                        commission: reward.commission,
                    })
                    .collect()
            });

            let loaded_addresses =
                meta.loaded_addresses
                    .clone()
                    .map(|loaded_addresses| LoadedAddresses {
                        writable: loaded_addresses.writable.clone(),
                        readonly: loaded_addresses.readonly.clone(),
                    });

            let return_data = meta.return_data.clone().map(|return_data| {
                let (data, encoding) = return_data.data;
                let data = match encoding {
                    rpc_client::UiReturnDataEncoding::Base64 => base64::prelude::BASE64_STANDARD
                        .decode(data)
                        .expect("invalid base-64 string"),
                };
                TransactionReturnData {
                    program_id: return_data.program_id,
                    data,
                }
            });

            TransactionStatusMeta {
                status: meta.err.is_none(),
                fee: meta.fee,
                pre_balances: meta.pre_balances.clone(),
                post_balances: meta.post_balances.clone(),
                inner_instructions,
                log_messages: meta.log_messages.clone().map(|log_messages| log_messages),
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_addresses,
                return_data,
                compute_units_consumed: meta.compute_units_consumed.clone().map(|cuc| cuc),
                cost_units: meta.cost_units.clone().map(|cu| cu),
            }
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
pub(crate) struct TransactionStatusMeta {
    // `true` if the transaction succeeded, `false` otherwise.
    pub(crate) status: bool,
    pub(crate) fee: u64,
    pub(crate) pre_balances: Vec<u64>,
    pub(crate) post_balances: Vec<u64>,
    pub(crate) inner_instructions: Option<Vec<Vec<super::instructions::Instruction>>>,
    pub(crate) log_messages: Option<Vec<String>>,
    pub(crate) pre_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub(crate) post_token_balances: Option<Vec<TransactionTokenBalance>>,
    pub(crate) rewards: Option<Vec<Reward>>,
    pub(crate) loaded_addresses: Option<LoadedAddresses>,
    pub(crate) return_data: Option<TransactionReturnData>,
    pub(crate) compute_units_consumed: Option<u64>,
    pub(crate) cost_units: Option<u64>,
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
                    .iter()
                    .map(|inner_instructions| {
                        inner_instructions
                            .instructions
                            .iter()
                            .cloned()
                            .map(|inst| super::instructions::Instruction {
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
            .map(|pre_token_balances| pre_token_balances.iter().cloned().map(From::from).collect());
        let post_token_balances = rpc_tx_meta.post_token_balances.map(|post_token_balances| {
            post_token_balances
                .iter()
                .cloned()
                .map(From::from)
                .collect()
        });

        let rewards = rpc_tx_meta.rewards.map(|rewards| {
            rewards
                .iter()
                .map(|reward| Reward {
                    pubkey: reward.pubkey.clone(),
                    lamports: reward.lamports,
                    post_balance: reward.post_balance,
                    reward_type: reward.reward_type.map(From::from),
                    commission: reward.commission,
                })
                .collect()
        });

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
        slot: u64,
        tx_index: u32,
        of_tx_meta: solana_storage_proto::confirmed_block::TransactionStatusMeta,
    ) -> Self {
        let inner_instructions = of_tx_meta
            .inner_instructions
            .iter()
            .map(|inner| {
                inner
                    .instructions
                    .iter()
                    .cloned()
                    .map(|inst| super::instructions::Instruction {
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
            .iter()
            .cloned()
            .map(TransactionTokenBalance::from)
            .collect();
        let post_token_balances = of_tx_meta
            .post_token_balances
            .iter()
            .cloned()
            .map(TransactionTokenBalance::from)
            .collect();

        let rewards = of_tx_meta
            .rewards
            .iter()
            .map(|reward| Reward {
                pubkey: reward.pubkey.clone(),
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: Some(reward.reward_type.into()),
                commission: if reward.commission.is_empty() {
                    None
                } else {
                    Some(reward.commission.parse().expect("commission parsing error"))
                },
            })
            .collect();

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

        let return_data = of_tx_meta.return_data.clone().map(|return_data| {
            let program_id = bs58::encode(&return_data.program_id).into_string();
            TransactionReturnData {
                program_id,
                data: return_data.data,
            }
        });

        TransactionStatusMeta {
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
        }
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct TransactionTokenBalance {
    account_index: u8,
    mint: String,
    ui_token_amount: TokenAmount,
    owner: Option<String>,
    program_id: Option<String>,
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

#[derive(Debug, Default, Deserialize, Clone)]
struct TokenAmount {
    ui_amount: Option<f64>,
    decimals: u8,
    amount: String,
    ui_amount_string: String,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct Reward {
    pubkey: String,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<RewardType>,
    commission: Option<u8>,
}

#[derive(Debug, Deserialize, Clone)]
enum RewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

impl std::fmt::Display for RewardType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RewardType::Fee => write!(f, "fee"),
            RewardType::Rent => write!(f, "rent"),
            RewardType::Staking => write!(f, "staking"),
            RewardType::Voting => write!(f, "voting"),
        }
    }
}

impl From<i32> for RewardType {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Fee,
            1 => Self::Rent,
            2 => Self::Staking,
            3 => Self::Voting,
            _ => panic!("invalid reward type value: {}", value),
        }
    }
}

impl From<rpc_client::RewardType> for RewardType {
    fn from(value: rpc_client::RewardType) -> Self {
        match value {
            rpc_client::RewardType::Fee => Self::Fee,
            rpc_client::RewardType::Rent => Self::Rent,
            rpc_client::RewardType::Staking => Self::Staking,
            rpc_client::RewardType::Voting => Self::Voting,
        }
    }
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct LoadedAddresses {
    writable: Vec<String>,
    readonly: Vec<String>,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct TransactionReturnData {
    program_id: String,
    data: Vec<u8>,
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

        self.slot.append_value(*slot);
    }

    pub(crate) fn build(self, range: BlockRange) -> BoxResult<TableRows> {
        let Self {
            mut special_block_num,
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
            mut slot,
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

        TableRows::new(table(range.network.clone()), range, columns).map_err(Into::into)
    }
}
