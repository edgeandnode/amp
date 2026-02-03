use std::sync::{Arc, LazyLock};

use anyhow::Context;
use datasets_common::{
    block_range::BlockRange,
    dataset::{SPECIAL_BLOCK_NUM, Table},
    network_id::NetworkId,
};
use datasets_raw::{
    arrow::{
        ArrayRef, DataType, Field, Int64Builder, Schema, SchemaRef, StringBuilder, UInt64Builder,
    },
    rows::{TableRowError, TableRows},
};
use solana_clock::Slot;

use crate::{of1_client, rpc_client, tables};

pub const TABLE_NAME: &str = "block_rewards";

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
        Field::new("pubkey", DataType::Utf8, false),
        Field::new("lamports", DataType::Int64, false),
        Field::new("post_balance", DataType::UInt64, false),
        Field::new("reward_type", DataType::Utf8, true),
        Field::new("commission", DataType::Utf8, true),
    ];

    Schema::new(fields)
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Reward {
    pub(crate) pubkey: String,
    pub(crate) lamports: i64,
    pub(crate) post_balance: u64,
    pub(crate) reward_type: Option<RewardType>,
    pub(crate) commission: Option<u8>,
}

#[derive(Debug, Clone)]
pub(crate) enum RewardType {
    /// The `Unspecified` variant only in the protobuf version of
    /// RewardType, it is not defined by the Solana spec.
    Unspecified,
    Fee,
    Rent,
    Staking,
    Voting,
}

impl From<solana_storage_proto::StoredExtendedReward> for Reward {
    fn from(value: solana_storage_proto::StoredExtendedReward) -> Self {
        Self {
            pubkey: value.pubkey,
            lamports: value.lamports,
            post_balance: value.post_balance,
            reward_type: value.reward_type.map(Into::into),
            commission: value.commission,
        }
    }
}

impl TryFrom<solana_storage_proto::confirmed_block::Reward> for Reward {
    type Error = anyhow::Error;

    fn try_from(value: solana_storage_proto::confirmed_block::Reward) -> Result<Self, Self::Error> {
        let reward = Self {
            pubkey: value.pubkey,
            lamports: value.lamports,
            post_balance: value.post_balance,
            reward_type: value
                .reward_type
                .try_into()
                .context("parsing proto reward type")
                .map(|reward| match reward {
                    RewardType::Unspecified => None,
                    reward => Some(reward),
                })?,
            commission: if value.commission.is_empty() {
                None
            } else {
                value
                    .commission
                    .parse()
                    .map(Some)
                    .context("parsing proto commission")?
            },
        };

        Ok(reward)
    }
}

impl From<rpc_client::Reward> for Reward {
    fn from(value: rpc_client::Reward) -> Self {
        Self {
            pubkey: value.pubkey,
            lamports: value.lamports,
            post_balance: value.post_balance,
            reward_type: value.reward_type.map(Into::into),
            commission: value.commission,
        }
    }
}

impl std::fmt::Display for RewardType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RewardType::Unspecified => write!(f, "unspecified"),
            RewardType::Fee => write!(f, "fee"),
            RewardType::Rent => write!(f, "rent"),
            RewardType::Staking => write!(f, "staking"),
            RewardType::Voting => write!(f, "voting"),
        }
    }
}

impl TryFrom<i32> for RewardType {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let typ = match value {
            0 => Self::Unspecified,
            1 => Self::Fee,
            2 => Self::Rent,
            3 => Self::Staking,
            4 => Self::Voting,
            _ => anyhow::bail!("invalid reward type: {value}"),
        };

        Ok(typ)
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

/// Solana block rewards.
#[derive(Debug, Default, Clone)]
pub(crate) struct BlockRewards {
    pub(crate) slot: Slot,
    pub(crate) rewards: Vec<Reward>,
}

impl BlockRewards {
    pub(crate) fn from_of1_rewards(
        slot: Slot,
        rewards: Option<of1_client::DecodedBlockRewards>,
    ) -> anyhow::Result<Self> {
        let rewards: Vec<Reward> = rewards
            .map(|rewards| {
                let rewards = match rewards {
                    of1_client::DecodedField::Proto(proto_rewards) => proto_rewards
                        .rewards
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<_, _>>()
                        .context("converting of1 block rewards")?,
                    of1_client::DecodedField::Bincode(bincode_rewards) => {
                        bincode_rewards.into_iter().map(Into::into).collect()
                    }
                };

                anyhow::Ok(rewards)
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Self { slot, rewards })
    }

    pub(crate) fn from_rpc_rewards(slot: Slot, rewards: Vec<rpc_client::Reward>) -> Self {
        let rewards = rewards.into_iter().map(Into::into).collect();
        Self { slot, rewards }
    }
}

/// A builder for converting [BlockRewards] into [TableRows].
pub(crate) struct BlockRewardsRowsBuilder {
    special_block_num: UInt64Builder,
    slot: UInt64Builder,
    pubkey: StringBuilder,
    lamports: Int64Builder,
    post_balance: UInt64Builder,
    reward_type: StringBuilder,
    commission: StringBuilder,
}

impl BlockRewardsRowsBuilder {
    /// Creates a new [BlockRewardsRowsBuilder].
    pub(crate) fn new() -> Self {
        Self {
            special_block_num: UInt64Builder::with_capacity(1),
            slot: UInt64Builder::with_capacity(1),
            pubkey: StringBuilder::with_capacity(1, tables::BASE58_ENCODED_HASH_LEN),
            lamports: Int64Builder::with_capacity(1),
            post_balance: UInt64Builder::with_capacity(1),
            reward_type: StringBuilder::with_capacity(1, 8),
            commission: StringBuilder::with_capacity(1, 4),
        }
    }

    /// Appends a [BlockRewards] to the builder.
    pub(crate) fn append(&mut self, rewards: &BlockRewards) {
        let BlockRewards { slot, rewards } = rewards;

        for reward in rewards {
            self.special_block_num.append_value(*slot);
            self.slot.append_value(*slot);
            self.pubkey.append_value(&reward.pubkey);
            self.lamports.append_value(reward.lamports);
            self.post_balance.append_value(reward.post_balance);
            self.reward_type
                .append_option(reward.reward_type.as_ref().map(|t| t.to_string()));
            self.commission
                .append_option(reward.commission.map(|c| c.to_string()));
        }
    }

    /// Builds the [TableRows] from the appended data.
    pub(crate) fn build(self, range: BlockRange) -> Result<TableRows, TableRowError> {
        let Self {
            mut special_block_num,
            mut slot,
            mut pubkey,
            mut lamports,
            mut post_balance,
            mut reward_type,
            mut commission,
        } = self;

        let columns = vec![
            Arc::new(special_block_num.finish()) as ArrayRef,
            Arc::new(slot.finish()),
            Arc::new(pubkey.finish()),
            Arc::new(lamports.finish()),
            Arc::new(post_balance.finish()),
            Arc::new(reward_type.finish()),
            Arc::new(commission.finish()),
        ];

        TableRows::new(table(range.network.clone()), range, columns)
    }
}
