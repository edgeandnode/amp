use std::sync::Arc;

use alloy::primitives::BlockHash;
use common::{
    BoxError, RawTableRows, SPECIAL_BLOCK_NUM, Table,
    arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    },
    metadata::segments::BlockRange,
};

pub fn table(network: String) -> Table {
    let name = "blocks".parse().expect("table name is valid");
    Table::new(name, schema().into(), network, vec![]).expect("table creation succeeds")
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("version", DataType::Utf8, true),
        Field::new("signature", DataType::FixedSizeBinary(96), true),
        Field::new("proposer_index", DataType::UInt64, true),
        Field::new("parent_root", DataType::FixedSizeBinary(32), true),
        Field::new("state_root", DataType::FixedSizeBinary(32), true),
        Field::new("randao_reveal", DataType::FixedSizeBinary(96), true),
        Field::new(
            "eth1_data_deposit_root",
            DataType::FixedSizeBinary(32),
            true,
        ),
        Field::new("eth1_data_deposit_count", DataType::UInt64, true),
        Field::new("eth1_data_block_hash", DataType::FixedSizeBinary(32), true),
        Field::new("graffiti", DataType::FixedSizeBinary(32), true),
    ])
}

pub fn json_to_row(network: &str, response: api::Response) -> Result<RawTableRows, BoxError> {
    let table = table(network.to_string());

    let slot = match &response {
        api::Response::SlotFilled { data, .. } => data.message.slot,
        api::Response::SlotSkipped { slot } => *slot,
    };
    let hash = match &response {
        api::Response::SlotFilled { data, .. } => data.message.state_root,
        api::Response::SlotSkipped { .. } => Default::default(),
    };
    let range = BlockRange {
        network: network.to_string(),
        numbers: slot..=slot,
        hash,
        prev_hash: BlockHash::ZERO, // Zero hash to prevent hash-chaining, because some slots are skipped
    };

    let columns: Vec<ArrayRef> = match response {
        api::Response::SlotFilled { version, data } => vec![
            Arc::new(UInt64Array::from(vec![data.message.slot])),
            Arc::new(UInt64Array::from(vec![data.message.slot])),
            Arc::new(StringArray::from(vec![version])),
            Arc::new(FixedSizeBinaryArray::from(vec![&data.signature.0])),
            Arc::new(UInt64Array::from(vec![data.message.proposer_index])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &data.message.parent_root.0,
            ])),
            Arc::new(FixedSizeBinaryArray::from(vec![&data.message.state_root.0])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &data.message.body.randao_reveal.0,
            ])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &data.message.body.eth1_data.deposit_root.0,
            ])),
            Arc::new(UInt64Array::from(vec![
                data.message.body.eth1_data.deposit_count,
            ])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &data.message.body.eth1_data.block_hash.0,
            ])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &data.message.body.graffiti.0,
            ])),
        ],
        api::Response::SlotSkipped { slot } => vec![
            Arc::new(UInt64Array::from(vec![slot])),
            Arc::new(UInt64Array::from(vec![slot])),
            Arc::new(StringArray::new_null(1)),
            Arc::new(FixedSizeBinaryArray::new_null(96, 1)),
            Arc::new(UInt64Array::new_null(1)),
            Arc::new(FixedSizeBinaryArray::new_null(32, 1)),
            Arc::new(FixedSizeBinaryArray::new_null(32, 1)),
            Arc::new(FixedSizeBinaryArray::new_null(96, 1)),
            Arc::new(FixedSizeBinaryArray::new_null(32, 1)),
            Arc::new(UInt64Array::new_null(1)),
            Arc::new(FixedSizeBinaryArray::new_null(32, 1)),
            Arc::new(FixedSizeBinaryArray::new_null(32, 1)),
        ],
    };
    RawTableRows::new(table, range, columns)
}

// reference:
//   - https://buf.build/pinax/firehose-beacon/file/b578ac9ef23645c692d8f64ad1deb3f8:sf/beacon/type/v1/type.proto
//   - https://ethereum.github.io/beacon-APIs/#/Beacon/getBlockV2
pub mod api {
    use alloy::primitives::FixedBytes;
    use serde_with::serde_as;

    #[derive(Debug, serde::Deserialize)]
    #[serde(untagged)]
    pub enum Response {
        #[serde(deserialize_with = "slot_skipped")]
        SlotSkipped {
            slot: u64,
        },
        SlotFilled {
            version: String,
            data: Box<Data>,
        },
    }
    #[serde_as]
    #[derive(Debug, serde::Deserialize)]
    pub struct Data {
        pub message: Message,
        pub signature: FixedBytes<96>,
    }
    #[serde_as]
    #[derive(Debug, serde::Deserialize)]
    pub struct Message {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub slot: u64,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub proposer_index: u64,
        pub parent_root: FixedBytes<32>,
        pub state_root: FixedBytes<32>,
        pub body: Body,
    }
    #[serde_as]
    #[derive(Debug, serde::Deserialize)]
    pub struct Body {
        pub randao_reveal: FixedBytes<96>,
        pub eth1_data: Eth1Data,
        pub graffiti: FixedBytes<32>,
    }
    #[serde_as]
    #[derive(Debug, serde::Deserialize)]
    pub struct Eth1Data {
        pub deposit_root: FixedBytes<32>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub deposit_count: u64,
        pub block_hash: FixedBytes<32>,
    }

    fn slot_skipped<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::Deserialize as _;
        #[derive(serde::Deserialize)]
        struct ErrorResponse {
            code: u16,
            message: String,
        }
        fn extract_skipped_slot(error: &ErrorResponse) -> Option<u64> {
            let ErrorResponse { code, message } = error;
            if *code != 404 {
                return None;
            }
            message
                .trim_start_matches("NOT_FOUND: beacon block at slot ")
                .parse()
                .ok()
        }
        let error = ErrorResponse::deserialize(deserializer)?;
        extract_skipped_slot(&error).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "Unexpected error response: code={}, message={}",
                error.code, error.message
            ))
        })
    }
}
