use std::sync::Arc;

use common::{
    BoxError, RawTableRows, SPECIAL_BLOCK_NUM, Table,
    arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    },
    metadata::segments::BlockRange,
};

pub fn table(network: String) -> Table {
    Table::new("blocks".to_string(), schema().into(), network)
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false),
        Field::new("block_num", DataType::UInt64, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("proposer_index", DataType::UInt64, false),
        Field::new("parent_root", DataType::FixedSizeBinary(32), false),
        Field::new("state_root", DataType::FixedSizeBinary(32), false),
        Field::new("execution_block_num", DataType::UInt64, false),
    ])
}

pub fn json_to_row(network: &str, response: api::Response) -> Result<RawTableRows, BoxError> {
    let table = table(network.to_string());
    let range = BlockRange {
        network: network.to_string(),
        numbers: response.data.message.slot..=response.data.message.slot,
        hash: response.data.message.state_root,
        prev_hash: Some(response.data.message.parent_root),
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![response.data.message.slot])),
        Arc::new(UInt64Array::from(vec![response.data.message.slot])),
        Arc::new(StringArray::from(vec![response.version])),
        Arc::new(UInt64Array::from(vec![
            response.data.message.proposer_index,
        ])),
        Arc::new(FixedSizeBinaryArray::from(vec![
            &response.data.message.parent_root.0,
        ])),
        Arc::new(FixedSizeBinaryArray::from(vec![
            &response.data.message.state_root.0,
        ])),
        Arc::new(UInt64Array::from(vec![
            response.data.message.body.execution_payload.block_number,
        ])),
    ];
    RawTableRows::new(table, range, columns)
}

// TODO: complete fields
// reference:
//   - https://buf.build/pinax/firehose-beacon/file/b578ac9ef23645c692d8f64ad1deb3f8:sf/beacon/type/v1/type.proto
//   - https://ethereum.github.io/beacon-APIs/#/Beacon/getBlockV2
pub mod api {
    use alloy::primitives::FixedBytes;
    use serde_with::serde_as;

    #[derive(Debug, serde::Deserialize)]
    pub struct Response {
        pub version: String,
        pub execution_optimistic: bool,
        pub finalized: bool,
        pub data: Data,
    }
    #[derive(Debug, serde::Deserialize)]
    pub struct Data {
        pub message: Message,
        pub signature: String,
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
        pub execution_payload: ExecutionPayload,
    }
    #[serde_as]
    #[derive(Debug, serde::Deserialize)]
    pub struct ExecutionPayload {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub block_number: u64,
    }
}
