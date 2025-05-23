pub mod db;
pub mod entities;
pub mod proto;

use anyhow::Context as _;
use common::{parquet::data_type::AsBytes, RawDatasetRows};

use crate::{
    proto::sf::substreams::rpc::v2::BlockScopedData,
    tables::{OutputType, Tables},
};

/// transform BlockScopedData to RecordBatch based on the module output type and schemas
pub fn transform(block: BlockScopedData, tables: &Tables) -> Result<RawDatasetRows, anyhow::Error> {
    let block_num = block.clock.unwrap().number;
    let value = block
        .output
        .context("no output")?
        .map_output
        .context("module output is empty")?
        .value;

    let table_rows = match &tables.output_type {
        OutputType::Proto(message_descriptor) => proto::pb_to_rows(
            message_descriptor,
            value.as_bytes(),
            &tables.tables,
            block_num,
        ),
        OutputType::DbOut => db::pb_to_rows(value.as_bytes(), &tables.tables, block_num),
        OutputType::Entities => entities::pb_to_rows(value.as_bytes(), &tables.tables, block_num),
    };
    Ok(table_rows?)
}
