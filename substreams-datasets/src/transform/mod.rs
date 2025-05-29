pub mod db;
pub mod entities;
pub mod proto;

use anyhow::Context as _;
use common::{parquet::data_type::AsBytes, RawDatasetRows, RawTableBlock};

use crate::{
    proto::sf::substreams::rpc::v2::BlockScopedData,
    tables::{OutputType, Tables},
};

/// transform BlockScopedData to RecordBatch based on the module output type and schemas
pub fn transform(
    block_data: BlockScopedData,
    tables: &Tables,
) -> Result<RawDatasetRows, anyhow::Error> {
    let clock = block_data.clock.as_ref().unwrap();
    let block = RawTableBlock {
        number: clock.number,
        network: tables.tables[0].network.clone(),
        hash: clock.id.parse().context("failed to parse block hash")?,
    };

    let value = block_data
        .output
        .context("no output")?
        .map_output
        .context("module output is empty")?
        .value;

    let table_rows = match &tables.output_type {
        OutputType::Proto(message_descriptor) => {
            proto::pb_to_rows(message_descriptor, value.as_bytes(), &tables.tables, &block)
        }
        OutputType::DbOut => db::pb_to_rows(value.as_bytes(), &tables.tables, &block),
        OutputType::Entities => entities::pb_to_rows(value.as_bytes(), &tables.tables, &block),
    };
    Ok(table_rows?)
}
