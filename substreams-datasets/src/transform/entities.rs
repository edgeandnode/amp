use common::{RawDatasetRows, Table};

use crate::proto::sf::substreams::v1::Package;

/// transform EntityChanges proto message to RecordBatch based on the schemas
pub(crate) fn pb_to_rows(
    _value: &[u8],
    _tables: &[Table],
    _block_num: u64,
) -> Result<RawDatasetRows, anyhow::Error> {
    todo!("Entities output type not implemented")
}

pub(crate) fn package_to_schemas(
    _package: &Package,
    _output_module: &str,
) -> Result<Vec<Table>, anyhow::Error> {
    todo!("Entities output type not implemented")
}
