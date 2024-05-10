use common::Table;

/// transform EntityChanges proto message to RecordBatch based on the schemas
pub(crate) fn pb_to_rows(
    _value: &[u8],
    _tables: &[Table],
    _block_num: u64,
) -> Result<common::DatasetRows, anyhow::Error> {
    todo!("Entities output type not implemented")
}
