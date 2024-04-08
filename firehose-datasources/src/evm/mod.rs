pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
use common::{DataSchema, DataSet};
pub use pb_to_rows::protobufs_to_rows;

pub mod tables;

mod pb_to_rows;

#[cfg(test)]
pub mod test_support;

pub fn dataset(network: String) -> DataSet {
    DataSet {
        name: "evm-firehose".to_string(),
        network,
        data_schema: DataSchema {
            tables: tables::all(),
        },
    }
}
