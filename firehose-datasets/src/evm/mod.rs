pub use crate::proto::sf::ethereum::r#type::v2 as pbethereum;
use common::{DataSchema, DataSet};

pub mod pb_to_rows;
pub mod tables;

pub fn dataset(network: String) -> DataSet {
    DataSet {
        name: "evm-firehose".to_string(),
        network,
        data_schema: DataSchema {
            tables: tables::all(),
        },
    }
}
