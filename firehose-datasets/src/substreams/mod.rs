pub mod client;
pub mod tables;
pub mod pb_to_rows;

use common::{DataSchema, DataSet, Table};

pub fn dataset(network: String, tables: Vec<Table>) -> DataSet {
    DataSet {
        name: "substreams".to_string(),
        network,
        data_schema: DataSchema {
            tables,
        },
    }
}
