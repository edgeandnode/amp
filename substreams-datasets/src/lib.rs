pub mod client;
pub mod tables;
pub(crate) mod pb_to_rows;

mod proto;

use common::{DataSchema, Dataset, Table};

pub fn dataset(network: String, tables: Vec<Table>) -> Dataset {
    Dataset {
        name: "substreams".to_string(),
        network,
        data_schema: DataSchema {
            tables,
        },
    }
}
