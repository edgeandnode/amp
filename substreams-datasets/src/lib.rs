pub mod client;
pub(crate) mod pb_to_rows;
pub mod tables;

mod proto;

use common::{DataSchema, Dataset, Table};

pub fn dataset(network: String, tables: Vec<Table>) -> Dataset {
    Dataset {
        name: "substreams".to_string(),
        network,
        data_schema: DataSchema {
            tables,
            scalar_udfs: vec![],
        },
    }
}
