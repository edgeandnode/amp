pub mod client;
pub mod tables;
pub mod transform;

mod proto;

use common::{Dataset, Table};

pub fn dataset(network: String, tables: Vec<Table>) -> Dataset {
    Dataset {
        name: "substreams".to_string(),
        network,
        tables,
    }
}
