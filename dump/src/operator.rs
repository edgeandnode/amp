use std::fmt;

use metadata_db::LocationId;
use serde::{Deserialize, Serialize};

/// This is currently very simple, but the operator abstraction is expected to become a central one.
///
/// Three kinds of operators are expected to exist:
/// - External Datasets, that read from an adapter and often write to all their tables at once.
/// - Views, which write the output of a SQL query to a single table.
/// - Stream Handlers, which run stateful user code over an ordered input stream, potentially writing
///   to multiple tables.
///
/// Currently, the "dump operator" is what have implemented so that's what we have here. This
/// representation is for the `operator` column in the `jobs` table.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    DumpDataset {
        // Dataset name.
        dataset: String,

        // One for each table in the dataset.
        locations: Vec<LocationId>,
    },
}

impl fmt::Debug for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
