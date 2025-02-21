//! The external API for embedding nozzle in other projects

pub use common::{
    catalog::physical::Catalog,
    config::Config,
    meta_tables::scanned_ranges::ranges_for_table,
    multirange::MultiRange,
    query_context::{parse_sql, QueryContext},
    Store,
};
pub use datafusion::arrow;
pub use datafusion::execution::runtime_env::RuntimeEnv;
pub use datafusion::execution::SendableRecordBatchStream;
pub use dataset_store::DatasetStore;
pub use metadata_db::{MetadataDb, TableId};
