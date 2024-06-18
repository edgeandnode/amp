pub mod table;

use std::sync::Arc;

use crate::{catalog::physical::PhysicalDataset, BoxError, Dataset, Timestamp};
use datafusion::parquet::arrow::ArrowWriter;
use object_store::{path::Path, ObjectStore};
use table::DatasetRow;

/// Write (or overwrite) the entry in `__datasets/<dataset_name>.parquet`.
pub async fn write(dataset: PhysicalDataset, store: Arc<dyn ObjectStore>) -> Result<(), BoxError> {
    let dataset_name = dataset.name().to_string();
    let datasets_table = table::TABLE_NAME;
    let path = Path::parse(format!("{}/{}.parquet", datasets_table, dataset_name))?;
    let row = DatasetRow::from(dataset).to_record_batch();
    let row_bytes = {
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, row.schema(), None)?;
        writer.write(&row)?;
        writer.close()?;
        buffer
    };
    store.put(&path, row_bytes.into()).await?;
    Ok(())
}

impl From<PhysicalDataset> for DatasetRow {
    fn from(table: PhysicalDataset) -> Self {
        let PhysicalDataset {
            dataset:
                Dataset {
                    name,
                    network,
                    tables: _,
                },
            url,
            tables,
        } = table;

        let tables = tables.into_iter().map(Into::into).collect();

        DatasetRow {
            name,
            network,
            url: url.to_string(),
            created_at: Timestamp::now(),
            tables,

            // Fields for derived datasets, not yet implemented
            sql_query: String::new(),
            query_plan: vec![],
            dependencies: vec![],
        }
    }
}
