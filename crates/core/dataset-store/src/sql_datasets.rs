use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

// Re-export types from common for backward compatibility
pub use common::manifest::sql_datasets::Error;
use common::{
    BoxError, Dataset, SPECIAL_BLOCK_NUM, Table,
    manifest::{
        derived::sort_tables_by_dependencies,
        sql_datasets::{DATASET_KIND, Manifest, SqlDataset},
    },
    query_context::{parse_sql, prepend_special_block_num_field},
};
use datafusion::sql::parser;
use futures::StreamExt as _;
use object_store::ObjectMeta;

use crate::DatasetStore;

pub(super) async fn dataset(
    store: Arc<DatasetStore>,
    dataset_def: common::DatasetValue,
) -> Result<SqlDataset, BoxError> {
    let def = Manifest::from_value(dataset_def)?;
    if def.kind != DATASET_KIND {
        return Err(format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into());
    }

    let mut files = store.dataset_defs_store.list(def.name.as_str());

    // List all `.sql` files in the dataset dir and infer the output schema to get `Table`s.
    let mut tables: Vec<Table> = vec![];
    let mut queries: BTreeMap<String, parser::Statement> = BTreeMap::new();
    while let Some(file) = files.next().await {
        let file: ObjectMeta = file?;

        // Unwrap: Listed paths are always files.
        let filename = file.location.filename().unwrap();
        let Some(table_name) = filename.strip_suffix(".sql") else {
            continue;
        };
        let raw_query = store
            .dataset_defs_store
            .get_string(file.location.clone())
            .await?;
        let query = parse_sql(&raw_query)?;
        let ctx = store.clone().planning_ctx_for_sql(&query).await?;
        let schema = ctx.sql_output_schema(query.clone()).await?;
        let network = {
            let tables = ctx.catalog().iter();
            let mut networks: BTreeSet<_> =
                tables.map(|t| t.table().network().to_string()).collect();
            if networks.len() > 1 {
                return Err(format!(
                    "table {} has dependencies in multiple networks: {:?}",
                    table_name, networks
                )
                .into());
            }
            networks.pop_first().unwrap()
        };
        let schema =
            if schema.fields().first().expect("schema not empty").name() != SPECIAL_BLOCK_NUM {
                prepend_special_block_num_field(&schema)
            } else {
                schema
            };
        let table = Table::new(
            table_name.to_string(),
            schema.as_ref().clone().into(),
            network,
        );
        tables.push(table);
        queries.insert(table_name.to_string(), query);
    }
    let tables = sort_tables_by_dependencies(&def.name, tables, &queries)?;

    Ok(SqlDataset {
        dataset: Dataset {
            kind: def.kind,
            network: def.network,
            name: def.name.to_string(),
            version: None,
            start_block: None,
            finalized_blocks_only: false,
            tables,
            functions: vec![],
        },
        queries,
    })
}
