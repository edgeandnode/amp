use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use common::{
    BoxError, Dataset, SPECIAL_BLOCK_NUM, Table,
    manifest::{derived::sort_tables_by_dependencies, sql_datasets::SqlDataset},
    plan_visitors::prepend_special_block_num_field,
    query_context::parse_sql,
};
use datafusion::sql::parser;
use datasets_derived::sql_dataset::Manifest;
use futures::future::BoxFuture;

use crate::DatasetStore;

pub(super) fn dataset(
    store: Arc<DatasetStore>,
    manifest: Manifest,
    sql_files: Vec<(String, String)>,
) -> BoxFuture<'static, Result<SqlDataset, BoxError>> {
    Box::pin(async move {
        let mut tables: Vec<Table> = vec![];
        let mut queries: BTreeMap<String, parser::Statement> = BTreeMap::new();

        for (table_name, raw_query) in sql_files {
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
        let tables = sort_tables_by_dependencies(&manifest.name, tables, &queries)?;

        Ok(SqlDataset {
            dataset: Dataset {
                name: manifest.name,
                version: Some(manifest.version),
                kind: manifest.kind.to_string(),
                network: manifest.network,
                start_block: None,
                tables,
                functions: vec![],
            },
            queries,
        })
    })
}
