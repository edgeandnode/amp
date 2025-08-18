use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use common::{
    BoxError, Dataset, DatasetValue, SPECIAL_BLOCK_NUM, Table,
    query_context::{parse_sql, prepend_special_block_num_field},
};
use datafusion::sql::parser;
use futures::StreamExt as _;
use object_store::ObjectMeta;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::DatasetStore;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct SqlDataset {
    pub dataset: Dataset,

    /// Maps a table name to the query that defines that table.
    pub queries: BTreeMap<String, parser::Statement>,
}

impl SqlDataset {
    pub fn name(&self) -> &str {
        &self.dataset.name
    }
}

pub const DATASET_KIND: &str = "sql";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DatasetDef {
    /// Dataset kind, must be `sql`.
    pub kind: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset name.
    pub name: String,
}

impl DatasetDef {
    pub fn from_value(value: common::DatasetValue) -> Result<Self, Error> {
        match value {
            DatasetValue::Toml(value) => value.try_into().map_err(From::from),
            DatasetValue::Json(value) => serde_json::from_value(value).map_err(From::from),
        }
    }
}

pub(super) async fn dataset(
    store: Arc<DatasetStore>,
    dataset_def: common::DatasetValue,
) -> Result<SqlDataset, BoxError> {
    let def = DatasetDef::from_value(dataset_def)?;
    if def.kind != DATASET_KIND {
        return Err(format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into());
    }

    let defs_store = store.dataset_defs_store();
    let mut files = defs_store.list(def.name.clone());

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
        let raw_query = defs_store.get_string(file.location.clone()).await?;
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

    Ok(SqlDataset {
        dataset: Dataset {
            kind: def.kind,
            network: def.network,
            name: def.name,
            version: None,
            tables,
            functions: vec![],
        },
        queries,
    })
}
