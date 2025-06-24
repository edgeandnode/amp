use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use common::{
    multirange::MultiRange,
    plan_visitors::{
        constrain_by_block_num, extract_table_references_from_plan,
        forbid_underscore_prefixed_aliases, order_by_block_num, propagate_block_num,
        unproject_special_block_num_column,
    },
    query_context::{parse_sql, prepend_special_block_num_field, QueryEnv},
    BlockNum, BoxError, Dataset, DatasetValue, QueryContext, Table, SPECIAL_BLOCK_NUM,
};
use datafusion::{execution::SendableRecordBatchStream, logical_expr::LogicalPlan, sql::parser};
use futures::StreamExt as _;
use object_store::ObjectMeta;
use serde::Deserialize;
use tracing::instrument;

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

#[derive(Debug, Deserialize)]
pub(super) struct DatasetDef {
    pub kind: String,
    pub network: String,
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
            tables,
            functions: vec![],
        },
        queries,
    })
}

// Get synced ranges for table
/// This will:
/// - Validate that dependencies have synced the required block range.
/// - Inject block range constraints into the plan.
/// - Inject 'order by block_num' into the plan.
/// - Execute the plan.
#[instrument(skip_all, err)]
pub async fn execute_plan_for_range(
    plan: LogicalPlan,
    ctx: &QueryContext,
    start: BlockNum,
    end: BlockNum,
    is_sql_dataset: bool,
) -> Result<SendableRecordBatchStream, BoxError> {
    let original_schema = plan.schema().clone();
    let tables = extract_table_references_from_plan(&plan)?;

    // Validate dependency block ranges
    {
        let needed_range = MultiRange::from_ranges(vec![(start, end)]).unwrap();
        for table in tables {
            let physical_table = ctx
                .get_table(&table)
                .ok_or::<BoxError>(format!("table {} not found", table).into())?;
            let ranges = physical_table.multi_range().await?;
            let synced = ranges.intersection(&needed_range) == needed_range;
            if !synced {
                return Err(format!("tried to query range {needed_range} of table {table} but it has not been synced").into());
            }
        }
    }

    let plan = {
        forbid_underscore_prefixed_aliases(&plan)?;
        let plan = propagate_block_num(plan)?;
        let plan = constrain_by_block_num(plan, start, end)?;
        let plan = order_by_block_num(plan);
        if is_sql_dataset {
            // SQL datasets always project the special block number column, because it has
            // to end up in the file.
            plan
        } else {
            unproject_special_block_num_column(plan, original_schema)?
        }
    };
    Ok(ctx.execute_plan(plan).await?)
}

/// This will:
/// - Plan the query against the configured datasets.
/// - Validate that the query is materializable.
/// - Execute the plan for a specified range
#[instrument(skip_all, err)]
pub async fn execute_query_for_range(
    query: parser::Statement,
    dataset_store: &Arc<DatasetStore>,
    env: QueryEnv,
    start: BlockNum,
    end: BlockNum,
) -> Result<SendableRecordBatchStream, BoxError> {
    let ctx = dataset_store.ctx_for_sql(&query, env).await?;
    let plan = ctx.plan_sql(query).await?;
    execute_plan_for_range(plan, &ctx, start, end, true).await
}

// All physical tables locations that have been queried
