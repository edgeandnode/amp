use std::sync::Arc;

use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    logical_expr::{DescribeTable, LogicalPlan},
    prelude::SessionContext,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};

use crate::Table;

pub mod dataset_access;
pub mod errors;
pub mod logical;
pub mod physical;
pub mod reader;
pub mod sql;

pub async fn schema_to_markdown(tables: Vec<Table>) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Schema\n");
    markdown.push_str(&format!(
        "Auto-generated file. See `schema_to_markdown` in `{}`.\n",
        file!()
    ));
    for table in tables {
        markdown.push_str(&format!("## {}\n", table.name()));
        markdown.push_str("````\n");
        markdown.push_str(&print_schema(&table).await);
        markdown.push_str("\n````\n");
    }

    markdown
}

async fn print_schema(table: &Table) -> String {
    let plan = LogicalPlan::DescribeTable(DescribeTable {
        schema: table.schema().clone(),
        output_schema: Arc::new(LogicalPlan::describe_schema().try_into().unwrap()),
    });
    let ctx = SessionContext::new();

    // Unwraps: No reason for a `describe` to fail.
    let df = ctx.execute_logical_plan(plan).await.unwrap();
    let batches = df.collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

#[derive(Debug, Clone)]
pub struct JobLabels {
    pub dataset_namespace: Namespace,
    pub dataset_name: Name,
    pub manifest_hash: datasets_common::hash::Hash,
}

impl JobLabels {
    /// Turn namespace, name, and manifest hash into a dataset reference with hash revision.
    pub fn dataset_reference(&self) -> Reference {
        Reference::new(
            self.dataset_namespace.clone(),
            self.dataset_name.clone(),
            Revision::Hash(self.manifest_hash.clone()),
        )
    }
}
