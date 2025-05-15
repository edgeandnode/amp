use std::{collections::BTreeSet, sync::Arc};

use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{DescribeTable, LogicalPlan, TableScan},
    prelude::SessionContext,
    sql::TableReference,
};

use crate::Table;

pub mod logical;
pub mod physical;

/// Collects names of tables scanned in a logical plan
pub fn collect_scanned_tables(plan: &LogicalPlan) -> BTreeSet<TableReference> {
    struct TableScanVisitor {
        table_refs: BTreeSet<TableReference>,
    }

    impl TreeNodeVisitor<'_> for TableScanVisitor {
        type Node = LogicalPlan;
        fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
            match node {
                // Look for table scans that are not view references.
                LogicalPlan::TableScan(TableScan {
                    table_name, source, ..
                }) if source.table_type() == TableType::Base
                    && source.get_logical_plan().is_none() =>
                {
                    self.table_refs.insert(table_name.clone());
                }
                _ => (),
            }
            Ok(TreeNodeRecursion::Continue)
        }
    }

    let mut visitor = TableScanVisitor {
        table_refs: BTreeSet::new(),
    };
    let _ = plan.visit(&mut visitor);
    visitor.table_refs
}

pub async fn schema_to_markdown(tables: Vec<Table>) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Schema\n");
    markdown.push_str(&format!(
        "Auto-generated file. See `schema_to_markdown` in `{}`.\n",
        file!()
    ));
    for table in tables {
        markdown.push_str(&format!("## {}\n", table.name));
        markdown.push_str("````\n");
        markdown.push_str(&print_schema(&table).await);
        markdown.push_str("\n````\n");
    }

    markdown
}

async fn print_schema(table: &Table) -> String {
    let plan = LogicalPlan::DescribeTable(DescribeTable {
        schema: table.schema.clone(),
        output_schema: Arc::new(LogicalPlan::describe_schema().try_into().unwrap()),
    });
    let ctx = SessionContext::new();

    // Unwraps: No reason for a `describe` to fail.
    let df = ctx.execute_logical_plan(plan).await.unwrap();
    let batches = df.collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}
