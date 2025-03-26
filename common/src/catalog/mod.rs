use std::{collections::BTreeSet, sync::Arc};

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{LogicalPlan, TableScan},
    sql::TableReference,
};
use physical::Catalog;

use crate::{config::Config, BoxError, Dataset, QueryContext, Table};

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

pub async fn schema_to_markdown(
    tables: Vec<Table>,
    dataset_kind: String,
) -> Result<String, BoxError> {
    let dataset = Dataset {
        kind: dataset_kind,
        name: "dataset".to_string(),
        tables,
    };
    let config = Config::in_memory();

    let env = Arc::new(config.make_runtime_env()?);
    let mut catalog = Catalog::empty();
    catalog
        .register(&dataset, config.data_store, None)
        .await
        .unwrap();
    let context = QueryContext::for_catalog(catalog, env).unwrap();

    let mut markdown = String::new();
    markdown.push_str("# Schema\n");
    markdown.push_str(&format!(
        "Auto-generated file. See `schema_to_markdown` in `{}`.\n",
        file!()
    ));
    for (table, pretty_schema) in context.print_schema().await? {
        markdown.push_str(&format!("## {}\n", table.table_name()));
        markdown.push_str("````\n");
        markdown.push_str(&pretty_schema);
        markdown.push_str("\n````\n");
    }

    Ok(markdown)
}
