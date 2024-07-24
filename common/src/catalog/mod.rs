use std::collections::BTreeSet;

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{LogicalPlan, TableScan},
    sql::TableReference,
};

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
