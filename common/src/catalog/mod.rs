use std::{collections::BTreeSet, ops::ControlFlow};

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

// Note: Switch to upstream once DataFusion 40 is released.
// See https://github.com/apache/datafusion/pull/10876
pub fn resolve_table_references(
    statement: &datafusion::sql::parser::Statement,
    enable_ident_normalization: bool,
) -> datafusion::common::Result<(Vec<TableReference>, Vec<TableReference>)> {
    use datafusion::sql::parser::{CopyToSource, CopyToStatement, Statement as DFStatement};
    use datafusion::sql::planner::object_name_to_table_reference;

    use datafusion::sql::sqlparser::ast::*;

    struct RelationVisitor {
        relations: BTreeSet<ObjectName>,
        all_ctes: BTreeSet<ObjectName>,
        ctes_in_scope: Vec<ObjectName>,
    }

    impl RelationVisitor {
        /// Record the reference to `relation`, if it's not a CTE reference.
        fn insert_relation(&mut self, relation: &ObjectName) {
            if !self.relations.contains(relation) && !self.ctes_in_scope.contains(relation) {
                self.relations.insert(relation.clone());
            }
        }
    }

    impl Visitor for RelationVisitor {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
            self.insert_relation(relation);
            ControlFlow::Continue(())
        }

        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &q.with {
                for cte in &with.cte_tables {
                    // The non-recursive CTE name is not in scope when evaluating the CTE itself, so this is valid:
                    // `WITH t AS (SELECT * FROM t) SELECT * FROM t`
                    // Where the first `t` refers to a predefined table. So we are careful here
                    // to visit the CTE first, before putting it in scope.
                    if !with.recursive {
                        // This is a bit hackish as the CTE will be visited again as part of visiting `q`,
                        // but thankfully `insert_relation` is idempotent.
                        cte.visit(self);
                    }
                    self.ctes_in_scope
                        .push(ObjectName(vec![cte.alias.name.clone()]));
                }
            }
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &q.with {
                for _ in &with.cte_tables {
                    // Unwrap: We just pushed these in `pre_visit_query`
                    self.all_ctes.insert(self.ctes_in_scope.pop().unwrap());
                }
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            if let Statement::ShowCreate {
                obj_type: ShowCreateObject::Table | ShowCreateObject::View,
                obj_name,
            } = statement
            {
                self.insert_relation(obj_name)
            }

            ControlFlow::Continue(())
        }
    }

    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
        all_ctes: BTreeSet::new(),
        ctes_in_scope: vec![],
    };

    fn visit_statement(statement: &DFStatement, visitor: &mut RelationVisitor) {
        match statement {
            DFStatement::Statement(s) => {
                let _ = s.as_ref().visit(visitor);
            }
            DFStatement::CreateExternalTable(table) => {
                visitor
                    .relations
                    .insert(ObjectName(vec![Ident::from(table.name.as_str())]));
            }
            DFStatement::CopyTo(CopyToStatement { source, .. }) => match source {
                CopyToSource::Relation(table_name) => {
                    visitor.insert_relation(table_name);
                }
                CopyToSource::Query(query) => {
                    query.visit(visitor);
                }
            },
            DFStatement::Explain(explain) => visit_statement(&explain.statement, visitor),
        }
    }

    visit_statement(statement, &mut visitor);

    let table_refs = visitor
        .relations
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion::common::Result<_>>()?;
    let ctes = visitor
        .all_ctes
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion::common::Result<_>>()?;
    Ok((table_refs, ctes))
}
