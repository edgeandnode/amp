use std::sync::Arc;

use datafusion::{
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{LogicalPlan, TableScan},
};

use crate::{
    context::exec::ExecContext,
    incrementalizer::NonIncrementalQueryError,
    plan_visitors::{is_incremental, propagate_block_num},
    sql::TableReference,
};

/// A plan that has `PlanTable` for its `TableProvider`s. It cannot be executed before being
/// first "attached" to a `ExecContext`.
#[derive(Debug, Clone)]
pub struct DetachedLogicalPlan(LogicalPlan);

impl DetachedLogicalPlan {
    /// Wraps a logical plan produced by a planning session context.
    pub(crate) fn new(plan: LogicalPlan) -> Self {
        Self(plan)
    }

    /// Attaches this plan to a query context by replacing `PlanTable` providers
    /// with actual `TableSnapshot` providers from the catalog.
    #[tracing::instrument(skip_all, err)]
    pub fn attach_to(self, ctx: &ExecContext) -> Result<LogicalPlan, AttachPlanError> {
        Ok(self
            .0
            .transform_with_subqueries(|mut node| match &mut node {
                // Insert the clauses in non-view table scans
                LogicalPlan::TableScan(TableScan {
                    table_name, source, ..
                }) if source.table_type() == TableType::Base
                    && source.get_logical_plan().is_none() =>
                {
                    let table_ref: TableReference<String> = table_name
                        .clone()
                        .try_into()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let provider = ctx
                        .get_table(&table_ref)
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    *source = Arc::new(DefaultTableSource::new(provider));
                    Ok(Transformed::yes(node))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map_err(AttachPlanError)?
            .data)
    }

    /// Validates that the plan can be processed incrementally.
    pub fn is_incremental(&self) -> Result<(), NonIncrementalQueryError> {
        is_incremental(&self.0)
    }

    /// Returns the output schema of this logical plan.
    pub fn schema(&self) -> DFSchemaRef {
        self.0.schema().clone()
    }

    /// Rewrites the plan to propagate `_block_num` through all nodes.
    pub fn propagate_block_num(self) -> Result<Self, DataFusionError> {
        Ok(Self(propagate_block_num(self.0)?))
    }

    /// Applies a visitor closure to each node in the logical plan tree.
    pub fn apply<F>(&self, f: F) -> Result<TreeNodeRecursion, DataFusionError>
    where
        F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError>,
    {
        self.0.apply(f)
    }
}

impl std::ops::Deref for DetachedLogicalPlan {
    type Target = LogicalPlan;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Failed to attach a detached logical plan to a query context
///
/// This occurs when transforming `PlanTable` references into actual
/// `TableSnapshot` references fails during plan attachment.
#[derive(Debug, thiserror::Error)]
#[error("failed to attach plan to query context")]
pub struct AttachPlanError(#[source] DataFusionError);
