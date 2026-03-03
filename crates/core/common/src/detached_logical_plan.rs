use datafusion::{common::DFSchemaRef, error::DataFusionError, logical_expr::LogicalPlan};

use crate::{
    context::exec::ExecContext,
    incrementalizer::NonIncrementalQueryError,
    plan_visitors::{is_incremental, propagate_block_num},
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

    /// Attaches this plan to a query context, replacing `PlanTable` table
    /// sources and `PlanJsUdf`-backed scalar functions with execution-ready
    /// providers.
    #[tracing::instrument(skip_all, err)]
    pub fn attach_to(self, ctx: &ExecContext) -> Result<LogicalPlan, AttachPlanError> {
        ctx.attach(self.0).map_err(AttachPlanError)
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
/// `QueryableSnapshot` references fails during plan attachment.
#[derive(Debug, thiserror::Error)]
#[error("failed to attach plan to query context")]
pub struct AttachPlanError(#[source] DataFusionError);
