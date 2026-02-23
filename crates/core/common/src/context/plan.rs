use std::sync::Arc;

use datafusion::{
    catalog::AsyncCatalogProvider as TableAsyncCatalogProvider, common::DFSchemaRef,
    error::DataFusionError, execution::config::SessionConfig, sql::parser,
};

pub use crate::context::session::is_user_input_error;
use crate::{
    context::{
        common::{INVALID_INPUT_CONTEXT, ReadOnlyCheckError, read_only_check},
        session::{SessionContext, SessionStateBuilder},
    },
    detached_logical_plan::DetachedLogicalPlan,
    func_catalog::catalog_provider::AsyncCatalogProvider as FuncAsyncCatalogProvider,
    plan_visitors::forbid_underscore_prefixed_aliases,
};

/// A context for planning SQL queries.
///
/// Delegates SQL planning and schema inference to a custom [`SessionContext`]
/// that performs async catalog pre-resolution before calling DataFusion.
/// Async-resolved catalogs are registered via `with_table_catalog` and
/// `with_func_catalog`.
pub struct PlanContext {
    session_ctx: SessionContext,
}

impl PlanContext {
    /// Infers the output schema of the query by planning it against empty tables.
    pub async fn sql_output_schema(
        &self,
        query: parser::Statement,
    ) -> Result<DFSchemaRef, DataFusionError> {
        let plan = self.statement_to_plan(query).await?;
        Ok(plan.schema().clone())
    }

    /// Plans a SQL statement into a validated [`LogicalPlan`].
    ///
    /// Delegates to the session for pure DF planning, then enforces
    /// consumer-level policies: forbidden underscore-prefixed aliases
    /// and read-only constraints.
    pub async fn statement_to_plan(
        &self,
        query: parser::Statement,
    ) -> Result<DetachedLogicalPlan, DataFusionError> {
        let plan = self.session_ctx.statement_to_plan(query).await?;

        forbid_underscore_prefixed_aliases(&plan)
            .map_err(|err| StatementToPlanError::ForbiddenAliases(err).into_datafusion_error())?;
        read_only_check(&plan)
            .map_err(|err| StatementToPlanError::ReadOnlyCheck(err).into_datafusion_error())?;

        Ok(DetachedLogicalPlan::new(plan))
    }

    /// Applies DataFusion logical optimizations to a detached plan.
    #[tracing::instrument(skip_all, err)]
    pub fn optimize(
        &self,
        plan: &DetachedLogicalPlan,
    ) -> Result<DetachedLogicalPlan, DataFusionError> {
        self.session_ctx
            .optimize(plan)
            .map(DetachedLogicalPlan::new)
    }
}

/// Builder for [`PlanContext`].
///
/// Composes a [`SessionStateBuilder`] internally. The exposed API covers
/// async table catalog registration (`with_table_catalog`) and async function
/// catalog registration (`with_func_catalog`). No internals of the state
/// builder are passed through directly.
pub struct PlanContextBuilder {
    state_builder: SessionStateBuilder,
}

impl PlanContextBuilder {
    /// Creates a new builder for configuring a [`PlanContext`].
    pub fn new(session_config: SessionConfig) -> Self {
        Self {
            state_builder: SessionStateBuilder::new(session_config),
        }
    }

    /// Registers a named async table catalog provider.
    ///
    /// Referenced catalogs are resolved before SQL planning via the
    /// pre-resolution pipeline in the custom session. The `name` must
    /// match the `SessionConfig` default catalog for the provider to
    /// be reachable — see [`SessionContextBuilder`] docs.
    pub fn with_table_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn TableAsyncCatalogProvider>,
    ) -> Self {
        self.state_builder = self.state_builder.with_table_catalog(name, provider);
        self
    }

    /// Registers a named async function catalog provider.
    ///
    /// Referenced qualified function catalogs are resolved before SQL planning
    /// and registered as `ScalarUDF`s. The `name` must match the
    /// `SessionConfig` default catalog for the provider to be reachable —
    /// see [`SessionContextBuilder`] docs.
    pub fn with_func_catalog(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn FuncAsyncCatalogProvider>,
    ) -> Self {
        self.state_builder = self.state_builder.with_func_catalog(name, provider);
        self
    }

    /// Builds the [`PlanContext`].
    pub fn build(self) -> PlanContext {
        PlanContext {
            session_ctx: SessionContext::new_with_state(self.state_builder.build()),
        }
    }
}

/// Failed to convert a SQL statement into a validated logical plan.
///
/// Covers all failure modes during policy-enforcing SQL planning in
/// [`PlanContext::statement_to_plan`]: alias validation and read-only
/// enforcement. Flattened to [`DataFusionError`] before leaving the method.
#[derive(Debug, thiserror::Error)]
enum StatementToPlanError {
    /// Query uses underscore-prefixed column aliases which are reserved
    #[error("query uses forbidden underscore-prefixed aliases")]
    ForbiddenAliases(#[source] DataFusionError),

    /// Query plan violates read-only constraints
    #[error("query plan violates read-only constraints")]
    ReadOnlyCheck(#[source] ReadOnlyCheckError),
}

impl StatementToPlanError {
    /// Converts into a [`DataFusionError`] with appropriate user-input tagging.
    fn into_datafusion_error(self) -> DataFusionError {
        match self {
            Self::ForbiddenAliases(err) => DataFusionError::Plan(format!(
                "query uses forbidden underscore-prefixed aliases: {err}"
            ))
            .context(INVALID_INPUT_CONTEXT),
            Self::ReadOnlyCheck(err) => {
                DataFusionError::Plan(format!("query plan violates read-only constraints: {err}"))
                    .context(INVALID_INPUT_CONTEXT)
            }
        }
    }
}
