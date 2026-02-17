use std::sync::Arc;

use datafusion::{
    catalog::MemorySchemaProvider,
    common::DFSchemaRef,
    error::DataFusionError,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionContext},
    sql::parser,
};

use crate::{
    catalog::logical::{LogicalCatalog, LogicalTable},
    context::common::{SqlToPlanError, builtin_udfs, sql_to_plan},
    detached_logical_plan::DetachedLogicalPlan,
    planning_table::PlanningTable,
};

/// A context for planning SQL queries.
pub struct PlanningContext {
    session_config: SessionConfig,
    catalog: LogicalCatalog,
}

impl PlanningContext {
    /// Creates a planning context from a logical catalog.
    pub fn new(session_config: SessionConfig, catalog: LogicalCatalog) -> Self {
        Self {
            session_config,
            catalog,
        }
    }

    /// Returns the logical tables registered in this planning context.
    pub fn logical_tables(&self) -> &[LogicalTable] {
        &self.catalog.tables
    }

    /// Infers the output schema of the query by planning it against empty tables.
    pub async fn sql_output_schema(
        &self,
        query: parser::Statement,
    ) -> Result<DFSchemaRef, PlanSqlError> {
        let ctx = new_session_ctx(self.session_config.clone());
        register_catalog(&ctx, &self.catalog).map_err(PlanSqlError::RegisterTable)?;
        let plan = sql_to_plan(&ctx, query)
            .await
            .map_err(PlanSqlError::SqlToPlan)?;
        Ok(plan.schema().clone())
    }

    /// Converts a parsed SQL statement into a detached logical plan.
    pub async fn plan_sql(
        &self,
        query: parser::Statement,
    ) -> Result<DetachedLogicalPlan, PlanSqlError> {
        let ctx = new_session_ctx(self.session_config.clone());
        register_catalog(&ctx, &self.catalog).map_err(PlanSqlError::RegisterTable)?;
        let plan = sql_to_plan(&ctx, query)
            .await
            .map_err(PlanSqlError::SqlToPlan)?;
        Ok(DetachedLogicalPlan::new(plan))
    }

    /// Applies DataFusion logical optimizations to a detached plan.
    #[tracing::instrument(skip_all, err)]
    pub async fn optimize_plan(
        &self,
        plan: &DetachedLogicalPlan,
    ) -> Result<DetachedLogicalPlan, OptimizePlanError> {
        let ctx = new_session_ctx(self.session_config.clone());
        register_catalog(&ctx, &self.catalog).map_err(OptimizePlanError::RegisterTable)?;
        ctx.state()
            .optimize(plan)
            .map_err(OptimizePlanError::Optimize)
            .map(DetachedLogicalPlan::new)
    }
}

/// Creates a bare DataFusion session context with builtin UDFs but no catalog tables.
///
/// Call [`register_catalog`] on the returned context to populate it with tables and UDFs
/// from a [`LogicalCatalog`].
fn new_session_ctx(config: SessionConfig) -> SessionContext {
    let ctx = {
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Default::default())
            .with_default_features()
            .build();
        SessionContext::new_with_state(state)
    };

    // Register the builtin UDFs
    for udf in builtin_udfs() {
        ctx.register_udf(udf);
    }

    ctx
}

/// Registers the tables and UDFs from a [`LogicalCatalog`] into a [`SessionContext`].
fn register_catalog(
    ctx: &SessionContext,
    catalog: &LogicalCatalog,
) -> Result<(), RegisterTableError> {
    // Register tables first to ensure schemas are created before UDF registration
    for table in catalog.tables.iter() {
        let schema_name = table.sql_table_ref_schema();

        // The catalog schema needs to be explicitly created or table creation will fail.
        if ctx
            .catalog(&ctx.catalog_names()[0])
            .unwrap()
            .schema(schema_name)
            .is_none()
        {
            let schema = Arc::new(MemorySchemaProvider::new());
            ctx.catalog(&ctx.catalog_names()[0])
                .unwrap()
                .register_schema(schema_name, schema)
                .unwrap();
        }

        // Register the table with a planning-only provider that exposes the schema but cannot be scanned.
        let table_schema = table.schema().clone();
        ctx.register_table(
            table.table_ref().clone(),
            Arc::new(PlanningTable::new(table_schema)),
        )
        .map_err(RegisterTableError)?;
    }

    // Register UDFs after tables to ensure any schema dependencies are resolved
    for udf in catalog.udfs.iter() {
        ctx.register_udf(udf.clone());
    }

    Ok(())
}

/// Failed to register a catalog table with the planning session context
///
/// This occurs when DataFusion rejects a table registration during planning
/// session creation, typically because a table with the same name already
/// exists or the table metadata is invalid.
#[derive(Debug, thiserror::Error)]
#[error("Failed to register catalog table with planning session context")]
pub struct RegisterTableError(#[source] DataFusionError);

/// Failed to plan a SQL query against the planning context
///
/// This error is shared by `plan_sql` and `sql_output_schema` because they
/// produce the exact same error variants.
#[derive(Debug, thiserror::Error)]
pub enum PlanSqlError {
    /// Failed to create a planning session context
    ///
    /// This occurs when building a `SessionContext` for SQL planning fails,
    /// typically due to a table registration error during context setup.
    #[error("failed to create planning session context")]
    RegisterTable(#[source] RegisterTableError),

    /// Failed to convert SQL to a logical plan
    ///
    /// This occurs during SQL-to-logical-plan conversion, including
    /// statement parsing, alias validation, and read-only enforcement.
    #[error("failed to convert SQL to logical plan: {0}")]
    SqlToPlan(#[source] SqlToPlanError),
}

impl PlanSqlError {
    /// Returns `true` if this error represents an invalid plan due to user input
    /// (forbidden aliases or read-only violations) rather than an internal failure.
    pub fn is_invalid_plan(&self) -> bool {
        matches!(self, Self::SqlToPlan(err) if err.is_invalid_plan())
    }
}

/// Failed to optimize a logical plan
///
/// This error covers failures during the logical optimization phase.
#[derive(Debug, thiserror::Error)]
pub enum OptimizePlanError {
    /// Failed to create a planning session context
    ///
    /// This occurs when building a `SessionContext` for optimization fails,
    /// typically due to a table registration error during context setup.
    #[error("failed to create planning session context")]
    RegisterTable(#[source] RegisterTableError),

    /// DataFusion optimizer failed to process the plan
    ///
    /// Possible causes:
    /// - Optimizer rule failure during logical optimization
    /// - Type inference errors
    /// - Schema inconsistencies
    #[error("failed to optimize logical plan")]
    Optimize(#[source] DataFusionError),
}
