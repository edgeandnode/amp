use datafusion::{
    error::DataFusionError,
    logical_expr::{LogicalPlan, ScalarUDF},
    prelude::{SQLOptions, SessionContext},
    sql::parser,
};

use crate::{
    block_num::BlockNumUdf,
    evm::udfs::{
        EvmDecodeHex, EvmDecodeLog, EvmDecodeParams, EvmDecodeType, EvmEncodeHex, EvmEncodeParams,
        EvmEncodeType, EvmTopic, ShiftUnits,
    },
    plan_visitors::forbid_underscore_prefixed_aliases,
};

/// Returns the built-in scalar UDFs registered in every session context.
pub fn builtin_udfs() -> Vec<ScalarUDF> {
    vec![
        BlockNumUdf::new().into(),
        EvmDecodeLog::new().into(),
        EvmDecodeLog::new().with_deprecated_name().into(),
        EvmTopic::new().into(),
        EvmEncodeParams::new().into(),
        EvmDecodeParams::new().into(),
        EvmEncodeType::new().into(),
        EvmDecodeType::new().into(),
        EvmEncodeHex::new().into(),
        EvmDecodeHex::new().into(),
        ShiftUnits::new().into(),
    ]
}

/// Converts a parsed SQL statement into a validated logical plan.
///
/// Validates that the plan does not use reserved underscore-prefixed aliases
/// and enforces read-only constraints (no DDL/DML).
#[tracing::instrument(skip_all, err)]
pub async fn sql_to_plan(
    ctx: &SessionContext,
    query: parser::Statement,
) -> Result<LogicalPlan, SqlToPlanError> {
    let plan = ctx
        .state()
        .statement_to_plan(query)
        .await
        .map_err(SqlToPlanError::StatementToPlan)?;

    forbid_underscore_prefixed_aliases(&plan).map_err(SqlToPlanError::ForbiddenAliases)?;
    read_only_check(&plan).map_err(SqlToPlanError::ReadOnlyCheck)?;

    Ok(plan)
}

/// Verifies that the logical plan contains no DDL, DML, or statement operations.
pub fn read_only_check(plan: &LogicalPlan) -> Result<(), ReadOnlyCheckError> {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(ReadOnlyCheckError)
}

/// Failed to convert SQL statement to a logical plan
///
/// This error covers all failure modes during the `sql_to_plan` pipeline:
/// statement-to-plan conversion, alias validation, and read-only enforcement.
#[derive(Debug, thiserror::Error)]
pub enum SqlToPlanError {
    /// DataFusion failed to convert the SQL statement into a logical plan
    ///
    /// Possible causes:
    /// - SQL syntax is valid but semantically incorrect (e.g., type mismatch)
    /// - Referenced columns do not exist in the schema
    /// - Unsupported SQL features
    #[error("failed to convert SQL statement to logical plan: {0}")]
    StatementToPlan(#[source] DataFusionError),

    /// Query uses underscore-prefixed column aliases which are reserved
    ///
    /// Column aliases starting with `_` are reserved for special columns
    /// like `_block_num`. Queries must not use these aliases.
    #[error("query uses forbidden underscore-prefixed aliases")]
    ForbiddenAliases(#[source] DataFusionError),

    /// Query plan violates read-only constraints
    ///
    /// The plan contains DDL (CREATE, DROP, ALTER), DML (INSERT, UPDATE, DELETE),
    /// or statement operations that are not allowed in a read-only context.
    #[error("query plan violates read-only constraints")]
    ReadOnlyCheck(#[source] ReadOnlyCheckError),
}

impl SqlToPlanError {
    /// Returns `true` if this error represents an invalid plan due to user input
    /// (forbidden aliases or read-only violations) rather than an internal failure.
    pub fn is_invalid_plan(&self) -> bool {
        matches!(self, Self::ForbiddenAliases(_) | Self::ReadOnlyCheck(_))
    }
}

/// Query plan violates read-only constraints
///
/// This occurs when a query plan contains DDL, DML, or statement operations
/// that are not allowed in the current read-only context.
#[derive(Debug, thiserror::Error)]
#[error("query plan violates read-only constraints")]
pub struct ReadOnlyCheckError(#[source] DataFusionError);
