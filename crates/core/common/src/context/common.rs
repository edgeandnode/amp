use datafusion::{error::DataFusionError, logical_expr::LogicalPlan, prelude::SQLOptions};

/// Context string used to tag user-input errors when flattening to `DataFusionError`.
///
/// Errors tagged with this context represent invalid user input (e.g., malformed
/// SQL references, forbidden aliases, read-only violations) and should be surfaced
/// as `BAD_REQUEST` / `invalid_argument` to the client. Internal errors are not
/// tagged and default to `INTERNAL_SERVER_ERROR` / `internal`.
pub(super) const INVALID_INPUT_CONTEXT: &str = "amp::invalid_input";

/// Verifies that the logical plan contains no DDL, DML, or statement operations.
pub(super) fn read_only_check(plan: &LogicalPlan) -> Result<(), ReadOnlyCheckError> {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
        .verify_plan(plan)
        .map_err(ReadOnlyCheckError)
}

/// Query plan violates read-only constraints
///
/// This occurs when a query plan contains DDL, DML, or statement operations
/// that are not allowed in the current read-only context.
#[derive(Debug, thiserror::Error)]
#[error("query plan violates read-only constraints")]
pub struct ReadOnlyCheckError(#[source] DataFusionError);
