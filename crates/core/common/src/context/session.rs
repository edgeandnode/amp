//! Session context wrapping shared session state with session metadata.
//!
//! Parallels DataFusion's `SessionContext` which wraps `Arc<RwLock<SessionState>>`
//! with a session ID and start time.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan, sql::parser};
use parking_lot::RwLock;

pub use crate::context::session_state::{SessionState, SessionStateBuilder, is_user_input_error};

/// A session context that wraps shared [`SessionState`] with session metadata.
///
/// Parallels DataFusion's `SessionContext` structure: holds a session ID,
/// start time, and an `Arc<RwLock<SessionState>>` for shared mutable access
/// to the underlying session state.
#[derive(Clone)]
pub struct SessionContext {
    session_id: String,
    session_start_time: DateTime<Utc>,
    state: Arc<RwLock<SessionState>>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContext {
    /// Creates a new session context with default [`SessionState`].
    pub fn new() -> Self {
        Self::new_with_state(SessionState::default())
    }

    /// Creates a new session context wrapping the given [`SessionState`].
    pub fn new_with_state(state: SessionState) -> Self {
        Self {
            session_id: uuid::Uuid::now_v7().to_string(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Returns the session ID.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the session start time.
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
    }

    /// Returns a clone of the inner [`SessionState`].
    ///
    /// Mirrors DataFusion's `SessionContext::state() -> SessionState` pattern.
    pub(crate) fn state(&self) -> SessionState {
        self.state.read().clone()
    }

    /// Returns a new [`SessionState`] with async catalog pre-resolution
    /// applied for the given SQL statement.
    ///
    /// Delegates to [`SessionState::resolved_state`].
    pub(crate) async fn resolved_state(
        &self,
        stmt: &parser::Statement,
    ) -> Result<SessionState, DataFusionError> {
        let state = self.state.read().clone();
        state.resolved_state(stmt).await
    }

    /// Plans a SQL statement into a [`LogicalPlan`].
    ///
    /// Delegates to [`SessionState::statement_to_plan`].
    pub async fn statement_to_plan(
        &self,
        stmt: parser::Statement,
    ) -> Result<LogicalPlan, DataFusionError> {
        let state = self.state.read().clone();
        state.statement_to_plan(stmt).await
    }

    /// Applies DataFusion logical optimizations to an existing plan.
    ///
    /// Delegates to [`SessionState::optimize`].
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        self.state.read().optimize(plan)
    }
}
