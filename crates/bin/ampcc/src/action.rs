//! Application actions for state management.
//!
//! Actions are the single source of truth for state mutations.
//! All state changes flow through the action handler.

use admin_client::{
    jobs::JobInfo,
    workers::{WorkerDetailResponse, WorkerInfo},
};
use worker::job::JobId;

use crate::{app::DataSource, auth::AuthStorage};

/// Actions that can be dispatched to mutate application state.
#[derive(Debug)]
pub enum Action {
    /// No-op action (used for events that don't need handling).
    None,

    /// Quit the application.
    Quit,

    // ========================================================================
    // Navigation Actions
    // ========================================================================
    /// Navigate to next item in focused pane.
    NavigateDown,

    /// Navigate to previous item in focused pane.
    NavigateUp,

    /// Page down in focused pane.
    PageDown(u16),

    /// Page up in focused pane.
    PageUp(u16),

    /// Cycle to next pane.
    NextPane,

    /// Cycle to previous pane.
    PrevPane,

    /// Toggle expand/collapse on selected dataset.
    ToggleExpand,

    /// Enter detail view for selected item.
    EnterDetail,

    // ========================================================================
    // Source Switching
    // ========================================================================
    /// Switch to local data source.
    SwitchToLocal,

    /// Switch to registry data source.
    SwitchToRegistry,

    /// Source switch completed.
    SourceSwitched(Result<DataSource, String>),

    // ========================================================================
    // Search Actions
    // ========================================================================
    /// Enter search mode.
    EnterSearchMode,

    /// Exit search mode.
    ExitSearchMode,

    /// Add character to search query.
    SearchInput(char),

    /// Remove character from search query.
    SearchBackspace,

    /// Submit search (exit search mode and refresh).
    SearchSubmit,

    // ========================================================================
    // Dataset Actions
    // ========================================================================
    /// Trigger dataset refresh.
    RefreshDatasets,

    /// Datasets loaded from source.
    DatasetsLoaded(Result<Vec<crate::app::DatasetEntry>, String>),

    /// Dataset versions loaded.
    VersionsLoaded {
        dataset_index: usize,
        versions: Result<Vec<crate::app::VersionEntry>, String>,
    },

    // ========================================================================
    // Manifest Actions
    // ========================================================================
    /// Load manifest for selected dataset.
    LoadManifest,

    /// Manifest loaded.
    ManifestLoaded(Option<serde_json::Value>),

    // ========================================================================
    // Jobs Actions (Local mode)
    // ========================================================================
    /// Refresh jobs list.
    RefreshJobs,

    /// Jobs loaded.
    JobsLoaded(Vec<JobInfo>),

    /// Stop a job.
    StopJob(JobId),

    /// Job stopped result.
    JobStopped(Result<(), String>),

    /// Delete a job.
    DeleteJob(JobId),

    /// Job deleted result.
    JobDeleted(Result<(), String>),

    // ========================================================================
    // Workers Actions (Local mode)
    // ========================================================================
    /// Refresh workers list.
    RefreshWorkers,

    /// Workers loaded.
    WorkersLoaded(Vec<WorkerInfo>),

    /// Load worker details.
    LoadWorkerDetail(String),

    /// Worker detail loaded.
    WorkerDetailLoaded(Option<WorkerDetailResponse>),

    // ========================================================================
    // Auth Actions
    // ========================================================================
    /// Check auth state on startup.
    AuthCheckOnStartup,

    /// Auth state loaded from disk.
    AuthStateLoaded(Option<AuthStorage>),

    /// Start the login flow.
    AuthLogin,

    /// Logout and clear credentials.
    AuthLogout,

    /// Device flow initiated - waiting for user confirmation.
    AuthDeviceFlowPending {
        user_code: String,
        verification_uri: String,
        device_code: String,
        code_verifier: String,
        interval: i64,
    },

    /// User confirmed device flow - open browser and start polling.
    AuthDeviceFlowConfirm,

    /// Poll for device token.
    AuthDeviceFlowPoll {
        device_code: String,
        code_verifier: String,
        interval: i64,
        /// If true, poll immediately without delay (first poll after user confirms).
        is_first_poll: bool,
    },

    /// Device flow completed successfully.
    AuthDeviceFlowComplete(AuthStorage),

    /// Cancel device flow.
    AuthDeviceFlowCancel,

    /// Auth error occurred.
    AuthError(String),

    /// Token refresh completed.
    AuthRefreshComplete(Result<AuthStorage, String>),

    // ========================================================================
    // Error Actions
    // ========================================================================
    /// Display an error message.
    Error(String),
}
